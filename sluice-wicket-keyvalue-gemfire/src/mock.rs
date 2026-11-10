//! A cluster for the tests (feature `mock`): servers answering the methods of the GemFire
//! service the client calls, over one store in memory, and a locator naming them by xDS. Not
//! GemFire, but what the client relies on of a service of it, as its API reference states it
//! and a cluster showed it: a swap compares the values (bytes by their content, a value of
//! another type never equal), a region not there fails a call `FAILED_PRECONDITION`, a stream's
//! first message is its config alone, the key set comes in batches, the token is a bearer one.
//! TLS as a cluster's: the servers' certificate, a client's required where a CA of them is
//! given (the locator's port requires none, as GemFire's).

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use prost::Message as _;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use crate::client::lock;
use crate::discovery::{EDS, RESOURCE};
use crate::proto::envoy::config::core::v3::address::Address as Kind;
use crate::proto::envoy::config::core::v3::socket_address::PortSpecifier;
use crate::proto::envoy::config::core::v3::{Address, HealthStatus, SocketAddress};
use crate::proto::envoy::config::endpoint::v3::lb_endpoint::HostIdentifier;
use crate::proto::envoy::config::endpoint::v3::{
    ClusterLoadAssignment, Endpoint, LbEndpoint, LocalityLbEndpoints,
};
use crate::proto::envoy::service::discovery::v3::aggregated_discovery_service_server::{
    AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
};
use crate::proto::envoy::service::discovery::v3::{DiscoveryRequest, DiscoveryResponse};
use crate::proto::gemfire::v1::gem_fire_server::{GemFire, GemFireServer};
use crate::proto::gemfire::v1::key::KeyValue;
use crate::proto::gemfire::v1::*;

/// What a cluster is.
pub struct Options {
    /// The regions there are.
    pub regions: Vec<String>,
    pub servers: usize,
    pub tls: Option<MockTls>,
    /// Required on every call to the servers (`authorization: Bearer <token>`).
    pub token: Option<String>,
    /// The most keys or entries a message of a server's stream holds.
    pub batch: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            regions: vec!["sluice".to_string()],
            servers: 1,
            tls: None,
            token: None,
            batch: 1000,
        }
    }
}

/// The TLS of the servers and the locator.
pub struct MockTls {
    /// Their certificate (PEM) and its key.
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    /// The CA of the client certificates the servers require (mTLS); None: they require none.
    pub client_ca_pem: Option<Vec<u8>>,
}

/// A running cluster on 127.0.0.1, stopped on drop.
pub struct Cluster {
    store: Arc<Store>,
    servers: Vec<MockServer>,
    locator: SocketAddr,
    locating: JoinHandle<()>,
    advertised: watch::Sender<Vec<String>>,
}

/// One server: its port kept while stopped.
struct MockServer {
    address: SocketAddr,
    tls: Option<ServerTlsConfig>,
    counters: Arc<Counters>,
    paused: watch::Sender<bool>,
    running: Mutex<Option<(oneshot::Sender<()>, JoinHandle<()>)>>,
}

#[derive(Default)]
struct Counters {
    calls: AtomicU64,
    connections: AtomicU64,
}

/// The store the servers share.
struct Store {
    regions: Mutex<HashMap<String, BTreeMap<Held, Value>>>,
    token: Option<String>,
    batch: usize,
    acks: AtomicU64,
    nacks: AtomicU64,
}

/// A key as held: ordered.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Held {
    String(String),
    Int(i32),
    Long(i64),
}

impl Held {
    fn of(key: Option<Key>) -> Result<Held, Status> {
        match key.and_then(|key| key.key_value) {
            Some(KeyValue::String(key)) => Ok(Held::String(key)),
            Some(KeyValue::Int(key)) => Ok(Held::Int(key)),
            Some(KeyValue::Long(key)) => Ok(Held::Long(key)),
            None => Err(Status::invalid_argument("no key")),
        }
    }

    fn key(&self) -> Key {
        let key_value = match self {
            Held::String(key) => KeyValue::String(key.clone()),
            Held::Int(key) => KeyValue::Int(*key),
            Held::Long(key) => KeyValue::Long(*key),
        };
        Key {
            key_value: Some(key_value),
        }
    }
}

impl Store {
    /// `f` on the region `name`: a region not there fails as GemFire's does.
    fn region<R>(
        &self,
        name: &str,
        f: impl FnOnce(&mut BTreeMap<Held, Value>) -> R,
    ) -> Result<R, Status> {
        let mut regions = lock(&self.regions);
        let region = regions
            .get_mut(name)
            .ok_or_else(|| Status::failed_precondition(format!("Region not found: {name}")))?;
        Ok(f(region))
    }
}

fn value(value: Option<Value>) -> Result<Value, Status> {
    value
        .filter(|value| value.value_value.is_some())
        .ok_or_else(|| Status::invalid_argument("no value"))
}

impl Cluster {
    pub async fn start(options: Options) -> Cluster {
        let store = Arc::new(Store {
            regions: Mutex::new(
                (options.regions.iter())
                    .map(|region| (region.clone(), BTreeMap::new()))
                    .collect(),
            ),
            token: options.token,
            batch: options.batch.max(1),
            acks: AtomicU64::new(0),
            nacks: AtomicU64::new(0),
        });
        let identity = options
            .tls
            .as_ref()
            .map(|tls| Identity::from_pem(&tls.cert_pem, &tls.key_pem));
        let mut servers = Vec::new();
        for _ in 0..options.servers {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tls = options.tls.as_ref().map(|tls| {
                let config = ServerTlsConfig::new().identity(identity.clone().unwrap());
                match &tls.client_ca_pem {
                    Some(ca) => config.client_ca_root(Certificate::from_pem(ca)),
                    None => config,
                }
            });
            let server = MockServer {
                address: listener.local_addr().unwrap(),
                tls,
                counters: Arc::default(),
                paused: watch::channel(false).0,
                running: Mutex::new(None),
            };
            server.serve(listener, &store);
            servers.push(server);
        }
        let (advertised, _) =
            watch::channel(servers.iter().map(|s| s.address.to_string()).collect());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let locator = listener.local_addr().unwrap();
        let mut builder = Server::builder();
        if let Some(identity) = identity {
            let tls = ServerTlsConfig::new().identity(identity);
            builder = builder.tls_config(tls).unwrap();
        }
        let service = AggregatedDiscoveryServiceServer::new(Locator {
            advertised: advertised.clone(),
            store: store.clone(),
        });
        let serving = builder
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener));
        let locating = tokio::spawn(async move {
            let _ = serving.await;
        });
        Cluster {
            store,
            servers,
            locator,
            locating,
            advertised,
        }
    }

    /// The servers' addresses (`127.0.0.1:port`), in order.
    pub fn servers(&self) -> Vec<String> {
        self.servers.iter().map(|s| s.address.to_string()).collect()
    }

    pub fn locator(&self) -> String {
        self.locator.to_string()
    }

    /// Has the locator name the servers `servers` (by index) alone from now on.
    pub fn advertise(&self, servers: &[usize]) {
        let addresses = servers.iter().map(|&n| self.servers[n].address.to_string());
        self.advertised.send_replace(addresses.collect());
    }

    /// The answers of the locator acknowledged, and refused.
    pub fn acks(&self) -> (u64, u64) {
        let acks = self.store.acks.load(Ordering::Relaxed);
        (acks, self.store.nacks.load(Ordering::Relaxed))
    }

    /// Stops the server `n`: its port refuses connections, its connections close once their
    /// calls are answered.
    pub async fn stop(&self, n: usize) {
        let server = &self.servers[n];
        server.paused.send_replace(false);
        let running = lock(&server.running).take();
        if let Some((stop, serving)) = running {
            let _ = stop.send(());
            let _ = serving.await;
        }
    }

    /// Starts the server `n` again on its port.
    pub async fn restart(&self, n: usize) {
        let server = &self.servers[n];
        let listener = TcpListener::bind(server.address).await.unwrap();
        server.serve(listener, &self.store);
    }

    /// Has the calls the server `n` receives wait while `paused`.
    pub fn pause(&self, n: usize, paused: bool) {
        self.servers[n].paused.send_replace(paused);
    }

    /// The calls the server `n` received.
    pub fn calls(&self, n: usize) -> u64 {
        self.servers[n].counters.calls.load(Ordering::Relaxed)
    }

    /// The connections the server `n` accepted.
    pub fn connections(&self, n: usize) -> u64 {
        self.servers[n].counters.connections.load(Ordering::Relaxed)
    }

    /// Writes `value` under the string `key` of `region`, as another client may.
    pub fn put(&self, region: &str, key: &str, value: Value) {
        let held = Held::String(key.to_string());
        self.store
            .region(region, |r| r.insert(held, value))
            .unwrap();
    }

    /// The string keys of `region`, in order.
    pub fn keys(&self, region: &str) -> Vec<String> {
        let keys = self.store.region(region, |region| {
            (region.keys())
                .filter_map(|key| match key {
                    Held::String(key) => Some(key.clone()),
                    _ => None,
                })
                .collect()
        });
        keys.unwrap()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.locating.abort();
        for server in &self.servers {
            if let Some((_, serving)) = lock(&server.running).take() {
                serving.abort();
            }
        }
    }
}

impl MockServer {
    fn serve(&self, listener: TcpListener, store: &Arc<Store>) {
        let counters = self.counters.clone();
        let incoming = TcpListenerStream::new(listener).map(move |accepted| {
            counters.connections.fetch_add(1, Ordering::Relaxed);
            accepted
        });
        let mut builder = Server::builder();
        if let Some(tls) = &self.tls {
            builder = builder.tls_config(tls.clone()).unwrap();
        }
        let service = GemFireServer::new(Service {
            store: store.clone(),
            counters: self.counters.clone(),
            paused: self.paused.subscribe(),
        });
        let (stop, stopped) = oneshot::channel::<()>();
        let serving = builder
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            });
        let serving = tokio::spawn(async move {
            let _ = serving.await;
        });
        *lock(&self.running) = Some((stop, serving));
    }
}

/// The service of one server.
struct Service {
    store: Arc<Store>,
    counters: Arc<Counters>,
    paused: watch::Receiver<bool>,
}

impl Service {
    /// Counts the call, holds it while the server is paused, and checks its token.
    async fn admit<T>(&self, request: &Request<T>) -> Result<(), Status> {
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        let _ = self.paused.clone().wait_for(|paused| !paused).await;
        let Some(token) = &self.store.token else {
            return Ok(());
        };
        let sent = request.metadata().get("authorization");
        match sent.and_then(|sent| sent.to_str().ok()) {
            Some(sent) if sent.strip_prefix("Bearer ") == Some(token) => Ok(()),
            _ => Err(Status::unauthenticated("Invalid or missing bearer token")),
        }
    }

    /// The region a stream's first message names.
    async fn config<T>(
        &self,
        messages: &mut Streaming<T>,
        config: impl FnOnce(T) -> Option<StreamConfig>,
    ) -> Result<String, Status> {
        let first = messages.message().await?.and_then(config);
        let region = first
            .ok_or_else(|| Status::invalid_argument("the first message holds no StreamConfig"))?
            .region_name;
        self.store.region(&region, |_| ())?;
        Ok(region)
    }
}

type Answers<T> = ReceiverStream<Result<T, Status>>;

#[tonic::async_trait]
impl GemFire for Service {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        self.admit(&request).await?;
        let request = request.into_inner();
        let (key, value) = (Held::of(request.key)?, value(request.value)?);
        self.store
            .region(&request.region_name, |r| r.insert(key, value))?;
        Ok(Response::new(PutResponse {}))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.admit(&request).await?;
        let request = request.into_inner();
        let key = Held::of(request.key)?;
        let value = self
            .store
            .region(&request.region_name, |r| r.get(&key).cloned())?;
        Ok(Response::new(GetResponse { value }))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        self.admit(&request).await?;
        let request = request.into_inner();
        let key = Held::of(request.key)?;
        let removed = self
            .store
            .region(&request.region_name, |r| r.remove(&key))?;
        Ok(Response::new(RemoveResponse {
            removed: removed.is_some(),
        }))
    }

    async fn put_if_absent(
        &self,
        request: Request<PutIfAbsentRequest>,
    ) -> Result<Response<PutIfAbsentResponse>, Status> {
        self.admit(&request).await?;
        let request = request.into_inner();
        let (key, value) = (Held::of(request.key)?, value(request.value)?);
        let inserted = self.store.region(&request.region_name, |region| {
            let absent = !region.contains_key(&key);
            if absent {
                region.insert(key, value);
            }
            absent
        })?;
        Ok(Response::new(PutIfAbsentResponse { inserted }))
    }

    async fn replace_if(
        &self,
        request: Request<ReplaceIfRequest>,
    ) -> Result<Response<ReplaceIfResponse>, Status> {
        self.admit(&request).await?;
        let request = request.into_inner();
        let (key, value) = (Held::of(request.key)?, value(request.value)?);
        let expected = self::value(request.expected_value)?;
        let replaced = self.store.region(&request.region_name, |region| {
            let holds = region.get(&key) == Some(&expected);
            if holds {
                region.insert(key, value);
            }
            holds
        })?;
        Ok(Response::new(ReplaceIfResponse { replaced }))
    }

    async fn contains_key(
        &self,
        request: Request<ContainsKeyRequest>,
    ) -> Result<Response<ContainsKeyResponse>, Status> {
        self.admit(&request).await?;
        let request = request.into_inner();
        let key = Held::of(request.key)?;
        let contains = self
            .store
            .region(&request.region_name, |r| r.contains_key(&key))?;
        Ok(Response::new(ContainsKeyResponse { contains }))
    }

    async fn put_stream(
        &self,
        request: Request<Streaming<PutStreamRequest>>,
    ) -> Result<Response<PutStreamResponse>, Status> {
        self.admit(&request).await?;
        let mut messages = request.into_inner();
        let region = self.config(&mut messages, |first| first.config).await?;
        while let Some(message) = messages.message().await? {
            let entries = (message.entries.into_iter())
                .map(|entry| Ok((Held::of(entry.key)?, value(entry.value)?)))
                .collect::<Result<Vec<_>, Status>>()?;
            self.store.region(&region, |r| r.extend(entries))?;
        }
        Ok(Response::new(PutStreamResponse {}))
    }

    type GetStreamStream = Answers<GetStreamResponse>;

    async fn get_stream(
        &self,
        request: Request<Streaming<GetStreamRequest>>,
    ) -> Result<Response<Self::GetStreamStream>, Status> {
        self.admit(&request).await?;
        let mut messages = request.into_inner();
        let region = self.config(&mut messages, |first| first.config).await?;
        let (answers, answered) = mpsc::channel(4);
        let (store, batch) = (self.store.clone(), self.store.batch);
        tokio::spawn(async move {
            loop {
                let keys = match messages.message().await {
                    Ok(Some(message)) => message.keys,
                    Ok(None) => return,
                    Err(status) => {
                        let _ = answers.send(Err(status)).await;
                        return;
                    }
                };
                let found = store.region(&region, |region| {
                    (keys.into_iter())
                        .filter_map(|key| {
                            let value = region.get(&Held::of(Some(key.clone())).ok()?)?;
                            let (key, value) = (Some(key), Some(value.clone()));
                            Some(Entry { key, value })
                        })
                        .collect::<Vec<_>>()
                });
                let found = match found {
                    Ok(found) => found,
                    Err(status) => {
                        let _ = answers.send(Err(status)).await;
                        return;
                    }
                };
                for entries in found.chunks(batch) {
                    let entries = entries.to_vec();
                    if answers
                        .send(Ok(GetStreamResponse { entries }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(answered)))
    }

    async fn remove_stream(
        &self,
        request: Request<Streaming<RemoveStreamRequest>>,
    ) -> Result<Response<RemoveStreamResponse>, Status> {
        self.admit(&request).await?;
        let mut messages = request.into_inner();
        let region = self.config(&mut messages, |first| first.config).await?;
        while let Some(message) = messages.message().await? {
            let keys = (message.keys.into_iter())
                .map(|key| Held::of(Some(key)))
                .collect::<Result<Vec<_>, Status>>()?;
            self.store.region(&region, |region| {
                for key in &keys {
                    region.remove(key);
                }
            })?;
        }
        Ok(Response::new(RemoveStreamResponse {}))
    }

    type KeySetStreamStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<KeySetStreamResponse, Status>>>;

    async fn key_set_stream(
        &self,
        request: Request<KeySetStreamRequest>,
    ) -> Result<Response<Self::KeySetStreamStream>, Status> {
        self.admit(&request).await?;
        let region = request.into_inner().region_name;
        let keys = self
            .store
            .region(&region, |r| r.keys().map(Held::key).collect::<Vec<_>>())?;
        let batches: Vec<_> = (keys.chunks(self.store.batch))
            .map(|keys| {
                Ok(KeySetStreamResponse {
                    keys: keys.to_vec(),
                })
            })
            .collect();
        Ok(Response::new(tokio_stream::iter(batches)))
    }
}

/// The locator's discovery: the servers advertised, as one assignment of `gemfire_grpc`,
/// again on each change.
struct Locator {
    advertised: watch::Sender<Vec<String>>,
    store: Arc<Store>,
}

#[tonic::async_trait]
impl AggregatedDiscoveryService for Locator {
    type StreamAggregatedResourcesStream = Answers<DiscoveryResponse>;

    async fn stream_aggregated_resources(
        &self,
        request: Request<Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamAggregatedResourcesStream>, Status> {
        let mut requests = request.into_inner();
        let mut advertised = self.advertised.subscribe();
        let store = self.store.clone();
        let (answers, answered) = mpsc::channel(4);
        tokio::spawn(async move {
            let (mut version, mut subscribed) = (0u64, false);
            loop {
                tokio::select! {
                    request = requests.message() => {
                        let Ok(Some(request)) = request else {
                            return;
                        };
                        if !request.response_nonce.is_empty() {
                            let counter = match request.error_detail {
                                Some(_) => &store.nacks,
                                None => &store.acks,
                            };
                            counter.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        subscribed = true;
                    }
                    changed = advertised.changed(), if subscribed => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
                version += 1;
                let answer = assignment(version, &advertised.borrow_and_update());
                if answers.send(Ok(answer)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(answered)))
    }
}

/// The answer naming `servers` (`host:port`), of health unknown, as GemFire's locator does.
fn assignment(version: u64, servers: &[String]) -> DiscoveryResponse {
    let lb_endpoints = servers
        .iter()
        .map(|server| {
            let (host, port) = server.rsplit_once(':').unwrap();
            let socket = SocketAddress {
                address: host.to_string(),
                port_specifier: Some(PortSpecifier::PortValue(port.parse().unwrap())),
            };
            let address = Address {
                address: Some(Kind::SocketAddress(socket)),
            };
            LbEndpoint {
                host_identifier: Some(HostIdentifier::Endpoint(Endpoint {
                    address: Some(address),
                })),
                health_status: HealthStatus::Unknown as i32,
            }
        })
        .collect();
    let assignment = ClusterLoadAssignment {
        cluster_name: RESOURCE.to_string(),
        endpoints: vec![LocalityLbEndpoints { lb_endpoints }],
    };
    DiscoveryResponse {
        version_info: version.to_string(),
        resources: vec![prost_types::Any {
            type_url: EDS.to_string(),
            value: assignment.encode_to_vec(),
        }],
        type_url: EDS.to_string(),
        nonce: version.to_string(),
    }
}
