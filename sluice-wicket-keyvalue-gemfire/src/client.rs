//! The calls to the servers. Each goes round them, a server failing set aside a while (tried
//! after the others), and moves to the next when its request never left, or when sending it
//! twice harms nothing (all but the swaps) and its connection failed or the server said it is
//! unavailable. A call ends once nothing moved for the timeout: no answer to a unary one, no
//! message of a stream sent or received. A call timing out on a server has the next one open a
//! new connection to it.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_stream::Stream;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request, Response, Status};

use crate::proto::gemfire::v1::gem_fire_client::GemFireClient;
use crate::proto::gemfire::v1::key::KeyValue;
use crate::proto::gemfire::v1::value::ValueValue;
use crate::proto::gemfire::v1::{
    ContainsKeyRequest, Entry, GetRequest, GetStreamRequest, Key, KeySetStreamRequest,
    PutIfAbsentRequest, PutRequest, PutStreamRequest, RemoveRequest, RemoveStreamRequest,
    ReplaceIfRequest, StreamConfig, Value,
};
use crate::{Endpoints, Error, Settings, Tls, discovery};

/// The most bytes of keys and values one message of a stream carries: well within the
/// servers' bound on a message (`gemfire.grpc.max-message-size`, 4 MiB by default); a larger
/// entry goes alone.
const CHUNK_BYTES: usize = 1 << 20;

/// What a key or an entry adds to a message besides its bytes, as counted against
/// [`CHUNK_BYTES`].
const ITEM_BYTES: usize = 16;

/// How long a server failing is set aside: the first time, doubled each failure in a row.
const ASIDE: Duration = Duration::from_secs(1);

/// The longest a server failing is set aside.
const ASIDE_MAX: Duration = Duration::from_secs(30);

/// TCP keepalive of the connections: a peer gone without a word is found within about a
/// minute of quiet, never by HTTP/2 pings (a server takes them as abuse when frequent).
const KEEPALIVE: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_RETRIES: u32 = 3;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// How the servers and the locators are reached: TLS, the token, the timeout.
pub(crate) struct Connect {
    tls: Option<ClientTlsConfig>,
    token: Option<MetadataValue<Ascii>>,
    pub(crate) timeout: Duration,
}

impl Connect {
    fn new(tls: Option<Tls>, token: Option<String>, timeout: Duration) -> Result<Connect, String> {
        let tls = tls.map(|tls| {
            let config = match tls.ca_pem {
                Some(pem) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)),
                None => ClientTlsConfig::new().with_webpki_roots(),
            };
            let config = match tls.client_pem {
                Some((cert, key)) => config.identity(Identity::from_pem(cert, key)),
                None => config,
            };
            match tls.server_name {
                Some(name) => config.domain_name(name),
                None => config,
            }
        });
        let token = match token {
            Some(token) => Some(
                MetadataValue::try_from(format!("Bearer {token}"))
                    .map_err(|_| "the token is no header value".to_string())?,
            ),
            None => None,
        };
        Ok(Connect {
            tls,
            token,
            timeout,
        })
    }

    /// The endpoint of `address` (`host:port`); its connection is opened on its first call.
    pub(crate) fn endpoint(&self, address: &str) -> Result<Endpoint, String> {
        let scheme = match self.tls {
            Some(_) => "https",
            None => "http",
        };
        let endpoint = Endpoint::from_shared(format!("{scheme}://{address}"))
            .map_err(|e| format!("{address}: {e}"))?
            .connect_timeout(self.timeout)
            .tcp_keepalive(Some(KEEPALIVE))
            .tcp_keepalive_interval(Some(KEEPALIVE_INTERVAL))
            .tcp_keepalive_retries(Some(KEEPALIVE_RETRIES));
        match &self.tls {
            Some(tls) => endpoint
                .tls_config(tls.clone())
                .map_err(|e| describe_error(address, &e)),
            None => Ok(endpoint),
        }
    }

    /// The server at `address`.
    pub(crate) fn node(&self, address: &str) -> Result<Arc<Node>, String> {
        Ok(Arc::new(Node {
            address: address.to_string(),
            endpoint: self.endpoint(address)?,
            state: Mutex::new(NodeState::default()),
        }))
    }

    /// `message` with the token.
    pub(crate) fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
        }
        request
    }
}

/// One server.
pub(crate) struct Node {
    pub(crate) address: String,
    endpoint: Endpoint,
    state: Mutex<NodeState>,
}

#[derive(Default)]
struct NodeState {
    /// The connection and its generation: None until a call needs it, or after a call timed
    /// out on it.
    channel: Option<(u64, Channel)>,
    generation: u64,
    /// The failures in a row, and until when the server is set aside for them.
    failures: u32,
    aside: Option<Instant>,
}

impl Node {
    fn channel(&self) -> (u64, Channel) {
        let mut state = lock(&self.state);
        if let Some((generation, channel)) = &state.channel {
            return (*generation, channel.clone());
        }
        let generation = state.generation;
        let channel = self.endpoint.connect_lazy();
        state.channel = Some((generation, channel.clone()));
        (generation, channel)
    }

    /// Drops the connection of `generation` (not one a call timing out at the same time
    /// renewed already): the next call opens a new one.
    fn renew(&self, generation: u64) {
        let mut state = lock(&self.state);
        if state.channel.as_ref().map(|(current, _)| *current) == Some(generation) {
            state.channel = None;
            state.generation = generation + 1;
        }
    }

    fn failed(&self) {
        let mut state = lock(&self.state);
        state.failures = state.failures.saturating_add(1);
        let aside = ASIDE.saturating_mul(1 << (state.failures - 1).min(5));
        state.aside = Some(Instant::now() + aside.min(ASIDE_MAX));
    }

    fn answered(&self) {
        let mut state = lock(&self.state);
        state.failures = 0;
        state.aside = None;
    }

    fn aside(&self, now: Instant) -> bool {
        lock(&self.state).aside.is_some_and(|until| until > now)
    }
}

/// The servers known, and why there are none when so.
#[derive(Clone)]
pub(crate) struct Servers {
    pub(crate) nodes: Arc<[Arc<Node>]>,
    pub(crate) why: Arc<str>,
}

impl Servers {
    pub(crate) fn none(why: &str) -> Servers {
        Servers {
            nodes: Arc::new([]),
            why: why.into(),
        }
    }
}

/// Whether a call may go to another server once its request may have left.
#[derive(Clone, Copy, PartialEq)]
enum Retry {
    /// Sending it twice harms nothing.
    Idempotent,
    /// Only when it never left: a swap.
    Unsent,
}

/// When a call last moved: its start, then each message of a stream sent or received.
#[derive(Clone)]
pub(crate) struct Moved(Arc<Mutex<Instant>>);

impl Moved {
    pub(crate) fn now() -> Moved {
        Moved(Arc::new(Mutex::new(Instant::now())))
    }

    pub(crate) fn touch(&self) {
        *lock(&self.0) = Instant::now();
    }

    /// When the call ends unless it moves.
    fn until(&self, timeout: Duration) -> Instant {
        *lock(&self.0) + timeout
    }

    /// `future`'s output, None once nothing moved for `timeout`.
    pub(crate) async fn within<F: Future>(
        &self,
        timeout: Duration,
        future: F,
    ) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);
        loop {
            tokio::select! {
                biased;
                output = &mut future => return Some(output),
                () = tokio::time::sleep_until(self.until(timeout)) => {
                    if self.until(timeout) <= Instant::now() {
                        return None;
                    }
                }
            }
        }
    }
}

/// The messages of a request stream, the call moved as each leaves.
pub(crate) struct Sending<T> {
    messages: std::vec::IntoIter<T>,
    moved: Moved,
}

impl<T> Sending<T> {
    pub(crate) fn new(messages: Vec<T>, moved: Moved) -> Sending<T> {
        Sending {
            messages: messages.into_iter(),
            moved,
        }
    }
}

impl<T: Unpin> Stream for Sending<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();
        let next = this.messages.next();
        if next.is_some() {
            this.moved.touch();
        }
        Poll::Ready(next)
    }
}

/// The client of one cluster.
pub(crate) struct Client {
    connect: Arc<Connect>,
    servers: watch::Receiver<Servers>,
    /// Where the next call starts going round.
    next: AtomicUsize,
    /// The discovery following the locators: it ends with the client.
    discovery: Option<tokio::task::AbortHandle>,
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(discovery) = &self.discovery {
            discovery.abort();
        }
    }
}

impl Client {
    pub(crate) fn open(settings: Settings) -> Result<Client, String> {
        let connect = Arc::new(Connect::new(
            settings.tls,
            settings.token,
            settings.timeout,
        )?);
        let (servers, discovery) = match settings.endpoints {
            Endpoints::Servers(addresses) => {
                let nodes = addresses
                    .iter()
                    .map(|address| connect.node(address))
                    .collect::<Result<Vec<_>, _>>()?;
                let servers = Servers {
                    nodes: nodes.into(),
                    why: "no server is named".into(),
                };
                (watch::channel(servers).1, None)
            }
            Endpoints::Locators(addresses) => {
                let locators = addresses
                    .iter()
                    .map(|address| Ok((address.clone(), connect.endpoint(address)?)))
                    .collect::<Result<Vec<_>, String>>()?;
                let (publish, servers) = watch::channel(Servers::none("no locator answered yet"));
                let following =
                    discovery::follow(settings.label, locators, connect.clone(), publish);
                (servers, Some(tokio::spawn(following).abort_handle()))
            }
        };
        Ok(Client {
            connect,
            servers,
            next: AtomicUsize::new(0),
            discovery,
        })
    }

    /// A call about to go round the servers, once there are some (the locators named none
    /// yet: within the timeout).
    async fn call(&self, retry: Retry, unary: bool) -> Result<Call<'_>, Error> {
        let moved = Moved::now();
        let mut servers = self.servers.clone();
        let wait = servers.wait_for(|servers| !servers.nodes.is_empty());
        let nodes = match moved.within(self.connect.timeout, wait).await {
            Some(Ok(servers)) => servers.nodes.clone(),
            _ => return Err(Error(format!("no servers: {}", self.servers.borrow().why))),
        };
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let (ready, aside): (Vec<_>, Vec<_>) = (0..nodes.len())
            .map(|n| nodes[(start + n) % nodes.len()].clone())
            .partition(|node| !node.aside(now));
        Ok(Call {
            connect: &self.connect,
            retry,
            unary,
            moved,
            order: ready
                .into_iter()
                .chain(aside)
                .collect::<Vec<_>>()
                .into_iter(),
            trying: None,
            failures: Vec::new(),
        })
    }

    /// The answer of a unary call of `message`, sent by `send`.
    async fn unary<M, R, F, Fut>(&self, retry: Retry, message: M, send: F) -> Result<R, Error>
    where
        M: Clone,
        F: Fn(GemFireClient<Channel>, Request<M>) -> Fut,
        Fut: Future<Output = Result<Response<R>, Status>>,
    {
        let mut call = self.call(retry, true).await?;
        while let Some(gemfire) = call.next() {
            let request = call.request(message.clone());
            let outcome = call.within(send(gemfire, request)).await;
            if let Some(done) = call.settle(outcome.map(|o| o.map(Response::into_inner))) {
                return done;
            }
        }
        Err(call.failed())
    }

    pub(crate) async fn get(&self, region: &str, key: &str) -> Result<Option<Value>, Error> {
        let message = GetRequest {
            region_name: region.to_string(),
            key: Some(string(key)),
        };
        let answer = self
            .unary(
                Retry::Idempotent,
                message,
                |mut gemfire, request| async move { gemfire.get(request).await },
            )
            .await?;
        Ok(answer.value.filter(|value| value.value_value.is_some()))
    }

    pub(crate) async fn put(&self, region: &str, key: &str, value: Bytes) -> Result<(), Error> {
        let message = PutRequest {
            region_name: region.to_string(),
            key: Some(string(key)),
            value: Some(bytes(value)),
        };
        self.unary(
            Retry::Idempotent,
            message,
            |mut gemfire, request| async move { gemfire.put(request).await },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn remove(&self, region: &str, key: &str) -> Result<(), Error> {
        let message = RemoveRequest {
            region_name: region.to_string(),
            key: Some(string(key)),
        };
        self.unary(
            Retry::Idempotent,
            message,
            |mut gemfire, request| async move { gemfire.remove(request).await },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn contains_key(&self, region: &str, key: &str) -> Result<bool, Error> {
        let message = ContainsKeyRequest {
            region_name: region.to_string(),
            key: Some(string(key)),
        };
        let answer = self
            .unary(
                Retry::Idempotent,
                message,
                |mut gemfire, request| async move { gemfire.contains_key(request).await },
            )
            .await?;
        Ok(answer.contains)
    }

    /// Whether `value` was written: only while `key` was absent.
    pub(crate) async fn put_if_absent(
        &self,
        region: &str,
        key: &str,
        value: Bytes,
    ) -> Result<bool, Error> {
        let message = PutIfAbsentRequest {
            region_name: region.to_string(),
            key: Some(string(key)),
            value: Some(bytes(value)),
        };
        let answer = self
            .unary(Retry::Unsent, message, |mut gemfire, request| async move {
                gemfire.put_if_absent(request).await
            })
            .await?;
        Ok(answer.inserted)
    }

    /// Whether `value` was written: only while `key` held `expected`.
    pub(crate) async fn replace_if(
        &self,
        region: &str,
        key: &str,
        expected: Bytes,
        value: Bytes,
    ) -> Result<bool, Error> {
        let message = ReplaceIfRequest {
            region_name: region.to_string(),
            key: Some(string(key)),
            value: Some(bytes(value)),
            expected_value: Some(bytes(expected)),
        };
        let answer = self
            .unary(Retry::Unsent, message, |mut gemfire, request| async move {
                gemfire.replace_if(request).await
            })
            .await?;
        Ok(answer.replaced)
    }

    /// The values `keys` hold, by key: the absent ones are not.
    pub(crate) async fn get_all(
        &self,
        region: &str,
        keys: &[String],
    ) -> Result<HashMap<String, Value>, Error> {
        let chunks = chunks(keys.iter().map(|key| (string(key), key.len())));
        let messages: Vec<GetStreamRequest> = std::iter::once(GetStreamRequest {
            config: Some(config(region)),
            keys: Vec::new(),
        })
        .chain(
            chunks
                .into_iter()
                .map(|keys| GetStreamRequest { config: None, keys }),
        )
        .collect();
        let mut call = self.call(Retry::Idempotent, false).await?;
        while let Some(mut gemfire) = call.next() {
            let moved = call.moved.clone();
            let request = call.request(Sending::new(messages.clone(), moved.clone()));
            let attempt = async {
                let mut found = HashMap::new();
                let mut answers = gemfire.get_stream(request).await?.into_inner();
                while let Some(answer) = answers.message().await? {
                    moved.touch();
                    for entry in answer.entries {
                        if let (Some(KeyValue::String(key)), Some(value)) =
                            (entry.key.and_then(|key| key.key_value), entry.value)
                            && value.value_value.is_some()
                        {
                            found.insert(key, value);
                        }
                    }
                }
                Ok(found)
            };
            let outcome = call.within(attempt).await;
            if let Some(done) = call.settle(outcome) {
                return done;
            }
        }
        Err(call.failed())
    }

    pub(crate) async fn put_all(
        &self,
        region: &str,
        entries: &[(String, Bytes)],
    ) -> Result<(), Error> {
        let chunks = chunks(entries.iter().map(|(key, value)| {
            let entry = Entry {
                key: Some(string(key)),
                value: Some(bytes(value.clone())),
            };
            (entry, key.len() + value.len())
        }));
        let messages: Vec<PutStreamRequest> = std::iter::once(PutStreamRequest {
            config: Some(config(region)),
            entries: Vec::new(),
        })
        .chain(chunks.into_iter().map(|entries| PutStreamRequest {
            config: None,
            entries,
        }))
        .collect();
        let mut call = self.call(Retry::Idempotent, false).await?;
        while let Some(mut gemfire) = call.next() {
            let request = call.request(Sending::new(messages.clone(), call.moved.clone()));
            let outcome = call.within(gemfire.put_stream(request)).await;
            if let Some(done) = call.settle(outcome.map(|o| o.map(drop))) {
                return done;
            }
        }
        Err(call.failed())
    }

    pub(crate) async fn remove_all(&self, region: &str, keys: &[String]) -> Result<(), Error> {
        let chunks = chunks(keys.iter().map(|key| (string(key), key.len())));
        let messages: Vec<RemoveStreamRequest> = std::iter::once(RemoveStreamRequest {
            config: Some(config(region)),
            keys: Vec::new(),
        })
        .chain(
            chunks
                .into_iter()
                .map(|keys| RemoveStreamRequest { config: None, keys }),
        )
        .collect();
        let mut call = self.call(Retry::Idempotent, false).await?;
        while let Some(mut gemfire) = call.next() {
            let request = call.request(Sending::new(messages.clone(), call.moved.clone()));
            let outcome = call.within(gemfire.remove_stream(request)).await;
            if let Some(done) = call.settle(outcome.map(|o| o.map(drop))) {
                return done;
            }
        }
        Err(call.failed())
    }

    /// The region's string keys folded into what `init` makes, by `fold` (from `init` again
    /// when the stream fails over to another server).
    pub(crate) async fn keys<A>(
        &self,
        region: &str,
        init: impl Fn() -> A,
        mut fold: impl FnMut(&mut A, String),
    ) -> Result<A, Error> {
        let message = KeySetStreamRequest {
            region_name: region.to_string(),
        };
        let mut call = self.call(Retry::Idempotent, false).await?;
        while let Some(mut gemfire) = call.next() {
            let moved = call.moved.clone();
            let request = call.request(message.clone());
            let fold = &mut fold;
            let attempt = async {
                let mut folded = init();
                let mut batches = gemfire.key_set_stream(request).await?.into_inner();
                while let Some(batch) = batches.message().await? {
                    moved.touch();
                    for key in batch.keys {
                        if let Some(KeyValue::String(key)) = key.key_value {
                            fold(&mut folded, key);
                        }
                    }
                }
                Ok(folded)
            };
            let outcome = call.within(attempt).await;
            if let Some(done) = call.settle(outcome) {
                return done;
            }
        }
        Err(call.failed())
    }
}

/// One call going round the servers.
struct Call<'c> {
    connect: &'c Connect,
    retry: Retry,
    /// A unary call tells the server its deadline.
    unary: bool,
    moved: Moved,
    order: std::vec::IntoIter<Arc<Node>>,
    /// The server of the attempt under way, and its connection's generation.
    trying: Option<(Arc<Node>, u64)>,
    /// Why the servers tried failed.
    failures: Vec<String>,
}

/// Why an attempt failed.
enum Failure {
    /// The server answered with an error: another would too.
    Answered,
    /// The request never left: no connection.
    Unsent,
    /// The request may have left: the connection failed, or the server is unavailable.
    Lost,
    /// The deadline the server was told passed.
    TimedOut,
}

impl Failure {
    fn of(status: &Status) -> Failure {
        let mut source = std::error::Error::source(status);
        let lost = source.is_some() || status.code() == Code::Unavailable;
        while let Some(e) = source {
            if e.is::<tonic::ConnectError>() {
                return Failure::Unsent;
            }
            if e.is::<tonic::TimeoutExpired>() {
                return Failure::TimedOut;
            }
            source = e.source();
        }
        match lost {
            true => Failure::Lost,
            false => Failure::Answered,
        }
    }
}

impl Call<'_> {
    /// The client of the next server to try: None when none is left, or the time is up.
    fn next(&mut self) -> Option<GemFireClient<Channel>> {
        if self.moved.until(self.connect.timeout) <= Instant::now() {
            return None;
        }
        let node = self.order.next()?;
        let (generation, channel) = node.channel();
        self.trying = Some((node, generation));
        Some(GemFireClient::new(channel).max_decoding_message_size(usize::MAX))
    }

    /// `message` with the token, and for a unary call the time left as its deadline.
    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = self.connect.request(message);
        if self.unary {
            let until = self.moved.until(self.connect.timeout);
            request.set_timeout(until.saturating_duration_since(Instant::now()));
        }
        request
    }

    async fn within<T>(
        &self,
        attempt: impl Future<Output = Result<T, Status>>,
    ) -> Option<Result<T, Status>> {
        self.moved.within(self.connect.timeout, attempt).await
    }

    /// What the attempt's `outcome` (None: nothing moved for the timeout) makes of the call:
    /// None to try the next server.
    fn settle<T>(&mut self, outcome: Option<Result<T, Status>>) -> Option<Result<T, Error>> {
        let (node, generation) = self.trying.take().expect("an attempt under way");
        let (failure, why) = match outcome {
            Some(Ok(answer)) => {
                node.answered();
                return Some(Ok(answer));
            }
            Some(Err(status)) => (Failure::of(&status), describe(&node.address, &status)),
            None => (Failure::TimedOut, String::new()),
        };
        // the deadline told the server, or nothing moved: one timeout
        let why = match failure {
            Failure::TimedOut => {
                let timeout = self.connect.timeout;
                format!("{}: no answer within {timeout:?}", node.address)
            }
            _ => why,
        };
        match failure {
            Failure::Answered => {
                node.answered();
                Some(Err(self.error(why)))
            }
            Failure::TimedOut => {
                node.renew(generation);
                node.failed();
                Some(Err(self.error(why)))
            }
            Failure::Unsent => {
                node.failed();
                self.failures.push(why);
                None
            }
            Failure::Lost => {
                node.failed();
                if self.retry == Retry::Idempotent {
                    self.failures.push(why);
                    return None;
                }
                Some(Err(self.error(why)))
            }
        }
    }

    /// The call's error: `last`, after why the servers tried before failed.
    fn error(&mut self, last: String) -> Error {
        self.failures.push(last);
        Error(self.failures.join("; "))
    }

    /// The call's error once no server is left to try.
    fn failed(self) -> Error {
        match self.failures.is_empty() {
            true => Error(format!("no answer within {:?}", self.connect.timeout)),
            false => Error(self.failures.join("; ")),
        }
    }
}

/// Why a call to `address` failed: the status, and what caused it.
pub(crate) fn describe(address: &str, status: &Status) -> String {
    let mut why = format!("{address}: {:?}", status.code());
    if !status.message().is_empty() {
        why = format!("{why}: {}", status.message());
    }
    causes(why, std::error::Error::source(status))
}

/// Why `e` (of `address`) happened, its causes after it.
pub(crate) fn describe_error(address: &str, e: &dyn std::error::Error) -> String {
    causes(format!("{address}: {e}"), e.source())
}

fn causes(mut why: String, mut source: Option<&dyn std::error::Error>) -> String {
    while let Some(e) = source {
        let cause = e.to_string();
        if !why.contains(&cause) {
            why = format!("{why}: {cause}");
        }
        source = e.source();
    }
    why
}

fn string(key: &str) -> Key {
    Key {
        key_value: Some(KeyValue::String(key.to_string())),
    }
}

fn bytes(value: Bytes) -> Value {
    Value {
        value_value: Some(ValueValue::Bytes(value)),
    }
}

fn config(region: &str) -> StreamConfig {
    StreamConfig {
        region_name: region.to_string(),
    }
}

/// `items` (each with its bytes) in messages of at most [`CHUNK_BYTES`] each.
fn chunks<T>(items: impl Iterator<Item = (T, usize)>) -> Vec<Vec<T>> {
    let (mut chunks, mut chunk, mut size) = (Vec::new(), Vec::new(), 0);
    for (item, bytes) in items {
        let bytes = bytes + ITEM_BYTES;
        if !chunk.is_empty() && size + bytes > CHUNK_BYTES {
            chunks.push(std::mem::take(&mut chunk));
            size = 0;
        }
        chunk.push(item);
        size += bytes;
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}
