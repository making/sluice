//! The servers the locators name: an xDS subscription (the aggregated discovery service, state
//! of the world) to the endpoints of `gemfire_grpc`, each answer acknowledged (an unusable one
//! refused, the servers kept), followed while it lasts. The locators are tried in order, a
//! round of them again after a pause while none answers; the servers known stay meanwhile, and
//! an answer naming none keeps them too (a locator that just started may know none yet).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Endpoint;

use crate::client::{Connect, Moved, Node, Servers, describe};
use crate::proto::envoy::config::core::v3::address::Address as Kind;
use crate::proto::envoy::config::core::v3::socket_address::PortSpecifier;
use crate::proto::envoy::config::core::v3::{HealthStatus, Node as Subscriber};
use crate::proto::envoy::config::endpoint::v3::ClusterLoadAssignment;
use crate::proto::envoy::config::endpoint::v3::lb_endpoint::HostIdentifier;
use crate::proto::envoy::service::discovery::v3::aggregated_discovery_service_client::AggregatedDiscoveryServiceClient;
use crate::proto::envoy::service::discovery::v3::{DiscoveryRequest, DiscoveryResponse};
use crate::proto::google::rpc::Status as Nack;

/// The type of the resources asked for: the endpoint discovery's.
pub const EDS: &str = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";

/// The resource a GemFire locator names its servers' gRPC endpoints by.
pub const RESOURCE: &str = "gemfire_grpc";

/// The pause after a round of the locators none answered, doubled each round in a row.
const PAUSE: Duration = Duration::from_secs(1);

/// The longest pause between rounds.
const PAUSE_MAX: Duration = Duration::from_secs(30);

/// `google.rpc.Code.INVALID_ARGUMENT`: why an answer is refused.
const INVALID_ARGUMENT: i32 = 3;

/// Follows the locators (by address), publishing the servers they name; runs until aborted.
pub(crate) async fn follow(
    label: String,
    locators: Vec<(String, Endpoint)>,
    connect: Arc<Connect>,
    publish: watch::Sender<Servers>,
) {
    let mut follower = Follower {
        label,
        connect,
        publish,
        logged: HashMap::new(),
    };
    let mut pause = PAUSE;
    loop {
        let mut answered = false;
        let mut failures = Vec::new();
        for (address, endpoint) in &locators {
            let followed = follower.subscribe(address, endpoint, &mut answered).await;
            let why = match followed {
                Ok(()) => format!("{address}: the locator ended the subscription"),
                Err(why) => why,
            };
            follower.log(address, Some(&why));
            failures.push(why);
        }
        // read by the calls finding no servers: none waits for it
        follower.publish.send_if_modified(|servers| {
            if servers.nodes.is_empty() {
                servers.why = format!("no locator answers: {}", failures.join("; ")).into();
            }
            false
        });
        pause = match answered {
            true => PAUSE,
            false => (pause * 2).min(PAUSE_MAX),
        };
        tokio::time::sleep(pause).await;
    }
}

struct Follower {
    label: String,
    connect: Arc<Connect>,
    publish: watch::Sender<Servers>,
    /// What was logged last of each locator: a failure repeating is not logged again.
    logged: HashMap<String, Option<String>>,
}

impl Follower {
    /// Follows the subscription to the locator at `address` until it ends (`answered` once it
    /// answered): why it ended.
    async fn subscribe(
        &mut self,
        address: &str,
        endpoint: &Endpoint,
        answered: &mut bool,
    ) -> Result<(), String> {
        let timeout = self.connect.timeout;
        let mut ads = AggregatedDiscoveryServiceClient::new(endpoint.connect_lazy())
            .max_decoding_message_size(usize::MAX);
        let (requests, sent) = mpsc::unbounded_channel();
        let _ = requests.send(discovery(String::new(), String::new(), None));
        let request = self.connect.request(UnboundedReceiverStream::new(sent));
        // the headers come with the first answer: both within the timeout
        let started = Moved::now();
        let opened = started.within(timeout, ads.stream_aggregated_resources(request));
        let mut answers = match opened.await {
            Some(Ok(answers)) => answers.into_inner(),
            Some(Err(status)) => return Err(describe(address, &status)),
            None => return Err(format!("{address}: no answer within {timeout:?}")),
        };
        let (mut accepted, mut first) = (String::new(), true);
        loop {
            let next = match first {
                true => started.within(timeout, answers.message()).await,
                false => Some(answers.message().await),
            };
            let answer = match next {
                Some(Ok(Some(answer))) => answer,
                Some(Ok(None)) => return Ok(()),
                Some(Err(status)) => return Err(describe(address, &status)),
                None => return Err(format!("{address}: no answer within {timeout:?}")),
            };
            (*answered, first) = (true, false);
            let nonce = answer.nonce.clone();
            match self.take(address, &answer) {
                Ok(()) => {
                    accepted = answer.version_info;
                    let _ = requests.send(discovery(accepted.clone(), nonce, None));
                }
                Err(why) => {
                    eprintln!(
                        "{}: gemfire locator {address}: refused an answer: {why}",
                        self.label
                    );
                    let nack = Nack {
                        code: INVALID_ARGUMENT,
                        message: why,
                    };
                    let _ = requests.send(discovery(accepted.clone(), nonce, Some(nack)));
                }
            }
        }
    }

    /// Publishes the servers `answer` names, unless it names none; the servers named before
    /// keep their connections.
    fn take(&mut self, address: &str, answer: &DiscoveryResponse) -> Result<(), String> {
        let addresses = servers(answer)?;
        let current = self.publish.borrow().nodes.clone();
        if addresses.is_empty() {
            if current.is_empty() {
                // read by the calls finding no servers: none waits for it
                self.publish.send_if_modified(|servers| {
                    servers.why = format!("the locator {address} names none").into();
                    false
                });
            } else {
                let kept = format!(
                    "{address}: names no servers: the {} known kept",
                    current.len()
                );
                self.log(address, Some(&kept));
            }
            return Ok(());
        }
        self.log(address, None);
        let unchanged = current.len() == addresses.len()
            && current
                .iter()
                .zip(&addresses)
                .all(|(node, address)| node.address == *address);
        if unchanged {
            return Ok(());
        }
        let nodes = addresses
            .iter()
            .map(
                |server| match current.iter().find(|node| node.address == *server) {
                    Some(node) => Ok(node.clone()),
                    None => self.connect.node(server),
                },
            )
            .collect::<Result<Vec<Arc<Node>>, String>>()?;
        println!(
            "{}: gemfire servers {} (locator {address})",
            self.label,
            addresses.join(", ")
        );
        self.publish.send_replace(Servers {
            nodes: nodes.into(),
            why: "".into(),
        });
        Ok(())
    }

    /// Logs `failure` of the locator at `address` unless it was the last logged of it; None:
    /// it answers (logged once it failed before).
    fn log(&mut self, address: &str, failure: Option<&str>) {
        let last = self.logged.entry(address.to_string()).or_default();
        if last.as_deref() == failure {
            return;
        }
        match failure {
            Some(why) => eprintln!("{}: gemfire locator {why}", self.label),
            None if last.is_some() => println!("{}: gemfire locator {address} answers", self.label),
            None => {}
        }
        *last = failure.map(String::from);
    }
}

/// A subscription (no `nonce`), the acknowledgement of an answer or its refusal (`nack`, the
/// version accepted before).
fn discovery(version: String, nonce: String, nack: Option<Nack>) -> DiscoveryRequest {
    DiscoveryRequest {
        version_info: version,
        node: Some(Subscriber {
            id: format!("sluice-wicket-{}", std::process::id()),
            cluster: "sluice".to_string(),
            user_agent_name: env!("CARGO_PKG_NAME").to_string(),
        }),
        resource_names: vec![RESOURCE.to_string()],
        type_url: EDS.to_string(),
        response_nonce: nonce,
        error_detail: nack,
    }
}

/// The servers `answer` names (`host:port`, sorted): its endpoints of `gemfire_grpc` that
/// serve (healthy, degraded or of unknown health).
fn servers(answer: &DiscoveryResponse) -> Result<Vec<String>, String> {
    if answer.type_url != EDS {
        return Err(format!("resources of type {}", answer.type_url));
    }
    let mut servers = BTreeSet::new();
    for resource in &answer.resources {
        if resource.type_url != EDS {
            return Err(format!("a resource of type {}", resource.type_url));
        }
        let assignment = ClusterLoadAssignment::decode(resource.value.as_slice())
            .map_err(|e| format!("a ClusterLoadAssignment: {e}"))?;
        if assignment.cluster_name != RESOURCE {
            continue;
        }
        let endpoints = assignment
            .endpoints
            .into_iter()
            .flat_map(|locality| locality.lb_endpoints);
        for endpoint in endpoints {
            let serving = matches!(
                HealthStatus::try_from(endpoint.health_status),
                Ok(HealthStatus::Unknown | HealthStatus::Healthy | HealthStatus::Degraded)
            );
            let Some(HostIdentifier::Endpoint(endpoint)) = endpoint.host_identifier else {
                continue;
            };
            let Some(Kind::SocketAddress(socket)) = endpoint.address.and_then(|a| a.address) else {
                continue;
            };
            let Some(PortSpecifier::PortValue(port)) = socket.port_specifier else {
                continue;
            };
            if !serving || socket.address.is_empty() {
                continue;
            }
            let host = match socket.address.contains(':') && !socket.address.starts_with('[') {
                true => format!("[{}]", socket.address),
                false => socket.address,
            };
            servers.insert(format!("{host}:{port}"));
        }
    }
    Ok(servers.into_iter().collect())
}
