//! The fan-out supervisor (`TunnelClient` of the Java client): one session per
//! node of the membership, at the node's public url.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use crate::BoxError;
use crate::proto;
use crate::session::{self, Backoff, Session, Shared, jitter_unit, spawn_session};

/// The fan-out supervisor. `bootstrap` is asked for the node list
/// (`ListNodes`) only: it may be a load balancer reaching a different node on
/// every connection, so no stream runs through it unless a node has no public
/// url of its own. The node list opens one session per node at its public url
/// (keyed by that url) and closes the sessions of nodes that left; the
/// membership snapshots the sessions learn (`events`) do the same. The
/// bootstrap is asked again whenever no stream is up, in case the whole
/// membership moved. A server without `ListNodes` is served by one session to
/// the bootstrap. All membership transitions run on this single task.
pub async fn supervise(
    mut events: mpsc::Receiver<Vec<proto::Node>>,
    spawn_events: mpsc::Sender<Vec<proto::Node>>,
    shared: Arc<Shared>,
    bootstrap: String,
) -> Result<(), BoxError> {
    let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
    let discovery = discover(&shared, &bootstrap, &spawn_events);
    tokio::pin!(discovery);
    loop {
        tokio::select! {
            () = &mut discovery => return Ok(()),
            Some(nodes) = events.recv() => reconcile(
                &mut sessions,
                endpoints(nodes, &bootstrap),
                &shared,
                &spawn_events,
            ),
        }
    }
}

/// Asks the bootstrap for the node list until a stream is up, and again
/// whenever none is (`TunnelClient#discoverLoop`); with the backoff of the
/// sessions between attempts. Never returns.
async fn discover(shared: &Shared, bootstrap: &str, events: &mpsc::Sender<Vec<proto::Node>>) {
    let mut backoff = Backoff::new();
    loop {
        if shared.up.load(Ordering::Acquire) > 0 {
            backoff.reset();
        } else {
            let nodes = session::discover(shared, bootstrap).await;
            if !nodes.is_empty() {
                let _ = events.send(nodes).await;
            }
        }
        tokio::time::sleep(backoff.next(jitter_unit())).await;
    }
}

/// The url each node is reached at, with its node id
/// (`TunnelClient#endpoints`): its public url, or the bootstrap for a node
/// without one; the first node of a url takes it.
fn endpoints(nodes: Vec<proto::Node>, bootstrap: &str) -> Vec<(String, String)> {
    let mut endpoints: Vec<(String, String)> = Vec::new();
    for node in nodes {
        let url = if node.public_url.is_empty() {
            bootstrap.to_string()
        } else {
            node.public_url
        };
        if !endpoints.iter().any(|(known, _)| *known == url) {
            endpoints.push((url, node.node_id));
        }
    }
    endpoints
}

/// Adopts the membership (`TunnelClient#reconcile`, url to node id, see
/// [`endpoints`]): opens a session to every new url, closes the sessions of
/// the urls no longer listed.
fn reconcile(
    sessions: &mut HashMap<String, Arc<Session>>,
    endpoints: Vec<(String, String)>,
    shared: &Arc<Shared>,
    events: &mpsc::Sender<Vec<proto::Node>>,
) {
    if endpoints.is_empty() {
        return;
    }
    for (url, node_id) in &endpoints {
        if !sessions.contains_key(url) {
            println!(
                "opening tunnel stream to node {} at {url}",
                if node_id.is_empty() { "?" } else { node_id }
            );
            let session = spawn_session(shared, node_id.clone(), url.clone(), events.clone());
            sessions.insert(url.clone(), session);
        }
    }
    sessions.retain(|url, session| {
        if endpoints.iter().any(|(listed, _)| listed == url) {
            return true;
        }
        println!("closing tunnel stream to {url}: no longer a member");
        session.token.cancel();
        false
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::proto::frame::Body;
    use crate::session::testing::*;

    fn node(node_id: &str, public_url: &str) -> proto::Node {
        proto::Node {
            node_id: node_id.into(),
            public_url: public_url.into(),
        }
    }

    #[test]
    fn endpoints_take_the_bootstrap_for_a_node_without_public_url_and_dedup_urls() {
        let nodes = vec![
            node("n0", "grpc://n0:8001"),
            node("n1", ""),
            node("n2", "grpc://n0:8001"),
        ];
        assert_eq!(
            endpoints(nodes, "grpc://lb:8001"),
            vec![
                ("grpc://n0:8001".to_string(), "n0".to_string()),
                ("grpc://lb:8001".to_string(), "n1".to_string()),
            ]
        );
    }

    /// The bootstrap only lists the nodes: every stream goes to a node's
    /// public url, none to the bootstrap, and the stream of a node that left
    /// the membership is closed.
    #[tokio::test]
    async fn the_bootstrap_lists_the_nodes_and_the_streams_go_to_their_public_urls() {
        let (shared, _, _) = hello_shared(Duration::ZERO).await;
        let (url0, mut streams0) = tunnel_server(None).await;
        let (url1, mut streams1) = tunnel_server(None).await;
        let (bootstrap, mut bootstrap_streams) =
            tunnel_server(Some(vec![node("n0", &url0), node("n1", &url1)])).await;
        let (events, received) = mpsc::channel(32);
        let supervisor = tokio::spawn(supervise(received, events, shared.clone(), bootstrap));

        let mut n0 = tokio::time::timeout(HANG, streams0.recv())
            .await
            .expect("hang guard")
            .expect("a stream to n0");
        let mut n1 = tokio::time::timeout(HANG, streams1.recv())
            .await
            .expect("hang guard")
            .expect("a stream to n1");
        n0.advertise().await;
        n0.ack(false);
        n1.advertise().await;
        n1.ack(false);
        // n1 leaves: its stream ends (within the hang guard)
        n0.send(Body::MembershipUpdate(proto::MembershipUpdate {
            nodes: vec![node("n0", &url0)],
            membership_version: 2,
        }));
        n1.ended().await;

        assert!(
            bootstrap_streams.try_recv().is_err(),
            "no stream to the bootstrap"
        );
        supervisor.abort();
    }
}
