//! The fan-out supervisor (`TunnelClient` of the Java client): one session per
//! node of the membership, renamed after the node its stream reached.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::BoxError;
use crate::proto;
use crate::session::{Session, Shared, spawn_session};

/// Supervisor events, one channel shared by every session.
pub enum Event {
    /// The stream is up and the advertise was acked by `node_id`.
    Connected {
        session: Arc<Session>,
        node_id: String,
    },
    /// A membership snapshot (pushed, or fetched via `ListNodes`).
    Membership { nodes: Vec<proto::Node> },
}

/// The fan-out supervisor: one session per known node, keyed by node id (the
/// bootstrap session keeps its endpoint as key until its first ack names the
/// node). All membership transitions run on this single task.
pub async fn supervise(
    mut events: mpsc::Receiver<Event>,
    spawn_events: mpsc::Sender<Event>,
    shared: Arc<Shared>,
    bootstrap: String,
) -> Result<(), BoxError> {
    let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
    let session = spawn_session(
        &shared,
        bootstrap.clone(),
        bootstrap.clone(),
        spawn_events.clone(),
    );
    sessions.insert(bootstrap, session);
    while let Some(event) = events.recv().await {
        match event {
            Event::Connected { session, node_id } => rekey(&mut sessions, session, node_id),
            Event::Membership { nodes } => reconcile(&mut sessions, nodes, &shared, &spawn_events),
        }
    }
    Ok(())
}

/// Renames `session` after the node id learned from its ack
/// (`TunnelClient#rekey`). A different session already mapped to the node is a
/// duplicate stream: the server keeps only the newest registration
/// (`SessionRegistry#register` closes the old one), so it is cancelled here.
fn rekey(sessions: &mut HashMap<String, Arc<Session>>, session: Arc<Session>, node_id: String) {
    if node_id.is_empty() {
        return;
    }
    let current = sessions
        .iter()
        .find(|(_, known)| Arc::ptr_eq(known, &session))
        .map(|(key, _)| key.clone());
    if current.as_deref() == Some(node_id.as_str()) {
        return; // already known under this name
    }
    if let Some(known) = sessions.get(&node_id)
        && !Arc::ptr_eq(known, &session)
    {
        println!("duplicate stream for node {node_id}; closing the older one");
        known.token.cancel();
    }
    if let Some(key) = current {
        sessions.remove(&key);
    }
    sessions.insert(node_id, session);
}

/// Adopts the membership (`TunnelClient#reconcile`): opens a stream per
/// unseen node -- adopting the session already dialing that endpoint, a second
/// stream to one node would make the server drop the first registration --
/// and drops the map entries of nodes that left; their tasks keep retrying
/// and re-enter via the ack rename.
fn reconcile(
    sessions: &mut HashMap<String, Arc<Session>>,
    nodes: Vec<proto::Node>,
    shared: &Arc<Shared>,
    events: &mpsc::Sender<Event>,
) {
    if nodes.is_empty() {
        return;
    }
    for node in &nodes {
        if node.node_id.is_empty() || sessions.contains_key(&node.node_id) {
            continue;
        }
        if node.public_url.is_empty() {
            continue; // no reachable address; the bootstrap stream covers it once acked
        }
        let same_endpoint = sessions
            .iter()
            .find(|(_, session)| session.url == node.public_url)
            .map(|(key, _)| key.clone());
        match same_endpoint {
            Some(key) => {
                let session = sessions.remove(&key).expect("key found in the same map");
                println!(
                    "node {} is the endpoint of the {key} session; adopting it",
                    node.node_id
                );
                rekey(sessions, session, node.node_id.clone());
            }
            None => {
                println!(
                    "opening tunnel stream to node {} at {}",
                    node.node_id, node.public_url
                );
                let session = spawn_session(
                    shared,
                    node.node_id.clone(),
                    node.public_url.clone(),
                    events.clone(),
                );
                sessions.insert(node.node_id.clone(), session);
            }
        }
    }
    let live: HashSet<String> = nodes.into_iter().map(|node| node.node_id).collect();
    sessions.retain(|key, _| live.contains(key));
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    fn session(url: &str) -> Arc<Session> {
        Arc::new(Session {
            url: url.into(),
            token: CancellationToken::new(),
        })
    }

    #[test]
    fn rekey_renames_the_session_and_cancels_the_duplicate() {
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        let bootstrap = session("grpc://127.0.0.1:8101");
        let other = session("grpcs://10.0.0.1:8201");
        sessions.insert("grpc://127.0.0.1:8101".into(), bootstrap.clone());
        sessions.insert("node-2".into(), other.clone());

        // the bootstrap ack names its node: the endpoint key becomes the node id
        rekey(&mut sessions, bootstrap.clone(), "node-1".into());
        assert_eq!(sessions.len(), 2);
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &bootstrap));
        assert!(sessions.contains_key("node-2"));
        assert!(!sessions.contains_key("grpc://127.0.0.1:8101"));

        // a second session acking as node-1 is a duplicate stream: it takes
        // over and the previous one is cancelled
        let duplicate = session("node-1-alt.example.com:8101");
        rekey(&mut sessions, duplicate.clone(), "node-1".into());
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &duplicate));
        assert!(bootstrap.token.is_cancelled());
        assert!(!duplicate.token.is_cancelled());
    }

    #[test]
    fn rekey_registers_the_node_even_without_a_map_entry() {
        // a prune may have dropped the entry before the ack arrived; the ack
        // re-inserts the session (TunnelClient#rekey puts unconditionally)
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        let session = session("grpc://127.0.0.1:8101");
        rekey(&mut sessions, session.clone(), "node-1".into());
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &session));
        assert!(!session.token.is_cancelled());
    }

    #[test]
    fn rekey_ignores_empty_and_known_node_ids() {
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        let session = session("grpc://127.0.0.1:8101");
        sessions.insert("node-1".into(), session.clone());
        rekey(&mut sessions, session.clone(), String::new());
        rekey(&mut sessions, session.clone(), "node-1".into());
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &session));
        assert!(!session.token.is_cancelled());
    }
}
