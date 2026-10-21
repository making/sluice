//! The state shared by the feeds and the watchers: one global sequence over
//! the lines of every node, a ring of the recent ones, and which nodes to tell
//! to feed (only those someone watches).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, watch};

use crate::proto::{
    AccessEvent, AccessFilter, Cursor, FeedControl, FeedStart, FeedStop, NodeState, WatchEvent,
    WatchGap, WatchHello, WatchRecord, feed_control, watch_event, watch_gap,
};

/// One line of the ring: an access log line or the lines a node dropped.
#[derive(Debug)]
pub struct Line {
    pub seq: u64,
    pub node_id: String,
    pub body: LineBody,
}

#[derive(Debug)]
pub enum LineBody {
    Event(AccessEvent),
    Dropped(u64),
}

/// What the watchers hear live, in the order of the global sequence.
#[derive(Debug, Clone)]
pub enum Item {
    Line(Arc<Line>),
    Node(NodeState),
}

/// The control a node's feed stream carries; `None` until the first change.
pub type ControlReceiver = watch::Receiver<Option<FeedControl>>;

pub struct Hub {
    epoch: u64,
    backfill: usize,
    ring_size: usize,
    live: broadcast::Sender<Item>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    seq: u64,
    ring: VecDeque<Arc<Line>>,
    nodes: BTreeMap<String, Node>,
    watchers: HashMap<u64, AccessFilter>,
    next_id: u64,
}

struct Node {
    boot_id: String,
    /// The highest `node_seq` received in the node's run `boot_id`.
    last_node_seq: u64,
    feed: Option<Feed>,
}

struct Feed {
    id: u64,
    control: watch::Sender<Option<FeedControl>>,
}

/// What a node is to feed: nothing, or the lines passing the filter.
#[derive(PartialEq)]
enum Desired {
    Stop,
    Start(AccessFilter),
}

impl Hub {
    /// A hub keeping the newest `ring_size` lines, a watch without a cursor
    /// beginning with the newest `backfill` of them.
    pub fn new(ring_size: usize, backfill: usize) -> Self {
        let ring_size = ring_size.max(1);
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(1);
        let (live, _) = broadcast::channel(ring_size.clamp(16, 1024));
        Self {
            epoch,
            backfill,
            ring_size,
            live,
            state: Mutex::new(State::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // a panic while holding the lock leaves the state consistent enough
        // for a best-effort view
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Registers the feed of a node; a feed it still had open is replaced,
    /// its control stream ended. Returns the feed's id and its control stream.
    pub fn connect(&self, node_id: &str, boot_id: &str) -> (u64, ControlReceiver) {
        let mut state = self.state();
        state.next_id += 1;
        let id = state.next_id;
        let (control, receiver) = watch::channel(None);
        let node = state
            .nodes
            .entry(node_id.to_string())
            .or_insert_with(|| Node {
                boot_id: boot_id.to_string(),
                last_node_seq: 0,
                feed: None,
            });
        if node.boot_id != boot_id {
            node.boot_id = boot_id.to_string();
            node.last_node_seq = 0;
        }
        let reconnected = node.feed.replace(Feed { id, control }).is_some();
        if !reconnected {
            let _ = self.live.send(Item::Node(NodeState {
                node_id: node_id.to_string(),
                connected: true,
            }));
        }
        Self::steer(&mut state, node_id);
        (id, receiver)
    }

    /// Marks the node disconnected, unless a newer feed replaced this one.
    pub fn disconnect(&self, node_id: &str, feed_id: u64) {
        let mut state = self.state();
        let Some(node) = state.nodes.get_mut(node_id) else {
            return;
        };
        if node.feed.as_ref().is_some_and(|feed| feed.id == feed_id) {
            node.feed = None;
            let _ = self.live.send(Item::Node(NodeState {
                node_id: node_id.to_string(),
                connected: false,
            }));
        }
    }

    /// Appends a line of the node to the ring and hands it to the watchers.
    pub fn publish(&self, node_id: &str, event: AccessEvent) {
        let mut state = self.state();
        if let Some(node) = state.nodes.get_mut(node_id) {
            node.last_node_seq = node.last_node_seq.max(event.node_seq);
        }
        self.append(&mut state, node_id, LineBody::Event(event));
    }

    /// Records the lines the node dropped, at their place in the sequence.
    pub fn dropped(&self, node_id: &str, count: u64) {
        let mut state = self.state();
        self.append(&mut state, node_id, LineBody::Dropped(count));
    }

    fn append(&self, state: &mut State, node_id: &str, body: LineBody) {
        state.seq += 1;
        let line = Arc::new(Line {
            seq: state.seq,
            node_id: node_id.to_string(),
            body,
        });
        state.ring.push_back(line.clone());
        while state.ring.len() > self.ring_size {
            state.ring.pop_front();
        }
        // under the lock, so a watch subscribing meanwhile sees each line once:
        // in its snapshot or live
        let _ = self.live.send(Item::Line(line));
    }

    /// Opens a watch: the events to send first (hello, node states, backfill),
    /// the live receiver continuing right after them, the sequence the
    /// snapshot reached, and the guard keeping the watcher registered.
    pub fn watch(self: &Arc<Self>, filter: AccessFilter, after: Option<Cursor>) -> Watch {
        let mut state = self.state();
        let live = self.live.subscribe();
        let resume = after.filter(|cursor| cursor.epoch == self.epoch);
        let mut first = vec![event(watch_event::Body::Hello(WatchHello {
            epoch: self.epoch,
            reset: after.is_some() && resume.is_none(),
        }))];
        first.extend(Self::node_states(&state));
        match resume {
            Some(cursor) => first.extend(Self::since(&state, &filter, cursor.seq)),
            None => {
                let mut newest: Vec<WatchEvent> = state
                    .ring
                    .iter()
                    .rev()
                    .filter(|line| passes(&filter, line))
                    .take(self.backfill)
                    .map(|line| record(line))
                    .collect();
                newest.reverse();
                first.extend(newest);
            }
        }
        state.next_id += 1;
        let id = state.next_id;
        state.watchers.insert(id, filter.clone());
        let nodes: Vec<String> = state.nodes.keys().cloned().collect();
        for node_id in nodes {
            Self::steer(&mut state, &node_id);
        }
        Watch {
            first,
            live,
            seq: state.seq,
            filter,
            guard: WatcherGuard {
                hub: self.clone(),
                id,
            },
        }
    }

    /// After the watcher fell behind the live channel: the lines past `seq`
    /// still in the ring (a gap for those that are not), the node states, a
    /// new live receiver continuing right after them and the sequence reached.
    pub fn catch_up(
        &self,
        filter: &AccessFilter,
        seq: u64,
    ) -> (Vec<WatchEvent>, broadcast::Receiver<Item>, u64) {
        let state = self.state();
        let live = self.live.subscribe();
        let mut events = Self::node_states(&state);
        events.extend(Self::since(&state, filter, seq));
        (events, live, state.seq)
    }

    fn node_states(state: &State) -> Vec<WatchEvent> {
        state
            .nodes
            .iter()
            .map(|(node_id, node)| {
                event(watch_event::Body::Node(NodeState {
                    node_id: node_id.clone(),
                    connected: node.feed.is_some(),
                }))
            })
            .collect()
    }

    /// The ring's lines past `seq` passing the filter, after a gap for the
    /// lines past `seq` the ring no longer holds.
    fn since(state: &State, filter: &AccessFilter, seq: u64) -> Vec<WatchEvent> {
        let oldest = state.ring.front().map_or(state.seq + 1, |line| line.seq);
        let mut events = Vec::new();
        let missed = oldest
            .saturating_sub(seq + 1)
            .min(state.seq.saturating_sub(seq));
        if missed > 0 {
            events.push(event(watch_event::Body::Gap(WatchGap {
                seq: 0,
                node_id: String::new(),
                dropped: missed,
                cause: watch_gap::Cause::Behind as i32,
            })));
        }
        events.extend(
            state
                .ring
                .iter()
                .filter(|line| line.seq > seq && passes(filter, line))
                .map(|line| record(line)),
        );
        events
    }

    /// Tells the node's feed what to send for the watchers now registered:
    /// the union of their filters naming it, or nothing at all.
    fn steer(state: &mut State, node_id: &str) {
        let filters: Vec<&AccessFilter> = state
            .watchers
            .values()
            .filter(|filter| {
                filter.node_ids.is_empty() || filter.node_ids.iter().any(|id| id == node_id)
            })
            .collect();
        let desired = if filters.is_empty() {
            Desired::Stop
        } else {
            Desired::Start(AccessFilter {
                route_keys: union(filters.iter().map(|filter| &filter.route_keys)),
                kinds: union(filters.iter().map(|filter| &filter.kinds)),
                node_ids: Vec::new(),
            })
        };
        let Some(node) = state.nodes.get(node_id) else {
            return;
        };
        let Some(feed) = node.feed.as_ref() else {
            return;
        };
        let after_node_seq = node.last_node_seq;
        feed.control.send_if_modified(|current| {
            let now = match current.as_ref().and_then(|control| control.body.as_ref()) {
                Some(feed_control::Body::Start(start)) => {
                    Desired::Start(start.filter.clone().unwrap_or_default())
                }
                Some(feed_control::Body::Stop(_)) => Desired::Stop,
                // a feed starts out stopped
                None => Desired::Stop,
            };
            if now == desired {
                return false;
            }
            *current = Some(FeedControl {
                body: Some(match &desired {
                    Desired::Stop => feed_control::Body::Stop(FeedStop {}),
                    Desired::Start(filter) => feed_control::Body::Start(FeedStart {
                        after_node_seq,
                        filter: Some(filter.clone()),
                    }),
                }),
            });
            true
        });
    }

    fn unregister(&self, id: u64) {
        let mut state = self.state();
        state.watchers.remove(&id);
        let nodes: Vec<String> = state.nodes.keys().cloned().collect();
        for node_id in nodes {
            Self::steer(&mut state, &node_id);
        }
    }
}

/// An open watch; see [`Hub::watch`].
pub struct Watch {
    pub first: Vec<WatchEvent>,
    pub live: broadcast::Receiver<Item>,
    pub seq: u64,
    pub filter: AccessFilter,
    pub guard: WatcherGuard,
}

/// Unregisters its watcher when dropped, so the nodes nobody else watches stop
/// feeding.
pub struct WatcherGuard {
    hub: Arc<Hub>,
    id: u64,
}

impl Drop for WatcherGuard {
    fn drop(&mut self) {
        self.hub.unregister(self.id);
    }
}

/// Empty (everything) when any of the lists is, else every entry once, sorted.
fn union<'a>(lists: impl Iterator<Item = &'a Vec<String>>) -> Vec<String> {
    let mut all = BTreeSet::new();
    for list in lists {
        if list.is_empty() {
            return Vec::new();
        }
        all.extend(list.iter().cloned());
    }
    all.into_iter().collect()
}

/// Whether the line passes the filter; a node's dropped lines pass unless the
/// filter excludes the node, as they may be of any route and kind.
pub fn passes(filter: &AccessFilter, line: &Line) -> bool {
    if !filter.node_ids.is_empty() && !filter.node_ids.contains(&line.node_id) {
        return false;
    }
    match &line.body {
        LineBody::Event(event) => {
            (filter.route_keys.is_empty() || filter.route_keys.contains(&event.route_key))
                && (filter.kinds.is_empty() || filter.kinds.contains(&event.kind))
        }
        LineBody::Dropped(_) => true,
    }
}

pub fn record(line: &Line) -> WatchEvent {
    match &line.body {
        LineBody::Event(access) => event(watch_event::Body::Record(WatchRecord {
            seq: line.seq,
            node_id: line.node_id.clone(),
            event: Some(access.clone()),
        })),
        LineBody::Dropped(count) => event(watch_event::Body::Gap(WatchGap {
            seq: line.seq,
            node_id: line.node_id.clone(),
            dropped: *count,
            cause: watch_gap::Cause::NodeDropped as i32,
        })),
    }
}

fn event(body: watch_event::Body) -> WatchEvent {
    WatchEvent { body: Some(body) }
}
