//! The state shared by the feeds and the watchers: one global sequence over
//! the lines of every node, a ring of the recent ones, and which nodes to tell
//! to feed (only those someone watches).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, watch};

use crate::proto::{
    AccessEvent, AccessFilter, Cursor, FeedControl, FeedStart, FeedStop, NodeState, WatchEvent,
    WatchGap, WatchHello, WatchRecord, access_event, feed_control, watch_event, watch_gap,
};

/// The most entries one list of a watch's filter may name: a client's choice,
/// so bounded before it is stored.
pub const MAX_FILTER_ENTRIES: usize = 1024;

/// One line of the ring: an access log line or the lines a node dropped.
#[derive(Debug)]
pub struct Line {
    pub seq: u64,
    /// When it was taken, in nanoseconds since the epoch: unique and ascending with `seq`.
    pub time_unix_nanos: u64,
    pub node_id: String,
    pub body: LineBody,
}

#[derive(Debug)]
pub enum LineBody {
    Event(AccessEvent),
    Dropped(u64),
}

/// The heap a line holds, near enough to bound the ring by: its structs and
/// the strings they own.
pub fn line_bytes(line: &Line) -> usize {
    let base = std::mem::size_of::<Line>() + line.node_id.len();
    match &line.body {
        LineBody::Event(event) => {
            base + std::mem::size_of::<AccessEvent>()
                + event.kind.len()
                + event.route_key.len()
                + event
                    .fields
                    .iter()
                    .map(|field: &access_event::Field| {
                        std::mem::size_of::<access_event::Field>()
                            + field.name.len()
                            + field.value.len()
                    })
                    .sum::<usize>()
        }
        LineBody::Dropped(_) => base,
    }
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
    /// The most bytes the ring keeps: its lines are dropped from the front
    /// until they fit.
    ring_bytes: usize,
    live: broadcast::Sender<Item>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    seq: u64,
    /// The last line's `time_unix_nanos`.
    time: u64,
    ring: VecDeque<Arc<Line>>,
    /// The bytes of `ring`'s lines, by [`line_bytes`].
    retained: usize,
    nodes: BTreeMap<String, Node>,
    watchers: HashMap<u64, AccessFilter>,
    /// What each node is to feed, from the watchers naming it, kept as the
    /// watchers register and unregister.
    steer: HashMap<String, Steer>,
    next_id: u64,
}

/// A node's watchers, counted so a registration adds one filter's entries
/// instead of reuniting every watcher's.
#[derive(Default)]
struct Steer {
    /// The watchers naming the node, or every node.
    watchers: usize,
    /// Those of them asking every route (or kind): nothing is then excluded.
    all_routes: usize,
    all_kinds: usize,
    routes: HashMap<String, usize>,
    kinds: HashMap<String, usize>,
}

impl Steer {
    fn add(&mut self, filter: &AccessFilter) {
        self.watchers += 1;
        if filter.route_keys.is_empty() {
            self.all_routes += 1;
        } else {
            for key in &filter.route_keys {
                *self.routes.entry(key.clone()).or_default() += 1;
            }
        }
        if filter.kinds.is_empty() {
            self.all_kinds += 1;
        } else {
            for kind in &filter.kinds {
                *self.kinds.entry(kind.clone()).or_default() += 1;
            }
        }
    }

    fn remove(&mut self, filter: &AccessFilter) {
        self.watchers = self.watchers.saturating_sub(1);
        if filter.route_keys.is_empty() {
            self.all_routes = self.all_routes.saturating_sub(1);
        } else {
            for key in &filter.route_keys {
                if let Some(count) = self.routes.get_mut(key) {
                    *count -= 1;
                    if *count == 0 {
                        self.routes.remove(key);
                    }
                }
            }
        }
        if filter.kinds.is_empty() {
            self.all_kinds = self.all_kinds.saturating_sub(1);
        } else {
            for kind in &filter.kinds {
                if let Some(count) = self.kinds.get_mut(kind) {
                    *count -= 1;
                    if *count == 0 {
                        self.kinds.remove(kind);
                    }
                }
            }
        }
    }

    fn desired(&self) -> Desired {
        if self.watchers == 0 {
            return Desired::Stop;
        }
        let mut routes: Vec<String> = if self.all_routes > 0 {
            Vec::new()
        } else {
            self.routes.keys().cloned().collect()
        };
        routes.sort();
        let mut kinds: Vec<String> = if self.all_kinds > 0 {
            Vec::new()
        } else {
            self.kinds.keys().cloned().collect()
        };
        kinds.sort();
        Desired::Start(AccessFilter {
            route_keys: routes,
            kinds,
            node_ids: Vec::new(),
        })
    }
}

struct Node {
    boot_id: String,
    /// The highest `node_seq` received in the node's run `boot_id`.
    last_node_seq: u64,
    feed: Option<Feed>,
    /// Since when it has no feed.
    gone: Option<Instant>,
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
    /// A hub keeping the newest `ring_size` lines within `ring_bytes` bytes, a
    /// watch without a cursor beginning with the newest `backfill` of them.
    pub fn new(ring_size: usize, ring_bytes: usize, backfill: usize) -> Self {
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
            ring_bytes: ring_bytes.max(1),
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
                gone: None,
            });
        if node.boot_id != boot_id {
            node.boot_id = boot_id.to_string();
            node.last_node_seq = 0;
        }
        let reconnected = node.feed.replace(Feed { id, control }).is_some();
        node.gone = None;
        if !reconnected {
            let _ = self.live.send(Item::Node(NodeState {
                node_id: node_id.to_string(),
                connected: true,
                forgotten: false,
            }));
        }
        // a node coming back is told what the watchers already ask of it
        Self::rebuild(&mut state, node_id);
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
            node.gone = Some(Instant::now());
            let _ = self.live.send(Item::Node(NodeState {
                node_id: node_id.to_string(),
                connected: false,
                forgotten: false,
            }));
        }
    }

    /// Forgets the nodes disconnected for `after` or longer, as a restart of
    /// the flume does: listed no more, which the watchers are told, their
    /// lines of the ring kept. One feeding again is a node as any new one: it
    /// sends again the recent lines it has at hand. How many it forgot.
    pub fn forget(&self, after: Duration) -> usize {
        let mut state = self.state();
        let long_gone = |node: &Node| node.gone.is_some_and(|since| since.elapsed() >= after);
        let forgotten: Vec<String> = (state.nodes.iter())
            .filter(|(_, node)| long_gone(node))
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node_id in &forgotten {
            state.nodes.remove(node_id);
            // made again from the watchers when it feeds again
            state.steer.remove(node_id);
            let _ = self.live.send(Item::Node(NodeState {
                node_id: node_id.clone(),
                connected: false,
                forgotten: true,
            }));
        }
        forgotten.len()
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
        // a clock standing still or set back: the next nanosecond
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos() as u64);
        state.time = now.max(state.time + 1);
        let line = Arc::new(Line {
            seq: state.seq,
            time_unix_nanos: state.time,
            node_id: node_id.to_string(),
            body,
        });
        state.retained += line_bytes(&line);
        state.ring.push_back(line.clone());
        // one line may be larger than the whole budget: it is kept alone, so
        // the ring stays bounded by the budget plus one event's size
        while state.ring.len() > self.ring_size
            || (state.retained > self.ring_bytes && state.ring.len() > 1)
        {
            if let Some(front) = state.ring.pop_front() {
                state.retained -= line_bytes(&front);
            }
        }
        // under the lock, so a watch subscribing meanwhile sees each line once:
        // in its snapshot or live
        let _ = self.live.send(Item::Line(line));
    }

    /// Opens a watch: the events to send first (hello, node states, backfill),
    /// the live receiver continuing right after them, the sequence the
    /// snapshot reached, and the guard keeping the watcher registered. The
    /// backfill of a watch resuming after no line of this run: the newest
    /// lines, or with `oldest` every line of the ring.
    pub fn watch(
        self: &Arc<Self>,
        filter: AccessFilter,
        after: Option<Cursor>,
        oldest: bool,
    ) -> Result<Watch, String> {
        Self::check(&filter)?;
        let mut state = self.state();
        let live = self.live.subscribe();
        let resume = after.filter(|cursor| cursor.epoch == self.epoch);
        let mut first = vec![event(watch_event::Body::Hello(WatchHello {
            epoch: self.epoch,
            reset: after.is_some() && resume.is_none(),
            build: Some(crate::build()),
        }))];
        first.extend(Self::node_states(&state));
        match resume {
            Some(cursor) => first.extend(Self::since(&state, &filter, cursor.seq)),
            None if oldest => first.extend(
                (state.ring.iter())
                    .filter(|line| passes(&filter, line))
                    .map(|line| record(line)),
            ),
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
        let affected = Self::affected(&state, &filter);
        for node_id in &affected {
            state.steer.entry(node_id.clone()).or_default().add(&filter);
        }
        state.watchers.insert(id, filter.clone());
        for node_id in &affected {
            if state.nodes.contains_key(node_id) {
                Self::steer(&mut state, node_id);
            }
        }
        Ok(Watch {
            first,
            live,
            seq: state.seq,
            filter,
            guard: WatcherGuard {
                hub: self.clone(),
                id,
            },
        })
    }

    /// Refuses a filter of more entries than [`MAX_FILTER_ENTRIES`] in a list.
    pub fn check(filter: &AccessFilter) -> Result<(), String> {
        for (what, entries) in [
            ("route keys", filter.route_keys.len()),
            ("kinds", filter.kinds.len()),
            ("node ids", filter.node_ids.len()),
        ] {
            if entries > MAX_FILTER_ENTRIES {
                return Err(format!(
                    "the watch filter names {entries} {what}: at most {MAX_FILTER_ENTRIES} are kept"
                ));
            }
        }
        Ok(())
    }

    /// The nodes a watcher's registration or removal steers: those it names,
    /// or, naming none, every node there is at the time (one connecting later
    /// is steered from the watchers then).
    fn affected(state: &State, filter: &AccessFilter) -> Vec<String> {
        if filter.node_ids.is_empty() {
            state.nodes.keys().cloned().collect()
        } else {
            let nodes: BTreeSet<String> = filter.node_ids.iter().cloned().collect();
            nodes.into_iter().collect()
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
                    forgotten: false,
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

    /// The node's watchers, read from every watcher: the node's state when it
    /// connects.
    fn rebuild(state: &mut State, node_id: &str) {
        let mut steer = Steer::default();
        for filter in state.watchers.values() {
            if filter.node_ids.is_empty() || filter.node_ids.iter().any(|id| id == node_id) {
                steer.add(filter);
            }
        }
        state.steer.insert(node_id.to_string(), steer);
    }

    /// Tells the node's feed what its watchers ask of it, or nothing at all.
    fn steer(state: &mut State, node_id: &str) {
        let desired = state
            .steer
            .get(node_id)
            .map(Steer::desired)
            .unwrap_or(Desired::Stop);
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
        let Some(filter) = state.watchers.remove(&id) else {
            return;
        };
        let affected = Self::affected(&state, &filter);
        for node_id in &affected {
            if let Some(steer) = state.steer.get_mut(node_id) {
                steer.remove(&filter);
            }
        }
        // a node gone from the table leaves nothing to steer; keep its entry
        // only while a watcher names it
        for node_id in &affected {
            let empty = state
                .steer
                .get(node_id)
                .is_some_and(|steer| steer.watchers == 0);
            if empty && !state.nodes.contains_key(node_id) {
                state.steer.remove(node_id);
            }
        }
        for node_id in &affected {
            if state.nodes.contains_key(node_id) {
                Self::steer(&mut state, node_id);
            }
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
            time_unix_nanos: line.time_unix_nanos,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn event(value: &str) -> AccessEvent {
        AccessEvent {
            node_seq: 0,
            time_unix_millis: 0,
            kind: "request".into(),
            route_key: "r".into(),
            fields: vec![access_event::Field {
                name: "v".into(),
                value: value.into(),
            }],
        }
    }

    fn filter(routes: &[&str], kinds: &[&str]) -> AccessFilter {
        AccessFilter {
            route_keys: routes.iter().map(|key| key.to_string()).collect(),
            kinds: kinds.iter().map(|kind| kind.to_string()).collect(),
            node_ids: Vec::new(),
        }
    }

    fn started(control: &mut ControlReceiver) -> AccessFilter {
        match control
            .borrow_and_update()
            .as_ref()
            .and_then(|control| control.body.as_ref())
        {
            Some(feed_control::Body::Start(start)) => start.filter.clone().unwrap_or_default(),
            other => panic!("not started: {other:?}"),
        }
    }

    #[test]
    fn the_ring_drops_its_oldest_lines_to_fit_its_byte_budget() {
        let hub = Hub::new(100, 4096, 10);
        for _ in 0..20 {
            hub.publish("n", event(&"v".repeat(256)));
        }
        let state = hub.state();
        assert!(!state.ring.is_empty());
        let retained: usize = state.ring.iter().map(|line| line_bytes(line)).sum();
        assert!(retained <= 4096, "{retained} bytes retained");
        assert_eq!(state.retained, retained);
    }

    #[test]
    fn one_line_larger_than_the_budget_is_kept_alone() {
        let hub = Hub::new(100, 16, 10);
        hub.publish("n", event(&"v".repeat(1024)));
        let state = hub.state();
        assert_eq!(state.ring.len(), 1);
        assert!(state.retained > 16);
    }

    #[test]
    fn a_watch_filter_is_capped() {
        let hub = Arc::new(Hub::new(10, 1 << 20, 10));
        let keys = |count: usize| -> Vec<String> { (0..count).map(|i| format!("r{i}")).collect() };
        let at = |count: usize| AccessFilter {
            route_keys: keys(count),
            ..Default::default()
        };
        assert!(hub.watch(at(MAX_FILTER_ENTRIES), None, false).is_ok());
        assert!(hub.watch(at(MAX_FILTER_ENTRIES + 1), None, false).is_err());
    }

    #[test]
    fn watchers_steer_a_node_as_they_come_and_go_and_one_long_gone_is_forgotten() {
        let hub = Arc::new(Hub::new(10, 1 << 20, 10));
        let (feed, mut control) = hub.connect("n1", "boot");
        let watch = hub
            .watch(filter(&["r1"], &[]), None, false)
            .expect("a small filter is accepted");
        assert_eq!(started(&mut control).route_keys, vec!["r1".to_string()]);
        drop(watch);
        let stop = control.borrow_and_update().clone();
        assert!(
            matches!(
                stop.as_ref().and_then(|control| control.body.as_ref()),
                Some(feed_control::Body::Stop(_))
            ),
            "a node whose watchers are gone is stopped"
        );

        // feeding, or gone not long: listed; gone for long: forgotten, the
        // watchers told, and a node as any new one when it feeds again
        let listed = |hub: &Arc<Hub>| {
            let watch = hub.watch(filter(&[], &[]), None, false).unwrap();
            let nodes = watch
                .first
                .into_iter()
                .filter_map(|event| match event.body {
                    Some(watch_event::Body::Node(node)) => Some((node.node_id, node.connected)),
                    _ => None,
                });
            nodes.collect::<Vec<_>>()
        };
        let fifth = AccessEvent {
            node_seq: 5,
            ..event("line")
        };
        hub.publish("n1", fifth);
        assert_eq!(hub.forget(Duration::ZERO), 0);
        hub.disconnect("n1", feed);
        assert_eq!(hub.forget(Duration::from_secs(3600)), 0);
        assert_eq!(listed(&hub), [("n1".to_string(), false)]);
        let mut watch = hub.watch(filter(&[], &[]), None, false).unwrap();
        assert_eq!(hub.forget(Duration::ZERO), 1);
        let Ok(Item::Node(told)) = watch.live.try_recv() else {
            panic!("the watchers are told");
        };
        assert!(told.forgotten && !told.connected && told.node_id == "n1");
        assert!(listed(&hub).is_empty());
        let (_feed, mut control) = hub.connect("n1", "boot");
        assert_eq!(listed(&hub), [("n1".to_string(), true)]);
        let started = control
            .borrow_and_update()
            .clone()
            .and_then(|control| control.body);
        let Some(feed_control::Body::Start(start)) = started else {
            panic!("a node watched is started");
        };
        assert_eq!(start.after_node_seq, 0);
    }
}
