//! A wicket's live access log: its lines (its guests' output, the requests
//! it answered) fed to a sluice-flume (`--flume`) while someone watches, as
//! a sluice node feeds its access log lines (`FlumeFeed` / `LiveLogTap` of
//! the server).
//!
//! ```text
//! the wicket's lines --> Tap --Feed (the wicket dials out)--> sluice-flume <--Watch-- console, cf-api
//! ```
//!
//! Every line goes to a ring of the recent ones ([`RING_SIZE`]), the backfill
//! once the flume starts the feed; while it feeds, the lines passing its
//! filter also go to a bounded queue ([`QUEUE_CAPACITY`]) the feed drains.
//! Nothing waits for the flume: a line past a full queue is dropped and
//! counted, and the count reaches the flume as a gap at its place, as do the
//! lines a wicket counts dropped before they reached the tap
//! ([`Tap::dropped`]). The feed stream is held open while the wicket runs
//! and redialed with a backoff of 1s doubling to 30s whenever it ends; a
//! flume down costs a warning per 5 minutes.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{Notify, mpsc};

use crate::proto::flume_client::FlumeClient;
use crate::proto::{
    AccessEvent, AccessFilter, FeedGap, FeedHello, FeedMessage, access_event, feed_control,
    feed_message,
};
use crate::{BoxError, session, tls};

/// The recent lines kept for the backfill.
pub const RING_SIZE: usize = 512;

/// The lines the feed may lag behind before lines are dropped.
pub const QUEUE_CAPACITY: usize = 4096;

const MIN_BACKOFF: Duration = Duration::from_secs(1);

const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How often a feed failing again is reported.
const WARN_EVERY: Duration = Duration::from_secs(300);

/// Bound on one connect attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Messages handed to gRPC ahead of what it sent: past them its flow
/// control holds the feed back, and the queue fills.
const SEND_BUFFER: usize = 16;

/// The flume a wicket feeds its live access log to (`--flume`).
#[derive(Clone, Debug, PartialEq)]
pub struct FlumeSettings {
    /// `grpc://host:port` or `grpcs://host:port`.
    pub url: String,
    /// Its bearer token.
    pub token: String,
    /// The PEM CA verifying a `grpcs://` flume; None: the webPKI roots.
    pub ca_cert: Option<PathBuf>,
    /// Skip the verification of a `grpcs://` flume.
    pub insecure: bool,
}

impl FlumeSettings {
    /// The flume of `url` (`--flume`), none when it is unset or empty: a
    /// `grpc://` or `grpcs://` URL whose `token` (`--flume-token`) is
    /// required.
    pub fn of(
        url: Option<String>,
        token: Option<String>,
        ca_cert: Option<PathBuf>,
        insecure: bool,
    ) -> Result<Option<FlumeSettings>, String> {
        let Some(url) = url.filter(|url| !url.is_empty()) else {
            return Ok(None);
        };
        if !url.starts_with("grpc://") && !url.starts_with("grpcs://") {
            return Err(format!(
                "--flume: expected grpc://host:port or grpcs://host:port, got '{url}'"
            ));
        }
        let token = token.filter(|token| !token.is_empty()).ok_or(
            "--flume-token (or `flume-token` in the --config file) is required with --flume",
        )?;
        Ok(Some(FlumeSettings {
            url,
            token,
            ca_cert,
            insecure,
        }))
    }
}

/// One line of the live access log.
#[derive(Debug)]
pub struct Line {
    /// Increases by one per line of this run.
    pub seq: u64,
    /// When it was logged, in milliseconds since the epoch.
    pub time_unix_millis: i64,
    /// `stdout`, `stderr` or `response`.
    pub kind: &'static str,
    /// The host the line is about, lowercase, without the port.
    pub route_key: String,
    pub fields: Vec<(&'static str, String)>,
}

/// What the feed sends next: a line, or how many lines were dropped there.
#[derive(Debug)]
enum Item {
    Line(Arc<Line>),
    Gap(u64),
}

/// The lines the flume asks for; an empty set passes every one.
#[derive(Debug, Default)]
struct Filter {
    route_keys: HashSet<String>,
    kinds: HashSet<String>,
}

impl Filter {
    fn of(filter: AccessFilter) -> Filter {
        Filter {
            route_keys: filter.route_keys.into_iter().collect(),
            kinds: filter.kinds.into_iter().collect(),
        }
    }

    fn passes(&self, kind: &str, route_key: &str) -> bool {
        (self.route_keys.is_empty() || self.route_keys.contains(route_key))
            && (self.kinds.is_empty() || self.kinds.contains(kind))
    }
}

/// Where the lines enter the live access log; [`feed`] takes them off.
/// Publishing never blocks: a short lock, no wait.
pub struct Tap {
    /// None without a flume: nothing is kept.
    state: Option<Mutex<State>>,
    /// Signalled when an item was queued.
    queued: Notify,
}

#[derive(Default)]
struct State {
    seq: u64,
    ring: VecDeque<Arc<Line>>,
    /// The flume's filter while it feeds.
    feeding: Option<Filter>,
    queue: VecDeque<Item>,
    /// The lines in `queue`, its gaps aside.
    lines: usize,
}

impl State {
    /// Counts `dropped` lines in a gap at the queue's end.
    fn gap(&mut self, dropped: u64) {
        match self.queue.back_mut() {
            Some(Item::Gap(gap)) => *gap += dropped,
            // past the capacity by this one gap: the lines dropped next count in it
            _ => self.queue.push_back(Item::Gap(dropped)),
        }
    }

    fn passes(&self, kind: &str, route_key: &str) -> bool {
        self.feeding
            .as_ref()
            .is_some_and(|filter| filter.passes(kind, route_key))
    }
}

impl Tap {
    /// A tap keeping the lines, for a feed.
    pub fn new() -> Arc<Tap> {
        Arc::new(Tap {
            state: Some(Mutex::new(State::default())),
            queued: Notify::new(),
        })
    }

    /// A tap keeping nothing: no flume.
    pub fn off() -> Arc<Tap> {
        Arc::new(Tap {
            state: None,
            queued: Notify::new(),
        })
    }

    /// Whether lines are kept: a publisher builds none otherwise.
    pub fn on(&self) -> bool {
        self.state.is_some()
    }

    fn state(&self) -> Option<MutexGuard<'_, State>> {
        // a panic under the lock leaves the state consistent enough for a log
        self.state
            .as_ref()
            .map(|state| state.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Takes a line: into the ring, and into the queue while the flume feeds
    /// and the line passes its filter -- past a full queue it is counted as
    /// dropped instead, in a gap at the queue's end.
    pub fn publish(
        &self,
        kind: &'static str,
        route_key: String,
        fields: Vec<(&'static str, String)>,
    ) {
        let time_unix_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as i64);
        let Some(mut state) = self.state() else {
            return;
        };
        state.seq += 1;
        let line = Arc::new(Line {
            seq: state.seq,
            time_unix_millis,
            kind,
            route_key,
            fields,
        });
        state.ring.push_back(line.clone());
        if state.ring.len() > RING_SIZE {
            state.ring.pop_front();
        }
        if !state.passes(line.kind, &line.route_key) {
            return;
        }
        if state.lines < QUEUE_CAPACITY {
            state.queue.push_back(Item::Line(line));
            state.lines += 1;
        } else {
            state.gap(1);
        }
        drop(state);
        self.queued.notify_one();
    }

    /// Counts `count` lines of `kind` about `route_key` that were dropped
    /// before they reached the tap (a guest's own buffer was full): in a gap
    /// at the queue's end while the flume feeds and such lines pass its
    /// filter, as a line dropped past the queue is.
    pub fn dropped(&self, kind: &'static str, route_key: &str, count: u64) {
        let Some(mut state) = self.state() else {
            return;
        };
        if count == 0 || !state.passes(kind, route_key) {
            return;
        }
        state.gap(count);
        drop(state);
        self.queued.notify_one();
    }

    /// Starts feeding the lines passing `filter`, or changes the filter:
    /// returns the recent lines past `after` passing it, oldest first, to
    /// send ahead of the queue -- which holds only the lines published from
    /// now on.
    fn start(&self, after: u64, filter: Filter) -> VecDeque<Arc<Line>> {
        let Some(mut state) = self.state() else {
            return VecDeque::new();
        };
        state.queue.clear();
        state.lines = 0;
        let backfill = state
            .ring
            .iter()
            .filter(|line| line.seq > after && filter.passes(line.kind, &line.route_key))
            .cloned()
            .collect();
        state.feeding = Some(filter);
        backfill
    }

    /// Stops feeding: the queue is emptied, and lines stay in the ring only.
    fn stop(&self) {
        if let Some(mut state) = self.state() {
            state.feeding = None;
            state.queue.clear();
            state.lines = 0;
        }
    }

    /// Whether the queue holds an item.
    fn pending(&self) -> bool {
        self.state().is_some_and(|state| !state.queue.is_empty())
    }

    /// The next item of the queue.
    fn next(&self) -> Option<Item> {
        let mut state = self.state()?;
        let item = state.queue.pop_front();
        if let Some(Item::Line(_)) = item {
            state.lines -= 1;
        }
        item
    }

    /// The lines of the ring, oldest first.
    #[cfg(any(test, feature = "testing"))]
    pub fn recent(&self) -> Vec<Arc<Line>> {
        self.state()
            .map(|state| state.ring.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// The instance indices a route's instances hold, which the `instance` of
/// their lines names: unique among those alive, as CF numbers an app's. Each with when it was
/// taken: its instance's start.
#[derive(Default)]
pub struct Indices(Mutex<BTreeMap<usize, SystemTime>>);

impl Indices {
    /// The lowest index from `from` up that no instance holds.
    pub fn take(self: &Arc<Self>, from: usize) -> Index {
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut index = from;
        for (taken, _) in held.range(from..) {
            if *taken != index {
                break;
            }
            index += 1;
        }
        held.insert(index, SystemTime::now());
        Index {
            indices: self.clone(),
            index,
        }
    }

    /// The indices held and their starts: the instances alive.
    pub fn held(&self) -> BTreeMap<usize, SystemTime> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// An instance's index, freed when it drops.
pub struct Index {
    indices: Arc<Indices>,
    pub index: usize,
}

impl Drop for Index {
    fn drop(&mut self) {
        let mut held = self.indices.0.lock().unwrap_or_else(|e| e.into_inner());
        held.remove(&self.index);
    }
}

/// Starts feeding `tap` to `flume` in a task of its own, for good, as the
/// node `wicket:<client_id>`; fails only when its `--flume-ca-cert` cannot
/// be read.
pub fn start(flume: FlumeSettings, tap: Arc<Tap>, client_id: &str) -> Result<(), BoxError> {
    let ca_pem = match &flume.ca_cert {
        Some(path) => Some(
            std::fs::read(path).map_err(|e| format!("--flume-ca-cert {}: {e}", path.display()))?,
        ),
        None => None,
    };
    let node_id = format!("wicket:{client_id}");
    println!("live access log: feeding {} as {node_id}", flume.url);
    tokio::spawn(feed(flume, ca_pem, tap, node_id));
    Ok(())
}

/// Feeds `tap` to the flume as the node `node_id` for good: one feed stream
/// at a time, redialed with a backoff whenever it ends.
pub async fn feed(flume: FlumeSettings, ca_pem: Option<Vec<u8>>, tap: Arc<Tap>, node_id: String) {
    // names this run: the flume tells it apart from the wicket's earlier ones
    let boot_id = uuid::Uuid::new_v4().to_string();
    let mut backoff = MIN_BACKOFF;
    let mut warned: Option<Instant> = None;
    let mut suppressed = 0u64;
    loop {
        let began = Instant::now();
        let Err(e) = stream(&flume, ca_pem.as_deref(), &tap, &node_id, &boot_id).await;
        tap.stop();
        if warned.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
            let more = match suppressed {
                0 => String::new(),
                n => format!(" ({n} more since)"),
            };
            eprintln!(
                "live access log: the feed to {} ended: {e}{more}",
                flume.url
            );
            warned = Some(Instant::now());
            suppressed = 0;
        } else {
            suppressed += 1;
        }
        // a stream that held a while had the flume up: retry soon
        if began.elapsed() > MAX_BACKOFF {
            backoff = MIN_BACKOFF;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One feed stream, until it ends: the hello, then what the flume's
/// controls ask for -- the backfill and the queue while it feeds, nothing
/// after a stop.
async fn stream(
    flume: &FlumeSettings,
    ca_pem: Option<&[u8]>,
    tap: &Tap,
    node_id: &str,
    boot_id: &str,
) -> Result<Infallible, BoxError> {
    let channel = endpoint(flume, ca_pem)?.connect().await?;
    let (messages, outgoing) = mpsc::channel(SEND_BUFFER);
    messages
        .send(FeedMessage {
            body: Some(feed_message::Body::Hello(FeedHello {
                node_id: node_id.to_string(),
                boot_id: boot_id.to_string(),
            })),
        })
        .await?;
    let outgoing = futures_util::stream::unfold(outgoing, |mut outgoing| async move {
        outgoing.recv().await.map(|message| (message, outgoing))
    });
    let mut request = tonic::Request::new(outgoing);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", flume.token).parse()?);
    let mut controls = FlumeClient::new(channel).feed(request).await?.into_inner();
    let mut backfill = VecDeque::new();
    let mut feeding = false;
    loop {
        let pending = feeding && (!backfill.is_empty() || tap.pending());
        tokio::select! {
            control = controls.message() => {
                let Some(control) = control? else {
                    return Err("the flume ended the feed".into());
                };
                match control.body {
                    Some(feed_control::Body::Start(start)) => {
                        let filter = Filter::of(start.filter.unwrap_or_default());
                        backfill = tap.start(start.after_node_seq, filter);
                        feeding = true;
                    }
                    Some(feed_control::Body::Stop(_)) => {
                        tap.stop();
                        backfill.clear();
                        feeding = false;
                    }
                    None => {}
                }
            }
            permit = messages.reserve(), if pending => {
                let permit = permit.map_err(|_| "the feed stream closed")?;
                let item = match backfill.pop_front() {
                    Some(line) => Some(Item::Line(line)),
                    None => tap.next(),
                };
                if let Some(item) = item {
                    permit.send(message(item));
                }
            }
            // a line queued meanwhile left a permit: no wake-up is lost
            () = tap.queued.notified(), if feeding && !pending => {}
        }
    }
}

fn message(item: Item) -> FeedMessage {
    let body = match item {
        Item::Line(line) => feed_message::Body::Event(AccessEvent {
            node_seq: line.seq,
            time_unix_millis: line.time_unix_millis,
            kind: line.kind.to_string(),
            route_key: line.route_key.clone(),
            fields: line
                .fields
                .iter()
                .map(|(name, value)| access_event::Field {
                    name: name.to_string(),
                    value: value.clone(),
                })
                .collect(),
        }),
        Item::Gap(dropped) => feed_message::Body::Gap(FeedGap { dropped }),
    };
    FeedMessage { body: Some(body) }
}

/// The flume's endpoint: connect timeout, keepalive (a flume gone without a
/// word ends the stream), TLS for `grpcs://` as for `--server`.
fn endpoint(
    flume: &FlumeSettings,
    ca_pem: Option<&[u8]>,
) -> Result<tonic::transport::Endpoint, BoxError> {
    let (secure, authority) = session::resolve_server(&flume.url);
    let mut endpoint = tonic::transport::Endpoint::from_shared(format!(
        "{}://{authority}",
        if secure { "https" } else { "http" }
    ))?
    .connect_timeout(CONNECT_TIMEOUT)
    .http2_keep_alive_interval(Duration::from_secs(30))
    .keep_alive_timeout(Duration::from_secs(10))
    .keep_alive_while_idle(true);
    if secure {
        endpoint = tls::configure(endpoint, flume.insecure, ca_pem)?;
    }
    Ok(endpoint)
}

/// A flume double: the flume's side of each feed stream, over raw h2.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use tokio::sync::mpsc;

    use crate::proto::{
        AccessFilter, FeedControl, FeedMessage, FeedStart, FeedStop, feed_control, feed_message,
    };
    use crate::session::testing::{HANG, grpc_frame, recv_message};

    /// One feed stream as the flume sees it: the wicket's messages in, the
    /// flume's controls out.
    pub struct FeedStream {
        /// The `authorization` the wicket sent.
        pub authorization: Option<String>,
        body: h2::RecvStream,
        send: h2::SendStream<bytes::Bytes>,
        buf: Vec<u8>,
    }

    impl FeedStream {
        /// The wicket's next message, within the hang guard; panics once it
        /// ended or reset its stream.
        pub async fn next(&mut self) -> feed_message::Body {
            let message: Option<FeedMessage> =
                tokio::time::timeout(HANG, recv_message(&mut self.body, &mut self.buf))
                    .await
                    .expect("hang guard")
                    .expect("the feed stream is open");
            message
                .and_then(|message| message.body)
                .expect("a message of the feed")
        }

        /// Asks for the lines past `after` passing `filter`.
        pub fn start(&mut self, after: u64, filter: AccessFilter) {
            self.control(feed_control::Body::Start(FeedStart {
                after_node_seq: after,
                filter: Some(filter),
            }));
        }

        /// Asks for nothing until the next start.
        pub fn stop(&mut self) {
            self.control(feed_control::Body::Stop(FeedStop {}));
        }

        fn control(&mut self, body: feed_control::Body) {
            self.send
                .send_data(grpc_frame(&FeedControl { body: Some(body) }), false)
                .unwrap();
        }
    }

    /// A flume handing each feed stream to the test; its other calls are
    /// answered UNIMPLEMENTED.
    pub async fn flume_server() -> (String, mpsc::Receiver<FeedStream>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("grpc://{}", listener.local_addr().unwrap());
        let (streams, received) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let streams = streams.clone();
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    // accept() drives the connection on
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        let response =
                            http::Response::builder().header("content-type", "application/grpc");
                        if request.uri().path() != "/sluice.v1.Flume/Feed" {
                            let unimplemented = response.header("grpc-status", "12").body(());
                            let _ = respond.send_response(unimplemented.unwrap(), true);
                            continue;
                        }
                        let send = respond
                            .send_response(response.body(()).unwrap(), false)
                            .unwrap();
                        let authorization = request
                            .headers()
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .map(String::from);
                        let stream = FeedStream {
                            authorization,
                            body: request.into_body(),
                            send,
                            buf: Vec::new(),
                        };
                        if streams.send(stream).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (url, received)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::feed_message::Body;

    fn publish(tap: &Tap, kind: &'static str, host: &str) {
        tap.publish(kind, host.to_string(), vec![("line", String::new())]);
    }

    fn filter(route_keys: &[&str], kinds: &[&str]) -> Filter {
        Filter {
            route_keys: route_keys.iter().map(|key| key.to_string()).collect(),
            kinds: kinds.iter().map(|kind| kind.to_string()).collect(),
        }
    }

    /// The queue's items: the lines by their seq, a gap as its negated count.
    fn drain(tap: &Tap) -> Vec<i64> {
        std::iter::from_fn(|| tap.next())
            .map(|item| match item {
                Item::Line(line) => line.seq as i64,
                Item::Gap(dropped) => -(dropped as i64),
            })
            .collect()
    }

    /// The ring keeps the newest lines whether fed or not; a start backfills
    /// those past the flume's position passing its filter, and queues the
    /// passing lines published after it, each once. Past a full queue the
    /// lines are dropped and counted in a gap at their place, behind the
    /// lines queued before them and ahead of those after, as are the lines
    /// dropped before the tap that pass. A stop empties the queue; a tap
    /// without a flume keeps nothing.
    #[test]
    fn an_index_is_the_lowest_free_until_it_drops() {
        let indices = Arc::new(Indices::default());
        let (zero, one) = (indices.take(0), indices.take(0));
        assert_eq!((zero.index, one.index), (0, 1));
        assert_eq!(indices.take(2).index, 2, "from where asked");
        drop(zero);
        assert_eq!(indices.take(0).index, 0, "the one freed");
        assert_eq!(indices.take(1).index, 2, "past the one held");
    }

    #[test]
    fn a_start_backfills_past_the_flume_and_queues_what_passes_with_its_drops_in_place() {
        let tap = Tap::new();
        publish(&tap, "stdout", "a"); // 1
        publish(&tap, "stdout", "b"); // 2
        publish(&tap, "response", "a"); // 3
        tap.dropped("stdout", "a", 7);
        assert!(!tap.pending(), "nothing queued before a start");
        let backfill = tap.start(1, filter(&["a"], &[]));
        assert_eq!(
            backfill.iter().map(|line| line.seq).collect::<Vec<_>>(),
            [3]
        );
        publish(&tap, "stdout", "b"); // 4: not passing
        tap.dropped("stdout", "b", 3);
        publish(&tap, "stderr", "a"); // 5
        tap.dropped("stdout", "a", 2);
        tap.dropped("stderr", "a", 1);
        assert_eq!(drain(&tap), [5, -3]);

        // a new filter: queued from now on only
        let backfill = tap.start(5, filter(&[], &["stdout"]));
        assert!(backfill.is_empty(), "{backfill:?}");
        for _ in 0..QUEUE_CAPACITY + 2 {
            publish(&tap, "stdout", "c");
        }
        tap.next(); // the feed takes one: room for one more line, after the gap
        publish(&tap, "stdout", "c");
        let items = drain(&tap);
        assert_eq!(items.len(), QUEUE_CAPACITY + 1);
        assert_eq!(items[QUEUE_CAPACITY - 2], 5 + QUEUE_CAPACITY as i64);
        assert_eq!(items[QUEUE_CAPACITY - 1], -2, "the drops, in place");
        assert_eq!(items[QUEUE_CAPACITY], 8 + QUEUE_CAPACITY as i64);

        tap.stop();
        publish(&tap, "stdout", "c");
        assert!(!tap.pending(), "nothing queued after a stop");
        let recent = tap.recent();
        assert_eq!(recent.len(), RING_SIZE);
        assert_eq!(recent.last().unwrap().seq, 9 + QUEUE_CAPACITY as u64);

        let off = Tap::off();
        publish(&off, "stdout", "a");
        off.dropped("stdout", "a", 1);
        assert!(off.start(0, Filter::default()).is_empty() && !off.pending());
    }

    /// The feed says hello as its node, with the flume's token; the flume's
    /// start has it send the recent lines past its position passing its
    /// filter, then the lines and the drops that follow, in order.
    #[tokio::test]
    async fn the_feed_sends_what_the_flume_starts() {
        let (url, mut streams) = testing::flume_server().await;
        let tap = Tap::new();
        publish(&tap, "stdout", "a"); // 1
        publish(&tap, "stdout", "b"); // 2: not passing
        publish(&tap, "stderr", "a"); // 3
        let flume = FlumeSettings::of(Some(url), Some("f1ume".into()), None, false)
            .unwrap()
            .unwrap();
        let feeding = tokio::spawn(feed(flume, None, tap.clone(), "wicket:test".into()));
        let mut stream = tokio::time::timeout(crate::session::testing::HANG, streams.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream.authorization.as_deref(), Some("Bearer f1ume"));
        let Body::Hello(hello) = stream.next().await else {
            panic!("no hello first");
        };
        assert_eq!(hello.node_id, "wicket:test");

        stream.start(
            1,
            AccessFilter {
                route_keys: vec!["a".into()],
                ..Default::default()
            },
        );
        let mut received = Vec::new();
        received.push(stream.next().await);
        publish(&tap, "stdout", "a"); // 4
        tap.dropped("stdout", "b", 5);
        tap.dropped("stderr", "a", 2);
        publish(&tap, "stdout", "a"); // 5
        for _ in 0..3 {
            received.push(stream.next().await);
        }
        let received: Vec<i64> = received
            .into_iter()
            .map(|body| match body {
                Body::Event(event) => {
                    assert_eq!(event.route_key, "a");
                    event.node_seq as i64
                }
                Body::Gap(gap) => -(gap.dropped as i64),
                Body::Hello(_) => panic!("a second hello"),
            })
            .collect();
        assert_eq!(received, [3, 4, -2, 5]);
        feeding.abort();

        assert!(
            FlumeSettings::of(None, None, None, false)
                .unwrap()
                .is_none()
        );
        for (url, token, error) in [
            (
                "f:1",
                Some("t"),
                "expected grpc://host:port or grpcs://host:port",
            ),
            ("grpcs://f:1", None, "--flume-token (or `flume-token`"),
        ] {
            let failed = FlumeSettings::of(Some(url.into()), token.map(String::from), None, false)
                .unwrap_err();
            assert!(failed.contains(error), "{failed}");
        }
    }
}
