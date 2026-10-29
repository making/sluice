//! The wicket's live access log: its guests' output lines (`stdout`,
//! `stderr`) and the requests it answered (`response`), fed to a sluice-flume
//! (`--flume`) while someone watches, as a sluice node feeds its access log
//! lines (`FlumeFeed` / `LiveLogTap` of the server).
//!
//! ```text
//! guest output, answered request --> Tap --Feed (the wicket dials out)--> sluice-flume <--Watch-- console, cf-api
//! ```
//!
//! Every line goes to a ring of the recent ones ([`RING_SIZE`]), the backfill
//! once the flume starts the feed; while it feeds, the lines passing its
//! filter also go to a bounded queue ([`QUEUE_CAPACITY`]) the feed drains.
//! Nothing waits for the flume: a line past a full queue is dropped and
//! counted, and the count reaches the flume as a gap at its place. The feed
//! stream is held open while the wicket runs and redialed with a backoff of
//! 1s doubling to 30s whenever it ends; a flume down costs a warning per 5
//! minutes.

use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::config::FlumeSettings;
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

/// One line of the live access log.
#[derive(Debug)]
pub struct Line {
    /// Increases by one per line of this run.
    pub seq: u64,
    /// When it was logged, in milliseconds since the epoch.
    pub time_unix_millis: i64,
    /// `stdout`, `stderr` or `response`.
    pub kind: &'static str,
    /// The host of the request it is about, lowercase, without the port.
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

    fn passes(&self, line: &Line) -> bool {
        (self.route_keys.is_empty() || self.route_keys.contains(&line.route_key))
            && (self.kinds.is_empty() || self.kinds.contains(line.kind))
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
        if !state
            .feeding
            .as_ref()
            .is_some_and(|filter| filter.passes(&line))
        {
            return;
        }
        if state.lines < QUEUE_CAPACITY {
            state.queue.push_back(Item::Line(line));
            state.lines += 1;
        } else if let Some(Item::Gap(dropped)) = state.queue.back_mut() {
            *dropped += 1;
        } else {
            // past the capacity by this one gap: the lines dropped next count in it
            state.queue.push_back(Item::Gap(1));
        }
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
            .filter(|line| line.seq > after && filter.passes(line))
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
    #[cfg(test)]
    pub(crate) fn recent(&self) -> Vec<Arc<Line>> {
        self.state()
            .map(|state| state.ring.iter().cloned().collect())
            .unwrap_or_default()
    }
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
    let mut request = tonic::Request::new(ReceiverStream::new(outgoing));
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

#[cfg(test)]
mod tests {
    use super::*;

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
    /// lines queued before them and ahead of those after. A stop empties the
    /// queue; a tap without a flume keeps nothing.
    #[test]
    fn a_start_backfills_past_the_flume_and_queues_what_passes_with_its_drops_in_place() {
        let tap = Tap::new();
        publish(&tap, "stdout", "a"); // 1
        publish(&tap, "stdout", "b"); // 2
        publish(&tap, "response", "a"); // 3
        assert!(!tap.pending(), "nothing queued before a start");
        let backfill = tap.start(1, filter(&["a"], &[]));
        assert_eq!(
            backfill.iter().map(|line| line.seq).collect::<Vec<_>>(),
            [3]
        );
        publish(&tap, "stdout", "b"); // 4: not passing
        publish(&tap, "stderr", "a"); // 5
        assert_eq!(drain(&tap), [5]);

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
        assert!(off.start(0, Filter::default()).is_empty() && !off.pending());
    }
}
