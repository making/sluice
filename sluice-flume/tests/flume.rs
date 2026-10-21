//! The flume over gRPC, as the nodes and the consoles reach it.

use std::time::Duration;

use sluice_flume::Settings;
use sluice_flume::proto::flume_client::FlumeClient;
use sluice_flume::proto::{
    AccessEvent, AccessFilter, Cursor, FeedControl, FeedGap, FeedHello, FeedMessage, FeedStart,
    NodeState, WatchEvent, WatchRequest, feed_control, feed_message, watch_event, watch_gap,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};

/// Hang guard of every wait.
const HANG: Duration = Duration::from_secs(10);

const TOKEN: &str = "flume-test-token";

type Client = FlumeClient<
    InterceptedService<Channel, Box<dyn FnMut(Request<()>) -> Result<Request<()>, Status> + Send>>,
>;

async fn flume(ring_size: usize, backfill: usize) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let settings = Settings {
        token: TOKEN.to_string(),
        ring_size,
        backfill,
        tls: None,
    };
    tokio::spawn(sluice_flume::serve(
        listener,
        settings,
        std::future::pending(),
    ));
    url
}

async fn client(url: &str, token: &str) -> Client {
    let channel = Channel::from_shared(url.to_string())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let header: tonic::metadata::MetadataValue<_> = format!("Bearer {token}").parse().unwrap();
    FlumeClient::with_interceptor(
        channel,
        Box::new(move |mut request: Request<()>| {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
            Ok(request)
        }),
    )
}

/// A node's feed: what it sends, and the controls the flume answers with.
struct Node {
    send: mpsc::Sender<FeedMessage>,
    controls: Streaming<FeedControl>,
}

impl Node {
    async fn connect(client: &mut Client, node_id: &str, boot_id: &str) -> Node {
        let (send, receive) = mpsc::channel(16);
        send.send(FeedMessage {
            body: Some(feed_message::Body::Hello(FeedHello {
                node_id: node_id.to_string(),
                boot_id: boot_id.to_string(),
            })),
        })
        .await
        .unwrap();
        let controls = client
            .feed(ReceiverStream::new(receive))
            .await
            .unwrap()
            .into_inner();
        Node { send, controls }
    }

    async fn event(&self, node_seq: u64, kind: &str, route_key: &str) {
        self.send
            .send(FeedMessage {
                body: Some(feed_message::Body::Event(access(node_seq, kind, route_key))),
            })
            .await
            .unwrap();
    }

    async fn control(&mut self) -> feed_control::Body {
        tokio::time::timeout(HANG, self.controls.message())
            .await
            .expect("a control in time")
            .unwrap()
            .expect("the control stream open")
            .body
            .unwrap()
    }
}

fn access(node_seq: u64, kind: &str, route_key: &str) -> AccessEvent {
    AccessEvent {
        node_seq,
        time_unix_millis: 1_700_000_000_000,
        kind: kind.to_string(),
        route_key: route_key.to_string(),
        fields: vec![],
    }
}

fn start(after_node_seq: u64, route_keys: &[&str], kinds: &[&str]) -> feed_control::Body {
    feed_control::Body::Start(FeedStart {
        after_node_seq,
        filter: Some(filter(route_keys, kinds, &[])),
    })
}

fn filter(route_keys: &[&str], kinds: &[&str], node_ids: &[&str]) -> AccessFilter {
    let owned = |values: &[&str]| values.iter().map(|value| value.to_string()).collect();
    AccessFilter {
        route_keys: owned(route_keys),
        kinds: owned(kinds),
        node_ids: owned(node_ids),
    }
}

async fn watch(
    client: &mut Client,
    filter: AccessFilter,
    after: Option<Cursor>,
) -> Streaming<WatchEvent> {
    client
        .watch(WatchRequest {
            filter: Some(filter),
            after,
        })
        .await
        .unwrap()
        .into_inner()
}

async fn next(events: &mut Streaming<WatchEvent>) -> watch_event::Body {
    tokio::time::timeout(HANG, events.message())
        .await
        .expect("an event in time")
        .unwrap()
        .expect("the watch open")
        .body
        .unwrap()
}

/// A watch event in a few words: `hello <epoch> reset=<bool>`, `node <id> up|down`,
/// `<seq> <node> <kind> <route> #<node seq>`, `gap <seq> <node> <dropped> <cause>`.
fn shape(body: &watch_event::Body) -> String {
    match body {
        watch_event::Body::Hello(hello) => format!("hello reset={}", hello.reset),
        watch_event::Body::Node(NodeState { node_id, connected }) => {
            format!("node {node_id} {}", if *connected { "up" } else { "down" })
        }
        watch_event::Body::Record(record) => {
            let event = record.event.as_ref().unwrap();
            format!(
                "{} {} {} {} #{}",
                record.seq, record.node_id, event.kind, event.route_key, event.node_seq
            )
        }
        watch_event::Body::Gap(gap) => format!(
            "gap {} {} {} {:?}",
            gap.seq,
            gap.node_id,
            gap.dropped,
            watch_gap::Cause::try_from(gap.cause).unwrap()
        ),
    }
}

async fn shapes(events: &mut Streaming<WatchEvent>, count: usize) -> Vec<String> {
    let mut shapes = Vec::new();
    for _ in 0..count {
        shapes.push(shape(&next(events).await));
    }
    shapes
}

fn epoch_of(body: &watch_event::Body) -> u64 {
    match body {
        watch_event::Body::Hello(hello) => hello.epoch,
        other => panic!("not a hello: {other:?}"),
    }
}

/// The nodes feed only while someone watches them, the union of the watchers'
/// filters; the watchers get the lines passing their own filter, a node's
/// dropped lines, and the nodes coming and going.
#[tokio::test]
async fn watchers_steer_the_feeds_and_get_the_lines_passing_their_filter() {
    let url = flume(100, 10).await;
    let mut client = client(&url, TOKEN).await;
    let mut node = Node::connect(&mut client, "node-1", "boot-a").await;

    let mut all = watch(&mut client, AccessFilter::default(), None).await;
    assert_eq!(shapes(&mut all, 2).await[1..], ["node node-1 up"]);
    assert_eq!(node.control().await, start(0, &[], &[]));

    let mut requests = watch(
        &mut client,
        filter(&["a.local"], &["request"], &["node-1"]),
        None,
    )
    .await;
    assert_eq!(shapes(&mut requests, 2).await[1..], ["node node-1 up"]);
    node.event(1, "accept", "").await;
    node.event(2, "request", "a.local").await;
    node.event(3, "request", "b.local").await;
    node.send
        .send(FeedMessage {
            body: Some(feed_message::Body::Gap(FeedGap { dropped: 5 })),
        })
        .await
        .unwrap();
    assert_eq!(
        shapes(&mut all, 4).await,
        [
            "1 node-1 accept  #1",
            "2 node-1 request a.local #2",
            "3 node-1 request b.local #3",
            "gap 4 node-1 5 NodeDropped"
        ]
    );
    assert_eq!(
        shapes(&mut requests, 2).await,
        ["2 node-1 request a.local #2", "gap 4 node-1 5 NodeDropped"]
    );

    // the unfiltered watcher leaves: the node feeds what the other one passes
    drop(all);
    assert_eq!(node.control().await, start(3, &["a.local"], &["request"]));
    drop(requests);
    assert_eq!(
        node.control().await,
        feed_control::Body::Stop(Default::default())
    );

    // the same run comes back: it resumes after the lines the flume has
    let mut other = watch(&mut client, filter(&[], &[], &["node-2"]), None).await;
    drop(node);
    assert_eq!(
        shapes(&mut other, 3).await[1..],
        ["node node-1 up", "node node-1 down"]
    );
    let mut later = watch(&mut client, filter(&[], &["close"], &[]), None).await;
    let snapshot = shapes(&mut later, 3).await;
    assert_eq!(
        snapshot[1..],
        ["node node-1 down", "gap 4 node-1 5 NodeDropped"]
    );
    let mut node = Node::connect(&mut client, "node-1", "boot-a").await;
    assert_eq!(node.control().await, start(3, &[], &["close"]));
    assert_eq!(shape(&next(&mut later).await), "node node-1 up");
    // a new run starts over
    let mut node = Node::connect(&mut client, "node-1", "boot-b").await;
    assert_eq!(node.control().await, start(0, &[], &["close"]));
}

/// A watch with a cursor resumes right after it, past a gap for the lines the
/// ring lost meanwhile; one without, or with a cursor of another run, begins
/// with the newest lines.
#[tokio::test]
async fn a_watch_resumes_after_its_cursor() {
    let url = flume(4, 2).await;
    let mut client = client(&url, TOKEN).await;
    let node = Node::connect(&mut client, "node-1", "boot").await;
    let mut first = watch(&mut client, AccessFilter::default(), None).await;
    let epoch = epoch_of(&next(&mut first).await);
    for node_seq in 1..=6 {
        node.event(node_seq, "request", "a.local").await;
    }
    // seen live: the flume holds all six
    assert_eq!(
        shapes(&mut first, 7).await[6],
        "6 node-1 request a.local #6"
    );

    let cursor = |epoch, seq| Some(Cursor { epoch, seq });
    let mut fresh = watch(&mut client, AccessFilter::default(), None).await;
    assert_eq!(
        shapes(&mut fresh, 4).await[2..],
        ["5 node-1 request a.local #5", "6 node-1 request a.local #6"]
    );
    let mut resumed = watch(&mut client, AccessFilter::default(), cursor(epoch, 3)).await;
    assert_eq!(
        shapes(&mut resumed, 5).await,
        [
            "hello reset=false",
            "node node-1 up",
            "4 node-1 request a.local #4",
            "5 node-1 request a.local #5",
            "6 node-1 request a.local #6"
        ]
    );
    let mut late = watch(&mut client, AccessFilter::default(), cursor(epoch, 1)).await;
    assert_eq!(
        shapes(&mut late, 4).await[2..],
        ["gap 0  1 Behind", "3 node-1 request a.local #3"]
    );
    let mut other_run = watch(&mut client, AccessFilter::default(), cursor(epoch + 1, 5)).await;
    assert_eq!(
        shapes(&mut other_run, 4).await,
        [
            "hello reset=true",
            "node node-1 up",
            "5 node-1 request a.local #5",
            "6 node-1 request a.local #6"
        ]
    );
}

/// A watcher reading slower than the lines come catches up from the ring, and
/// is told how many it missed once the ring moved past them: each line reaches
/// it or is counted.
#[tokio::test]
async fn a_slow_watcher_catches_up_or_learns_the_gap() {
    const LINES: u64 = 5000;
    let url = flume(16, 0).await;
    let mut client = client(&url, TOKEN).await;
    let node = Node::connect(&mut client, "node-1", "boot").await;
    let mut slow = watch(&mut client, AccessFilter::default(), None).await;
    assert_eq!(shapes(&mut slow, 2).await[1], "node node-1 up");
    let mut fast = watch(&mut client, AccessFilter::default(), None).await;
    assert_eq!(shapes(&mut fast, 2).await[1], "node node-1 up");
    let feeding = tokio::spawn(async move {
        for node_seq in 1..=LINES {
            node.event(node_seq, "request", "a.local").await;
        }
        node
    });
    let fast_tally = tally(&mut fast, LINES).await;
    let _node = feeding.await.unwrap();
    let slow_tally = tally(&mut slow, LINES).await;
    assert!(
        slow_tally.1 > 0,
        "the slow watcher missed none: {slow_tally:?}"
    );
    assert_eq!(slow_tally.0 + slow_tally.1, LINES);
    assert_eq!(fast_tally.0 + fast_tally.1, LINES);
}

/// Reads until the line `last`: the lines received and the lines missed.
async fn tally(events: &mut Streaming<WatchEvent>, last: u64) -> (u64, u64) {
    let (mut received, mut missed) = (0, 0);
    loop {
        match next(events).await {
            watch_event::Body::Record(record) => {
                received += 1;
                if record.seq == last {
                    return (received, missed);
                }
            }
            watch_event::Body::Gap(gap) => missed += gap.dropped,
            _ => {}
        }
    }
}

#[tokio::test]
async fn calls_without_the_token_are_refused_but_the_health_check() {
    let url = flume(16, 0).await;
    let mut wrong = client(&url, "wrong").await;
    let refused = wrong
        .watch(WatchRequest::default())
        .await
        .expect_err("a watch with the wrong token");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
    let (_send, receive) = mpsc::channel::<FeedMessage>(1);
    let refused = wrong
        .feed(ReceiverStream::new(receive))
        .await
        .expect_err("a feed with the wrong token");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);

    let channel = Channel::from_shared(url).unwrap().connect().await.unwrap();
    let health = tonic_health::pb::health_client::HealthClient::new(channel)
        .check(tonic_health::pb::HealthCheckRequest {
            service: "sluice.v1.Flume".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        health.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
}
