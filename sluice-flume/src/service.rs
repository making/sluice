//! The gRPC face of the hub: `Feed` for the nodes, `Watch` for the consoles.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::hub::{self, Hub, Item};
use crate::proto::flume_server::Flume;
use crate::proto::{FeedControl, FeedMessage, WatchEvent, WatchRequest, feed_message, watch_event};

/// Events a watcher may have in flight before its task waits for it.
const WATCH_BUFFER: usize = 64;

pub struct FlumeService {
    hub: Arc<Hub>,
}

impl FlumeService {
    pub fn new(hub: Arc<Hub>) -> Self {
        Self { hub }
    }
}

type ControlStream = Pin<Box<dyn Stream<Item = Result<FeedControl, Status>> + Send>>;

#[tonic::async_trait]
impl Flume for FlumeService {
    type FeedStream = ControlStream;

    async fn feed(
        &self,
        request: Request<Streaming<FeedMessage>>,
    ) -> Result<Response<Self::FeedStream>, Status> {
        let mut incoming = request.into_inner();
        let hello = match incoming.message().await? {
            Some(FeedMessage {
                body: Some(feed_message::Body::Hello(hello)),
            }) if !hello.node_id.is_empty() => hello,
            _ => {
                return Err(Status::invalid_argument(
                    "a feed begins with a hello naming its node",
                ));
            }
        };
        let (feed_id, control) = self.hub.connect(&hello.node_id, &hello.boot_id);
        let hub = self.hub.clone();
        tokio::spawn(async move {
            // ends with the node's stream, or when the node sends garbage
            while let Ok(Some(message)) = incoming.message().await {
                match message.body {
                    Some(feed_message::Body::Event(event)) => hub.publish(&hello.node_id, event),
                    Some(feed_message::Body::Gap(gap)) if gap.dropped > 0 => {
                        hub.dropped(&hello.node_id, gap.dropped)
                    }
                    _ => {}
                }
            }
            hub.disconnect(&hello.node_id, feed_id);
        });
        let controls = WatchStream::new(control).filter_map(|control| control.map(Ok));
        Ok(Response::new(Box::pin(controls)))
    }

    type WatchStream = ReceiverStream<Result<WatchEvent, Status>>;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let request = request.into_inner();
        let watch = self
            .hub
            .watch(request.filter.unwrap_or_default(), request.after);
        let (sender, receiver) = mpsc::channel(WATCH_BUFFER);
        tokio::spawn(relay(self.hub.clone(), watch, sender));
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

/// Sends the watch's first events, then the live ones passing its filter,
/// until the watcher goes away; a watcher that fell behind catches up from the
/// ring.
async fn relay(hub: Arc<Hub>, watch: hub::Watch, sender: mpsc::Sender<Result<WatchEvent, Status>>) {
    let hub::Watch {
        first,
        mut live,
        mut seq,
        filter,
        guard,
    } = watch;
    let mut pending = first;
    loop {
        for event in pending.drain(..) {
            if sender.send(Ok(event)).await.is_err() {
                drop(guard);
                return;
            }
        }
        let received = tokio::select! {
            received = live.recv() => received,
            // gone while nothing flows
            () = sender.closed() => break,
        };
        match received {
            Ok(Item::Line(line)) => {
                if line.seq <= seq {
                    continue;
                }
                seq = line.seq;
                if hub::passes(&filter, &line) {
                    pending.push(hub::record(&line));
                }
            }
            Ok(Item::Node(state)) => pending.push(WatchEvent {
                body: Some(watch_event::Body::Node(state)),
            }),
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let (events, resubscribed, reached) = hub.catch_up(&filter, seq);
                pending = events;
                live = resubscribed;
                seq = reached;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    drop(guard);
}
