//! The shutdown's drain (`TunnelClient#drain` of the Java client): every node
//! stream withdraws the routes and ends once the server acknowledged it and no
//! connection is left on it, within the grace.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::session::Shared;

/// The shutdown's drain ([`drain`]).
#[derive(Default)]
pub struct Drain {
    /// Cancelled once the drain began: every live stream withdraws the
    /// routes, no stream is opened or reconnected.
    pub begin: CancellationToken,
    /// Cancelled once the drain is over (the grace ran out, a second signal):
    /// every stream ends at once, failing its connections.
    pub abort: CancellationToken,
}

/// How a drain ended.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainEnd {
    /// Every stream ended: drained, closed for a server predating the drain,
    /// or failed.
    Drained,
    /// The grace ran out first.
    GraceExpired,
    /// `interrupt` came first.
    Interrupted,
}

/// Drains the sessions (`TunnelClient#drain`): every live stream withdraws
/// the routes and keeps serving its connections until the server
/// acknowledged the withdrawal and none is left, then ends; a session not
/// connected just stops. After `grace`, or once `interrupt` resolves, the
/// streams left end at once, failing their connections. Returns once every
/// session task ended (at most a few seconds after that).
pub async fn drain(
    shared: &Shared,
    grace: Duration,
    interrupt: impl Future<Output = ()>,
) -> DrainEnd {
    shared.drain.begin.cancel();
    shared.tasks.close();
    println!(
        "draining: withdrawing the routes; waiting up to {}s for {} open connection(s)",
        grace.as_secs(),
        shared.limit.open()
    );
    let end = tokio::select! {
        () = shared.tasks.wait() => DrainEnd::Drained,
        () = tokio::time::sleep(grace) => DrainEnd::GraceExpired,
        () = interrupt => DrainEnd::Interrupted,
    };
    let open = shared.limit.open();
    match end {
        DrainEnd::Drained => println!("drain ended with {open} open connection(s)"),
        DrainEnd::GraceExpired => eprintln!(
            "drain grace {}s expired; closing {open} open connection(s)",
            grace.as_secs()
        ),
        DrainEnd::Interrupted => eprintln!("drain aborted; closing {open} open connection(s)"),
    }
    shared.drain.abort.cancel();
    // the streams end on the abort without waiting on the server
    if tokio::time::timeout(Duration::from_secs(5), shared.tasks.wait())
        .await
        .is_err()
    {
        eprintln!("drain: a session did not end within 5s of the abort");
    }
    end
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::proto::{self, frame::Body};
    use crate::session::testing::*;
    use crate::tunnel;

    fn start_drain(shared: &Arc<Shared>, grace: Duration) -> tokio::task::JoinHandle<DrainEnd> {
        let shared = shared.clone();
        tokio::spawn(async move { drain(&shared, grace, std::future::pending()).await })
    }

    /// Waits until no connection holds a slot of `limit`, at most [`HANG`].
    async fn all_closed(limit: &tunnel::Limit) -> bool {
        let start = tokio::time::Instant::now();
        while limit.open() != 0 && start.elapsed() < HANG {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        limit.open() == 0
    }

    /// Against a server supporting the drain: the routes are withdrawn on
    /// the stream, the connection open keeps being served after the server
    /// acknowledged the withdrawal, and the stream is ended once it closed;
    /// the server's end of the stream ends the drain.
    #[tokio::test]
    async fn a_drain_ends_the_stream_after_the_withdrawn_ack_once_the_last_connection_closed() {
        let (shared, limit, target) = hello_shared(Duration::ZERO).await;
        let (mut stream, _events) = served_session(&shared).await;
        assert!(!stream.advertise().await.withdraw);
        stream.ack(false);
        stream.connect(1, &target);
        assert_eq!(stream.exchange(1, "/before").await, "HTTP/1.1 200 OK");

        let drained = start_drain(&shared, HANG * 6);
        let withdrawal = stream.advertise().await;
        assert!(withdrawal.withdraw);
        assert_eq!(
            withdrawal.upstreams,
            shared.routes.current().advertised,
            "the routes repeated"
        );
        assert_eq!(withdrawal.connection_window, tunnel::WINDOW);
        assert_eq!(limit.open(), 1, "precondition: the connection is open");
        stream.ack(true);
        assert_eq!(
            stream.exchange(1, "/after").await,
            "HTTP/1.1 200 OK",
            "served after the withdrawn ack"
        );
        stream.send(Body::Close(proto::Close { conn_id: 1 }));
        stream
            .next(|body| matches!(body, Body::Close(close) if close.conn_id == 1).then_some(()))
            .await;
        assert!(
            stream.ended().await,
            "the request stream is ended, not reset"
        );
        stream.end();

        let end = tokio::time::timeout(HANG * 7, drained)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(end, DrainEnd::Drained);
        assert_eq!(limit.open(), 0);
    }

    /// A connection still open when the grace runs out is closed with its
    /// stream.
    #[tokio::test]
    async fn the_drain_grace_closes_the_connections_still_open() {
        let (shared, limit, target) = hello_shared(Duration::ZERO).await;
        let (mut stream, _events) = served_session(&shared).await;
        stream.advertise().await;
        stream.ack(false);
        stream.connect(1, &target);
        assert_eq!(stream.exchange(1, "/before").await, "HTTP/1.1 200 OK");

        let drained = start_drain(&shared, Duration::from_secs(1));
        assert!(stream.advertise().await.withdraw);
        stream.ack(true);
        assert_eq!(limit.open(), 1, "precondition: the connection is open");

        let end = tokio::time::timeout(HANG, drained).await.unwrap().unwrap();
        assert_eq!(end, DrainEnd::GraceExpired);
        assert!(!stream.ended().await, "the stream is reset");
        assert!(all_closed(&limit).await, "the connection is closed");
    }

    /// Against a server predating the drain: its ack to the opening
    /// advertise, arriving after the withdrawal was sent, ends nothing (the
    /// acks are counted); its ack to the withdrawing advertise, without
    /// `withdrawn`, ends the stream and with it the drain at once.
    #[tokio::test]
    async fn a_plain_ack_to_the_withdrawal_ends_the_stream_at_once() {
        let (shared, limit, target) = hello_shared(Duration::ZERO).await;
        let (mut stream, _events) = served_session(&shared).await;
        assert!(!stream.advertise().await.withdraw);
        // a connection served before any ack: the stream is up
        stream.connect(1, &target);
        assert_eq!(stream.exchange(1, "/before").await, "HTTP/1.1 200 OK");

        let drained = start_drain(&shared, HANG * 6);
        assert!(stream.advertise().await.withdraw);
        stream.ack(false);
        assert_eq!(
            stream.exchange(1, "/between").await,
            "HTTP/1.1 200 OK",
            "the ack to the opening advertise ended nothing"
        );
        stream.ack(false);

        let end = tokio::time::timeout(HANG, drained).await.unwrap().unwrap();
        assert_eq!(end, DrainEnd::Drained);
        assert!(!stream.ended().await, "the stream is reset");
        assert!(all_closed(&limit).await, "the connection is closed");
    }
}
