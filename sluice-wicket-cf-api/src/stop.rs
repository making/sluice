//! The cf-api's stop (SIGTERM, stdin's end): no new connection; each connection answers the
//! request under way and closes, an idle one at once; the requests' handlers and the
//! controller's puts under way finish, and no other put begins (`crate::serve` waits for them
//! within `--stop-grace`). Of several instances, the one reconciling the wickets lets go of the
//! lease first, once the edits it makes ended or `crate::lease::EDITS_END` passed: the next
//! takes over while the requests are answered, its turn's puts cut as its turn ends, an edit
//! still under way sent to the next (`crate::api`).

use std::future::Future;

use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

tokio::task_local! {
    /// Set in a request's handler: its puts begin during a stop too.
    static REQUEST: ();
}

/// What a stop waits for, and whether it began.
#[derive(Clone, Default)]
pub struct Stop {
    began: CancellationToken,
    connections: TaskTracker,
    requests: TaskTracker,
    edits: TaskTracker,
    puts: TaskTracker,
}

impl Stop {
    /// The stop begins: [`Stop::stopped`] is ready, a put not of a request never begins.
    pub fn begin(&self) {
        self.began.cancel();
        self.connections.close();
        self.requests.close();
        self.edits.close();
        self.puts.close();
    }

    pub fn stopping(&self) -> bool {
        self.began.is_cancelled()
    }

    /// Ready once the stop began.
    pub fn stopped(&self) -> WaitForCancellationFutureOwned {
        self.began.clone().cancelled_owned()
    }

    /// `serving` as a connection's: the stop waits for it to end.
    pub fn connection<F: Future>(&self, serving: F) -> impl Future<Output = F::Output> + use<F> {
        self.connections.track_future(serving)
    }

    /// `handling` as a request's: the stop waits for it, and its puts begin during the stop.
    /// A task it spawns is not of it.
    pub fn request<F: Future>(&self, handling: F) -> impl Future<Output = F::Output> + use<F> {
        self.requests.track_future(REQUEST.scope((), handling))
    }

    /// `making` as an edit made in this instance's turn: the lease is let go of after it
    /// ([`Stop::edited`]).
    pub fn edit<F: Future>(&self, making: F) -> impl Future<Output = F::Output> + use<F> {
        self.edits.track_future(making)
    }

    /// Ready once the stop began and the edits under way in this instance's turn ended.
    pub async fn edited(&self) {
        self.began.cancelled().await;
        self.edits.wait().await;
    }

    /// A put to a wicket begins: the stop waits for it while the token is held. Once the stop
    /// began, one not of a request never does, as if the process ended there: what it was for
    /// is taken up at the next start.
    pub async fn put(&self) -> TaskTrackerToken {
        if self.stopping() && REQUEST.try_with(|()| ()).is_err() {
            std::future::pending::<()>().await;
        }
        self.puts.token()
    }

    /// The connections, the requests and the puts under way.
    pub fn under_way(&self) -> (usize, usize, usize) {
        (self.connections.len(), self.requests.len(), self.puts.len())
    }

    /// Ready once the stop began and the connections, the requests and then the puts under way
    /// ended: a request's puts begun before it ended.
    pub async fn drained(&self) {
        self.began.cancelled().await;
        self.connections.wait().await;
        self.requests.wait().await;
        self.puts.wait().await;
    }
}
