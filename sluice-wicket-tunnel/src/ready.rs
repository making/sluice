//! A wicket's readiness, as its probe asks it (`--health`): whether it serves. Not while its routes
//! start; then, through a server, once every node of the membership took its routes and none is
//! held for a controller ([`Shared::readiness`]), not from the drain on; in local mode alone,
//! once none is held ([`Routes::readiness`]). For a bound from the start of the routes (the grace)
//! it waits as well for each route to have had an instance ready: a route's readiness after that
//! is its app's, not the wicket's (a server sends its host to another wicket while it has none,
//! see [`crate::routes`]); one without such an instance at the bound is logged.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::time::Instant;

use crate::routes::{Routes, Table};
use crate::session::Shared;

/// What tells a wicket's readiness, set once its routes started.
pub struct Readiness<T> {
    /// How long from the start of the routes it waits for each to have had an instance ready.
    grace: Duration,
    /// The routes and when they started.
    serving: OnceLock<(Serving<T>, Instant)>,
}

enum Serving<T> {
    /// Local mode alone.
    Local(Arc<Routes<T>>),
    /// Through a server, on `--listen` too or not.
    Tunnel(Arc<Shared<T>>),
}

impl<T: Table> Readiness<T> {
    /// Not ready: the routes are starting. Once they started, waits `grace` at most for each to
    /// have had an instance ready.
    pub fn starting(grace: Duration) -> Arc<Readiness<T>> {
        Arc::new(Readiness {
            grace,
            serving: OnceLock::new(),
        })
    }

    /// The routes started, served on the listener alone (bound by now).
    pub fn local(&self, routes: Arc<Routes<T>>) {
        self.started(Serving::Local(routes));
    }

    /// The routes started, served through the server by the sessions of `shared`.
    pub fn tunnel(&self, shared: Arc<Shared<T>>) {
        self.started(Serving::Tunnel(shared));
    }

    /// Takes the routes started; logs those with no instance ready at the grace's end.
    fn started(&self, serving: Serving<T>) {
        let routes = match &serving {
            Serving::Local(routes) => routes.clone(),
            Serving::Tunnel(shared) => shared.routes.clone(),
        };
        if self.serving.set((serving, Instant::now())).is_err() || self.grace.is_zero() {
            return;
        }
        let grace = self.grace;
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let never = routes.never_ready();
            if !never.is_empty() {
                eprintln!(
                    "ready without an instance ready of {} ({}s from the start): \
                     not waited for any longer",
                    never.join(", "),
                    grace.as_secs()
                );
            }
        });
    }

    /// Whether the wicket serves; Ok names the routes with no instance ready, if any; Err says why
    /// not.
    pub fn check(&self) -> Result<Option<String>, String> {
        let Some((serving, since)) = self.serving.get() else {
            return Err("starting the routes".into());
        };
        let waiting = since.elapsed() < self.grace;
        match serving {
            Serving::Local(routes) => routes.readiness(waiting),
            Serving::Tunnel(shared) => shared.readiness(waiting),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::routes::testing::Fixed;
    use crate::session::testing::*;

    /// Waits until what `readiness` says passes `said`, at most [`HANG`]; what it says then.
    async fn until(
        readiness: &Readiness<Fixed>,
        said: impl Fn(&Result<Option<String>, String>) -> bool,
    ) -> Result<Option<String>, String> {
        let start = tokio::time::Instant::now();
        while !said(&readiness.check()) && start.elapsed() < HANG {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        readiness.check()
    }

    /// Through a server: not ready while starting, nor before the server took the routes; ready
    /// after, not while a route is held, has had no instance ready yet within the grace, or a
    /// reload is not taken yet, nor from the drain on; ready with a route that had one and has
    /// none now, which it names. In local mode alone, ready as the routes are, and past the
    /// grace whether a route had an instance ready or not.
    #[tokio::test]
    async fn ready_once_the_server_took_the_routes_and_not_from_the_drain_on() {
        let (shared, _, _) = hello_shared(Duration::ZERO);
        let readiness = Readiness::starting(Duration::from_secs(3600));
        assert_eq!(readiness.check(), Err("starting the routes".into()));
        readiness.tunnel(shared.clone());
        assert_eq!(readiness.check(), Err("no server node known yet".into()));

        let (mut stream, _events) = served_session(&shared).await;
        stream.advertise().await;
        let behind = readiness.check().unwrap_err();
        assert!(
            behind.starts_with("no stream up that took the routes of generation 1 to grpc://"),
            "{behind}"
        );
        stream.ack(false);
        assert_eq!(until(&readiness, Result::is_ok).await, Ok(None));

        // ready since the start: the app's readiness from then on
        let unready = shared.routes.current().unready.clone();
        unready.lock().unwrap().push("hello.local".into());
        assert_eq!(
            readiness.check(),
            Ok(Some("no instance ready for hello.local".into()))
        );
        let held = shared.routes.current().held.clone();
        held.lock().unwrap().push("hello.local".into());
        assert_eq!(
            readiness.check(),
            Err("held until their owners confirm them: hello.local".into())
        );
        held.lock().unwrap().clear();
        let reloaded = Fixed::hosts(2, &["hello.local", "b.local", "c.local"]);
        reloaded.unready.lock().unwrap().push("b.local".into());
        let b = reloaded.unready.clone();
        shared.routes.publish(reloaded);
        stream.advertise().await;
        let behind = readiness.check().unwrap_err();
        assert!(
            behind.starts_with("no stream up that took the routes of generation 2"),
            "{behind}"
        );
        stream.ack(false);
        assert_eq!(
            until(&readiness, |said| *said != Err(behind.clone())).await,
            Err("no instance ready yet for b.local".into())
        );
        b.lock().unwrap().clear();
        assert_eq!(readiness.check(), Ok(None));

        let drain = shared.clone();
        tokio::spawn(
            async move { crate::drain::drain(&drain, HANG, std::future::pending()).await },
        );
        assert_eq!(
            until(&readiness, Result::is_err).await,
            Err("draining".into())
        );

        tokio::time::pause();
        let grace = Duration::from_secs(60);
        let local = Readiness::starting(grace);
        let routes = Fixed::hosts(1, &["a.local", "b.local"]);
        routes.unready.lock().unwrap().push("a.local".into());
        local.local(Routes::new(routes));
        assert_eq!(
            local.check(),
            Err("no instance ready yet for a.local".into())
        );
        tokio::time::advance(grace).await;
        assert_eq!(
            local.check(),
            Ok(Some("no instance ready for a.local".into()))
        );
    }
}
