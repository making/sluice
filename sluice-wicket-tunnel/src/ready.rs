//! A wicket's readiness, as its probe asks it (`--health`): whether it serves. Not while its routes
//! start; then, through a server, once every node of the membership took its routes and every
//! route has an instance ready ([`Shared::readiness`]), not from the drain on; in local mode
//! alone, once every route has an instance ready ([`Routes::readiness`]).

use std::sync::{Arc, OnceLock};

use crate::routes::{Routes, Table};
use crate::session::Shared;

/// What tells a wicket's readiness, set once its routes started.
pub struct Readiness<T> {
    serving: OnceLock<Serving<T>>,
}

enum Serving<T> {
    /// Local mode alone.
    Local(Arc<Routes<T>>),
    /// Through a server, on `--listen` too or not.
    Tunnel(Arc<Shared<T>>),
}

impl<T: Table> Readiness<T> {
    /// Not ready: the routes are starting.
    pub fn starting() -> Arc<Readiness<T>> {
        Arc::new(Readiness {
            serving: OnceLock::new(),
        })
    }

    /// The routes started, served on the listener alone (bound by now).
    pub fn local(&self, routes: Arc<Routes<T>>) {
        let _ = self.serving.set(Serving::Local(routes));
    }

    /// The routes started, served through the server by the sessions of `shared`.
    pub fn tunnel(&self, shared: Arc<Shared<T>>) {
        let _ = self.serving.set(Serving::Tunnel(shared));
    }

    /// Whether the wicket serves; Err says why not.
    pub fn check(&self) -> Result<(), String> {
        match self.serving.get() {
            None => Err("starting the routes".into()),
            Some(Serving::Local(routes)) => routes.readiness(),
            Some(Serving::Tunnel(shared)) => shared.readiness(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::routes::testing::Fixed;
    use crate::session::testing::*;

    /// Waits until `readiness` says `ready` (Ok) or not, at most [`HANG`]; what it says then.
    async fn until(readiness: &Readiness<Fixed>, ready: bool) -> Result<(), String> {
        let start = tokio::time::Instant::now();
        while readiness.check().is_ok() != ready && start.elapsed() < HANG {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        readiness.check()
    }

    /// Through a server: not ready while starting, nor before the server took the routes; ready
    /// after, not while a route has no instance ready or a reload is not taken yet, nor from the
    /// drain on. In local mode alone, ready as the routes are.
    #[tokio::test]
    async fn ready_once_the_server_took_the_routes_and_not_from_the_drain_on() {
        let (shared, _, _) = hello_shared(Duration::ZERO);
        let readiness = Readiness::starting();
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
        assert_eq!(until(&readiness, true).await, Ok(()));

        let unready = shared.routes.current().unready.clone();
        unready.lock().unwrap().push("hello.local".into());
        assert_eq!(
            readiness.check(),
            Err("no instance ready for hello.local".into())
        );
        unready.lock().unwrap().clear();
        shared
            .routes
            .publish(Fixed::hosts(2, &["hello.local", "b.local"]));
        stream.advertise().await;
        assert!(
            until(&readiness, false).await.is_err(),
            "generation 2 not taken"
        );
        stream.ack(false);
        assert_eq!(until(&readiness, true).await, Ok(()));

        let drain = shared.clone();
        tokio::spawn(
            async move { crate::drain::drain(&drain, HANG, std::future::pending()).await },
        );
        assert_eq!(until(&readiness, false).await, Err("draining".into()));

        let local = Readiness::starting();
        local.local(Routes::new(Fixed::hosts(1, &["a.local"])));
        assert_eq!(local.check(), Ok(()));
    }
}
