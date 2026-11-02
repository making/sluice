//! Relay workers: single-threaded runtimes, a thread each. A task placed on one runs
//! there to its end, and so do the tasks it spawns and the sockets it opens: a node
//! session's connections stay on its worker with its stream, and a frame relayed wakes
//! no other thread. A work-stealing runtime wakes its idle workers to steal for each
//! frame: with a worker per core, a request relayed through a server took 2.6 times
//! the CPU (see `.kb/wicket-firecracker-findings.md`).

use std::io;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;

/// The workers of a wicket.
pub struct Workers {
    handles: Vec<Handle>,
    /// Each worker's stop and thread, until [`Workers::shutdown`].
    threads: Mutex<Vec<(oneshot::Sender<()>, JoinHandle<()>)>>,
}

impl Workers {
    /// Starts `count` workers (one at least), their threads named `<name>-<n>`.
    pub fn start(count: usize, name: &str) -> io::Result<Workers> {
        let mut handles = Vec::new();
        let mut threads = Vec::new();
        for n in 0..count.max(1) {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            handles.push(runtime.handle().clone());
            let (stop, stopped) = oneshot::channel::<()>();
            let thread = std::thread::Builder::new()
                .name(format!("{name}-{n}"))
                .spawn(move || {
                    let _ = runtime.block_on(stopped);
                    // the tasks left are dropped, and what they hold with them
                    runtime.shutdown_timeout(Duration::from_secs(5));
                })?;
            threads.push((stop, thread));
        }
        Ok(Workers {
            handles,
            threads: Mutex::new(threads),
        })
    }

    /// The worker for a new relay: the one with the fewest tasks alive (the first of
    /// them), a node session's counting its connections.
    pub fn place(&self) -> &Handle {
        self.handles
            .iter()
            .min_by_key(|handle| handle.metrics().num_alive_tasks())
            .expect("one worker at least")
    }

    /// Stops the workers, their tasks dropped, and waits for their threads.
    pub fn shutdown(&self) {
        let threads = std::mem::take(&mut *self.threads.lock().unwrap());
        for (stop, thread) in threads {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tasks placed go to the worker with the fewest alive, run on its thread with
    /// what they spawn, and are dropped by the shutdown.
    #[test]
    fn tasks_go_to_the_least_busy_worker_and_end_with_the_shutdown() {
        let workers = Workers::start(2, "test-worker").unwrap();
        let thread = || std::thread::current().name().unwrap().to_string();
        let (held, holding) = std::sync::mpsc::channel();
        let (dropped, drops) = std::sync::mpsc::channel::<()>();
        for _ in 0..3 {
            let (held, dropped) = (held.clone(), dropped.clone());
            workers.place().spawn(async move {
                let spawned = tokio::spawn(async move { thread() }).await.unwrap();
                held.send((thread(), spawned)).unwrap();
                let _dropped = dropped;
                std::future::pending::<()>().await
            });
            let (placed, spawned) = holding.recv().unwrap();
            assert_eq!(placed, spawned);
        }
        drop(dropped);
        let alive: Vec<_> = workers
            .handles
            .iter()
            .map(|handle| handle.metrics().num_alive_tasks())
            .collect();
        assert_eq!(alive, [2, 1]);
        workers.shutdown();
        assert!(drops.recv().is_err(), "every task dropped");
    }
}
