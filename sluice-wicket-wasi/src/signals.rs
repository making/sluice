//! The signals of the process: SIGHUP reloads the routes, SIGTERM / SIGINT
//! drain.

use tokio::sync::mpsc;

/// Sends a message to `hangups` on every SIGHUP (which no longer ends the
/// process); a reload is pending at most once.
#[cfg(unix)]
pub fn forward_hangups(hangups: mpsc::Sender<()>) -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut hangup = signal(SignalKind::hangup())?;
    tokio::spawn(async move {
        while hangup.recv().await.is_some() {
            let _ = hangups.try_send(());
        }
    });
    Ok(())
}

/// No SIGHUP where there are no unix signals: the files are still polled.
#[cfg(not(unix))]
pub fn forward_hangups(_hangups: mpsc::Sender<()>) -> std::io::Result<()> {
    Ok(())
}

/// SIGTERM and SIGINT. Once installed, neither ends the process by itself.
#[cfg(unix)]
pub struct Signals {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    pub fn install() -> std::io::Result<Signals> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Signals {
            term: signal(SignalKind::terminate())?,
            int: signal(SignalKind::interrupt())?,
        })
    }

    /// The next one received.
    pub async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.term.recv() => "SIGTERM",
            _ = self.int.recv() => "SIGINT",
        }
    }
}

/// Ctrl-C where there are no unix signals.
#[cfg(not(unix))]
pub struct Signals;

#[cfg(not(unix))]
impl Signals {
    pub fn install() -> std::io::Result<Signals> {
        Ok(Signals)
    }

    pub async fn recv(&mut self) -> &'static str {
        let _ = tokio::signal::ctrl_c().await;
        "Ctrl-C"
    }
}
