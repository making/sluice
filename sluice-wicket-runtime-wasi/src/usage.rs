//! What an instance (a store) uses of the host, as the listings tell it
//! (`sluice_wicket_console::status::Usage`), measured as it runs: no thread of its own reads it.
//!
//! - **CPU time**: the time each poll of its store's futures took on the wicket's threads
//!   ([`Metered`]), which is the instance's CPU time unless the host took the thread away
//!   meanwhile. Two clock reads a poll (`CLOCK_MONOTONIC`, the vDSO's), where the thread's own CPU
//!   clock is a system call each. Its rate is over the last [`WINDOW`] or two
//! - **memory**: its linear memories' bytes, as its limiter lets them grow, and their most:
//!   a linear memory never shrinks, so they are one but on a store whose memory a pool reset
//! - **disk**: none, a route's files being read-only and shared by its instances
//!
//! A growth past its cap refused is noted: the trap that follows is out of memory.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use sluice_wicket_console::status::Usage;

/// The CPU over a window or two of this is a rate.
pub const WINDOW: Duration = Duration::from_secs(5);

/// What one store uses.
pub struct Meter {
    started: Instant,
    cpu: Mutex<Windows>,
    memory: AtomicU64,
    peak: AtomicU64,
    refused: AtomicBool,
}

/// CPU time, in nanoseconds: in all, and in the last two windows it ran in.
#[derive(Default)]
struct Windows {
    total: u64,
    /// The window of `current`, counted from [`origin`].
    at: u64,
    current: u64,
    /// The window before `at`'s.
    previous: u64,
}

/// What the windows are counted from.
fn origin() -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

/// The window of `at`: in whole seconds, a division of `u64`s every poll.
fn window(at: Instant) -> u64 {
    at.saturating_duration_since(origin()).as_secs() / WINDOW.as_secs()
}

impl Default for Meter {
    fn default() -> Self {
        Meter::new()
    }
}

impl Meter {
    /// The meter of a store made now.
    pub fn new() -> Meter {
        let started = Instant::now();
        Meter {
            started,
            cpu: Mutex::new(Windows {
                at: window(started),
                ..Windows::default()
            }),
            memory: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            refused: AtomicBool::new(false),
        }
    }

    fn cpu(&self) -> std::sync::MutexGuard<'_, Windows> {
        self.cpu.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Its store ran `took` at `now`.
    fn ran(&self, now: Instant, took: Duration) {
        let took = took.as_nanos() as u64;
        let at = window(now);
        let mut cpu = self.cpu();
        cpu.total += took;
        if at != cpu.at {
            cpu.previous = if at == cpu.at + 1 { cpu.current } else { 0 };
            cpu.current = 0;
            cpu.at = at;
        }
        cpu.current += took;
    }

    /// Its linear memories hold `bytes` from now on.
    pub fn holds(&self, bytes: u64) {
        self.memory.store(bytes, Ordering::Relaxed);
        self.peak.fetch_max(bytes, Ordering::Relaxed);
    }

    /// A growth of its linear memory past its cap was refused.
    pub fn refused(&self) {
        self.refused.store(true, Ordering::Relaxed);
    }

    /// Whether a growth past its cap was refused: out of memory.
    pub fn out_of_memory(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    /// What it used as of `now`: its CPU rate over the window of `now` and the one before (or
    /// since its start, within them).
    pub fn usage(&self, now: Instant) -> Usage {
        let (total, recent) = {
            let cpu = self.cpu();
            let at = window(now);
            let recent = match at.checked_sub(cpu.at) {
                Some(0) => cpu.current + cpu.previous,
                Some(1) => cpu.current,
                _ => 0,
            };
            (cpu.total, recent)
        };
        let from = origin() + WINDOW * (window(now).saturating_sub(1) as u32);
        let over = now.saturating_duration_since(from.max(self.started));
        Usage {
            cpu_ns: total,
            cpu: Usage::rate(recent, over),
            memory_bytes: self.memory.load(Ordering::Relaxed),
            memory_peak_bytes: self.peak.load(Ordering::Relaxed),
            disk_bytes: None,
        }
    }
}

pin_project_lite::pin_project! {
    /// `inner`, each poll's time counted to `meter`.
    pub struct Metered<'a, F> {
        #[pin]
        inner: F,
        meter: &'a Meter,
    }
}

impl<'a, F> Metered<'a, F> {
    pub fn new(meter: &'a Meter, inner: F) -> Self {
        Metered { inner, meter }
    }
}

impl<F: Future> Future for Metered<'_, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let start = Instant::now();
        let polled = this.inner.poll(cx);
        let end = Instant::now();
        this.meter.ran(end, end - start);
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The time a store's polls took counts in all and as a rate over the last window or two,
    /// none past them; its memory and its most; a growth refused is out of memory.
    #[test]
    fn a_stores_time_and_memory_are_counted() {
        let meter = Meter::new();
        let start = meter.started;
        let second = Duration::from_secs(1);
        meter.ran(start, second);
        let usage = meter.usage(start + 2 * second);
        assert_eq!(usage.cpu_ns, 1_000_000_000);
        // a second over the two since its start, or less of them in its first window
        assert!(usage.cpu >= 0.5 && usage.cpu <= 1.0, "{}", usage.cpu);
        meter.ran(start + WINDOW, 2 * second);
        let later = meter.usage(start + 2 * WINDOW);
        assert_eq!(later.cpu_ns, 3_000_000_000);
        assert!(later.cpu > 0.0, "the window before");
        assert_eq!(meter.usage(start + 4 * WINDOW).cpu, 0.0, "none since");
        assert_eq!(meter.usage(start + 4 * WINDOW).cpu_ns, 3_000_000_000);

        meter.holds(64 << 10);
        meter.holds(128 << 10);
        let usage = meter.usage(start);
        assert_eq!(
            (usage.memory_bytes, usage.memory_peak_bytes),
            (128 << 10, 128 << 10)
        );
        assert_eq!(usage.disk_bytes, None);
        assert!(!meter.out_of_memory());
        meter.refused();
        assert!(meter.out_of_memory());
    }
}
