//! One instance at a time reconciles the wickets (`crate::controller`): the holder of the lease,
//! a row of the store several instances share (`crate::postgres`), renewed by its holder and,
//! once it lapsed by the store's clock, taken by the next instance trying. Each take begins a
//! turn of its own epoch, above every one before (the store's clock in milliseconds at least):
//! the wickets refuse a write of an epoch below the highest they saw of the owner
//! (`sluice_wicket_console::fence`), and the store a change of the leader alone once another took
//! the lease (`Store::lead`). A holder that cannot renew ends its turn before the lease lapses, by
//! its own clock from when it sent the last renewal that held.
//!
//! The one process of a data directory (no `--state`) leads from its start, as the directory's
//! lock is its alone: its turn has no epoch, its writes none, as before the lease.
//!
//! A wicket started again holds what the owner put until it confirms it, [`HELD`] at most: the
//! next leader lists it within the lease, a take's pause and a listing's, all of which stay below
//! it, so a takeover is a start of the cf-api to the wickets.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::controller::{HELD, MAX_PAUSE};
use crate::postgres::{Postgres, Shared, Taken};

/// How long a lease lasts past its last renewal, by the store's clock.
pub const LEASE: Duration = Duration::from_secs(10);

/// How often its holder renews it: several times in a lease.
pub const RENEW_EVERY: Duration = Duration::from_secs(2);

/// How often another instance tries to take it.
pub const TAKE_EVERY: Duration = Duration::from_secs(1);

const _: () = assert!(
    LEASE.as_millis() + TAKE_EVERY.as_millis() + MAX_PAUSE.as_millis() <= HELD.as_millis(),
    "a wicket's hold outlasts a takeover"
);

/// How often a store not answering is said.
const WARN_EVERY: Duration = Duration::from_secs(30);

/// A turn of this instance reconciling the wickets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Term {
    /// The lease's; None: the turn of the one process of a data directory, which sends none.
    pub epoch: Option<u64>,
}

/// The lease's timing: [`LEASE`], [`RENEW_EVERY`], [`TAKE_EVERY`] but in the tests.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    pub lease: Duration,
    pub renew_every: Duration,
    pub take_every: Duration,
}

impl Default for Timing {
    fn default() -> Timing {
        Timing {
            lease: LEASE,
            renew_every: RENEW_EVERY,
            take_every: TAKE_EVERY,
        }
    }
}

/// The lease as this instance takes, renews and lets go of it.
pub struct Lease {
    /// None: the one process of a data directory's, held from its start.
    postgres: Option<Arc<Postgres>>,
    shared: Arc<Shared>,
    timing: Timing,
}

impl Lease {
    pub fn new(postgres: Option<Arc<Postgres>>, shared: Arc<Shared>, timing: Timing) -> Lease {
        Lease {
            postgres,
            shared,
            timing,
        }
    }

    /// Keeps the lease: renewed while this instance's turn lasts (ended before it lapses when it
    /// cannot be), taken whenever it lapsed otherwise. Each turn begins and ends in the store's
    /// terms (`Store::terms`); one ended elsewhere (the store or a wicket found another took the
    /// lease) is taken as ended.
    pub async fn run(self: Arc<Self>) {
        let Some(postgres) = &self.postgres else {
            return;
        };
        let timing = self.timing;
        // the turn's end by this instance's clock: before the lease lapses by the store's
        let ending = |sent: Instant| sent + timing.lease.mul_f64(0.9);
        // a turn begun before this ran (none in a start) is over: its lease lapsed meanwhile
        let mut deadline = Instant::now();
        let mut renewal = Instant::now();
        let mut said: Option<Instant> = None;
        let mut warn = |what: &str, e: &str| {
            if said.is_none_or(|said| said.elapsed() >= WARN_EVERY) {
                eprintln!("LEASE: {what}: {e}");
                said = Some(Instant::now());
            }
        };
        let mut followed = None;
        let mut terms = self.shared.term.subscribe();
        loop {
            let term = *terms.borrow_and_update();
            match term {
                Some(Term { epoch: Some(epoch) }) => {
                    tokio::select! {
                        () = tokio::time::sleep_until(renewal.min(deadline)) => {}
                        _ = terms.changed() => continue,
                    }
                    let lapsed = || {
                        let why = "its lease not renewed in time";
                        self.shared.end_term(Some(epoch), why);
                    };
                    if Instant::now() >= deadline {
                        lapsed();
                        continue;
                    }
                    let sent = Instant::now();
                    let renewed = tokio::select! {
                        renewed = postgres.renew(epoch, timing.lease) => renewed,
                        () = tokio::time::sleep_until(deadline) => {
                            lapsed();
                            continue;
                        }
                    };
                    match renewed {
                        Ok(true) => deadline = ending(sent),
                        Ok(false) => {
                            self.shared
                                .end_term(Some(epoch), "another instance took the lease");
                            continue;
                        }
                        Err(e) => warn("not renewed", &e),
                    }
                    renewal = sent + timing.renew_every;
                }
                // the one process's: nothing to keep
                Some(Term { epoch: None }) => return,
                None => {
                    let sent = Instant::now();
                    match postgres.take(timing.lease).await {
                        Ok(Taken::Taken(epoch)) => {
                            (deadline, renewal) = (ending(sent), sent + timing.renew_every);
                            self.shared.leading.send_replace(Some(epoch));
                            self.shared.leader.send_replace(None);
                            self.shared
                                .term
                                .send_replace(Some(Term { epoch: Some(epoch) }));
                            println!(
                                "LEASE: this instance reconciles the wickets (epoch {epoch}), \
                                 its lease renewed every {}s for {}s",
                                timing.renew_every.as_secs_f32(),
                                timing.lease.as_secs_f32()
                            );
                            followed = None;
                            continue;
                        }
                        Ok(Taken::Held(epoch, address)) => {
                            self.shared.leader.send_if_modified(|leader| {
                                let moved = *leader != address;
                                *leader = address;
                                moved
                            });
                            self.shared.leading.send_replace(Some(epoch));
                            if followed != Some(epoch) {
                                println!(
                                    "LEASE: another instance reconciles the wickets (epoch \
                                     {epoch}): taken over once its lease lapses"
                                );
                                followed = Some(epoch);
                            }
                        }
                        Err(e) => warn("not taken", &e),
                    }
                    tokio::time::sleep_until(sent + timing.take_every).await;
                }
            }
        }
    }

    /// Lets go of the lease this instance holds, if any, its turn ended: another instance takes
    /// it at once.
    pub async fn release(&self) {
        let term = *self.shared.term.borrow();
        let Some(Term { epoch }) = term else {
            return;
        };
        let Some(postgres) = &self.postgres else {
            // the one process's: no lease to say it of
            self.shared.term.send_replace(None);
            return;
        };
        if let Some(epoch) = epoch
            && let Err(e) = postgres.release(epoch).await
        {
            eprintln!("LEASE: not let go of (it lapses): {e}");
        }
        self.shared.end_term(epoch, "it stops");
    }
}
