//! The VMs' budget: the VMs alive at once (`--max-instances`), what they take of the host's
//! memory (`--max-memory-mib`) and their CPUs (`--max-cpus`) over every route, and a route's
//! own cap (`instances=N`), which a running route may change ([`Capacity::cap`]). A VM holds
//! its share, a [`Slot`], from before its `firecracker` starts until it is killed.
//!
//! Memory is counted as the host is charged it at most: a VM's guest memory, what its
//! `firecracker` takes besides and its log (`cgroup::vm_charge_mib`), and a cloned route's
//! snapshot, its `memory-mib` ([`Need::snapshot`]: what its template wrote, and the rest as
//! its clones touch it), from before its template starts until the route is gone
//! ([`Reserved`]). A snapshot is the host's memory on a tmpfs, whose room it takes as well;
//! on a disk it is neither ([`Snapshots`]). In cgroups of their own (`cgroup`) the VMs are
//! held to it: each to its own, and to what it is charged of its route's snapshot.
//!
//! CPUs are counted in hundredths (a route's `cpu` is percent of one): a VM takes its vCPUs,
//! or its route's `cpu` where that is less, whatever it runs; the budget may be more than the
//! CPUs the host has, by as much as its operator states.
//!
//! Slots go to the VMs asking for one in the order they asked
//! ([`Capacity::take`]), but for one held back by its route's cap alone, or by the room for
//! the snapshot it writes, which keeps no other route waiting: no VM going frees either. A
//! warm clone is restored only into room nobody
//! waits for ([`Capacity::try_take`]), and gives its slot up to a VM that does:
//! the first in line, finding no room within the global limits, has a spare VM
//! of another route killed ([`Pool::evict`]) -- a warm clone, a reuse route's
//! clone serving no connection, a pool's VM beyond its `warm` serving none.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

/// What one VM takes: of the host's memory and its CPUs, counted against its route's cap.
#[derive(Clone, Copy, Debug)]
pub struct Need {
    /// MiB the VM may be charged (`cgroup::vm_charge_mib`).
    pub memory: u64,
    /// MiB of the snapshot it writes, which outlives it ([`Slot::reserved`]): a template's.
    pub snapshot: u64,
    /// Hundredths of a CPU.
    pub cpu: u64,
    pub route: u64,
}

/// Where the routes' snapshots are (the state dir), and so what one takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Snapshots {
    /// On a disk: none of the host's memory.
    Disk,
    /// On a tmpfs: the host's memory, and the room the tmpfs has, MiB, where it has a size.
    Memory(Option<u64>),
}

/// What bounds the VMs.
#[derive(Clone, Debug)]
pub struct Budget {
    pub max_instances: usize,
    /// MiB of the host's memory; None = no budget of it (`max_instances` x the largest VM
    /// bounds it).
    pub max_memory_mib: Option<u64>,
    /// Where the memory budget comes from, for a refusal.
    pub memory_of: String,
    /// Hundredths of a CPU; None = no budget of them.
    pub max_cpu: Option<u64>,
    pub snapshots: Snapshots,
}

impl Budget {
    /// `max_instances` VMs and no more: no budget of the memory nor of the CPUs, the
    /// snapshots on a disk.
    pub fn of(max_instances: usize) -> Budget {
        Budget {
            max_instances,
            max_memory_mib: None,
            memory_of: String::new(),
            max_cpu: None,
            snapshots: Snapshots::Disk,
        }
    }
}

/// `cpu`, hundredths of a CPU, in CPUs: `2`, `0.5`.
pub fn cpus(cpu: u64) -> String {
    match cpu % 100 {
        0 => (cpu / 100).to_string(),
        part if part % 10 == 0 => format!("{}.{}", cpu / 100, part / 10),
        part => format!("{}.{part:02}", cpu / 100),
    }
}

/// What a hold counts as in the routes' VMs: no route's.
const HELD: u64 = u64::MAX;

/// Spare VMs a route keeps: serving no connection, and not needed for one.
pub trait Pool: Send + Sync {
    /// Kills one spare VM, freeing its slot; false when there is none.
    fn evict(&self) -> bool;
}

pub struct Capacity {
    max_instances: usize,
    /// MiB.
    max_memory_mib: u64,
    memory_of: String,
    /// Hundredths of a CPU.
    max_cpu: u64,
    snapshots: Snapshots,
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Default)]
struct State {
    vms: usize,
    /// MiB: the VMs', and the snapshots' that count.
    memory: u64,
    cpu: u64,
    /// MiB of the snapshots that count.
    snapshots: u64,
    /// Route -> its VMs alive and its cap.
    routes: HashMap<u64, Room>,
    /// The VMs waiting for a slot, their tickets and needs, in the order they
    /// asked.
    waiting: VecDeque<(u64, Need)>,
    next_ticket: u64,
    /// Route -> its spare VMs (a route may have more than one kind).
    pools: Vec<(u64, Weak<dyn Pool>)>,
}

/// A route's share of the budget.
#[derive(Default)]
struct Room {
    alive: usize,
    /// Its VMs alive at once at most; None = the global limits alone.
    cap: Option<usize>,
}

/// Why a VM finds no room.
#[derive(Debug, PartialEq, Eq)]
enum Short {
    /// `--max-instances`, `--max-memory-mib` or `--max-cpus`: another route's spare VM may
    /// give way.
    Global,
    /// What no other route's VM going frees: the route's own cap, or the room for the
    /// snapshot it writes.
    Own,
}

/// A VM's share of the budget, given back when dropped.
pub struct Slot {
    capacity: Arc<Capacity>,
    need: Need,
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot").field("need", &self.need).finish()
    }
}

impl Slot {
    /// The share of the snapshot the VM writes, out of the slot's: it stays as the route
    /// lives, the VM gone.
    pub fn reserved(&mut self) -> Reserved {
        Reserved {
            capacity: self.capacity.clone(),
            mib: std::mem::take(&mut self.need.snapshot),
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        {
            let mut state = self.capacity.state.lock().unwrap();
            state.vms -= 1;
            state.memory -= self.need.memory + self.need.snapshot;
            state.snapshots -= self.need.snapshot;
            state.cpu -= self.need.cpu;
            if let Some(room) = state.routes.get_mut(&self.need.route) {
                room.alive -= 1;
            }
        }
        self.capacity.changed.notify_waiters();
    }
}

/// A snapshot's share of the budget, given back when dropped: its route gone.
pub struct Reserved {
    capacity: Arc<Capacity>,
    /// MiB, as it counts ([`Capacity::snapshot_mib`]).
    mib: u64,
}

impl Drop for Reserved {
    fn drop(&mut self) {
        {
            let mut state = self.capacity.state.lock().unwrap();
            state.memory -= self.mib;
            state.snapshots -= self.mib;
        }
        self.capacity.changed.notify_waiters();
    }
}

/// No room was found in time.
#[derive(Clone, Debug)]
pub struct Full(String);

impl Full {
    /// The routes need more than the budget: `why`.
    pub fn over(why: String) -> Full {
        Full(why)
    }

    /// Named after the route that asked.
    pub fn of(self, label: &str) -> Full {
        Full(format!("route {label}: {}", self.0))
    }
}

impl std::fmt::Display for Full {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Full {}

impl Capacity {
    pub fn new(budget: Budget) -> Arc<Capacity> {
        Arc::new(Capacity {
            max_instances: budget.max_instances,
            max_memory_mib: budget.max_memory_mib.unwrap_or(u64::MAX),
            memory_of: budget.memory_of,
            max_cpu: budget.max_cpu.unwrap_or(u64::MAX),
            snapshots: budget.snapshots,
            state: Mutex::default(),
            changed: Notify::new(),
        })
    }

    /// What `mib` MiB of snapshots count: all of it on a tmpfs, none on a disk.
    pub fn snapshot_mib(&self, mib: u64) -> u64 {
        match self.snapshots {
            Snapshots::Disk => 0,
            Snapshots::Memory(_) => mib,
        }
    }

    /// The room the snapshots have besides the memory budget, MiB: the state dir's tmpfs's;
    /// None = none of its own.
    pub fn snapshot_room(&self) -> Option<u64> {
        match self.snapshots {
            Snapshots::Memory(room) => room,
            Snapshots::Disk => None,
        }
    }

    /// Lets a waiting VM of another route kill a spare VM of `pool`.
    pub fn register(&self, route: u64, pool: Weak<dyn Pool>) {
        self.state.lock().unwrap().pools.push((route, pool));
    }

    /// The VMs of `route` alive at once at most from now on (None = the global limits
    /// alone), for what waits for room too. Those past it stay until they go.
    pub fn cap(&self, route: u64, cap: Option<usize>) {
        self.state
            .lock()
            .unwrap()
            .routes
            .entry(route)
            .or_default()
            .cap = cap;
        self.changed.notify_waiters();
    }

    /// Whether `route` has more VMs alive than its cap: it was lowered.
    pub fn over(&self, route: u64) -> bool {
        let state = self.state.lock().unwrap();
        let room = state.routes.get(&route);
        room.is_some_and(|room| room.cap.is_some_and(|cap| room.alive > cap))
    }

    /// What keeps `need` out of `state`, if anything.
    fn short(&self, state: &State, need: &Need) -> Option<Short> {
        let room = state.routes.get(&need.route);
        let snapshot = self.snapshot_mib(need.snapshot);
        let memory = need.memory.saturating_add(snapshot);
        if room.is_some_and(|room| room.cap.is_some_and(|cap| room.alive >= cap))
            || state.snapshots.saturating_add(snapshot) > self.snapshot_room().unwrap_or(u64::MAX)
        {
            Some(Short::Own)
        } else if state.vms >= self.max_instances
            || state.memory.saturating_add(memory) > self.max_memory_mib
            || state.cpu.saturating_add(need.cpu) > self.max_cpu
        {
            Some(Short::Global)
        } else {
            None
        }
    }

    fn grant(self: &Arc<Self>, state: &mut State, need: Need) -> Slot {
        // as it counts from here on: the slot gives back what it took
        let need = Need {
            snapshot: self.snapshot_mib(need.snapshot),
            ..need
        };
        state.vms += 1;
        state.memory += need.memory + need.snapshot;
        state.snapshots += need.snapshot;
        state.cpu += need.cpu;
        state.routes.entry(need.route).or_default().alive += 1;
        Slot {
            capacity: self.clone(),
            need,
        }
    }

    /// Whether a VM waiting before `ticket` (every one, without) may take the
    /// room: one held back by its route's cap, or by its snapshot's room, alone may not.
    fn ahead(&self, state: &State, ticket: Option<u64>) -> bool {
        state
            .waiting
            .iter()
            .take_while(|(queued, _)| Some(*queued) != ticket)
            .any(|(_, need)| self.short(state, need) != Some(Short::Own))
    }

    /// The room of a VM of `need` going down, beside the budget as long as it is not gone, for
    /// the one that comes back: its room is not taken meanwhile, for as long as that takes.
    pub fn hold(self: &Arc<Self>, need: Need) -> Slot {
        let mut state = self.state.lock().unwrap();
        let held = Need {
            route: HELD,
            ..need
        };
        self.grant(&mut state, held)
    }

    /// A slot for `need` whatever the budget: `held` holds its room, and goes once it has it.
    pub fn in_room_of(self: &Arc<Self>, _held: &Slot, need: Need) -> Slot {
        let mut state = self.state.lock().unwrap();
        self.grant(&mut state, need)
    }

    /// A slot if one fits now and no VM waits for one: a warm clone's.
    pub fn try_take(self: &Arc<Self>, need: Need) -> Option<Slot> {
        let mut state = self.state.lock().unwrap();
        (!self.ahead(&state, None) && self.short(&state, &need).is_none())
            .then(|| self.grant(&mut state, need))
    }

    /// A slot in turn, within `wait`. The first in line finding no room
    /// within the global limits has a spare VM of another route killed.
    pub async fn take(self: &Arc<Self>, need: Need, wait: Duration) -> Result<Slot, Full> {
        let deadline = Instant::now() + wait;
        let ticket = {
            let mut state = self.state.lock().unwrap();
            let ticket = state.next_ticket;
            state.next_ticket += 1;
            state.waiting.push_back((ticket, need));
            ticket
        };
        // gives the turn up when the wait is dropped (the connection ended)
        let mut queued = Queued {
            capacity: self,
            ticket: Some(ticket),
        };
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let evict = {
                let mut state = self.state.lock().unwrap();
                let first = !self.ahead(&state, Some(ticket));
                match self.short(&state, &need) {
                    None if first => {
                        state.waiting.retain(|(queued, _)| *queued != ticket);
                        queued.ticket = None;
                        let slot = self.grant(&mut state, need);
                        drop(state);
                        // the next in line may fit too
                        self.changed.notify_waiters();
                        return Ok(slot);
                    }
                    Some(Short::Global) if first => Self::pools(&mut state, need.route),
                    _ => Vec::new(),
                }
            };
            // outside the lock: the VM's slot takes it on its way out
            if evict.iter().any(|pool| pool.evict()) {
                continue;
            }
            tokio::select! {
                () = notified => {}
                () = tokio::time::sleep_until(deadline) => {
                    let state = self.state.lock().unwrap();
                    let room = state.routes.get(&need.route);
                    let none = || "-".to_string();
                    let snapshots = self.snapshot_room().map_or(String::new(), |room| {
                        format!("snapshots {}/{room} MiB, ", state.snapshots)
                    });
                    return Err(Full(format!(
                        "no room for a VM within {}ms (VMs {}/{}, memory {}/{} MiB, CPUs {}/{}, \
                         {snapshots}the route's {}/{})",
                        wait.as_millis(),
                        state.vms,
                        self.max_instances,
                        state.memory,
                        self.max_memory_mib().map_or_else(none, |mib| mib.to_string()),
                        cpus(state.cpu),
                        self.max_cpu().map_or_else(none, cpus),
                        room.map_or(0, |room| room.alive),
                        room.and_then(|room| room.cap)
                            .map_or_else(none, |cap| cap.to_string()),
                    )));
                }
            }
        }
    }

    /// The pools of other routes than `route` to evict from, in the order
    /// they came, dropping the gone ones.
    fn pools(state: &mut State, route: u64) -> Vec<Arc<dyn Pool>> {
        state.pools.retain(|(_, pool)| pool.strong_count() > 0);
        state
            .pools
            .iter()
            .filter(|(owner, _)| *owner != route)
            .filter_map(|(_, pool)| pool.upgrade())
            .collect()
    }

    /// A slot as soon as one fits with no VM waiting: a warm clone's.
    pub async fn take_spare(self: &Arc<Self>, need: Need) -> Slot {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(slot) = self.try_take(need) {
                return slot;
            }
            notified.await;
        }
    }

    /// The VMs alive, what they and the snapshots take of the memory budget (MiB) and their
    /// CPUs (hundredths), over every route.
    pub fn usage(&self) -> (usize, u64, u64) {
        let state = self.state.lock().unwrap();
        (state.vms, state.memory, state.cpu)
    }

    /// The VMs alive of `route`.
    pub fn alive(&self, route: u64) -> usize {
        let state = self.state.lock().unwrap();
        state.routes.get(&route).map_or(0, |room| room.alive)
    }

    pub fn max_instances(&self) -> usize {
        self.max_instances
    }

    /// MiB; None = no memory budget.
    pub fn max_memory_mib(&self) -> Option<u64> {
        (self.max_memory_mib != u64::MAX).then_some(self.max_memory_mib)
    }

    /// Where the memory budget comes from.
    pub fn memory_of(&self) -> &str {
        &self.memory_of
    }

    /// Hundredths of a CPU; None = no CPU budget.
    pub fn max_cpu(&self) -> Option<u64> {
        (self.max_cpu != u64::MAX).then_some(self.max_cpu)
    }
}

/// A VM's place in line, given up when dropped before its turn came.
struct Queued<'a> {
    capacity: &'a Arc<Capacity>,
    ticket: Option<u64>,
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket {
            self.capacity
                .state
                .lock()
                .unwrap()
                .waiting
                .retain(|(queued, _)| *queued != ticket);
            // the next in line may be first now
            self.capacity.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A VM of `memory` MiB and `cpu` hundredths of a CPU.
    fn need(memory: u64, cpu: u64, route: u64) -> Need {
        Need {
            memory,
            snapshot: 0,
            cpu,
            route,
        }
    }

    /// A budget of `max_instances` VMs, `memory` MiB and `cpu` hundredths of a CPU, the
    /// snapshots at `snapshots`.
    fn capacity(
        max_instances: usize,
        memory: Option<u64>,
        cpu: Option<u64>,
        snapshots: Snapshots,
    ) -> Arc<Capacity> {
        Capacity::new(Budget {
            max_memory_mib: memory,
            max_cpu: cpu,
            snapshots,
            ..Budget::of(max_instances)
        })
    }

    /// Warm clones held as slots, given up one by one.
    struct Warm(Mutex<Vec<Slot>>, AtomicUsize);

    impl Pool for Warm {
        fn evict(&self) -> bool {
            let evicted = self.0.lock().unwrap().pop().is_some();
            if evicted {
                self.1.fetch_add(1, Ordering::SeqCst);
            }
            evicted
        }
    }

    /// The limits count VMs, memory, CPUs and a route's VMs; a wait gets the slot
    /// a VM gives back, in the order of asking, and a warm clone only takes
    /// room nobody waits for. A hold keeps the room of a VM gone from them, and gives it to
    /// the VM that comes back. A template's snapshot counts beside it and stays, the template
    /// gone, until its route is: on a tmpfs, of the memory and of the tmpfs's room; on a disk,
    /// of neither.
    #[tokio::test(start_paused = true)]
    async fn slots_go_in_turn_within_the_limits() {
        let capacity = capacity(3, Some(1024), Some(300), Snapshots::Disk);
        let big = capacity
            .take(need(768, 100, 1), Duration::ZERO)
            .await
            .unwrap();
        capacity.cap(2, Some(1));
        let small = capacity.try_take(need(128, 50, 2)).unwrap();
        assert!(
            capacity.try_take(need(128, 50, 2)).is_none(),
            "the route's cap"
        );
        assert!(
            capacity.try_take(need(256, 50, 3)).is_none(),
            "the memory: 768 + 128 + 256"
        );
        assert!(
            capacity.try_take(need(64, 200, 3)).is_none(),
            "the CPUs: 1 + 0.5 + 2"
        );
        let full = capacity
            .take(need(256, 200, 3), Duration::from_millis(100))
            .await
            .unwrap_err();
        let counted = "memory 896/1024 MiB, CPUs 1.5/3";
        assert!(full.to_string().contains(counted), "{full}");

        // short of CPUs alone
        let waiting = tokio::spawn({
            let capacity = capacity.clone();
            async move {
                let wide = need(64, 200, 3);
                capacity.take(wide, Duration::from_secs(10)).await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiting.is_finished(), "past the CPUs: it waits");
        assert!(
            capacity.try_take(need(64, 50, 4)).is_none(),
            "room nobody waits for only"
        );
        drop(big);
        let third = waiting.await.unwrap().unwrap();
        assert!(capacity.try_take(need(64, 50, 4)).is_some(), "and the rest");
        drop((small, third));

        let whole = need(1024, 300, 5);
        let vm = capacity.try_take(whole).unwrap();
        let held = capacity.hold(whole);
        drop(vm);
        assert!(capacity.try_take(need(64, 50, 6)).is_none(), "held");
        let back = capacity.in_room_of(&held, need(1024, 300, 7));
        assert_eq!(capacity.alive(7), 1);
        drop(held);
        assert_eq!(capacity.usage(), (1, 1024, 300));
        drop(back);
        assert_eq!(cpus(1205), "12.05");

        let template = Need {
            snapshot: 256,
            ..need(128, 100, 8)
        };
        let mut slot = capacity.try_take(template).unwrap();
        let reserved = slot.reserved();
        assert_eq!(capacity.usage(), (1, 128, 100), "on a disk: no memory");
        drop((slot, reserved));

        let on_tmpfs = self::capacity(3, Some(1024), None, Snapshots::Memory(Some(256)));
        let mut slot = on_tmpfs.try_take(template).unwrap();
        let reserved = slot.reserved();
        assert_eq!(on_tmpfs.usage(), (1, 384, 100));
        drop(slot);
        assert_eq!(on_tmpfs.usage(), (0, 256, 0), "it stays, the template gone");
        assert!(
            on_tmpfs.try_take(need(769, 100, 9)).is_none(),
            "the memory: 256 + 769"
        );
        let another = Need {
            snapshot: 1,
            ..need(1, 100, 9)
        };
        let waiting = tokio::spawn({
            let on_tmpfs = on_tmpfs.clone();
            async move { on_tmpfs.take(another, Duration::from_millis(100)).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            on_tmpfs.try_take(need(1, 100, 9)).is_some(),
            "the tmpfs's room, which no VM going frees: it keeps none waiting"
        );
        let full = waiting.await.unwrap().unwrap_err();
        let counted = "memory 256/1024 MiB, CPUs 0/-, snapshots 256/256 MiB";
        assert!(full.to_string().contains(counted), "{full}");
        drop(reserved);
        assert_eq!(on_tmpfs.usage(), (0, 0, 0));
        assert!(on_tmpfs.try_take(another).is_some(), "its route gone");
    }

    /// The first in line short of global room has another route's spare VM
    /// killed, past the pools with none; a route at its own cap waits for its
    /// own VMs.
    #[tokio::test(start_paused = true)]
    async fn a_wait_has_another_routes_warm_clone_give_way() {
        let capacity = capacity(2, None, None, Snapshots::Disk);
        let empty: Arc<dyn Pool> = Arc::new(Warm(Mutex::new(Vec::new()), AtomicUsize::new(0)));
        capacity.register(4, Arc::downgrade(&empty));
        let warm = Arc::new(Warm(
            Mutex::new(vec![
                capacity.try_take(need(128, 100, 1)).unwrap(),
                capacity.try_take(need(128, 100, 1)).unwrap(),
            ]),
            AtomicUsize::new(0),
        ));
        let pool: Arc<dyn Pool> = warm.clone();
        capacity.register(1, Arc::downgrade(&pool));

        let slot = capacity
            .take(need(128, 100, 2), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(warm.1.load(Ordering::SeqCst), 1, "one clone gave way");

        // route 1 at its cap waits for its own VMs, and keeps nobody waiting
        capacity.cap(1, Some(1));
        let capped = tokio::spawn({
            let capacity = capacity.clone();
            async move {
                capacity
                    .take(need(128, 100, 1), Duration::from_millis(50))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(slot);
        let other = capacity
            .take(need(128, 100, 3), Duration::ZERO)
            .await
            .unwrap();
        assert!(
            capped.await.unwrap().is_err(),
            "its own route's clone is no room for a route at its cap"
        );
        assert_eq!(warm.1.load(Ordering::SeqCst), 1);
        drop(other);
    }

    /// A route's cap changed: what waits for room finds it at once, up; down, the VMs past it
    /// stay until they go, and nothing more is started.
    #[tokio::test(start_paused = true)]
    async fn a_routes_cap_changes_under_its_vms() {
        let capacity = capacity(8, None, None, Snapshots::Disk);
        capacity.cap(1, Some(1));
        let first = capacity.try_take(need(128, 100, 1)).unwrap();
        let waiting = tokio::spawn({
            let capacity = capacity.clone();
            async move {
                capacity
                    .take(need(128, 100, 1), Duration::from_secs(10))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiting.is_finished(), "at its cap");
        capacity.cap(1, Some(2));
        let second = waiting.await.unwrap().unwrap();
        assert!(!capacity.over(1));

        capacity.cap(1, Some(1));
        assert!(capacity.over(1), "2 alive past 1");
        assert!(capacity.try_take(need(128, 100, 1)).is_none());
        drop(second);
        assert!(!capacity.over(1));
        assert!(
            capacity.try_take(need(128, 100, 1)).is_none(),
            "1 alive of 1"
        );
        drop(first);
        assert!(capacity.try_take(need(128, 100, 1)).is_some());

        capacity.cap(1, None);
        assert!(!capacity.over(1), "no cap");
    }
}
