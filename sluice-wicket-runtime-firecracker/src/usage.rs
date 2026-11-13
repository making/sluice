//! What a VM uses of the host, as the listings tell it ([`Usage`]): read from its `firecracker`
//! process alike with the jail or without it -- its CPU clock (every thread's time: the vCPUs',
//! the VMM's, the API's) and `/proc/<pid>/status` (`VmRSS`, and its peak `VmHWM`, a clone's
//! counting the pages of its snapshot it maps) -- and what its guest uses of its disk, asked of
//! `/init` (`wire::USAGE_QUESTION`). A sampler reads every VM alive every [`INTERVAL`], the CPU
//! over the interval its rate; a listing reads what it took last.
//!
//! A process is read by its pid, and its pidfd tells when it ended: what is read of the pid after
//! that may be another process's, and is dropped.
//!
//! A VM the host kernel's OOM killer took is told apart from any other end: in a cgroup of its
//! own (`cgroup`: the keeper's, jailed; the wicket's, where its own is delegated to it), by the
//! cgroup's `memory.events` ([`oom_kills`]); without one, by the host's count of the OOM killer's
//! kills (`/proc/vmstat`) rising while it lived ([`Meter::host_oom_killed`]): a VM killed
//! (SIGKILL) by another process as the OOM killer took one elsewhere reads as one.

use std::os::fd::{AsRawFd as _, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use sluice_wicket_console::status::Usage;
use tokio::io::AsyncWriteExt as _;

use crate::wire;

/// How often the VMs are read: their CPU over that long is their rate.
pub const INTERVAL: Duration = Duration::from_secs(5);

/// How long a guest may take to answer what it uses: a VM's own wait, not the others'.
const ASK_TIMEOUT: Duration = Duration::from_secs(1);

/// The longest answer of a guest's use the wicket reads.
const MAX_ANSWER: usize = 256;

/// How long a VM's end may take to be known once its log stream ended.
pub const END_TIMEOUT: Duration = Duration::from_secs(1);

/// A host count of the OOM killer's kills read this close to a VM's end may come after it.
const OOM_MARGIN: Duration = Duration::from_secs(1);

/// The VMs alive, for the sampler.
static METERS: Mutex<Vec<Weak<Meter>>> = Mutex::new(Vec::new());

/// The host's OOM kills as the sampler read them, the last two times: when, how many.
static HOST_OOM_KILLS: Mutex<[Option<(Instant, u64)>; 2]> = Mutex::new([None, None]);

/// What the sampler reads of one VM's process, and what it read last.
pub struct Meter {
    pid: libc::pid_t,
    /// Readable once the process ended; none where the keeper sent none.
    pidfd: Option<Arc<OwnedFd>>,
    /// Its spawn: its CPU clock counts from there.
    started: Instant,
    /// The host's OOM kills before its spawn, as the sampler read them.
    host_oom_kills: Option<u64>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// When it was read last, and its CPU time then.
    read: Option<(Instant, u64)>,
    usage: Option<Usage>,
    /// Its guest's vsock socket: a VM with a disk, whose guest is asked what it uses of it.
    disk: Option<PathBuf>,
}

impl Meter {
    /// The meter of the VM process `pid` spawned now, its pidfd `pidfd`, read by the sampler
    /// from now on.
    pub fn start(pid: libc::pid_t, pidfd: Option<Arc<OwnedFd>>) -> Arc<Meter> {
        let host = HOST_OOM_KILLS.lock().unwrap_or_else(|e| e.into_inner());
        let meter = Arc::new(Meter {
            pid,
            pidfd,
            started: Instant::now(),
            host_oom_kills: host[1].map(|(_, count)| count),
            state: Mutex::default(),
        });
        drop(host);
        let mut meters = METERS.lock().unwrap_or_else(|e| e.into_inner());
        meters.push(Arc::downgrade(&meter));
        meter
    }

    /// Its guest, at the vsock socket `vsock`, is asked what it uses of its disk from now on.
    pub fn ask_disk(&self, vsock: &Path) {
        self.state().disk = Some(vsock.to_path_buf());
    }

    /// What it used as last read; none before it was.
    pub fn usage(&self) -> Option<Usage> {
        self.state().usage
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether its process ended.
    fn ended(&self) -> bool {
        let Some(pidfd) = &self.pidfd else {
            return false;
        };
        let mut poll = libc::pollfd {
            fd: pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll of one pollfd, at once
        unsafe { libc::poll(&mut poll, 1, 0) == 1 }
    }

    /// What it used, read at `at`: its CPU time, memory and memory's peak, and of its disk.
    fn took(&self, at: Instant, (cpu_ns, memory, peak): (u64, u64, u64), disk: Option<u64>) {
        let mut state = self.state();
        let (since, before) = state.read.unwrap_or((self.started, 0));
        let cpu = Usage::rate(cpu_ns.saturating_sub(before), at - since);
        state.read = Some((at, cpu_ns));
        state.usage = Some(Usage {
            cpu_ns,
            cpu,
            memory_bytes: memory,
            memory_peak_bytes: peak,
            disk_bytes: disk,
        });
    }

    /// Whether the host kernel's OOM killer took a process since its spawn, or since the
    /// sampler's read a moment before now: a VM without a cgroup of its own, killed (SIGKILL)
    /// by no one of the wicket, took that kill (`forker::Child`).
    pub fn host_oom_killed(&self) -> bool {
        let Some(now) = host_oom_kills() else {
            return false;
        };
        let host = *HOST_OOM_KILLS.lock().unwrap_or_else(|e| e.into_inner());
        let before = host
            .iter()
            .rev()
            .flatten()
            .find(|(read, _)| read.elapsed() >= OOM_MARGIN)
            .map(|(_, count)| *count);
        before
            .max(self.host_oom_kills)
            .is_some_and(|before| now > before)
    }
}

/// The count `oom_kill` of a cgroup's `memory.events`, or of `/proc/vmstat`: the processes the
/// kernel's OOM killer took there.
pub fn oom_kills(text: &str) -> Option<u64> {
    let count = text.lines().find_map(|line| line.strip_prefix("oom_kill "));
    count.and_then(|count| count.trim().parse().ok())
}

/// The processes the host kernel's OOM killer took since its boot.
fn host_oom_kills() -> Option<u64> {
    oom_kills(&std::fs::read_to_string("/proc/vmstat").ok()?)
}

/// What a process uses now: its CPU time, in nanoseconds, and its memory and memory's peak, in
/// bytes; None once it ended.
fn read(pid: libc::pid_t) -> Option<(u64, u64, u64)> {
    let mut clock: libc::clockid_t = 0;
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: the CPU clock of a process, read into a timespec of ours
    let read = unsafe {
        libc::clock_getcpuclockid(pid, &mut clock) == 0
            && libc::clock_gettime(clock, &mut time) == 0
    };
    if !read {
        return None;
    }
    let cpu = (time.tv_sec as u64) * 1_000_000_000 + time.tv_nsec as u64;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let kib = |name: &str| {
        let line = status.lines().find_map(|line| line.strip_prefix(name))?;
        let kib = line.trim().strip_suffix("kB")?.trim().parse::<u64>().ok()?;
        Some(kib * 1024)
    };
    // a process that ended has no memory to tell
    Some((cpu, kib("VmRSS:")?, kib("VmHWM:")?))
}

/// What the guest at the vsock socket `vsock` answers it uses, within [`ASK_TIMEOUT`].
async fn ask(vsock: &Path) -> Option<wire::GuestUsage> {
    let asked = async {
        let mut stream = crate::vm::dial_at(vsock, wire::CONTROL_PORT).await.ok()?;
        let question = format!("{}\n", wire::USAGE_QUESTION);
        stream.write_all(question.as_bytes()).await.ok()?;
        let answer = wire::read_short_line(&mut stream, MAX_ANSWER).await.ok()?;
        wire::parse_usage(String::from_utf8_lossy(&answer).trim_end())
    };
    tokio::time::timeout(ASK_TIMEOUT, asked)
        .await
        .ok()
        .flatten()
}

/// Reads every VM alive every [`INTERVAL`], for good.
pub async fn sample() {
    let mut ticks = tokio::time::interval(INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let meters: Vec<Arc<Meter>> = {
            let mut meters = METERS.lock().unwrap_or_else(|e| e.into_inner());
            meters.retain(|meter| meter.strong_count() > 0);
            meters.iter().filter_map(Weak::upgrade).collect()
        };
        // /proc's files: short reads, off the runtime's threads
        let read = tokio::task::spawn_blocking(move || {
            if let Some(count) = host_oom_kills() {
                let mut host = HOST_OOM_KILLS.lock().unwrap_or_else(|e| e.into_inner());
                *host = [host[1], Some((Instant::now(), count))];
            }
            let read: Vec<_> = (meters.into_iter())
                .map(|meter| {
                    let read = read(meter.pid).filter(|_| !meter.ended());
                    (meter, Instant::now(), read)
                })
                .collect();
            read
        })
        .await;
        let Ok(read) = read else {
            continue;
        };
        // the guests with a disk, each asked at once: a wedged one holds no other
        let mut asked = tokio::task::JoinSet::new();
        for (meter, at, read) in read {
            let Some(read) = read else {
                continue;
            };
            let disk = meter.state().disk.clone();
            let Some(vsock) = disk else {
                meter.took(at, read, None);
                continue;
            };
            asked.spawn(async move {
                let disk = ask(&vsock).await.and_then(|usage| usage.disk);
                meter.took(at, read, disk.map(|(used, _)| used));
            });
        }
        while asked.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A process read as the sampler reads a VM's (the test's own): its CPU time and memory, the
    /// CPU over the time between two reads its rate; once a process ended, by its pidfd; an OOM
    /// kill counted as the kernel's files say it.
    #[tokio::test]
    async fn a_process_is_read_as_a_vms() {
        use std::os::fd::FromRawFd as _;
        let pidfd = |pid: libc::pid_t| {
            // SAFETY: pidfd_open of a pid, no flags (a pidfd is close-on-exec)
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as libc::c_int;
            assert!(fd >= 0, "a pidfd of {pid}");
            // SAFETY: a new fd of ours
            Some(Arc::new(unsafe { OwnedFd::from_raw_fd(fd) }))
        };
        // SAFETY: getpid
        let own = unsafe { libc::getpid() };
        let (cpu, memory, peak) = read(own).expect("the test's own process");
        assert!(
            cpu > 0 && memory > 0 && peak >= memory,
            "{cpu} {memory} {peak}"
        );
        let meter = Meter::start(own, pidfd(own));
        assert!(!meter.ended());
        let start = meter.started;
        meter.took(
            start + Duration::from_secs(2),
            (1_000_000_000, 10, 20),
            Some(4096),
        );
        let usage = meter.usage().unwrap();
        assert_eq!((usage.cpu, usage.disk_bytes), (0.5, Some(4096)));
        meter.took(
            start + Duration::from_secs(4),
            (5_000_000_000, 30, 40),
            None,
        );
        let usage = meter.usage().unwrap();
        assert_eq!(
            (
                usage.cpu_ns,
                usage.cpu,
                usage.memory_bytes,
                usage.memory_peak_bytes
            ),
            (5_000_000_000, 2.0, 30, 40)
        );

        let mut child = tokio::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as libc::pid_t;
        let meter = Meter::start(pid, pidfd(pid));
        assert!(read(pid).is_some());
        assert!(!meter.ended(), "alive");
        child.kill().await.unwrap();
        assert!(meter.ended());

        let events = "low 0\nhigh 0\nmax 4\noom 2\noom_kill 1\noom_group_kill 1\n";
        assert_eq!(oom_kills(events), Some(1));
        assert!(host_oom_kills().is_some(), "/proc/vmstat counts them");
    }
}
