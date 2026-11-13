//! The forker: the process forking the wicket's VMs where no keeper does (without the jail).
//!
//! ```text
//! wicket --a VM to fork, socket pair--> forker --clone, CLONE_PARENT--> firecracker (the wicket's child)
//! ```
//!
//! The wicket starts it first, single-threaded, as itself again ([`FORK`]), and asks it for
//! each VM ([`Forker`]). A process of its own, because:
//!
//! - a fork of the wicket copies its page tables holding its memory map lock (1-2ms of no
//!   page fault, no allocation growing the heap, on any of its threads, for every VM
//!   spawned). The forker is small and single-threaded, its fork cheap, and what a VM needs
//!   before its exec is done between the two ([`launch`]): one exec a VM, where spawning the
//!   wicket again as a launcher took two
//! - a VM enters its cgroup (`cgroup`) as it is cloned (`clone3`'s `CLONE_INTO_CGROUP`).
//!   Moved there once started (`cgroup.procs`), the kernel has the move wait for an RCU grace
//!   period: 17-21ms a VM. The wicket's seccomp filter refuses `clone3` (its flags are out of
//!   a filter's reach), so the forker starts before it, under a filter of its own
//!   (`seccomp::Filter::forker`), and each VM installs the wicket's before its exec
//!
//! The VM is the wicket's child, not the forker's (`CLONE_PARENT`): the wicket reaps it and
//! reads how it ended ([`Child`]), it never outlives the wicket (its parent death signal,
//! SIGKILL, set before its exec: the parent the wicket's first thread, which lives as long),
//! the kernel's OOM killer takes it before the wicket (`jail::OomScore`), and the forker holds
//! nothing of it. What the forker forks runs as the wicket's user under
//! the wicket's filter, whatever is asked of it; no process of that user traces it or writes
//! its memory (not dumpable), a wicket taken over neither.

use std::ffi::OsString;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};

use crate::BoxError;
use crate::cgroup::{Cgroups, Held, Own};
use crate::disk;
use crate::jail::{CLONE_INTO_CGROUP, CLONE_PIDFD, CloneArgs, OomScore, check, failed};
use crate::keeper::{MESSAGE, Role, Size, VmEnd, receive, send};
use crate::seccomp::Filter;
use crate::usage::Meter;
use crate::vm::set_soft_open_files;

/// The first argument of the wicket's forker mode, see [`serve`].
pub const FORK: &str = "--fork-vms";

/// A VM to fork, as the wicket asks for it: sent with its log (its stdout and stderr) and,
/// `cgroup`, its cgroup's directory.
#[derive(Serialize, Deserialize, Debug)]
struct Request {
    program: PathBuf,
    args: Vec<OsString>,
    /// Its working directory.
    dir: PathBuf,
    /// Its soft open file limit.
    open_files: u64,
    /// Its file size limit, bytes.
    file_size: u64,
    /// Its disk, whose lock it holds.
    disk: Option<PathBuf>,
    cgroup: bool,
}

/// The forker's answer: the VM's pid, sent with a pidfd of it; or why there is none.
type Answer = Result<libc::pid_t, String>;

/// The forker mode (`<wicket> --fork-vms <wicket pid> <socket fd>`), run before anything
/// else: forks a VM for each request on the socket ([`fork`]), under its filter, until the
/// wicket ends -- its end of the socket closed (returns then), or itself (the forker's
/// parent death signal). A terminal's signals are the wicket's to take: the forker blocks
/// them all.
pub fn serve(mut args: impl Iterator<Item = OsString>) -> io::Result<()> {
    let mut number = || {
        args.next()
            .and_then(|arg| arg.to_str()?.parse::<i32>().ok())
    };
    let (Some(wicket), Some(fd)) = (number(), number()) else {
        return Err(io::Error::other(format!(
            "usage: {FORK} <wicket pid> <socket fd>"
        )));
    };
    // opened while it may be: not dumpable, a process's files in /proc are root's
    let oom = OomScore::open()
        .inspect_err(|e| {
            eprintln!(
                "the VMs' oom_score_adj: {e}; the kernel's OOM killer may take the wicket \
                 before a VM"
            )
        })
        .ok();
    // SAFETY: plain calls on the fd the wicket gave us and on our own process
    let socket = unsafe {
        check(libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC))?;
        let socket = OwnedFd::from_raw_fd(fd);
        check(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL))?;
        if libc::getppid() != wicket {
            return Err(io::Error::other("the wicket ended"));
        }
        check(libc::prctl(libc::PR_SET_DUMPABLE, 0))?;
        // told apart from the wicket where processes are listed
        libc::prctl(libc::PR_SET_NAME, c"sluice-forker".as_ptr());
        let mut all: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut all);
        libc::sigprocmask(libc::SIG_BLOCK, &all, std::ptr::null_mut());
        socket
    };
    let null = std::fs::File::open("/dev/null")?;
    let filter = Filter::unprivileged();
    Filter::forker().install()?;
    let mut buffer = vec![0u8; MESSAGE];
    loop {
        let (n, fds) = match receive(socket.as_raw_fd(), &mut buffer) {
            Ok((0, _)) => return Ok(()),
            Ok(received) => received,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        let forked = serde_json::from_slice::<Request>(&buffer[..n])
            .map_err(|e| format!("a request it cannot read: {e}"))
            .and_then(|request| {
                let forked = fork(&request, fds, null.as_fd(), wicket, &filter, oom.as_ref());
                forked.map_err(|e| e.to_string())
            });
        let (answer, pidfd): (Answer, _) = match forked {
            Ok((pid, pidfd)) => (Ok(pid), Some(pidfd)),
            Err(e) => (Err(e), None),
        };
        let bytes = serde_json::to_vec(&answer).expect("an answer serializes");
        let fds: Vec<RawFd> = pidfd.iter().map(|fd| fd.as_raw_fd()).collect();
        send(socket.as_raw_fd(), &bytes, &fds)?;
    }
}

/// Forks the VM of `request`, the wicket's child (`CLONE_PARENT`), with `fds`: its log and,
/// asked for, its cgroup's directory, which it starts in (`clone3`; without one, `clone`,
/// which every seccomp profile takes: Docker's refuses `clone3`). Its pid and a pidfd of it.
/// The child becomes the VM ([`launch`]), or says why not in its log and exits 127; its
/// `oom_score_adj` a VM's, the forker's own raised to it for the fork (`oom`).
fn fork(
    request: &Request,
    fds: Vec<OwnedFd>,
    null: BorrowedFd<'_>,
    wicket: libc::pid_t,
    filter: &Filter,
    oom: Option<&OomScore>,
) -> io::Result<(libc::pid_t, OwnedFd)> {
    let mut fds = fds.into_iter();
    let log = fds
        .next()
        .ok_or_else(|| io::Error::other("a VM needs its log"))?;
    let cgroup = match request.cgroup {
        true => Some(
            fds.next()
                .ok_or_else(|| io::Error::other("no cgroup came with the request"))?,
        ),
        false => None,
    };
    let mut pidfd: libc::c_int = -1;
    let raised = oom.is_some_and(|oom| oom.raise().is_ok());
    // SAFETY: a clone without a stack: fork's semantics. The child only runs `launch` (no
    // lock another thread holds: the forker is single-threaded) and exits.
    let pid = unsafe {
        match &cgroup {
            Some(cgroup) => {
                let args = CloneArgs {
                    flags: libc::CLONE_PARENT as u64 | CLONE_PIDFD | CLONE_INTO_CGROUP,
                    pidfd: &mut pidfd as *mut libc::c_int as u64,
                    cgroup: cgroup.as_raw_fd() as u64,
                    // the exit signal is the forker's own (SIGCHLD), as CLONE_PARENT has it
                    ..CloneArgs::default()
                };
                libc::syscall(
                    libc::SYS_clone3,
                    &args as *const CloneArgs,
                    std::mem::size_of::<CloneArgs>(),
                )
            }
            None => libc::syscall(
                libc::SYS_clone,
                (libc::CLONE_PARENT | libc::CLONE_PIDFD | libc::SIGCHLD) as libc::c_ulong,
                0_usize,
                &mut pidfd as *mut libc::c_int,
                0_usize,
                0_usize,
            ),
        }
    };
    // the forker's own again: in the forker alone, the file is its
    let failure = io::Error::last_os_error();
    if let (true, Some(oom), 1.. | -1) = (raised, oom, pid) {
        let _ = oom.lower();
    }
    match pid {
        -1 => {
            let call = match cgroup {
                Some(_) => "clone3 into the VM's cgroup",
                None => "clone",
            };
            Err(failed(call)(failure))
        }
        0 => {
            let error = launch(request, log.as_fd(), null, wicket, filter);
            let message = format!("{}: {error}\n", request.program.display());
            // SAFETY: a write of a buffer of its length to stderr (the log), then the end
            // of the child without unwinding into the forker's frames
            unsafe {
                libc::write(2, message.as_ptr().cast(), message.len());
                libc::_exit(127)
            }
        }
        // SAFETY: the pidfd the clone made for us (close-on-exec, as every pidfd)
        pid => Ok((pid as libc::pid_t, unsafe { OwnedFd::from_raw_fd(pidfd) })),
    }
}

/// The child's part, between the fork and the exec: its standard streams (the log, and no
/// input), the parent death signal (SIGKILL; the wicket ended before it was set? then no
/// VM), the soft open file limit, the file size limit (bytes: a guest's output to its VM's
/// log bounded, as a jail's) and no core (of a VM's memory), its working directory, its
/// disk's lock ([`disk::hold`]), the wicket's seccomp filter, and the program. Returns only
/// when that failed.
fn launch(
    request: &Request,
    log: BorrowedFd<'_>,
    null: BorrowedFd<'_>,
    wicket: libc::pid_t,
    filter: &Filter,
) -> io::Error {
    let prepared = (|| {
        // SAFETY: plain calls on our fds and our own process
        unsafe {
            // the forker blocks every signal, and std ignores SIGPIPE
            let mut all: libc::sigset_t = std::mem::zeroed();
            libc::sigfillset(&mut all);
            libc::sigprocmask(libc::SIG_UNBLOCK, &all, std::ptr::null_mut());
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
            // stdout and stderr to the log first: a failure from here on lands there
            check(libc::dup2(log.as_raw_fd(), 1))?;
            check(libc::dup2(log.as_raw_fd(), 2))?;
            check(libc::dup2(null.as_raw_fd(), 0))?;
            check(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL))?;
            if libc::getppid() != wicket {
                return Err(io::Error::other("the wicket ended before the VM started"));
            }
        }
        set_soft_open_files(request.open_files)?;
        // soft and hard, at most the hard one
        for (resource, limit) in [
            (libc::RLIMIT_FSIZE, request.file_size),
            (libc::RLIMIT_CORE, 0),
        ] {
            let mut current = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: plain getrlimit / setrlimit on a valid struct
            check(unsafe { libc::getrlimit(resource, &mut current) })?;
            let limit = limit.min(current.rlim_max);
            let limit = libc::rlimit {
                rlim_cur: limit,
                rlim_max: limit,
            };
            // SAFETY: as above
            check(unsafe { libc::setrlimit(resource, &limit) })?;
        }
        std::env::set_current_dir(&request.dir)?;
        if let Some(disk) = &request.disk {
            disk::hold(disk)?;
        }
        filter.install()
    })();
    match prepared {
        Ok(()) => std::process::Command::new(&request.program)
            .args(&request.args)
            .exec(),
        Err(e) => e,
    }
}

/// What a VM is forked with ([`Forker::fork`]).
#[derive(Debug)]
pub struct Launch {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    /// The VM's directory, made: its working directory.
    pub dir: PathBuf,
    /// Its log, made anew: its stdout and stderr.
    pub log: PathBuf,
    /// Its soft open file limit.
    pub open_files: u64,
    /// Its file size limit, bytes.
    pub file_size: u64,
    /// Its disk, whose lock it holds.
    pub disk: Option<PathBuf>,
    /// Its place, its part there and what it is given: its cgroup's.
    pub place: String,
    pub role: Role,
    pub size: Size,
}

/// The wicket's end of the forker. A thread of its own asks it for each VM in turn (the
/// forker forks one at a time), so no runtime thread waits for a fork; it holds the VMs'
/// cgroups too, where the wicket has them ([`Own`]), a cgroup's files written by it.
pub struct Forker {
    work: Sender<Work>,
    cgroups: Option<Arc<Mutex<Own>>>,
}

/// What the forking thread does next.
enum Work {
    Fork(Box<Fork>),
    /// A VM of the shared group was reaped: the group's weight is to follow.
    Settle,
}

/// A VM to fork, for the caller in the runtime `runtime`.
struct Fork {
    launch: Launch,
    runtime: Handle,
    /// For the VM's reaper.
    work: Sender<Work>,
    done: oneshot::Sender<Result<Child, BoxError>>,
}

impl Forker {
    /// Starts the forker, `program` (the wicket itself) in its forker mode: before the
    /// wicket's seccomp filter and its other threads. The wicket's end of the socket pair.
    pub fn start(program: &Path) -> io::Result<OwnedFd> {
        let mut pair = [0; 2];
        // SAFETY: socketpair into an array of two
        check(unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                0,
                pair.as_mut_ptr(),
            )
        })?;
        // SAFETY: two new fds of ours
        let (socket, theirs) =
            unsafe { (OwnedFd::from_raw_fd(pair[0]), OwnedFd::from_raw_fd(pair[1])) };
        // its end across its exec: no other process is spawned meanwhile
        // SAFETY: fcntl of our fd
        check(unsafe { libc::fcntl(theirs.as_raw_fd(), libc::F_SETFD, 0) })?;
        std::process::Command::new(program)
            .arg(FORK)
            .arg(std::process::id().to_string())
            .arg(theirs.as_raw_fd().to_string())
            // the wicket's environment (its token among it) is no VM's business; PATH
            // finds a bare --firecracker
            .env_clear()
            .envs(std::env::var_os("PATH").map(|path| ("PATH", path)))
            .stdin(Stdio::null())
            .spawn()
            .map_err(|e| io::Error::other(format!("starting the forker: {e}")))?;
        Ok(socket)
    }

    /// The forker at `socket`, asked by a thread of its own from here on; `cgroups`: the
    /// VMs' own, where the wicket has them.
    pub fn new(socket: OwnedFd, cgroups: Option<Cgroups>) -> io::Result<Forker> {
        let cgroups = cgroups.map(|cgroups| Arc::new(Mutex::new(Own::new(cgroups))));
        let (work, asked) = std::sync::mpsc::channel();
        let held = cgroups.clone();
        std::thread::Builder::new()
            .name("forking".into())
            .spawn(move || forking(socket, held, asked))?;
        Ok(Forker { work, cgroups })
    }

    /// Forks a VM of `launch`, in its cgroup where the wicket has them: the wicket's child,
    /// watched from then on in the caller's runtime ([`Child`]).
    pub async fn fork(&self, launch: Launch) -> Result<Child, BoxError> {
        let (done, forked) = oneshot::channel();
        let fork = Fork {
            launch,
            runtime: Handle::current(),
            work: self.work.clone(),
            done,
        };
        self.work
            .send(Work::Fork(Box::new(fork)))
            .map_err(|_| "no forking thread")?;
        forked.await.map_err(|_| "no forking thread")?
    }

    /// The place `place` is gone: its template's cgroup with it.
    pub fn forget(&self, place: &str) {
        if let Some(cgroups) = &self.cgroups {
            lock(cgroups).forget(place);
        }
    }
}

fn lock(cgroups: &Mutex<Own>) -> MutexGuard<'_, Own> {
    cgroups.lock().unwrap_or_else(|e| e.into_inner())
}

/// The forking thread: each VM asked of the forker at `socket` in turn, entered into its
/// cgroup first; the shared group's weight written once due. Until the wicket's end. The
/// forker gone, no VM starts any more: the wicket is told to stop (SIGTERM), once.
fn forking(socket: OwnedFd, cgroups: Option<Arc<Mutex<Own>>>, asked: Receiver<Work>) {
    let mut buffer = vec![0u8; MESSAGE];
    let mut gone = false;
    loop {
        let due = cgroups.as_ref().and_then(|cgroups| lock(cgroups).settle());
        let next = match due {
            None => asked.recv().ok(),
            Some(due) => match asked.recv_timeout(due) {
                Ok(next) => Some(next),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => None,
            },
        };
        let fork = match next {
            Some(Work::Fork(fork)) => fork,
            Some(Work::Settle) => continue,
            None => return,
        };
        let forked = match gone {
            true => Err("no VM starts any more".into()),
            false => fork_one(&socket, &mut buffer, cgroups.as_ref(), &fork),
        };
        if let Err(Lost::Forker(e)) = &forked
            && !gone
        {
            gone = true;
            eprintln!("the forker ended ({e}): no VM starts any more; stopping the wicket");
            // SAFETY: a signal to ourselves, which the wicket takes as its operator's
            unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
        }
        // its caller gave up (a start aborted): the VM is dropped, killed
        let _ = fork.done.send(forked.map_err(BoxError::from));
    }
}

/// Why a VM was not forked.
#[derive(Debug)]
enum Lost {
    /// The forker is gone.
    Forker(String),
    Vm(String),
}

impl From<&str> for Lost {
    fn from(e: &str) -> Lost {
        Lost::Forker(e.to_string())
    }
}

impl From<Lost> for BoxError {
    fn from(lost: Lost) -> BoxError {
        match lost {
            Lost::Forker(e) => format!("the forker ended: {e}").into(),
            Lost::Vm(e) => e.into(),
        }
    }
}

/// One VM: its log made, its cgroup entered ([`Own::enter`]), the forker asked ([`ask`]),
/// its reaper started ([`Child::watch`]).
fn fork_one(
    socket: &OwnedFd,
    buffer: &mut [u8],
    cgroups: Option<&Arc<Mutex<Own>>>,
    fork: &Fork,
) -> Result<Child, Lost> {
    let launch = &fork.launch;
    let log = std::fs::File::create(&launch.log)
        .map_err(|e| Lost::Vm(format!("{}: {e}", launch.log.display())))?;
    let entered = cgroups
        .map(|cgroups| lock(cgroups).enter(&launch.place, launch.role, &launch.size))
        .transpose()
        .map_err(|e| Lost::Vm(format!("the VM's cgroup: {e}")))?;
    let (held, cgroup) = entered.unzip();
    let request = Request {
        program: launch.program.clone(),
        args: launch.args.clone(),
        dir: launch.dir.clone(),
        open_files: launch.open_files,
        file_size: launch.file_size,
        disk: launch.disk.clone(),
        cgroup: cgroup.is_some(),
    };
    let held = held.zip(cgroups.cloned());
    match ask(socket, buffer, &request, log, cgroup) {
        Ok((pid, pidfd)) => Ok(Child::watch(pid, pidfd, held, fork)),
        Err(e) => {
            if let Some((held, cgroups)) = held {
                lock(&cgroups).leave(held);
            }
            Err(e)
        }
    }
}

/// Asks the forker at `socket` for the VM of `request`, with its `log` and `cgroup`: its
/// pid and a pidfd of it.
fn ask(
    socket: &OwnedFd,
    buffer: &mut [u8],
    request: &Request,
    log: std::fs::File,
    cgroup: Option<OwnedFd>,
) -> Result<(libc::pid_t, OwnedFd), Lost> {
    let bytes = serde_json::to_vec(request).map_err(|e| Lost::Vm(e.to_string()))?;
    let fds: Vec<RawFd> = std::iter::once(log.as_raw_fd())
        .chain(cgroup.iter().map(|cgroup| cgroup.as_raw_fd()))
        .collect();
    send(socket.as_raw_fd(), &bytes, &fds).map_err(|e| match e.raw_os_error() {
        Some(libc::EMSGSIZE) => Lost::Vm(format!("a request of {} bytes: {e}", bytes.len())),
        _ => Lost::Forker(e.to_string()),
    })?;
    let (n, fds) = loop {
        match receive(socket.as_raw_fd(), buffer) {
            Ok((0, _)) => return Err("no answer".into()),
            Ok(received) => break received,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Lost::Forker(e.to_string())),
        }
    };
    let answer: Answer = serde_json::from_slice(&buffer[..n])
        .map_err(|e| Lost::Forker(format!("an answer it cannot read: {e}")))?;
    match (answer, fds.into_iter().next()) {
        (Ok(pid), Some(pidfd)) => Ok((pid, pidfd)),
        (Ok(_), None) => Err(Lost::Vm("the forker sent no pidfd of the VM".into())),
        (Err(e), _) => Err(Lost::Vm(e)),
    }
}

/// How a VM's process ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exit {
    Code(i32),
    Signal(i32),
}

impl std::fmt::Display for Exit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exit::Code(code) => write!(f, "exit {code}"),
            Exit::Signal(signal) => write!(f, "signal {signal}"),
        }
    }
}

/// A VM's `firecracker` the forker forked, the wicket's child: killed as it is dropped,
/// and reaped by a task of the runtime it was forked in, which tells how it ended -- the
/// kernel's OOM killer's kill as its cgroup counted it or, without one, as the host's count
/// of them rose (`usage::Meter::host_oom_killed`) and no one of the wicket killed it -- and
/// gives its cgroup back.
pub struct Child {
    pidfd: Arc<OwnedFd>,
    /// The wicket killed it.
    killed: Arc<AtomicBool>,
    exited: watch::Receiver<Option<VmEnd>>,
    meter: Arc<Meter>,
}

impl Child {
    /// The child `pid` of the pidfd `pidfd`, forked for `fork`, holding `held` of the
    /// cgroups: watched from here on.
    fn watch(
        pid: libc::pid_t,
        pidfd: OwnedFd,
        held: Option<(Held, Arc<Mutex<Own>>)>,
        fork: &Fork,
    ) -> Child {
        let pidfd = Arc::new(pidfd);
        let meter = Meter::start(pid, Some(pidfd.clone()));
        let killed = Arc::new(AtomicBool::new(false));
        let (tell, exited) = watch::channel(None);
        let child = Child {
            pidfd: pidfd.clone(),
            killed: killed.clone(),
            exited,
            meter: meter.clone(),
        };
        let work = fork.work.clone();
        fork.runtime.spawn(async move {
            let exit = reaped(pidfd).await;
            let oom = match held {
                Some((held, cgroups)) => {
                    let shared = matches!(held, Held::Serving { shared: true, .. });
                    let oom = lock(&cgroups).leave(held);
                    if shared {
                        let _ = work.send(Work::Settle);
                    }
                    oom
                }
                None => {
                    exit == Some(Exit::Signal(libc::SIGKILL))
                        && !killed.load(Ordering::Relaxed)
                        && meter.host_oom_killed()
                }
            };
            let status = exit.map_or_else(|| "ended".to_string(), |exit| exit.to_string());
            let _ = tell.send(Some(VmEnd {
                status,
                log: String::new(),
                oom,
            }));
        });
        child
    }

    /// How it ended, once it did.
    pub fn exited(&self) -> &watch::Receiver<Option<VmEnd>> {
        &self.exited
    }

    /// What reads what it uses.
    pub fn meter(&self) -> &Arc<Meter> {
        &self.meter
    }

    /// Kills it (SIGKILL): its end is told as any.
    pub fn kill(&self) {
        self.killed.store(true, Ordering::Relaxed);
        // SAFETY: a signal to the process of our pidfd, whatever its pid is by now
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.pidfd.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        self.kill();
    }
}

/// How the child of `pidfd` ended, once it did, reaped; None where the kernel does not
/// tell.
async fn reaped(pidfd: Arc<OwnedFd>) -> Option<Exit> {
    // readable once the process ended; looked at now and then where it cannot be awaited
    // SAFETY: the pidfd stays open, the same fd, as long as the AsyncFd holds it
    let ready = unsafe { AsyncFd::register_with_interest(pidfd.clone(), Interest::READABLE) }.ok();
    loop {
        let guard = match &ready {
            Some(ready) => Some(ready.readable().await.ok()?),
            None => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                None
            }
        };
        match waited(&pidfd) {
            Ok(None) => {
                if let Some(mut guard) = guard {
                    guard.clear_ready();
                }
            }
            ended => return ended.ok().flatten(),
        }
    }
}

/// How the child of `pidfd` ended, reaping it; None while it lives on.
fn waited(pidfd: &OwnedFd) -> io::Result<Option<Exit>> {
    // SAFETY: a zeroed siginfo_t is valid, filled by waitid
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: waitid of the pidfd, without waiting
    check(unsafe {
        libc::waitid(
            libc::P_PIDFD,
            pidfd.as_raw_fd() as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG,
        )
    })?;
    // SAFETY: filled by the successful call; a pid of 0, nothing ended yet
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    // SAFETY: as above, of a child that ended
    let status = unsafe { info.si_status() };
    Ok(Some(match info.si_code {
        libc::CLD_EXITED => Exit::Code(status),
        _ => Exit::Signal(status),
    }))
}
