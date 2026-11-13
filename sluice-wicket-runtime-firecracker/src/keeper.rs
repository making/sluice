//! The keeper: the wicket's privileged half in jail mode (`jail-ids`). Started as root,
//! it runs the wicket as `jail-user` -- the tunnel, the routes, everything reading what
//! the guests send -- and spawns, kills and reaps the VMs at the wicket's request, each
//! in a jail of its own (`jail`):
//!
//! ```text
//! keeper (root) --runs--> wicket (jail-user) --requests, socket pair--> keeper
//!       \--clone3--> firecracker, jailed (an id of jail-ids, its namespaces, root, cgroup)
//! ```
//!
//! The wicket drives its VMs itself (their API and vsock sockets are in directories of
//! its group), and reads what each uses of its process (a pidfd of it comes with the spawn's
//! answer); the keeper tells how each ended, a kill of the OOM killer's as its cgroup counted
//! it. The keeper trusts none of its requests: a VM gets the files the wicket
//! opened (passed as fds: no path of the wicket's is opened as root), an id, directory
//! and cgroup of the keeper's choosing, and its route's `firecracker`, which the keeper
//! read from its own setting.
//!
//! The keeper forwards SIGTERM, SIGINT and SIGHUP to the wicket and ends with it, its
//! VMs killed and their files and cgroups removed; the wicket and the VMs die with the
//! keeper (their parent death signal).

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::io;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::io::unix::AsyncFd;
use tokio::sync::{mpsc, oneshot, watch};

use crate::BoxError;
use crate::config::JailSettings;
use crate::jail::{self, Access, Cgroup, Cgroups, Ids, Jail, Limits, Template};
use crate::seccomp::Filter;

/// The environment variable naming the wicket's end of the socket pair.
pub const KEEPER_FD: &str = "SLUICE_WICKET_RUNTIME_FIRECRACKER_KEEPER_FD";

/// The fd the wicket's end is given.
const WICKET_FD: RawFd = 3;

/// A message's bytes at most.
const MESSAGE: usize = 16 * 1024;

/// Fds one request carries at most.
const MAX_FDS: usize = 3;

/// What a VM is charged besides its guest memory, its log and a template's snapshot, in
/// MiB, for `memory-mib` MiB of guest memory: `firecracker`'s heap and KVM's structures (about
/// 3 MiB measured), a vsock stream's buffers (64KiB each at most), and the page tables
/// of the guest memory (about 0.25% of what it touched).
pub fn vm_overhead_mib(mem: u64) -> u64 {
    32 + mem / 128
}

/// A VM's log at most (`RLIMIT_FSIZE`; past it Firecracker ends the VM): its own lines,
/// and its guest's console with `SLUICE_WICKET_RUNTIME_FIRECRACKER_SERIAL_CONSOLE` (or a
/// guest re-enabling its serial device).
const LOG_MIB: u64 = 16;

/// A VM's `RLIMIT_FSIZE`, bytes, for `memory-mib` MiB of guest memory: its log, and a
/// template's snapshot, which it writes.
pub fn vm_file_size(role: Role, mem: u64) -> u64 {
    match role {
        Role::Template => (mem + LOG_MIB) << 20,
        Role::Boot | Role::Clone => LOG_MIB << 20,
    }
}

/// Threads of a VM besides its vCPUs (its VMM and API threads, a few more).
const THREADS: u64 = 16;

/// `RLIMIT_NOFILE` of a VM at most, whatever the wicket asks.
const MAX_OPEN_FILES: u64 = 1 << 20;

/// The copy of the host CPU's files the jails hold (`jail::JAIL_HOST_CPU`), in the
/// keeper's directory.
const HOST_CPU_COPY: &str = "cpu0";

/// How long a VM's spawn may wait for an id to be freed (the reap of a VM killed).
const ID_WAIT_MS: i32 = 5000;

/// A VM's part in its route.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Booted, serving the route's connections.
    Boot,
    /// A cloned route's template: booted, snapshotted into the route's
    /// directory.
    Template,
    /// Restored from the route's snapshot (sealed).
    Clone,
}

/// What a VM is given, which its cgroup bounds.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Size {
    pub vcpus: u32,
    /// Its guest memory, MiB.
    pub mem: u32,
    /// The host CPU time it may take, percent of one CPU; None = its vCPUs bound it.
    pub cpu: Option<u32>,
}

/// A request of the wicket's.
#[derive(Serialize, Deserialize, Debug)]
enum Request {
    /// A route's VMs are to come, from the files passed with it: the kernel, the rootfs,
    /// and an app's disk. Its `firecracker` is copied, its directory made.
    Open,
    /// A VM of `route`.
    Spawn {
        route: u64,
        role: Role,
        size: Size,
        open_files: u64,
    },
    /// Kill the VM (unanswered: its exit is reported).
    Kill { vm: u64 },
    /// The route's template ended, make its snapshot read-only: answered once done.
    Seal { route: u64 },
    /// The route ended: its files and cgroup go once its VMs did (unanswered).
    Close { route: u64 },
}

#[derive(Serialize, Deserialize, Debug)]
struct Call {
    /// 0: unanswered.
    id: u64,
    request: Request,
}

#[derive(Serialize, Deserialize, Debug)]
enum Answer {
    Opened {
        route: u64,
        /// The route's snapshot directory.
        snapshot: PathBuf,
    },
    /// Sent with a pidfd of the VM's `firecracker`.
    Spawned {
        vm: u64,
        /// The VM's directory: `firecracker.log`, and `run/` holding its sockets.
        dir: PathBuf,
        /// Its `firecracker`'s pid, the wicket's PID namespace being the keeper's.
        pid: libc::pid_t,
    },
    Sealed,
}

/// A message of the keeper's.
#[derive(Serialize, Deserialize, Debug)]
enum Message {
    Reply {
        id: u64,
        result: Result<Answer, String>,
    },
    /// A VM ended: `exit N`, `signal N`, the tail of its log (which the id's next VM
    /// truncates), and whether the kernel's OOM killer took it (its cgroup said so).
    Exited {
        vm: u64,
        status: String,
        log: String,
        oom: bool,
    },
}

/// How a VM the keeper spawned ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmEnd {
    /// `exit N`, `signal N`.
    pub status: String,
    /// The tail of its log.
    pub log: String,
    /// The kernel's OOM killer took it.
    pub oom: bool,
}

/// Sends one message with `fds` on the socket `fd`.
fn send(fd: RawFd, bytes: &[u8], fds: &[RawFd]) -> io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr() as *mut libc::c_void,
        iov_len: bytes.len(),
    };
    // SAFETY: a zeroed msghdr is valid (no name, no control)
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    let size = std::mem::size_of_val(fds);
    // SAFETY: CMSG_SPACE is arithmetic
    let mut control = vec![0u8; unsafe { libc::CMSG_SPACE(size as u32) } as usize];
    if !fds.is_empty() {
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = control.len();
        // SAFETY: the control buffer holds one header and `fds`
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(size as u32) as usize;
            std::ptr::copy_nonoverlapping(fds.as_ptr().cast::<u8>(), libc::CMSG_DATA(header), size);
        }
    }
    // SAFETY: a valid message on our socket
    match unsafe { libc::sendmsg(fd, &message, libc::MSG_NOSIGNAL) } {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

/// Receives one message into `buffer`, and the fds it carries; 0 bytes: the peer
/// ended.
fn receive(fd: RawFd, buffer: &mut [u8]) -> io::Result<(usize, Vec<OwnedFd>)> {
    let mut iov = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    // SAFETY: CMSG_SPACE is arithmetic
    let mut control = vec![
        0u8;
        unsafe { libc::CMSG_SPACE((MAX_FDS * std::mem::size_of::<RawFd>()) as u32) }
            as usize
    ];
    // SAFETY: a zeroed msghdr is valid
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len();
    // SAFETY: a valid message to fill from our socket
    let n = unsafe { libc::recvmsg(fd, &mut message, libc::MSG_CMSG_CLOEXEC) };
    if n == -1 {
        return Err(io::Error::last_os_error());
    }
    let mut fds = Vec::new();
    // SAFETY: walking the control messages the kernel wrote
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(header);
                let count = ((*header).cmsg_len - libc::CMSG_LEN(0) as usize)
                    / std::mem::size_of::<RawFd>();
                for i in 0..count {
                    let fd = std::ptr::read_unaligned(data.cast::<RawFd>().add(i));
                    fds.push(OwnedFd::from_raw_fd(fd));
                }
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(io::Error::other("a message too long, or too many fds"));
    }
    Ok((n as usize, fds))
}

fn check(ret: libc::c_int) -> io::Result<()> {
    match ret {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

/// The user the wicket runs as.
#[derive(Debug, PartialEq, Eq)]
struct User {
    uid: u32,
    gid: u32,
    groups: Vec<u32>,
}

impl User {
    /// `<uid>:<gid>`, or a user's name (its groups from the group database).
    fn resolve(spec: &str) -> io::Result<User> {
        if let Some((uid, gid)) = spec.split_once(':') {
            let number = |n: &str| {
                n.parse::<u32>().map_err(|_| {
                    io::Error::other(format!("jail-user {spec}: expected <name> or <uid>:<gid>"))
                })
            };
            let (uid, gid) = (number(uid)?, number(gid)?);
            return Ok(User {
                uid,
                gid,
                groups: vec![gid],
            });
        }
        let name = CString::new(spec).map_err(|_| io::Error::other("jail-user: a NUL byte"))?;
        // SAFETY: getpwnam_r into buffers of their sizes
        let (uid, gid) = unsafe {
            let mut entry: libc::passwd = std::mem::zeroed();
            let mut buffer = vec![0 as libc::c_char; 16 * 1024];
            let mut found = std::ptr::null_mut();
            let rc = libc::getpwnam_r(
                name.as_ptr(),
                &mut entry,
                buffer.as_mut_ptr(),
                buffer.len(),
                &mut found,
            );
            if rc != 0 || found.is_null() {
                return Err(io::Error::other(format!("jail-user {spec}: no such user")));
            }
            (entry.pw_uid, entry.pw_gid)
        };
        let mut groups = vec![0 as libc::gid_t; 64];
        loop {
            let mut count = groups.len() as libc::c_int;
            // SAFETY: getgrouplist into a buffer of `count`
            let rc =
                unsafe { libc::getgrouplist(name.as_ptr(), gid, groups.as_mut_ptr(), &mut count) };
            if rc != -1 {
                groups.truncate(count as usize);
                break;
            }
            groups.resize(count as usize + 16, 0);
        }
        Ok(User { uid, gid, groups })
    }
}

/// Runs the keeper: as root, with `jail`, the routes' `firecracker` and the VMs' files
/// under `state` (default `/run`), the wicket's command line its own. Returns the
/// wicket's exit code.
pub fn run(
    jail: &JailSettings,
    firecracker: PathBuf,
    state: Option<PathBuf>,
    max_instances: usize,
) -> ExitCode {
    // first, single-threaded: the wicket and the jails inherit it
    if let Err(e) = Filter::keeper().install() {
        eprintln!("keeper: the seccomp filter: {e}");
        return ExitCode::FAILURE;
    }
    match Keeper::start(jail, firecracker, state, max_instances) {
        Ok(mut keeper) => {
            let code = keeper.serve();
            keeper.close();
            code
        }
        Err(e) => {
            eprintln!("keeper: {e}");
            ExitCode::FAILURE
        }
    }
}

/// A route, as the keeper knows it.
struct Route {
    /// Its copy of `firecracker`.
    firecracker: OwnedFd,
    /// `route-<n>`: its rootfs and kernel (attached in the template), its snapshot
    /// directory (`snapshot`).
    dir: PathBuf,
    /// Its template's cgroup, made with it.
    cgroup: Option<Cgroup>,
    /// An app's disk: attached in the template, its VM's to write once given its id, and
    /// locked per VM on an open file of its own.
    disk: Option<OwnedFd>,
    sealed: bool,
    closing: bool,
}

/// A VM alive (not reaped).
struct Vm {
    route: u64,
    role: Role,
    id: u32,
    /// Whether its CPU is capped: its cgroup's class.
    capped: bool,
    pid: libc::pid_t,
    /// `vm-<n>`, its id's: its sockets removed once it is reaped.
    dir: PathBuf,
}

/// A spawn waiting for an id.
struct Waiting {
    call: u64,
    since: std::time::Instant,
    route: u64,
    role: Role,
    size: Size,
    open_files: u64,
}

struct Keeper {
    /// The keeper's end of the socket pair; None once the wicket closed its end.
    socket: Option<OwnedFd>,
    signals: OwnedFd,
    /// The wicket's pid, and its wait status once reaped.
    wicket: libc::pid_t,
    ended: Option<libc::c_int>,
    /// The wicket's user: a disk it passes is its file, or a VM's of the ids before.
    user: u32,
    /// The wicket's group: the VMs' directories are its.
    group: u32,
    ids: Ids,
    free: BTreeSet<u32>,
    /// The ids whose directory (`vm-<n>`) is made, kept for the next VM of the id.
    made: HashSet<u32>,
    cgroups: Cgroups,
    /// The clones' cgroups by id and whether their CPU is capped, made with the first
    /// and reused.
    clones: HashMap<(u32, bool), Cgroup>,
    /// The keeper's directory: `jail` (the template namespace's root), `route-<n>`,
    /// `vm-<n>`.
    dir: PathBuf,
    /// The mount namespace the jails' are copied from.
    template: Template,
    /// `/dev/null`, the VMs' stdin.
    null: OwnedFd,
    pidfd: OwnedFd,
    firecracker: PathBuf,
    routes: HashMap<u64, Route>,
    next_route: u64,
    vms: HashMap<u64, Vm>,
    pids: HashMap<libc::pid_t, u64>,
    next_vm: u64,
    waiting: VecDeque<Waiting>,
    /// Seals waiting for their route's template to be reaped: call, route.
    seals: Vec<(u64, u64)>,
    /// The VMs' seccomp filter, installed in each jail.
    filter: Filter,
}

impl Keeper {
    fn start(
        jail: &JailSettings,
        firecracker: PathBuf,
        state: Option<PathBuf>,
        max_instances: usize,
    ) -> io::Result<Keeper> {
        let user = User::resolve(&jail.user)?;
        if user.uid == 0 {
            return Err(io::Error::other(
                "jail-user is root: the wicket runs as a user of its own",
            ));
        }
        let ids = jail.ids;
        if ids.contains(user.uid) || ids.contains(user.gid) {
            return Err(io::Error::other(format!(
                "jail-ids {}-{} hold jail-user's uid or gid",
                ids.first, ids.last
            )));
        }
        if (ids.count() as usize) < max_instances {
            return Err(io::Error::other(format!(
                "jail-ids {}-{}: {} ids for --max-instances {max_instances}: one per VM alive",
                ids.first,
                ids.last,
                ids.count()
            )));
        }
        let firecracker = executable(&firecracker)?;
        // the keeper's directory, the wicket's group's (it reaches its VMs' sockets)
        let base = state.unwrap_or_else(|| PathBuf::from("/run"));
        let base = std::path::absolute(base)?;
        std::fs::create_dir_all(&base)?;
        let dir = base.join(format!(
            "sluice-wicket-runtime-firecracker-{}",
            std::process::id()
        ));
        let longest = dir
            .join(format!("vm-{}", ids.count() - 1))
            .join("run/api.sock");
        if longest.as_os_str().len() > 107 {
            return Err(io::Error::other(format!(
                "{}: the VMs' sockets need a path below 108 bytes; choose a shorter --state-dir",
                longest.display()
            )));
        }
        std::fs::DirBuilder::new().mode(0o750).create(&dir)?;
        let started = Self::started(jail, user, ids, firecracker, dir.clone());
        if started.is_err() {
            let _ = std::fs::remove_dir_all(&dir);
        }
        started
    }

    /// The keeper started in its directory `dir`: the wicket spawned.
    fn started(
        jail: &JailSettings,
        user: User,
        ids: Ids,
        firecracker: PathBuf,
        dir: PathBuf,
    ) -> io::Result<Keeper> {
        owned(&dir, 0, user.gid, 0o750)?;
        // the wicket's, for the paths of its command line; the keeper's is / from here on
        let working = std::env::current_dir()?;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(dir.join("jail"))?;
        if !crate::state::on_tmpfs(&dir) {
            eprintln!(
                "keeper: warning: {} is not on a tmpfs: a cloned route's snapshot (what its \
                 template wrote of its memory) is written to disk",
                dir.display()
            );
        }
        if jail::JAIL_HOST_CPU {
            jail::copy_host_cpu(Path::new(jail::HOST_CPU), &dir.join(HOST_CPU_COPY))?;
        }
        let template = Template::build(&dir)?;
        let null = std::fs::File::open("/dev/null")?.into();
        let pidfd = jail::own_pidfd()?;
        let cgroups = Cgroups::setup(jail.cgroup.as_deref())?;

        // the signals the keeper reads (blocked before the wicket starts: its exit is
        // never missed; the wicket unblocks them)
        // SAFETY: plain signal mask and signalfd calls on a set we fill
        let signals = unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            for signal in [libc::SIGCHLD, libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
                libc::sigaddset(&mut set, signal);
            }
            check(libc::sigprocmask(
                libc::SIG_BLOCK,
                &set,
                std::ptr::null_mut(),
            ))?;
            let fd = libc::signalfd(-1, &set, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK);
            check(fd)?;
            OwnedFd::from_raw_fd(fd)
        };
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
        // a wicket not reading never stalls the keeper for long
        let timeout = libc::timeval {
            tv_sec: 1,
            tv_usec: 0,
        };
        // SAFETY: setsockopt with a timeval of its size
        check(unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDTIMEO,
                (&timeout as *const libc::timeval).cast(),
                std::mem::size_of::<libc::timeval>() as libc::socklen_t,
            )
        })?;
        let wicket = match spawn_wicket(&user, theirs, &working) {
            Ok(wicket) => wicket,
            Err(e) => {
                cgroups.close();
                return Err(e);
            }
        };
        // the VMs need no network: an empty namespace of the keeper's, theirs alike (the
        // wicket, started before, keeps the host's; it dies with the keeper on a failure)
        // SAFETY: unshare of the network namespace
        if let Err(e) = check(unsafe { libc::unshare(libc::CLONE_NEWNET) }) {
            cgroups.close();
            return Err(io::Error::other(format!(
                "a network namespace for the VMs: {e}"
            )));
        }
        println!(
            "keeper: the wicket runs as {}:{} (pid {wicket}); its VMs in jails as ids {}-{}, \
             their files in {}, their cgroups in {}",
            user.uid,
            user.gid,
            ids.first,
            ids.last,
            dir.display(),
            cgroups.dir().display()
        );
        Ok(Keeper {
            socket: Some(socket),
            signals,
            wicket,
            ended: None,
            user: user.uid,
            group: user.gid,
            free: (ids.first..=ids.last).collect(),
            ids,
            made: HashSet::new(),
            cgroups,
            clones: HashMap::new(),
            dir,
            template,
            null,
            pidfd,
            firecracker,
            routes: HashMap::new(),
            next_route: 0,
            vms: HashMap::new(),
            pids: HashMap::new(),
            next_vm: 0,
            waiting: VecDeque::new(),
            seals: Vec::new(),
            filter: Filter::unprivileged(),
        })
    }

    /// Serves the wicket's requests until it ended; its exit code.
    fn serve(&mut self) -> ExitCode {
        let mut buffer = vec![0u8; MESSAGE];
        while self.ended.is_none() {
            let mut fds = vec![libc::pollfd {
                fd: self.signals.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            }];
            if let Some(socket) = &self.socket {
                fds.push(libc::pollfd {
                    fd: socket.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                });
            }
            // a spawn waiting for an id gives up in time
            let timeout = if self.waiting.is_empty() { -1 } else { 100 };
            // SAFETY: poll on an array of its length
            if unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) } == -1 {
                let e = io::Error::last_os_error();
                if e.kind() != io::ErrorKind::Interrupted {
                    eprintln!("keeper: poll: {e}");
                    break;
                }
                continue;
            }
            if fds[0].revents != 0 {
                self.signalled();
            }
            if fds.get(1).is_some_and(|fd| fd.revents != 0) {
                self.read(&mut buffer);
            }
            self.expire();
        }
        let status = self.ended.unwrap_or(1);
        if libc::WIFEXITED(status) {
            ExitCode::from(libc::WEXITSTATUS(status) as u8)
        } else {
            ExitCode::from(128u8.wrapping_add(libc::WTERMSIG(status) as u8))
        }
    }

    /// Reads the signals pending: reaps on SIGCHLD (they coalesce: a reap takes every
    /// child ended), forwards the others to the wicket.
    fn signalled(&mut self) {
        loop {
            // SAFETY: a zeroed siginfo is valid to fill
            let mut info: libc::signalfd_siginfo = unsafe { std::mem::zeroed() };
            let size = std::mem::size_of::<libc::signalfd_siginfo>();
            // SAFETY: a read of one siginfo into a buffer of its size (non-blocking)
            let n = unsafe {
                libc::read(
                    self.signals.as_raw_fd(),
                    (&mut info as *mut libc::signalfd_siginfo).cast(),
                    size,
                )
            };
            if n != size as isize {
                return;
            }
            match info.ssi_signo as libc::c_int {
                libc::SIGCHLD => self.reap(),
                signal => {
                    // SAFETY: kill of the wicket, our child
                    unsafe { libc::kill(self.wicket, signal) };
                }
            }
        }
    }

    /// Reaps every child ended: the wicket, a VM, an orphan (the keeper may be PID 1).
    fn reap(&mut self) {
        loop {
            let mut status = 0;
            // SAFETY: waitpid on any child, without blocking
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid <= 0 {
                return;
            }
            if pid == self.wicket {
                self.ended = Some(status);
            } else if let Some(vm) = self.pids.remove(&pid) {
                self.ended_vm(vm, status);
            }
        }
    }

    /// A VM was reaped: its exit reported with its log's tail and whether the OOM killer took
    /// it (its cgroup's count, read before the cgroup holds another VM), its `run` emptied,
    /// its id freed; the seals and spawns waiting on it go on.
    fn ended_vm(&mut self, vm: u64, status: libc::c_int) {
        let Some(ended) = self.vms.remove(&vm) else {
            return;
        };
        let cgroup = match ended.role {
            Role::Template => {
                (self.routes.get_mut(&ended.route)).and_then(|route| route.cgroup.as_mut())
            }
            Role::Boot | Role::Clone => self.clones.get_mut(&(ended.id, ended.capped)),
        };
        let oom = cgroup.is_some_and(Cgroup::oom_killed);
        let log = crate::vm::log_tail(&ended.dir.join("firecracker.log"));
        // the id's next VM finds its run directory empty
        if let Ok(entries) = std::fs::read_dir(ended.dir.join("run")) {
            for entry in entries.flatten() {
                let _ = std::fs::remove_file(entry.path())
                    .or_else(|_| std::fs::remove_dir_all(entry.path()));
            }
        }
        self.free.insert(ended.id);
        self.tell(
            &Message::Exited {
                vm,
                status: describe(status),
                log,
                oom,
            },
            None,
        );
        if ended.role == Role::Template {
            let (now, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.seals)
                .into_iter()
                .partition(|(_, route)| *route == ended.route);
            self.seals = later;
            for (call, route) in now {
                let result = self.seal(route).map(|()| Answer::Sealed);
                self.reply(call, result);
            }
        }
        self.closed(ended.route);
        while !self.waiting.is_empty() && !self.free.is_empty() {
            let waiting = self.waiting.pop_front().expect("not empty");
            let spawned = self.spawn(&waiting);
            self.reply_spawned(waiting.call, spawned);
        }
    }

    /// Fails the spawns that waited too long for an id.
    fn expire(&mut self) {
        let limit = std::time::Duration::from_millis(ID_WAIT_MS as u64);
        while self
            .waiting
            .front()
            .is_some_and(|waiting| waiting.since.elapsed() > limit)
        {
            let waiting = self.waiting.pop_front().expect("not empty");
            self.reply(
                waiting.call,
                Err(format!(
                    "no id of jail-ids {}-{} freed within {ID_WAIT_MS}ms",
                    self.ids.first, self.ids.last
                )),
            );
        }
    }

    /// Reads one request and handles it.
    fn read(&mut self, buffer: &mut [u8]) {
        let Some(socket) = &self.socket else {
            return;
        };
        let (n, fds) = match receive(socket.as_raw_fd(), buffer) {
            Ok((0, _)) | Err(_) => {
                // the wicket ended (its exit follows) or broke its end
                self.socket = None;
                return;
            }
            Ok(received) => received,
        };
        let call: Call = match serde_json::from_slice(&buffer[..n]) {
            Ok(call) => call,
            Err(e) => {
                eprintln!("keeper: a request it cannot read: {e}");
                return;
            }
        };
        let result = match call.request {
            Request::Open => self.open(fds),
            Request::Spawn {
                route,
                role,
                size,
                open_files,
            } => {
                let waiting = Waiting {
                    call: call.id,
                    since: std::time::Instant::now(),
                    route,
                    role,
                    size,
                    open_files,
                };
                if self.free.is_empty() {
                    // answered once an id is freed, or in time
                    self.waiting.push_back(waiting);
                    return;
                }
                let spawned = self.spawn(&waiting);
                self.reply_spawned(call.id, spawned);
                return;
            }
            Request::Kill { vm } => {
                if let Some(vm) = self.vms.get(&vm) {
                    // SAFETY: kill of our child, not reaped yet
                    unsafe { libc::kill(vm.pid, libc::SIGKILL) };
                }
                return;
            }
            Request::Seal { route } => {
                let template = self
                    .vms
                    .values()
                    .find(|vm| vm.route == route && vm.role == Role::Template);
                if let Some(template) = template {
                    // answered once it is reaped: no write of its after the seal
                    // SAFETY: kill of our child, not reaped yet
                    unsafe { libc::kill(template.pid, libc::SIGKILL) };
                    self.seals.push((call.id, route));
                    return;
                }
                self.seal(route).map(|()| Answer::Sealed)
            }
            Request::Close { route } => {
                if let Some(closing) = self.routes.get_mut(&route) {
                    closing.closing = true;
                }
                self.closed(route);
                return;
            }
        };
        self.reply(call.id, result);
    }

    fn reply(&self, id: u64, result: Result<Answer, String>) {
        self.tell(&Message::Reply { id, result }, None);
    }

    /// Answers the spawn `call`: the VM, sent with its pidfd.
    fn reply_spawned(&self, call: u64, spawned: Result<(Answer, OwnedFd), String>) {
        match spawned {
            Ok((answer, pidfd)) => {
                let reply = Message::Reply {
                    id: call,
                    result: Ok(answer),
                };
                self.tell(&reply, Some(&pidfd));
            }
            Err(e) => self.reply(call, Err(e)),
        }
    }

    fn tell(&self, message: &Message, fd: Option<&OwnedFd>) {
        let Some(socket) = &self.socket else {
            return;
        };
        let bytes = serde_json::to_vec(message).expect("a message serializes");
        let fds: Vec<RawFd> = fd.iter().map(|fd| fd.as_raw_fd()).collect();
        if let Err(e) = send(socket.as_raw_fd(), &bytes, &fds) {
            eprintln!("keeper: a message to the wicket lost: {e}");
        }
    }

    /// A route from the wicket's files: its kernel and rootfs, regular files, and its disk: a
    /// regular file open for writing, the wicket's own or a VM's before (nothing else of the
    /// host's is given away to a VM's id, [`Keeper::jail`]).
    fn open(&mut self, mut fds: Vec<OwnedFd>) -> Result<Answer, String> {
        let disk = (fds.len() > 2).then(|| fds.pop()).flatten();
        let [kernel, rootfs]: [OwnedFd; 2] = fds
            .try_into()
            .map_err(|_| "a route needs its kernel and rootfs".to_string())?;
        for (name, fd) in [("kernel", &kernel), ("rootfs", &rootfs)] {
            let mode = jail::mode(fd.as_fd()).map_err(|e| format!("the route's {name}: {e}"))?;
            if let Some(why) = jail::unshareable(mode) {
                return Err(format!("the route's {name}: {why}"));
            }
        }
        if let Some(disk) = &disk {
            let file = fstat(disk).map_err(|e| format!("the route's disk: {e}"))?;
            // SAFETY: fcntl of an fd of ours
            let flags = unsafe { libc::fcntl(disk.as_raw_fd(), libc::F_GETFL) };
            if file.st_mode & libc::S_IFMT != libc::S_IFREG
                || flags & libc::O_ACCMODE != libc::O_RDWR
                || (file.st_uid != self.user && !self.ids.contains(file.st_uid))
            {
                return Err(
                    "the route's disk: a regular file of the wicket's, open for writing".into(),
                );
            }
        }
        let firecracker = jail::sealed_copy(&self.firecracker)
            .map_err(|e| format!("a copy of {}: {e}", self.firecracker.display()))?;
        let route = self.next_route;
        self.next_route += 1;
        let name = PathBuf::from(format!("route-{route}"));
        let dir = self.dir.join(&name);
        // its files attached in the template, over empty files of its directory
        let made = (|| {
            std::fs::DirBuilder::new().mode(0o750).create(&dir)?;
            owned(&dir, 0, self.group, 0o750)?;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(dir.join("snapshot"))?;
            let files = [
                (jail::ROUTE_KERNEL, Some(&kernel), Access::ReadOnly),
                (jail::ROUTE_ROOTFS, Some(&rootfs), Access::ReadOnly),
                (jail::ROUTE_DISK, disk.as_ref(), Access::ReadWrite),
            ];
            for (file, fd, access) in files {
                let Some(fd) = fd else { continue };
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(dir.join(file))?;
                self.template.attach(fd.as_fd(), &name.join(file), access)?;
            }
            Ok::<_, io::Error>(())
        })();
        if let Err(e) = made {
            self.forget(&name);
            return Err(format!("{}: {e}", dir.display()));
        }
        let snapshot = dir.join("snapshot");
        self.routes.insert(
            route,
            Route {
                firecracker,
                dir,
                cgroup: None,
                disk,
                sealed: false,
                closing: false,
            },
        );
        Ok(Answer::Opened { route, snapshot })
    }

    /// Removes the route `name`'s files: detached from the template, its directory
    /// removed.
    fn forget(&self, name: &Path) {
        for file in [jail::ROUTE_KERNEL, jail::ROUTE_ROOTFS, jail::ROUTE_DISK] {
            self.template.detach(&name.join(file));
        }
        let _ = std::fs::remove_dir_all(self.dir.join(name));
    }

    /// Spawns a VM in a jail: a free id (there is one), its directory and cgroup; and a pidfd
    /// of it.
    fn spawn(&mut self, request: &Waiting) -> Result<(Answer, OwnedFd), String> {
        let route = self
            .routes
            .get(&request.route)
            .filter(|route| !route.closing)
            .ok_or_else(|| format!("no route {}", request.route))?;
        let template_alive = self
            .vms
            .values()
            .any(|vm| vm.route == request.route && vm.role == Role::Template);
        match request.role {
            Role::Template if route.sealed || template_alive => {
                return Err("a route has one template".into());
            }
            Role::Clone if !route.sealed => return Err("the route's snapshot is not sealed".into()),
            Role::Template | Role::Clone if route.disk.is_some() => {
                return Err("a disk has one VM: a booted one".into());
            }
            _ => {}
        }
        let id = self.free.pop_first().expect("an id is free");
        let dir = self.dir.join(format!("vm-{}", id - self.ids.first));
        let (pid, pidfd) = match self.jail(id, &dir, request) {
            Ok(spawned) => spawned,
            Err(e) => {
                self.free.insert(id);
                return Err(format!("a jail for the VM: {e}"));
            }
        };
        let vm = self.next_vm;
        self.next_vm += 1;
        self.pids.insert(pid, vm);
        self.vms.insert(
            vm,
            Vm {
                route: request.route,
                role: request.role,
                id,
                capped: request.size.cpu.is_some(),
                pid,
                dir: dir.clone(),
            },
        );
        Ok((Answer::Spawned { vm, dir, pid }, pidfd))
    }

    /// Starts the VM of `request` as the id `id`, its directory `dir`: the directory
    /// made for the id's first VM and kept (the reap empties its `run`), its log afresh,
    /// its cgroup (the route's for a template, the id's for a VM serving, in the
    /// class of its CPU, capped or not) made for the first and limited for each. Its pid and
    /// a pidfd of it.
    fn jail(
        &mut self,
        id: u32,
        dir: &Path,
        request: &Waiting,
    ) -> io::Result<(libc::pid_t, OwnedFd)> {
        let n = id - self.ids.first;
        if !self.made.contains(&id) {
            std::fs::DirBuilder::new().mode(0o750).create(dir)?;
            owned(dir, 0, self.group, 0o750)?;
            // the VM makes its sockets there, the wicket's group's (setgid) so the
            // wicket reaches them
            let run = dir.join("run");
            std::fs::DirBuilder::new().mode(0o700).create(&run)?;
            owned(&run, id, self.group, 0o2770)?;
            self.made.insert(id);
        }
        let log = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o640)
            .custom_flags(libc::O_APPEND | libc::O_CLOEXEC)
            .open(dir.join("firecracker.log"))?;
        std::os::unix::fs::fchown(&log, Some(0), Some(self.group))?;
        let mem = u64::from(request.size.mem);
        let overhead = (vm_overhead_mib(mem) + LOG_MIB) << 20;
        let memory = match request.role {
            Role::Boot | Role::Clone => (mem << 20) + overhead,
            // its snapshot, which it writes, is charged to it (to the route's cgroup,
            // for the route's lifetime)
            Role::Template => ((2 * mem) << 20) + overhead,
        };
        let route = self.routes.get(&request.route).expect("checked");
        // the disk: the VM's id's to write, the wicket's group keeping its way to it; and the
        // VM's lock, on an open file of its own going with it (a holder going at once waited
        // for, a second at most: the wicket waited for the VM on it before)
        let locked = match &route.disk {
            Some(disk) => {
                std::os::unix::fs::fchown(disk, Some(id), Some(self.group))?;
                let file = std::fs::File::from(disk.try_clone()?);
                file.set_permissions(std::fs::Permissions::from_mode(0o660))?;
                let locked = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_CLOEXEC)
                    .open(format!("/proc/self/fd/{}", disk.as_raw_fd()))?;
                if !crate::disk::take(locked.as_raw_fd())? {
                    return Err(io::Error::other("the disk is in use by another VM"));
                }
                Some((locked, fstat(disk)?.st_size as u64))
            }
            None => None,
        };
        let file_size =
            vm_file_size(request.role, mem).max(locked.as_ref().map_or(0, |(_, bytes)| *bytes));
        let limits = Limits {
            memory,
            pids: u64::from(request.size.vcpus) + THREADS,
            cpu: request
                .size
                .cpu
                .map(|percent| u64::from(percent) * jail::CPU_PERIOD_US / 100),
        };
        let capped = limits.cpu.is_some();
        let route = self.routes.get_mut(&request.route).expect("checked");
        let cgroup = match request.role {
            Role::Template => {
                if route.cgroup.is_none() {
                    let name = format!("route-{}", request.route);
                    route.cgroup = Some(self.cgroups.child(&name, capped)?);
                }
                route.cgroup.as_mut().expect("made")
            }
            Role::Boot | Role::Clone => match self.clones.entry((id, capped)) {
                std::collections::hash_map::Entry::Occupied(cgroup) => cgroup.into_mut(),
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(self.cgroups.child(&format!("vm-{n}"), capped)?)
                }
            },
        };
        cgroup.limit(limits)?;
        // the jail binds them from the keeper's directory
        let route_in_dir = PathBuf::from(format!("route-{}", request.route));
        let run_in_dir = PathBuf::from(format!("vm-{n}/run"));
        let snapshot_in_dir = route_in_dir.join("snapshot");
        let snapshot = match request.role {
            Role::Boot => None,
            Role::Template => {
                owned(&route.dir.join("snapshot"), id, self.group, 0o2770)?;
                Some((snapshot_in_dir.as_path(), Access::ReadWrite))
            }
            Role::Clone => Some((snapshot_in_dir.as_path(), Access::ReadOnly)),
        };
        jail::spawn(&Jail {
            template: &self.template,
            id,
            firecracker: route.firecracker.as_fd(),
            route: &route_in_dir,
            kernel: request.role != Role::Clone,
            disk: locked.as_ref().map(|(file, _)| file.as_fd()),
            run: &run_in_dir,
            snapshot,
            host_cpu: jail::JAIL_HOST_CPU.then_some(Path::new(HOST_CPU_COPY)),
            null: self.null.as_fd(),
            log: log.as_fd(),
            keeper: self.pidfd.as_fd(),
            cgroup: cgroup.fd(),
            open_files: request.open_files.min(MAX_OPEN_FILES),
            file_size,
            filter: &self.filter,
        })
    }

    /// Makes the route's snapshot read-only (its template reaped): its files, regular
    /// and the template's alone, root's, readable by the clones' ids.
    fn seal(&mut self, route: u64) -> Result<(), String> {
        let sealing = self
            .routes
            .get_mut(&route)
            .ok_or_else(|| format!("no route {route}"))?;
        let dir = sealing.dir.join("snapshot");
        let sealed = (|| -> io::Result<()> {
            for entry in std::fs::read_dir(&dir)? {
                let path = entry?.path();
                // no symlink followed: a template's file is its own
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(&path)?;
                if !file.metadata()?.is_file() {
                    return Err(io::Error::other(format!("{}: not a file", path.display())));
                }
                std::os::unix::fs::fchown(&file, Some(0), Some(0))?;
                file.set_permissions(std::fs::Permissions::from_mode(0o444))?;
            }
            owned(&dir, 0, 0, 0o755)
        })();
        sealed.map_err(|e| format!("sealing {}: {e}", dir.display()))?;
        sealing.sealed = true;
        Ok(())
    }

    /// Removes a route closing once none of its VMs is left.
    fn closed(&mut self, route: u64) {
        let closing = self.routes.get(&route).is_some_and(|route| route.closing);
        if !closing || self.vms.values().any(|vm| vm.route == route) {
            return;
        }
        if self.routes.remove(&route).is_some() {
            self.forget(Path::new(&format!("route-{route}")));
            self.cgroups.remove(&format!("route-{route}"));
        }
    }

    /// The end: the VMs killed and reaped, their files and cgroups removed.
    fn close(&mut self) {
        for vm in self.vms.values() {
            // SAFETY: kill of our child, not reaped yet
            unsafe { libc::kill(vm.pid, libc::SIGKILL) };
        }
        for (pid, _) in std::mem::take(&mut self.pids) {
            let mut status = 0;
            // SAFETY: waitpid on our child
            unsafe { libc::waitpid(pid, &mut status, 0) };
        }
        let killed = self.vms.len();
        self.vms.clear();
        self.routes.clear();
        let _ = std::fs::remove_dir_all(&self.dir);
        // the cgroups empty once their pages are freed: a VM's memory goes with its exit
        self.cgroups.close();
        println!("keeper: the wicket ended; {killed} VM(s) killed");
    }
}

/// A wait status, for the log: `exit N`, `signal N`.
fn describe(status: libc::c_int) -> String {
    if libc::WIFEXITED(status) {
        format!("exit {}", libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        format!("signal {}", libc::WTERMSIG(status))
    } else {
        format!("status {status}")
    }
}

/// What the kernel knows of the open file `fd`.
fn fstat(fd: &OwnedFd) -> io::Result<libc::stat> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat into a buffer of its type
    check(unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) })?;
    // SAFETY: filled by the successful call
    Ok(unsafe { stat.assume_init() })
}

/// Sets `path`'s owner and mode (setgid included, which `mkdir`'s mode drops).
fn owned(path: &Path, uid: u32, gid: u32, mode: u32) -> io::Result<()> {
    std::os::unix::fs::lchown(path, Some(uid), Some(gid))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

/// `firecracker` as a path: a bare name looked up on the keeper's PATH.
fn executable(program: &Path) -> io::Result<PathBuf> {
    if program.components().count() > 1 {
        return Ok(program.to_path_buf());
    }
    std::env::var_os("PATH")
        .iter()
        .flat_map(std::env::split_paths)
        .map(|dir| dir.join(program))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            io::Error::other(format!(
                "{} is not on the keeper's PATH: give --firecracker a path",
                program.display()
            ))
        })
}

/// Spawns the wicket, this program again with its command line, in the directory `working`,
/// as `user`, its end of the socket pair at [`WICKET_FD`]; in a process group of its own (the
/// keeper alone gets a terminal's signals, and forwards them once). Its parent death signal
/// set: it ends with the keeper.
fn spawn_wicket(user: &User, socket: OwnedFd, working: &Path) -> io::Result<libc::pid_t> {
    // SAFETY: getpid
    let keeper = unsafe { libc::getpid() };
    let raw = socket.as_raw_fd();
    let groups = user.groups.clone();
    let (uid, gid) = (user.uid, user.gid);
    let mut command = std::process::Command::new("/proc/self/exe");
    command
        .args(std::env::args_os().skip(1))
        .env(KEEPER_FD, WICKET_FD.to_string())
        .current_dir(working)
        .process_group(0);
    // SAFETY: only async-signal-safe calls between fork and exec
    unsafe {
        command.pre_exec(move || {
            if raw == WICKET_FD {
                // dup2 to itself keeps close-on-exec: cleared by hand
                check(libc::fcntl(raw, libc::F_SETFD, 0))?;
            } else {
                check(libc::dup2(raw, WICKET_FD))?;
            }
            // the signals the keeper reads stay blocked across fork and exec
            let mut none: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut none);
            check(libc::sigprocmask(
                libc::SIG_SETMASK,
                &none,
                std::ptr::null_mut(),
            ))?;
            check(libc::setgroups(groups.len(), groups.as_ptr()))?;
            check(libc::setresgid(gid, gid, gid))?;
            check(libc::setresuid(uid, uid, uid))?;
            // after the ids: their change clears it
            check(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0))?;
            if libc::getppid() != keeper {
                return Err(io::Error::other("the keeper ended"));
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .map_err(|e| io::Error::other(format!("starting the wicket: {e}")))?;
    drop(socket);
    Ok(child.id() as libc::pid_t)
}

/// The wicket's end of the keeper: requests sent (by a writer task), answers and VM
/// exits received (by a reader task).
pub struct Client {
    calls: mpsc::UnboundedSender<(Vec<u8>, Vec<OwnedFd>)>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Received>>>>,
    next: AtomicU64,
}

/// An answer, with the exit watch and the pidfd of a VM spawned.
type Received = Result<(Answer, Option<Watched>), String>;

/// A VM spawned's exit watch, and its pidfd.
type Watched = (watch::Receiver<Option<VmEnd>>, Option<OwnedFd>);

/// A VM the keeper spawned.
pub struct Spawned {
    pub vm: u64,
    /// `firecracker.log`, and `run/` holding its sockets.
    pub dir: PathBuf,
    /// Its `firecracker`'s pid, and a pidfd of it.
    pub pid: libc::pid_t,
    pub pidfd: Option<OwnedFd>,
    /// How it ended, once it did.
    pub exited: watch::Receiver<Option<VmEnd>>,
}

impl Client {
    /// The keeper's end in the environment ([`KEEPER_FD`]), taken from it: before the
    /// runtime starts (the environment changes single-threaded).
    pub fn fd_from_env() -> Option<io::Result<OwnedFd>> {
        let value = std::env::var_os(KEEPER_FD)?;
        // SAFETY: single-threaded, before the runtime
        unsafe { std::env::remove_var(KEEPER_FD) };
        let fd: RawFd = match value.to_str().and_then(|fd| fd.parse().ok()) {
            Some(fd) => fd,
            None => return Some(Err(io::Error::other(format!("{KEEPER_FD}: not an fd")))),
        };
        // SAFETY: the fd the keeper gave us, ours alone; close-on-exec and non-blocking
        // from now on
        unsafe {
            if libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) == -1
                || libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) == -1
            {
                return Some(Err(io::Error::other(format!(
                    "{KEEPER_FD}={fd}: {}",
                    io::Error::last_os_error()
                ))));
            }
            Some(Ok(OwnedFd::from_raw_fd(fd)))
        }
    }

    /// The client of the keeper at `fd`, its reader and writer tasks started.
    pub fn start(fd: OwnedFd) -> io::Result<Arc<Client>> {
        // SAFETY: an OwnedFd stays open, the same fd, as long as the AsyncFd owns it
        let socket = unsafe { AsyncFd::register(fd) }.map_err(|e| e.into_parts().1)?;
        let socket = Arc::new(socket);
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Received>>>> = Arc::default();
        let exits: Arc<Mutex<HashMap<u64, watch::Sender<Option<VmEnd>>>>> = Arc::default();
        let (calls, mut outgoing) = mpsc::unbounded_channel::<(Vec<u8>, Vec<OwnedFd>)>();
        let writer = socket.clone();
        tokio::spawn(async move {
            while let Some((bytes, fds)) = outgoing.recv().await {
                let raw: Vec<RawFd> = fds.iter().map(|fd| fd.as_raw_fd()).collect();
                loop {
                    let Ok(mut ready) = writer.writable().await else {
                        return;
                    };
                    match ready.try_io(|socket| send(socket.as_raw_fd(), &bytes, &raw)) {
                        Ok(Ok(())) => break,
                        Ok(Err(e)) => {
                            eprintln!("keeper: a request lost: {e}");
                            break;
                        }
                        Err(_would_block) => continue,
                    }
                }
            }
        });
        let reader = socket.clone();
        let answered = pending.clone();
        let undo = calls.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; MESSAGE];
            loop {
                let Ok(mut ready) = reader.readable().await else {
                    break;
                };
                let (n, fds) = match ready.try_io(|socket| receive(socket.as_raw_fd(), &mut buffer))
                {
                    Ok(Ok((0, _))) | Ok(Err(_)) => break,
                    Ok(Ok(received)) => received,
                    Err(_would_block) => continue,
                };
                match serde_json::from_slice::<Message>(&buffer[..n]) {
                    Ok(Message::Reply { id, result }) => {
                        // a VM's exit watch, before its exit can be read, and its pidfd
                        let result = result.map(|answer| {
                            let watched = match &answer {
                                Answer::Spawned { vm, .. } => {
                                    let (tell, exited) = watch::channel(None);
                                    exits.lock().unwrap().insert(*vm, tell);
                                    Some((exited, fds.into_iter().next()))
                                }
                                _ => None,
                            };
                            (answer, watched)
                        });
                        let waiter = answered.lock().unwrap().remove(&id);
                        let unreceived = match waiter {
                            Some(waiter) => waiter.send(result).err(),
                            None => Some(result),
                        };
                        // its caller gave up (a start aborted): what it got, undone
                        let undone = match unreceived {
                            Some(Ok((Answer::Spawned { vm, .. }, _))) => {
                                exits.lock().unwrap().remove(&vm);
                                Some(Request::Kill { vm })
                            }
                            Some(Ok((Answer::Opened { route, .. }, _))) => {
                                Some(Request::Close { route })
                            }
                            _ => None,
                        };
                        if let Some(request) = undone {
                            let call = Call { id: 0, request };
                            let bytes = serde_json::to_vec(&call).expect("a request serializes");
                            let _ = undo.send((bytes, Vec::new()));
                        }
                    }
                    Ok(Message::Exited {
                        vm,
                        status,
                        log,
                        oom,
                    }) => {
                        if let Some(tell) = exits.lock().unwrap().remove(&vm) {
                            let _ = tell.send(Some(VmEnd { status, log, oom }));
                        }
                    }
                    Err(e) => eprintln!("keeper: a message it cannot read: {e}"),
                }
            }
            // the keeper ended: nothing will be answered (and the wicket dies with it)
            answered.lock().unwrap().clear();
            for (_, tell) in exits.lock().unwrap().drain() {
                let _ = tell.send(Some(VmEnd {
                    status: "the keeper ended".into(),
                    log: String::new(),
                    oom: false,
                }));
            }
        });
        Ok(Arc::new(Client {
            calls,
            pending,
            next: AtomicU64::new(1),
        }))
    }

    async fn call(&self, request: Request, fds: Vec<OwnedFd>) -> Received {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (answer, answered) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, answer);
        let bytes = serde_json::to_vec(&Call { id, request }).expect("a request serializes");
        if self.calls.send((bytes, fds)).is_err() {
            self.pending.lock().unwrap().remove(&id);
            return Err("the keeper ended".into());
        }
        answered
            .await
            .unwrap_or_else(|_| Err("the keeper ended".into()))
    }

    fn tell(&self, request: Request) {
        let bytes = serde_json::to_vec(&Call { id: 0, request }).expect("a request serializes");
        let _ = self.calls.send((bytes, Vec::new()));
    }

    /// Opens a route of `kernel`, `rootfs` and `disk`: its id and snapshot directory.
    pub async fn open(
        &self,
        kernel: std::fs::File,
        rootfs: std::fs::File,
        disk: Option<std::fs::File>,
    ) -> Result<(u64, PathBuf), BoxError> {
        let mut fds: Vec<OwnedFd> = vec![kernel.into(), rootfs.into()];
        fds.extend(disk.map(OwnedFd::from));
        match self.call(Request::Open, fds).await? {
            (Answer::Opened { route, snapshot }, _) => Ok((route, snapshot)),
            (answer, _) => Err(format!("keeper: unexpected {answer:?}").into()),
        }
    }

    /// Spawns a VM of `route`.
    pub async fn spawn(
        &self,
        route: u64,
        role: Role,
        size: Size,
        open_files: u64,
    ) -> Result<Spawned, BoxError> {
        let request = Request::Spawn {
            route,
            role,
            size,
            open_files,
        };
        match self.call(request, Vec::new()).await? {
            (Answer::Spawned { vm, dir, pid }, Some((exited, pidfd))) => Ok(Spawned {
                vm,
                dir,
                pid,
                pidfd,
                exited,
            }),
            (answer, _) => Err(format!("keeper: unexpected {answer:?}").into()),
        }
    }

    /// Kills a VM.
    pub fn kill(&self, vm: u64) {
        self.tell(Request::Kill { vm });
    }

    /// Seals the route's snapshot, its template killed first.
    pub async fn seal(&self, route: u64) -> Result<(), BoxError> {
        match self.call(Request::Seal { route }, Vec::new()).await? {
            (Answer::Sealed, _) => Ok(()),
            (answer, _) => Err(format!("keeper: unexpected {answer:?}").into()),
        }
    }

    /// Closes a route: its files go once its VMs did.
    pub fn close(&self, route: u64) {
        self.tell(Request::Close { route });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wicket's user by name (its groups from the group database) or by ids.
    #[test]
    fn the_user_is_a_name_or_ids() {
        assert_eq!(
            User::resolve("1000:1001").unwrap(),
            User {
                uid: 1000,
                gid: 1001,
                groups: vec![1001]
            }
        );
        let root = User::resolve("root").unwrap();
        assert_eq!((root.uid, root.gid), (0, 0));
        assert!(root.groups.contains(&0));
        for invalid in ["no-such-user-of-sluice", "1000:x"] {
            assert!(User::resolve(invalid).is_err(), "{invalid}");
        }
    }
}
