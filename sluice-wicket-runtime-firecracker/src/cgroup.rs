//! The VMs' cgroups (v2): what bounds a VM's memory and threads and gives it its share of the
//! CPUs, with the jail or without it.
//!
//! - in jail mode the keeper (`keeper`, root) makes them, under `--jail-cgroup` or beside its
//!   own, and clones each VM into its cgroup
//! - without the jail the wicket makes them beside its own, where that is delegated to it
//!   ([`Cgroups::delegated`]: a systemd service's with `Delegate=yes`, a scope of
//!   `systemd-run --user --scope -p Delegate=yes`, a container's it may write), and the forker
//!   (`forker`) forks each VM into its cgroup; where it is not, the VMs have none, and the
//!   wicket's own bounds them all
//!
//! The layout ([`Cgroups`]) and what a cgroup allows its VM ([`Limits::of`]) are the same
//! either way; a cgroup's number is an id of the jail's there, one of the wicket's own here
//! ([`Own`]).

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::ffi::CStr;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use crate::jail::{check, failed, path};
use crate::keeper::{Role, Size};

/// The VMs' cgroups (v2), under a parent of theirs: `route-<n>` for a route's template (what
/// it writes of its snapshot is charged there as long as the route lives), `vm-<n>` for a
/// booted VM and `route-<n>-vm-<m>` for a clone ([`serving`]), one per VM alive (an id of the
/// jail's, a number of [`Own`]) and reused (no cgroup made and removed per clone). Each is a
/// group of the cpu controller: its `cpu.weight` the VM's share of the CPUs among them
/// ([`weight`]: by what it holds of the CPU budget), its `cpu.max` its route's `cpu`. So the
/// VMs share the CPUs they contend for by what each was given, and the wicket's own cgroup,
/// where it is their sibling (`keeper/`, or `wicket/` without a keeper: [`OWN_WEIGHT`]),
/// before them.
///
/// A clone's cgroup is its route's own, reused by the route's clones alone and gone with the
/// route: of the snapshot's memory file, sparse, a page its template did not write is
/// charged to the clone first to touch it, and stays charged to that clone's cgroup as long
/// as the snapshot ([`Limits::of`] allows for it). In a cgroup any VM took, it would be
/// counted against a VM that has nothing of it.
///
/// But a clone of a connection's own without `cpu` ([`Limits::weight`] none) is in `shared/`,
/// one group of the cpu controller for all of them, their cgroups in it without: a group per
/// VM, scheduled on its own, costs 8% of the clone rate, and nothing of a VM that lives on.
/// Its threads share the group's CPUs as equals; the group's weight is what its VMs alive hold
/// together, written as they come and go ([`Shared`]).
#[derive(Clone)]
pub struct Cgroups {
    /// The parent, absolute.
    dir: PathBuf,
    /// Whether the keeper made it: removed at the end.
    made: bool,
    /// Whether the VMs' cgroups are groups of the cpu controller: the hierarchy has it.
    cpu: bool,
}

/// The cgroup v2 hierarchy.
pub(crate) const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The controllers the VMs' cgroups use.
const CONTROLLERS: &str = "+memory +pids";

/// The controller of the VMs' shares and of a route's `cpu`, enabled with [`CONTROLLERS`]
/// down to the VMs' cgroups where the hierarchy has it: without it the VMs have no share of
/// their own, and a route's `cpu` is refused (jailed: it fails its VMs' spawn).
const CPU_CONTROLLER: &str = "+cpu";

/// The class of the VMs sharing one group of the cpu controller ([`Cgroups`]).
const SHARED: &str = "shared";

/// The classes of an earlier keeper's cgroups: removed with what it left.
const EARLIER: [&str; 2] = ["capped", "uncapped"];

/// The leaf the keeper leaves its own cgroup for, the wicket with it; and the wicket's
/// without a keeper.
const KEEPER: &str = "keeper";
const WICKET: &str = "wicket";

/// The period of a VM's `cpu.max`, microseconds.
pub const CPU_PERIOD_US: u64 = 100_000;

/// `cpu.weight` per CPU a VM holds of the budget: a tenth of the kernel's default, so the
/// shared group's weight follows its VMs up to 1000 CPUs ([`MAX_WEIGHT`]).
const CPU_WEIGHT: u64 = 10;

/// The most a `cpu.weight` is.
const MAX_WEIGHT: u64 = 10_000;

/// The `cpu.weight` of the leaf the wicket runs in, where the VMs' cgroups are its siblings:
/// the most, its threads run before theirs. They relay every connection, and take the CPUs
/// they need, not more.
const OWN_WEIGHT: u64 = MAX_WEIGHT;

/// How long the shared group's weight stays as written while its VMs come and go: written
/// at every spawn and reap it cost 3% of the clone rate.
const SHARED_WEIGHT_INTERVAL: Duration = Duration::from_millis(100);

/// What a VM's `firecracker` may be charged of its own, in MiB: its heap and KVM's structures
/// (about 3 MiB measured), a vsock stream's buffers (64KiB each at most).
const VMM_MIB: u64 = 32;

/// The MiB of guest memory whose page tables may take one (about 0.25% of what a guest
/// touched).
const PAGE_TABLES: u64 = 128;

/// What a VM is charged besides its guest memory, its log and a template's snapshot, in
/// MiB, for `memory-mib` MiB of guest memory: its `firecracker`'s own ([`VMM_MIB`]) and the
/// page tables of the guest memory ([`PAGE_TABLES`]).
pub fn vm_overhead_mib(mem: u64) -> u64 {
    VMM_MIB + mem / PAGE_TABLES
}

/// What a VM of `memory-mib` MiB of guest memory may be charged of its own, in MiB: its guest
/// memory, what it is charged besides ([`vm_overhead_mib`]) and its log (`keeper::LOG_MIB`).
/// Its share of the memory budget (`capacity`), and what its cgroup allows it ([`Limits::of`])
/// but for a snapshot, which the budget counts once, for its route.
pub fn vm_charge_mib(mem: u64) -> u64 {
    mem + vm_overhead_mib(mem) + crate::keeper::LOG_MIB
}

/// [`vm_charge_mib`] in words, for a refusal.
pub fn vm_charge_said() -> String {
    let fixed = VMM_MIB + crate::keeper::LOG_MIB;
    format!("memory-mib + {fixed} MiB + memory-mib/{PAGE_TABLES}")
}

/// Threads of a VM besides its vCPUs (its VMM and API threads, a few more).
const THREADS: u64 = 16;

/// The `cpu.weight` of a cgroup whose VMs hold `cpu` hundredths of a CPU of the budget.
pub fn weight(cpu: u64) -> u64 {
    (cpu.saturating_mul(CPU_WEIGHT).div_ceil(100)).clamp(1, MAX_WEIGHT)
}

/// What a VM's cgroup allows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// `memory.max`, bytes.
    pub memory: u64,
    /// `pids.max`: its threads.
    pub pids: u64,
    /// `cpu.max`'s quota per [`CPU_PERIOD_US`], microseconds; None = no limit.
    pub cpu: Option<u64>,
    /// `cpu.weight`: its share of the CPUs; None = none of its own, its cgroup no group of
    /// the cpu controller (a VM of the shared group; a hierarchy without the controller).
    pub weight: Option<u64>,
}

impl Limits {
    /// What the cgroup of a VM of `size` in `role` allows it: what a VM may be charged
    /// ([`vm_charge_mib`]), and its guest memory again for what it is charged of its route's
    /// snapshot -- a template what it writes of it, a clone what it is first to touch of the
    /// rest, the clones before it in its cgroup too: the snapshot's size at most, all of the
    /// route's cgroups together --; its vCPUs' threads and [`THREADS`]; its route's `cpu`;
    /// and, where the cgroups are groups of the cpu controller (`cpu`), its share by what it
    /// holds of the CPU budget, unless it is one of the shared group.
    pub fn of(role: Role, size: &Size, cpu: bool) -> Limits {
        let mem = u64::from(size.mem);
        let snapshot = match role {
            Role::Boot => 0,
            Role::Template | Role::Clone => mem,
        };
        Limits {
            memory: (vm_charge_mib(mem) + snapshot) << 20,
            pids: u64::from(size.vcpus) + THREADS,
            cpu: size
                .cpu
                .map(|percent| u64::from(percent) * CPU_PERIOD_US / 100),
            weight: (!size.shares(role) && cpu).then(|| weight(size.cpus())),
        }
    }
}

impl Cgroups {
    /// The keeper's: the VMs' parent cgroup `path` (under the hierarchy's root), made if
    /// missing, the controllers enabled down to it; or, None, the keeper's own cgroup
    /// (delegated to it: a systemd service's with `Delegate=yes`, a container's), which the
    /// keeper leaves for a leaf ([`Cgroups::beside`]). Then as [`Cgroups::prepare`] has them.
    pub fn setup(path: Option<&str>) -> io::Result<Cgroups> {
        let root = Path::new(CGROUP_ROOT);
        if !is_cgroup2(root) {
            return Err(io::Error::other(format!(
                "{CGROUP_ROOT} is not a cgroup v2 hierarchy: the jail needs one"
            )));
        }
        let cgroups = match path {
            Some(path) => {
                let relative = Path::new(path.trim_start_matches('/'));
                if relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
                {
                    return Err(io::Error::other(format!(
                        "jail-cgroup {path}: expected a path under {CGROUP_ROOT} without . or .."
                    )));
                }
                let dir = root.join(relative);
                let made = !dir.exists();
                writable(root)?;
                std::fs::create_dir_all(&dir).map_err(failed(&dir.display().to_string()))?;
                // the root's subtree_control and every one below it, down to the parent's
                let busy = "it holds processes: choose a jail-cgroup none of whose \
                            ancestors (but the root) does";
                let mut at = root.to_path_buf();
                enable(&at, busy, true)?;
                for component in relative.components() {
                    at.push(component);
                    enable(&at, busy, true)?;
                }
                Cgroups::at(dir, made)
            }
            None => {
                let dir = own(root)?;
                writable(root)?;
                let busy = "other processes share the keeper's cgroup, so it cannot hold the \
                            VMs' cgroups: run the wicket as a service with Delegate=yes, in a \
                            container, or set jail-cgroup";
                Cgroups::beside(dir, KEEPER, busy)?
            }
        };
        cgroups.prepare("; choose another jail-cgroup")?;
        Ok(cgroups)
    }

    /// The wicket's own, without a keeper: the VMs' parent its own cgroup, where that is
    /// delegated to it, which it leaves for a leaf ([`Cgroups::beside`]) before anything of it
    /// starts; then as [`Cgroups::prepare`] has them. Delegated: its directory, the processes
    /// in it and its children's controllers the wicket's to write, the memory and pids
    /// controllers its to enable, no other process in it (a shell's it was started from), and
    /// `clone3` not refused, by which a VM is forked into its cgroup. Else why not, nothing
    /// changed: the VMs have no cgroup of their own then. An error: delegated, and not set up.
    pub fn delegated() -> io::Result<Result<Cgroups, String>> {
        let root = Path::new(CGROUP_ROOT);
        if !is_cgroup2(root) {
            return Ok(Err(format!("{CGROUP_ROOT} is not a cgroup v2 hierarchy")));
        }
        let dir = match own(root) {
            Ok(dir) => dir,
            Err(e) => return Ok(Err(e.to_string())),
        };
        let named = dir.display();
        for file in ["", "cgroup.procs", "cgroup.subtree_control"] {
            let file = path(&dir.join(file))?;
            // SAFETY: access of a NUL-terminated path
            if unsafe { libc::access(file.as_ptr(), libc::W_OK) } != 0 {
                return Ok(Err(format!("{named} is not the wicket's to write")));
            }
        }
        let has = std::fs::read_to_string(dir.join("cgroup.controllers")).unwrap_or_default();
        let has = |name: &str| has.split_whitespace().any(|has| has == name);
        let mut needed = CONTROLLERS.split_whitespace().map(|name| &name[1..]);
        if let Some(missing) = needed.find(|name| !has(name)) {
            return Ok(Err(format!("{named} has no {missing} controller")));
        }
        let procs = match std::fs::read_to_string(dir.join("cgroup.procs")) {
            Ok(procs) => procs,
            Err(e) => return Ok(Err(format!("{named}: its processes: {e}"))),
        };
        let own = std::process::id().to_string();
        if let Some(other) = procs.lines().find(|pid| *pid != own) {
            return Ok(Err(format!("another process (pid {other}) shares {named}")));
        }
        // SAFETY: clone3 of no arguments: EINVAL where the call is allowed
        unsafe { libc::syscall(libc::SYS_clone3, std::ptr::null::<u8>(), 0) };
        if io::Error::last_os_error().raw_os_error() != Some(libc::EINVAL) {
            return Ok(Err(
                "clone3, which forks a VM into its cgroup, is refused here (a seccomp \
                 profile's)"
                    .into(),
            ));
        }
        let busy = "another process shares the wicket's cgroup, so it cannot hold the VMs' \
                    cgroups";
        let cgroups = Cgroups::beside(dir, WICKET, busy)?;
        cgroups.prepare("")?;
        Ok(Ok(cgroups))
    }

    /// The VMs' parent the caller's own cgroup `dir`, which it leaves for the leaf `leaf` of
    /// it, of [`OWN_WEIGHT`]: cgroup v2 enables no controller for the children of a cgroup
    /// holding processes (`busy` says so of another's).
    fn beside(dir: PathBuf, leaf: &str, busy: &str) -> io::Result<Cgroups> {
        let leaf = dir.join(leaf);
        match std::fs::create_dir(&leaf) {
            Err(e) if e.kind() != io::ErrorKind::AlreadyExists => {
                return Err(failed(&leaf.display().to_string())(e));
            }
            _ => {}
        }
        std::fs::write(leaf.join("cgroup.procs"), "0")
            .map_err(failed(&leaf.display().to_string()))?;
        enable(&dir, busy, true)?;
        let cgroups = Cgroups::at(dir, false);
        if cgroups.cpu {
            let weight = leaf.join("cpu.weight");
            std::fs::write(&weight, OWN_WEIGHT.to_string())
                .map_err(failed(&weight.display().to_string()))?;
        }
        Ok(cgroups)
    }

    /// The shared class made, without the cpu controller for the cgroups in it; the VMs'
    /// cgroups a previous keeper or wicket left removed. Fails when one still holds a process
    /// (`hint`: what to do about it).
    fn prepare(&self, hint: &str) -> io::Result<()> {
        let class = self.dir.join(SHARED);
        match std::fs::create_dir(&class) {
            Err(e) if e.kind() != io::ErrorKind::AlreadyExists => {
                return Err(failed(&class.display().to_string())(e));
            }
            _ => {}
        }
        let busy = format!("it holds processes, another wicket's{hint}");
        enable(&class, &busy, false)?;
        for stale in self.children()? {
            let procs = std::fs::read_to_string(stale.join("cgroup.procs")).unwrap_or_default();
            if !procs.trim().is_empty() {
                return Err(io::Error::other(format!(
                    "{} holds processes: another wicket's VMs{hint}",
                    stale.display()
                )));
            }
            std::fs::remove_dir(&stale).map_err(failed(&stale.display().to_string()))?;
        }
        for earlier in EARLIER {
            let _ = std::fs::remove_dir(self.dir.join(earlier));
        }
        Ok(())
    }

    /// The VMs' cgroups in `dir`, the controllers enabled for them: with the cpu controller
    /// where it is among them.
    fn at(dir: PathBuf, made: bool) -> Cgroups {
        let control = dir.join("cgroup.subtree_control");
        let enabled = std::fs::read_to_string(control).unwrap_or_default();
        let cpu = enabled.split_whitespace().any(|name| name == "cpu");
        Cgroups { dir, made, cpu }
    }

    /// The parent cgroup.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Whether the VMs' cgroups are groups of the cpu controller.
    pub fn cpu(&self) -> bool {
        self.cpu
    }

    /// Where the cgroups of the VMs that share one group of the cpu controller are (`shared`),
    /// or the others'.
    fn class(&self, shared: bool) -> PathBuf {
        match shared {
            true => self.dir.join(SHARED),
            false => self.dir.clone(),
        }
    }

    /// The shared group's `cpu.weight`, open to write what its VMs hold ([`weight`]); none
    /// without the cpu controller.
    fn shared_weight(&self) -> Option<std::fs::File> {
        let weight = self.class(true).join("cpu.weight");
        let open = || std::fs::OpenOptions::new().write(true).open(weight).ok();
        self.cpu.then(open).flatten()
    }

    /// The cgroup `name`, of the VMs that share one group of the cpu controller (`shared`) or
    /// a group of its own, made if missing.
    pub fn child(&self, name: &str, shared: bool) -> io::Result<Cgroup> {
        let dir = self.class(shared).join(name);
        match std::fs::create_dir(&dir) {
            Err(e) if e.kind() != io::ErrorKind::AlreadyExists => {
                return Err(failed(&dir.display().to_string())(e));
            }
            _ => {}
        }
        let path = path(&dir)?;
        // SAFETY: open of a NUL-terminated path
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        check(fd).map_err(failed(&dir.display().to_string()))?;
        Ok(Cgroup {
            // SAFETY: a new fd of ours
            dir: unsafe { OwnedFd::from_raw_fd(fd) },
            limits: None,
            events: None,
            oom_kills: 0,
        })
    }

    /// Removes the cgroup `name`, shared or not (its processes ended; the pages charged
    /// to it stay charged to it, a dying cgroup, until they are freed).
    pub fn remove(&self, name: &str) {
        for shared in [true, false] {
            let _ = std::fs::remove_dir(self.class(shared).join(name));
        }
    }

    /// The VMs' cgroups, `route-*` and `vm-*`: the parent's, the shared class's, and those
    /// of an earlier keeper's classes.
    fn children(&self) -> io::Result<Vec<PathBuf>> {
        let mut children = Vec::new();
        let earlier = EARLIER.map(|class| self.dir.join(class));
        for dir in [self.dir.clone(), self.class(true)]
            .into_iter()
            .chain(earlier)
        {
            let entries = match std::fs::read_dir(&dir) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                entries => entries?,
            };
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if entry.file_type()?.is_dir()
                    && (name.starts_with("route-") || name.starts_with("vm-"))
                {
                    children.push(entry.path());
                }
            }
        }
        Ok(children)
    }

    /// Removes the VMs' cgroups, the shared class, and the parent if the keeper made it. A
    /// cgroup still holding a process (a VM killed, not ended yet) is waited for, `wait` at
    /// most.
    pub fn close(&self, wait: Duration) {
        let deadline = Instant::now() + wait;
        loop {
            let busy = |child: &PathBuf| {
                let removed = std::fs::remove_dir(child);
                removed.is_err_and(|e| e.raw_os_error() == Some(libc::EBUSY))
            };
            let left = self.children().unwrap_or_default();
            let busy = left.iter().filter(|child| busy(child)).count();
            if busy == 0 || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let _ = std::fs::remove_dir(self.class(true));
        if self.made {
            let _ = std::fs::remove_dir(&self.dir);
        }
    }
}

/// A VM's cgroup: its directory, open for `clone3`, its limits as last written, and the OOM
/// killer's kills in it as last read.
pub struct Cgroup {
    dir: OwnedFd,
    limits: Option<Limits>,
    /// Its `memory.events`, open from its first read on.
    events: Option<std::fs::File>,
    oom_kills: u64,
}

impl Cgroup {
    pub fn fd(&self) -> BorrowedFd<'_> {
        self.dir.as_fd()
    }

    /// Whether the kernel's OOM killer took a process of it since the last call: its
    /// `memory.events`' `oom_kill` rose. Asked as each VM it held is reaped, one at a time,
    /// it tells whether the OOM killer took that VM (whose limit is the cgroup's, or the
    /// host's memory ran out). An error reads as none.
    pub fn oom_killed(&mut self) -> bool {
        use std::os::unix::fs::FileExt as _;
        if self.events.is_none() {
            // SAFETY: openat of a NUL-terminated name in our directory
            let fd = unsafe {
                libc::openat(
                    self.dir.as_raw_fd(),
                    c"memory.events".as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC,
                )
            };
            // SAFETY: a new fd of ours
            self.events =
                (fd >= 0).then(|| std::fs::File::from(unsafe { OwnedFd::from_raw_fd(fd) }));
        }
        let Some(events) = &self.events else {
            return false;
        };
        // read from its start each time: the kernel writes it anew
        let mut text = [0u8; 512];
        let Ok(read) = events.read_at(&mut text, 0) else {
            return false;
        };
        let text = String::from_utf8_lossy(&text[..read]);
        let Some(count) = crate::usage::oom_kills(&text) else {
            return false;
        };
        let rose = count > self.oom_kills;
        self.oom_kills = count;
        rose
    }

    /// Sets `limits`, unless they are the ones set last (a reused cgroup's).
    pub fn limit(&mut self, limits: Limits) -> io::Result<()> {
        if self.limits == Some(limits) {
            return Ok(());
        }
        for (file, value) in [
            (c"memory.max", limits.memory),
            // the VM's threads alone: on OOM it is killed whole
            (c"memory.oom.group", 1),
            (c"pids.max", limits.pids),
        ] {
            self.write(file, &value.to_string())?;
        }
        // none of it swapped: its limit bounds all it takes, and a guest's memory stays off
        // the host's disk (a kernel not accounting swap has no such file)
        let _ = self.write(c"memory.swap.max", "0");
        match (limits.weight, limits.cpu) {
            // a group of the cpu controller: its share, and its route's `cpu`, or none again
            // where the VM before it of the id had one
            (Some(weight), quota) => {
                self.write(c"cpu.weight", &weight.to_string())?;
                let capped = self.limits.is_some_and(|before| before.cpu.is_some());
                if quota.is_some() || capped {
                    let quota = quota.map_or("max".to_string(), |quota| quota.to_string());
                    self.write(c"cpu.max", &format!("{quota} {CPU_PERIOD_US}"))?;
                }
            }
            (None, Some(_)) => {
                return Err(io::Error::other(
                    "cpu.max: the cpu controller is not enabled for the VMs' cgroups (cpu \
                     needs it)",
                ));
            }
            (None, None) => {}
        }
        self.limits = Some(limits);
        Ok(())
    }

    /// Writes `value` into the cgroup's `file`.
    fn write(&self, file: &CStr, value: &str) -> io::Result<()> {
        // SAFETY: openat of a NUL-terminated name in our directory, a write of a buffer
        // of its length, the close of our fd
        unsafe {
            let fd = libc::openat(
                self.dir.as_raw_fd(),
                file.as_ptr(),
                libc::O_WRONLY | libc::O_CLOEXEC,
            );
            check(fd).map_err(failed(&format!("{file:?}")))?;
            let written = libc::write(fd, value.as_ptr().cast(), value.len());
            let error = io::Error::last_os_error();
            libc::close(fd);
            if written != value.len() as isize {
                return Err(failed(&format!("{file:?} {value}"))(error));
            }
        }
        Ok(())
    }
}

/// The caller's own cgroup, under the hierarchy at `root`.
pub(crate) fn own(root: &Path) -> io::Result<PathBuf> {
    let own = std::fs::read_to_string("/proc/self/cgroup")?;
    let own = own
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| io::Error::other("/proc/self/cgroup names no cgroup v2"))?;
    Ok(match own.trim_matches('/') {
        "" => root.to_path_buf(),
        own => root.join(own),
    })
}

/// Whether `dir` is the root of a cgroup v2 mount.
fn is_cgroup2(dir: &Path) -> bool {
    let Ok(path) = path(dir) else {
        return false;
    };
    let mut fs = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: a NUL-terminated path and a statfs buffer to fill
    if unsafe { libc::statfs(path.as_ptr(), fs.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: filled by the successful call
    unsafe { fs.assume_init() }.f_type == libc::CGROUP2_SUPER_MAGIC
}

/// Makes the cgroup mount at `root` writable where it is mounted read-only (a
/// container's, by default): the keeper holds `CAP_SYS_ADMIN` there.
fn writable(root: &Path) -> io::Result<()> {
    let path = path(root)?;
    let mut fs = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: a NUL-terminated path and a buffer to fill
    check(unsafe { libc::statvfs(path.as_ptr(), fs.as_mut_ptr()) })?;
    // SAFETY: filled by the successful call
    if unsafe { fs.assume_init() }.f_flag & libc::ST_RDONLY == 0 {
        return Ok(());
    }
    // SAFETY: a remount of the mount's own flags, clearing read-only
    check(unsafe {
        libc::mount(
            std::ptr::null(),
            path.as_ptr(),
            std::ptr::null(),
            libc::MS_REMOUNT | libc::MS_BIND | libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            std::ptr::null(),
        )
    })
    .map_err(failed(&format!(
        "{CGROUP_ROOT} is read-only; remounting it"
    )))
}

/// Enables [`CONTROLLERS`] for the children of the cgroup `dir`, and, `cpu`,
/// [`CPU_CONTROLLER`] where `dir` has it (unchecked); `busy` says why it cannot when
/// `dir` holds processes (cgroup v2 enables no controller for the children of a cgroup
/// holding processes).
fn enable(dir: &Path, busy: &str, cpu: bool) -> io::Result<()> {
    let control = dir.join("cgroup.subtree_control");
    std::fs::write(&control, CONTROLLERS).map_err(|e| {
        let why = match e.raw_os_error() {
            Some(libc::EBUSY) => format!(": {busy}"),
            _ => String::new(),
        };
        io::Error::new(
            e.kind(),
            format!("{}: {CONTROLLERS}: {e}{why}", control.display()),
        )
    })?;
    let available = std::fs::read_to_string(dir.join("cgroup.controllers")).unwrap_or_default();
    if cpu && available.split_whitespace().any(|name| name == "cpu") {
        let _ = std::fs::write(&control, CPU_CONTROLLER);
    }
    Ok(())
}

/// The shared group's `cpu.weight` ([`Cgroups`]): what its VMs alive hold of the CPU budget
/// together, written as they come and go, [`SHARED_WEIGHT_INTERVAL`] apart at least.
pub struct Shared {
    /// The group's `cpu.weight`; none without the cpu controller.
    weight: Option<std::fs::File>,
    /// What its VMs alive hold, hundredths of a CPU.
    pub cpu: u64,
    /// The weight written last, and when.
    written: Option<(u64, Instant)>,
}

impl Shared {
    /// The shared group of `cgroups`, no VM in it yet.
    pub fn of(cgroups: &Cgroups) -> Shared {
        Shared {
            weight: cgroups.shared_weight(),
            cpu: 0,
            written: None,
        }
    }

    /// Writes the weight of what its VMs hold where that is due; else how long until it is.
    /// A group without a VM keeps the weight it has: no thread runs by it.
    pub fn settle(&mut self) -> Option<Duration> {
        use std::io::Write as _;
        let weight = weight(self.cpu);
        let file = self.weight.as_mut().filter(|_| self.cpu > 0)?;
        if let Some((written, at)) = self.written {
            let wait = SHARED_WEIGHT_INTERVAL.saturating_sub(at.elapsed());
            if written == weight {
                return None;
            } else if !wait.is_zero() {
                return Some(wait);
            }
        }
        if let Err(e) = file.write_all(weight.to_string().as_bytes()) {
            eprintln!("the shared group's cpu.weight {weight}: {e}");
        }
        self.written = Some((weight, Instant::now()));
        None
    }
}

/// The name of the cgroup of a VM serving, the `number`th: a clone's is its route's own
/// (`route`, as its template's is named), a booted VM's any booted VM's.
pub fn serving(route: Option<&str>, number: u32) -> String {
    match route {
        Some(route) => format!("{route}-vm-{number}"),
        None => format!("vm-{number}"),
    }
}

/// The cgroups of the wicket's own VMs, without a keeper to hold them: a VM serving takes
/// the lowest number no VM alive holds -- of the booted VMs, or of its place's clones --,
/// from its fork until it is reaped (the keeper's are the jail's ids), its cgroup that
/// number's ([`serving`]), made for the first and reused; a template's is its place's
/// (`route-<n>`). A place's cgroups are kept as long as the place.
pub struct Own {
    cgroups: Cgroups,
    /// The booted VMs' cgroups.
    booted: Numbered,
    /// The clones' cgroups, by place.
    clones: HashMap<String, Numbered>,
    /// The templates' cgroups, by place.
    templates: HashMap<String, Cgroup>,
    shared: Shared,
}

/// Cgroups by number, the lowest free taken first.
#[derive(Default)]
struct Numbered {
    /// The numbers taken once and free again.
    free: BTreeSet<u32>,
    /// The next number never taken.
    next: u32,
    /// By number and whether they are of the shared group.
    cgroups: HashMap<(u32, bool), Cgroup>,
}

/// What a VM holds of [`Own`]: its cgroup.
#[derive(Debug)]
pub enum Held {
    /// A VM serving: the place whose clone it is (none: booted), its number, whether it is
    /// one of the shared group, and what it holds of the CPU budget, hundredths of a CPU.
    Serving {
        place: Option<String>,
        number: u32,
        shared: bool,
        cpu: u64,
    },
    /// A template, of the place named.
    Template { place: String },
}

impl Own {
    pub fn new(cgroups: Cgroups) -> Own {
        Own {
            shared: Shared::of(&cgroups),
            cgroups,
            booted: Numbered::default(),
            clones: HashMap::new(),
            templates: HashMap::new(),
        }
    }

    /// The cgroup a VM of `size` in `role` starts in, at the place `place`, limited as
    /// [`Limits::of`] has it: what the VM holds of it, and its directory, the forker's to
    /// fork the VM into.
    pub fn enter(&mut self, place: &str, role: Role, size: &Size) -> io::Result<(Held, OwnedFd)> {
        let limits = Limits::of(role, size, self.cgroups.cpu());
        let entered = |cgroup: &mut Cgroup| {
            cgroup.limit(limits)?;
            cgroup.fd().try_clone_to_owned()
        };
        if role == Role::Template {
            let cgroup = match self.templates.entry(place.to_string()) {
                Entry::Occupied(cgroup) => cgroup.into_mut(),
                Entry::Vacant(vacant) => vacant.insert(self.cgroups.child(place, false)?),
            };
            let held = Held::Template {
                place: place.to_string(),
            };
            return Ok((held, entered(cgroup)?));
        }
        let (shared, cpu) = (size.shares(role), size.cpus());
        let place = (role == Role::Clone).then(|| place.to_string());
        let numbered = match &place {
            Some(place) => self.clones.entry(place.clone()).or_default(),
            None => &mut self.booted,
        };
        let number = numbered.free.pop_first().unwrap_or(numbered.next);
        let made = match numbered.cgroups.entry((number, shared)) {
            Entry::Occupied(cgroup) => Ok(cgroup.into_mut()),
            Entry::Vacant(vacant) => {
                let name = serving(place.as_deref(), number);
                let made = self.cgroups.child(&name, shared);
                made.map(|cgroup| vacant.insert(cgroup))
            }
        };
        match made.and_then(entered) {
            Ok(dir) => {
                numbered.next = numbered.next.max(number + 1);
                if shared {
                    self.shared.cpu += cpu;
                }
                let held = Held::Serving {
                    place,
                    number,
                    shared,
                    cpu,
                };
                Ok((held, dir))
            }
            Err(e) => {
                // a number never taken stays the next
                if number < numbered.next {
                    numbered.free.insert(number);
                }
                Err(e)
            }
        }
    }

    /// The VM holding `held` was reaped (or never started): whether the kernel's OOM killer
    /// took it, as its cgroup counted it before it holds another VM; its number is free.
    pub fn leave(&mut self, held: Held) -> bool {
        match held {
            Held::Serving {
                place,
                number,
                shared,
                cpu,
            } => {
                if shared {
                    self.shared.cpu -= cpu;
                }
                let numbered = match &place {
                    Some(place) => self.clones.get_mut(place),
                    None => Some(&mut self.booted),
                };
                let Some(numbered) = numbered else {
                    // its place went before it did: its cgroup goes now
                    self.cgroups.remove(&serving(place.as_deref(), number));
                    return false;
                };
                let cgroup = numbered.cgroups.get_mut(&(number, shared));
                let oom = cgroup.is_some_and(Cgroup::oom_killed);
                numbered.free.insert(number);
                oom
            }
            Held::Template { place } => match self.templates.get_mut(&place) {
                Some(cgroup) => cgroup.oom_killed(),
                // its place went before it did: its cgroup goes now
                None => {
                    self.cgroups.remove(&place);
                    false
                }
            },
        }
    }

    /// The place `place` is gone: its template's cgroup and its clones' with it, one whose
    /// VM is not reaped yet once it is ([`Own::leave`]).
    pub fn forget(&mut self, place: &str) {
        if self.templates.remove(place).is_some() {
            self.cgroups.remove(place);
        }
        let clones = self.clones.remove(place).unwrap_or_default();
        for (number, _) in clones.cgroups.keys() {
            self.cgroups.remove(&serving(Some(place), *number));
        }
    }

    /// The shared group's weight written where due ([`Shared::settle`]).
    pub fn settle(&mut self) -> Option<Duration> {
        self.shared.settle()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a cgroup allows its VM follows what the VM is given: its guest memory and what
    /// it is charged besides, twice the memory for a template and for a clone (what each is
    /// charged of the snapshot), its vCPUs' threads, its route's `cpu`, a share by the CPUs it
    /// holds unless it is one of the shared group. The shared
    /// group's weight is what its VMs hold, 10 a CPU and 1 at least: written at once, then an
    /// interval apart at least, and not for a group left without a VM.
    #[test]
    fn a_vms_limits_and_the_shared_groups_weight_follow_what_the_vms_hold() {
        let size = |cpu, shared| Size {
            vcpus: 2,
            mem: 256,
            cpu,
            shared,
        };
        let mib = |mib: u64| mib << 20;
        assert_eq!(
            Limits::of(Role::Boot, &size(None, false), true),
            Limits {
                memory: mib(256 + 32 + 2 + 16),
                pids: 18,
                cpu: None,
                weight: Some(20),
            }
        );
        assert_eq!(
            Limits::of(Role::Template, &size(Some(50), true), true),
            Limits {
                memory: mib(2 * 256 + 32 + 2 + 16),
                pids: 18,
                cpu: Some(50_000),
                weight: Some(5),
            },
            "a template, and a capped VM, in a group of its own"
        );
        let shared = Limits::of(Role::Clone, &size(None, true), true);
        assert_eq!((shared.cpu, shared.weight), (None, None));
        assert_eq!(shared.memory, mib(2 * 256 + 32 + 2 + 16), "a clone");
        assert_eq!(
            vm_charge_mib(256),
            256 + 32 + 2 + 16,
            "its share of the budget"
        );
        assert_eq!(
            (serving(None, 3), serving(Some("route-1"), 0)),
            ("vm-3".into(), "route-1-vm-0".into())
        );
        let unweighted = Limits::of(Role::Boot, &size(Some(300), false), false);
        assert_eq!((unweighted.cpu, unweighted.weight), (Some(300_000), None));

        let path = std::env::temp_dir().join(format!("sfc-weight-{}", std::process::id()));
        let mut shared = Shared {
            weight: Some(std::fs::File::create(&path).unwrap()),
            cpu: 150,
            written: None,
        };
        let written = |shared: &Shared| shared.written.map(|(weight, _)| weight);
        assert_eq!(shared.settle(), None);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "15");
        shared.cpu = 5;
        let due = shared.settle().expect("written a moment ago");
        assert!(due <= SHARED_WEIGHT_INTERVAL, "{due:?}");
        shared.cpu = 0;
        assert_eq!(shared.settle(), None, "no VM: nothing to weigh");
        assert_eq!(written(&shared), Some(15));
        shared.cpu = 5;
        std::thread::sleep(SHARED_WEIGHT_INTERVAL);
        assert_eq!(shared.settle(), None);
        assert_eq!(written(&shared), Some(1), "a twentieth of a CPU");
        assert_eq!(weight(2_000_000), 10_000, "the most a weight is");
        std::fs::remove_file(&path).unwrap();
    }
}
