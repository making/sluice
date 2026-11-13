//! The jail of one VM, as the keeper (`keeper`, root) sets it up for each `firecracker`
//! it spawns: what Firecracker's `jailer` does (`docs/jailer.md` of its repository),
//! with the differences noted.
//!
//! - new mount, PID, IPC and UTS namespaces, `firecracker` PID 1 of its own; the network
//!   namespace is the keeper's, empty, the VMs' alike (one per VM costs about 0.9ms of
//!   each spawn; the jailer's is the host's unless `--netns`)
//! - its root a tmpfs of its own, read-only, chrooted into: `/dev/kvm` (a node of the
//!   VM's), `/rootfs` and `/vmlinux` (bound read-only from the files the wicket opened),
//!   `/disk` (an app's disk, the same but writable), `/run` (bound from the VM's directory
//!   of the keeper's: the API and vsock sockets) and `/snapshot` (the route's snapshot
//!   directory: writable by its template, read-only to its clones); on aarch64
//!   `/sys/devices/system/cpu/cpu0` (bound from the keeper's copy
//!   of the host's: the caches and `MIDR_EL1` Firecracker reads, the files the jailer
//!   copies). No `/dev/net/tun`, `/dev/urandom` or `/dev/userfaultfd`: a vsock-only
//!   VM restored from a file needs none
//! - a cgroup of its own (`cgroup`), entered by `clone3` (`CLONE_INTO_CGROUP`)
//! - a uid and gid of its own, no supplementary group, the capability bounding set
//!   dropped, no new privileges; the wicket's seccomp filter (`seccomp`), then
//!   Firecracker's as it installs them
//! - the open file, file size and core limits; umask 007 (its sockets reach the wicket
//!   through the group of their directory)
//! - its program the route's copy of `firecracker`, a sealed memfd executed by
//!   `execveat`: the jailer copies the binary into every jail, so that no two VMs share
//!   its pages; the clones of a route share their snapshot's pages anyway, so they share
//!   their route's copy, and two routes share nothing
//! - the parent death signal (SIGKILL) set after the uid change, which clears it: a VM
//!   never outlives the keeper (the jailer's `firecracker` outlives it)
//!
//! A VM's mount namespace is a copy of the [`Template`]'s, not the host's: copying the
//! host's (`clone3`'s `CLONE_NEWNS`) copies every mount of the host in the keeper's
//! thread, under the kernel's mount locks, and bound the clone rate to about 900 VMs/s.
//! The template holds the keeper's directory and, attached once per route, the route's
//! files: the jailer hard-links them into each jail (one filesystem); a file of the
//! wicket's is bound instead, from a detached mount of the fd the wicket passed
//! (`open_tree`; a mount of another namespace cannot be bound). Nothing is mounted where
//! the host sees it, nothing is left to unmount.
//!
//! The jail's tmpfs is moved onto the template's root and chrooted into, the keeper's
//! directory unmounted first: the template's root, empty, stays beneath it. `pivot_root`
//! would drop that as well, but walks every thread of the host to move the ones rooted
//! at the old root (`chroot_fs_refs`), holding the kernel's task list lock, which every
//! fork and exit of the host takes: 0.25-1ms per VM among 2,300 threads, the keeper's
//! next spawn and the VMs' own threads stalled behind it.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::seccomp::Filter;

/// `CLONE_INTO_CGROUP` (Linux 5.7), which libc does not name for every target.
pub(crate) const CLONE_INTO_CGROUP: u64 = 0x2_0000_0000;

/// `CLONE_PIDFD` (Linux 5.2).
pub(crate) const CLONE_PIDFD: u64 = 0x1000;

/// `open_tree`'s `OPEN_TREE_CLONE` (Linux 5.2), which libc does not name for every
/// target.
const OPEN_TREE_CLONE: libc::c_uint = 1;

/// `move_mount`'s `MOVE_MOUNT_F_EMPTY_PATH`.
const MOVE_MOUNT_F_EMPTY_PATH: libc::c_uint = 4;

/// `/dev/kvm`'s device number.
const KVM: (u32, u32) = (10, 232);

/// The jail's root: a few directories, a device node and the bind mounts' targets.
const ROOT_OPTIONS: &str = "mode=0755,size=64k,nr_inodes=64";

/// The template's root: the keeper's directory and the jails' mountpoint.
const TEMPLATE_OPTIONS: &str = "mode=0755,size=16k,nr_inodes=16";

/// Where the template holds the keeper's directory, and mounts a jail's root.
const TEMPLATE_STATE: &str = "/state";
const TEMPLATE_JAIL: &str = "/jail";

/// Where the VM's directory is bound: its API socket ([`API_SOCKET`]) and vsock socket
/// ([`VSOCK_SOCKET`]) are made there.
pub const RUN: &str = "/run";

/// The API socket, as `firecracker` sees it.
pub const API_SOCKET: &str = "/run/api.sock";

/// The vsock socket, as `firecracker` sees it.
pub const VSOCK_SOCKET: &str = "/run/v.sock";

/// The kernel, as `firecracker` sees it.
pub const KERNEL: &str = "/vmlinux";

/// The rootfs, as `firecracker` sees it: the same in every VM of a route, as its
/// snapshot names it.
pub const ROOTFS: &str = "/rootfs";

/// The route's snapshot directory, as `firecracker` sees it.
pub const SNAPSHOT: &str = "/snapshot";

/// The route's disk, as `firecracker` sees it.
pub const DISK: &str = "/disk";

/// Whether a bound file or directory is writable in the jail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    ReadOnly,
    ReadWrite,
}

/// What a VM's jail holds and how its `firecracker` runs; paths are in the keeper's
/// directory.
pub struct Jail<'a> {
    /// The mount namespace a VM's own is copied from.
    pub template: &'a Template,
    /// The VM's uid, and gid.
    pub id: u32,
    /// The route's `firecracker`, a sealed memfd.
    pub firecracker: BorrowedFd<'a>,
    /// The route's directory: its rootfs and kernel, as [`Template::attach`] attached
    /// them.
    pub route: &'a Path,
    /// Whether the kernel is bound: a VM booted, not restored.
    pub kernel: bool,
    /// The route's disk: bound at [`DISK`] from the route's directory, and this open file of
    /// it, locked, the VM's across its exec (`disk`): the lock goes with `firecracker`.
    pub disk: Option<BorrowedFd<'a>>,
    /// The VM's directory, bound at [`RUN`].
    pub run: &'a Path,
    /// The route's snapshot directory, bound at [`SNAPSHOT`].
    pub snapshot: Option<(&'a Path, Access)>,
    /// The copy of [`HOST_CPU`]'s files ([`copy_host_cpu`]), bound at [`HOST_CPU`].
    pub host_cpu: Option<&'a Path>,
    /// `/dev/null`, `firecracker`'s stdin.
    pub null: BorrowedFd<'a>,
    /// `firecracker`'s stdout and stderr.
    pub log: BorrowedFd<'a>,
    /// A pidfd of the keeper: readable, the keeper ended before the parent death signal
    /// was set.
    pub keeper: BorrowedFd<'a>,
    /// The cgroup the VM starts in, its directory.
    pub cgroup: BorrowedFd<'a>,
    /// `RLIMIT_NOFILE`.
    pub open_files: u64,
    /// `RLIMIT_FSIZE`, bytes: a template writes its memory, the others their log alone.
    pub file_size: u64,
    /// The VM's seccomp filter before Firecracker's ([`Filter::unprivileged`]).
    pub filter: &'a Filter,
}

/// The names of a route's files in its directory.
pub const ROUTE_ROOTFS: &str = "rootfs";
pub const ROUTE_KERNEL: &str = "vmlinux";
pub const ROUTE_DISK: &str = "disk";

/// The host CPU's sysfs directory, where Firecracker reads its caches (the guest's cache
/// topology) and `MIDR_EL1` on aarch64: the jailer copies those files into each jail.
pub const HOST_CPU: &str = "/sys/devices/system/cpu/cpu0";

/// Whether a jail holds a copy of [`HOST_CPU`]'s files ([`copy_host_cpu`]), bound at the
/// same path: Firecracker reads them on aarch64 alone.
pub const JAIL_HOST_CPU: bool = cfg!(target_arch = "aarch64");

/// The files of a cache level (`cache/index<n>`) Firecracker reads, the jailer's list.
const CACHE_FILES: [&str; 6] = [
    "size",
    "level",
    "type",
    "shared_cpu_map",
    "coherency_line_size",
    "number_of_sets",
];

/// The last cache level of arm64 Linux (`index0` to `index7`).
const MAX_CACHE_INDEX: u8 = 7;

/// `MIDR_EL1`, in a CPU's sysfs directory.
const MIDR_EL1: &str = "regs/identification/midr_el1";

/// Copies what Firecracker reads of `from` (a CPU's sysfs directory) into `to`, made:
/// each cache level's files (the levels up to the first missing one; a file missing is
/// skipped, as the jailer does) and `MIDR_EL1` (where it exists), root's and readable by
/// every id. sysfs files are read whole: their size says nothing.
pub fn copy_host_cpu(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
    let make = |dir: &Path| {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o755)
            .create(dir)
            .map_err(failed(&dir.display().to_string()))
    };
    let copy = |name: &Path| {
        let Ok(bytes) = std::fs::read(from.join(name)) else {
            return Ok(());
        };
        let target = to.join(name);
        make(target.parent().expect("a file in a directory"))?;
        std::fs::write(&target, bytes)
            .and_then(|()| {
                std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o444))
            })
            .map_err(failed(&target.display().to_string()))
    };
    make(to)?;
    for index in 0..=MAX_CACHE_INDEX {
        let level = Path::new("cache").join(format!("index{index}"));
        if !from.join(&level).is_dir() {
            break;
        }
        for file in CACHE_FILES {
            copy(&level.join(file))?;
        }
    }
    copy(Path::new(MIDR_EL1))
}

fn cstring(bytes: &[u8]) -> io::Result<CString> {
    CString::new(bytes).map_err(|_| io::Error::other("a path holds a NUL byte"))
}

pub(crate) fn path(path: &Path) -> io::Result<CString> {
    cstring(path.as_os_str().as_bytes())
}

/// A path of the jail (`/run`) in the template, where the jail's root is mounted.
fn inside(jailed: &str) -> io::Result<CString> {
    cstring(format!("{TEMPLATE_JAIL}{jailed}").as_bytes())
}

/// A path of the keeper's directory (`vm-3/run`) in the template.
fn kept(relative: &Path) -> io::Result<CString> {
    path(&Path::new(TEMPLATE_STATE).join(relative))
}

pub(crate) fn check(ret: libc::c_int) -> io::Result<()> {
    match ret {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

/// `what`'s error, naming it.
pub(crate) fn failed(what: &str) -> impl FnOnce(io::Error) -> io::Error + '_ {
    move |e| io::Error::new(e.kind(), format!("{what}: {e}"))
}

/// The flags a bind takes in a jail: no device, no set-uid, no program; read-only.
fn restrict(target: &CString, access: Access) -> io::Result<()> {
    let mut flags =
        libc::MS_REMOUNT | libc::MS_BIND | libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC;
    if access == Access::ReadOnly {
        flags |= libc::MS_RDONLY;
    }
    // SAFETY: a remount of a NUL-terminated path
    check(unsafe {
        libc::mount(
            std::ptr::null(),
            target.as_ptr(),
            std::ptr::null(),
            flags,
            std::ptr::null(),
        )
    })
}

/// The mount namespace the VMs' are copied from: a tmpfs root holding the keeper's
/// directory (`/state`) and an empty `/jail`, where each VM's root is mounted, and the
/// routes' files attached in the keeper's directory. The keeper builds it and enters it
/// to attach and detach; it is single-threaded, and always returns to its own
/// namespace.
pub struct Template {
    namespace: OwnedFd,
    /// The keeper's own namespace, to return to.
    own: OwnedFd,
}

impl Template {
    /// Builds the template of the keeper's directory `state`, whose empty `jail` holds
    /// the template's root.
    pub fn build(state: &Path) -> io::Result<Template> {
        let open = |path: &CString| {
            // SAFETY: open of a NUL-terminated path
            let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
            check(fd)?;
            // SAFETY: a new fd of ours
            Ok::<_, io::Error>(unsafe { OwnedFd::from_raw_fd(fd) })
        };
        let ns = cstring(b"/proc/self/ns/mnt")?;
        let own = open(&ns).map_err(failed("/proc/self/ns/mnt"))?;
        let root = path(&state.join("jail"))?;
        let bound = path(&state.join("jail/state"))?;
        let jail = path(&state.join("jail/jail"))?;
        let source = path(state)?;
        let options = cstring(TEMPLATE_OPTIONS.as_bytes())?;
        // SAFETY: mount namespace calls on NUL-terminated paths and our fds
        let built = unsafe {
            check(libc::unshare(libc::CLONE_NEWNS)).map_err(failed("unshare"))?;
            let template = open(&ns);
            let built = (|| {
                let template = template?;
                check(libc::mount(
                    std::ptr::null(),
                    c"/".as_ptr(),
                    std::ptr::null(),
                    libc::MS_REC | libc::MS_PRIVATE,
                    std::ptr::null(),
                ))?;
                check(libc::mount(
                    c"tmpfs".as_ptr(),
                    root.as_ptr(),
                    c"tmpfs".as_ptr(),
                    libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
                    options.as_ptr().cast(),
                ))?;
                check(libc::mkdir(bound.as_ptr(), 0o755))?;
                check(libc::mkdir(jail.as_ptr(), 0o755))?;
                check(libc::mount(
                    source.as_ptr(),
                    bound.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                ))?;
                restrict(&bound, Access::ReadWrite)?;
                check(libc::chdir(root.as_ptr()))?;
                check(
                    libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".".as_ptr())
                        as libc::c_int,
                )?;
                check(libc::umount2(c".".as_ptr(), libc::MNT_DETACH))?;
                check(libc::mount(
                    std::ptr::null(),
                    c"/".as_ptr(),
                    std::ptr::null(),
                    libc::MS_REMOUNT
                        | libc::MS_RDONLY
                        | libc::MS_NOSUID
                        | libc::MS_NODEV
                        | libc::MS_NOEXEC,
                    std::ptr::null(),
                ))?;
                Ok::<_, io::Error>(template)
            })();
            back(&own)?;
            built
        };
        Ok(Template {
            namespace: built.map_err(failed("the jails' template namespace"))?,
            own,
        })
    }

    /// Attaches the file `fd` (of the keeper's namespace) at `at`, an empty file of the
    /// keeper's directory, in the template: as `access` says, its propagation private.
    pub fn attach(&self, fd: BorrowedFd<'_>, at: &Path, access: Access) -> io::Result<()> {
        let target = kept(at)?;
        // SAFETY: open_tree of an fd with an empty path, in the file's namespace
        let tree = unsafe {
            libc::syscall(
                libc::SYS_open_tree,
                fd.as_raw_fd(),
                c"".as_ptr(),
                OPEN_TREE_CLONE
                    | libc::O_CLOEXEC as libc::c_uint
                    | libc::AT_EMPTY_PATH as libc::c_uint,
            )
        } as libc::c_int;
        check(tree).map_err(failed("open_tree"))?;
        // SAFETY: a new fd of ours
        let tree = unsafe { OwnedFd::from_raw_fd(tree) };
        self.inside(|| {
            // SAFETY: a detached mount attached on a NUL-terminated path
            unsafe {
                check(libc::syscall(
                    libc::SYS_move_mount,
                    tree.as_raw_fd(),
                    c"".as_ptr(),
                    libc::AT_FDCWD,
                    target.as_ptr(),
                    MOVE_MOUNT_F_EMPTY_PATH,
                ) as libc::c_int)?;
                // a clone of the host's mount, in its peer group: no longer
                check(libc::mount(
                    std::ptr::null(),
                    target.as_ptr(),
                    std::ptr::null(),
                    libc::MS_PRIVATE,
                    std::ptr::null(),
                ))?;
            }
            restrict(&target, access)
        })
        .map_err(failed(&format!("attaching {}", at.display())))
    }

    /// Detaches what is attached at `at` in the template.
    pub fn detach(&self, at: &Path) {
        let Ok(target) = kept(at) else {
            return;
        };
        let _ = self.inside(|| {
            // SAFETY: umount2 of a NUL-terminated path
            check(unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) })
        });
    }

    /// Runs `work` in the template, then returns to the keeper's namespace.
    fn inside(&self, work: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        // SAFETY: setns of our fds (the caller is single-threaded)
        check(unsafe { libc::setns(self.namespace.as_raw_fd(), libc::CLONE_NEWNS) })
            .map_err(failed("setns to the template"))?;
        let worked = work();
        // SAFETY: as above
        unsafe { back(&self.own) }?;
        worked
    }
}

/// Back to the keeper's own namespace `own`, its root and working directory.
///
/// # Safety
///
/// The caller is single-threaded.
unsafe fn back(own: &OwnedFd) -> io::Result<()> {
    // SAFETY: setns of our fd, then chdir to a NUL-terminated path
    unsafe {
        check(libc::setns(own.as_raw_fd(), libc::CLONE_NEWNS))
            .map_err(failed("setns to the keeper's namespace"))?;
        check(libc::chdir(c"/".as_ptr()))
    }
}

/// The jail's paths and `firecracker`'s arguments as C strings, made before the clone:
/// the child allocates nothing.
struct Prepared {
    /// Where the jail's tmpfs is mounted, and its options.
    mountpoint: CString,
    options: CString,
    /// The keeper's directory in the template.
    state: CString,
    dev: CString,
    kvm: CString,
    /// Directories made before the binds: the parents of a bind's target.
    dirs: Vec<CString>,
    /// A file of the template's, where it is bound and whether it is a directory.
    binds: Vec<(CString, CString, Access, bool)>,
    argv: Vec<CString>,
}

/// The arguments of `clone3`.
#[repr(C)]
#[derive(Default)]
pub(crate) struct CloneArgs {
    pub(crate) flags: u64,
    pub(crate) pidfd: u64,
    pub(crate) child_tid: u64,
    pub(crate) parent_tid: u64,
    pub(crate) exit_signal: u64,
    pub(crate) stack: u64,
    pub(crate) stack_size: u64,
    pub(crate) tls: u64,
    pub(crate) set_tid: u64,
    pub(crate) set_tid_size: u64,
    pub(crate) cgroup: u64,
}

/// Clones a child into a new PID namespace and the jail's cgroup, which jails itself
/// (its other namespaces made in it: in parallel, not in the caller's thread) and
/// executes `firecracker`; its pid and a pidfd of it. The caller is single-threaded (the
/// child, a copy of it, takes no lock another thread may hold) and reaps the child
/// (SIGCHLD); the child's failures go to the jail's log, and it exits 127.
pub fn spawn(jail: &Jail<'_>) -> io::Result<(libc::pid_t, OwnedFd)> {
    let mut binds = vec![(
        kept(&jail.route.join(ROUTE_ROOTFS))?,
        inside(ROOTFS)?,
        Access::ReadOnly,
        false,
    )];
    if jail.kernel {
        binds.push((
            kept(&jail.route.join(ROUTE_KERNEL))?,
            inside(KERNEL)?,
            Access::ReadOnly,
            false,
        ));
    }
    if jail.disk.is_some() {
        binds.push((
            kept(&jail.route.join(ROUTE_DISK))?,
            inside(DISK)?,
            Access::ReadWrite,
            false,
        ));
    }
    binds.push((kept(jail.run)?, inside(RUN)?, Access::ReadWrite, true));
    if let Some((dir, access)) = jail.snapshot {
        binds.push((kept(dir)?, inside(SNAPSHOT)?, access, true));
    }
    let mut dirs = Vec::new();
    if let Some(copy) = jail.host_cpu {
        // its parents, outermost first: /sys, /sys/devices, ...
        let mut parents: Vec<_> = Path::new(HOST_CPU).ancestors().skip(1).collect();
        parents.pop(); // the root
        for parent in parents.into_iter().rev() {
            dirs.push(inside(&parent.to_string_lossy())?);
        }
        binds.push((kept(copy)?, inside(HOST_CPU)?, Access::ReadOnly, true));
    }
    let prepared = Prepared {
        mountpoint: cstring(TEMPLATE_JAIL.as_bytes())?,
        options: cstring(ROOT_OPTIONS.as_bytes())?,
        state: cstring(TEMPLATE_STATE.as_bytes())?,
        dev: inside("/dev")?,
        kvm: inside("/dev/kvm")?,
        dirs,
        binds,
        argv: vec![
            cstring(b"firecracker")?,
            cstring(b"--api-sock")?,
            cstring(API_SOCKET.as_bytes())?,
        ],
    };
    let mut pidfd: libc::c_int = -1;
    let args = CloneArgs {
        flags: libc::CLONE_NEWPID as u64 | CLONE_INTO_CGROUP | CLONE_PIDFD,
        pidfd: &mut pidfd as *mut libc::c_int as u64,
        child_tid: 0,
        parent_tid: 0,
        exit_signal: libc::SIGCHLD as u64,
        // none: the child runs on a copy of this stack, as after fork
        stack: 0,
        stack_size: 0,
        tls: 0,
        set_tid: 0,
        set_tid_size: 0,
        cgroup: jail.cgroup.as_raw_fd() as u64,
    };
    // SAFETY: clone3 with a valid argument struct and no stack: fork's semantics. The
    // child only runs `jailed` (no lock taken: the caller is single-threaded) and exits.
    let pid = unsafe {
        libc::syscall(
            libc::SYS_clone3,
            &args as *const CloneArgs,
            std::mem::size_of::<CloneArgs>(),
        )
    };
    match pid {
        -1 => Err(failed("clone3")(io::Error::last_os_error())),
        0 => {
            let Err(error) = jailed(jail, &prepared);
            let message = format!("jail: {error}\n");
            // SAFETY: a write of a buffer of its length to stderr (the log), then the end
            // of the child without unwinding into the keeper's frames
            unsafe {
                libc::write(2, message.as_ptr().cast(), message.len());
                libc::_exit(127)
            }
        }
        // SAFETY: the pidfd clone3 made for us (close-on-exec, as every pidfd)
        pid => Ok((pid as libc::pid_t, unsafe { OwnedFd::from_raw_fd(pidfd) })),
    }
}

/// The child's part: jails itself and executes `firecracker`; returns only on a failure.
fn jailed(jail: &Jail<'_>, prepared: &Prepared) -> io::Result<std::convert::Infallible> {
    // SAFETY (the whole function): system calls on the C strings prepared above and the
    // fds of the jail, all valid for its length.
    unsafe {
        // the keeper blocks the signals it reads from a signalfd, and std ignores SIGPIPE
        let mut all: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut all);
        libc::sigprocmask(libc::SIG_UNBLOCK, &all, std::ptr::null_mut());
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);

        // stdout and stderr to the log first: a failure from here on lands there
        check(libc::dup2(jail.log.as_raw_fd(), 1)).map_err(failed("stdout"))?;
        check(libc::dup2(jail.log.as_raw_fd(), 2)).map_err(failed("stderr"))?;
        check(libc::dup2(jail.null.as_raw_fd(), 0)).map_err(failed("stdin"))?;

        // a copy of the template (its mounts private: nothing the jail mounts
        // propagates), and IPC and UTS namespaces of its own
        check(libc::setns(
            jail.template.namespace.as_raw_fd(),
            libc::CLONE_NEWNS,
        ))
        .map_err(failed("setns to the template"))?;
        check(libc::unshare(
            libc::CLONE_NEWNS | libc::CLONE_NEWIPC | libc::CLONE_NEWUTS,
        ))
        .map_err(failed("unshare"))?;
        check(libc::mount(
            c"tmpfs".as_ptr(),
            prepared.mountpoint.as_ptr(),
            c"tmpfs".as_ptr(),
            libc::MS_NOSUID | libc::MS_NOEXEC,
            prepared.options.as_ptr().cast(),
        ))
        .map_err(failed("the jail's tmpfs"))?;
        check(libc::mkdir(prepared.dev.as_ptr(), 0o755)).map_err(failed("/dev"))?;
        check(libc::mknod(
            prepared.kvm.as_ptr(),
            libc::S_IFCHR | 0o600,
            libc::makedev(KVM.0, KVM.1),
        ))
        .map_err(failed("mknod /dev/kvm"))?;
        check(libc::chown(prepared.kvm.as_ptr(), jail.id, jail.id)).map_err(failed("/dev/kvm"))?;
        for dir in &prepared.dirs {
            check(libc::mkdir(dir.as_ptr(), 0o755))
                .map_err(|e| io::Error::new(e.kind(), format!("{dir:?}: {e}")))?;
        }
        for (source, target, access, directory) in &prepared.binds {
            let bound = (|| {
                if *directory {
                    check(libc::mkdir(target.as_ptr(), 0o755))?;
                } else {
                    let fd = libc::open(
                        target.as_ptr(),
                        libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_CLOEXEC,
                        0o600,
                    );
                    check(fd)?;
                    libc::close(fd);
                }
                check(libc::mount(
                    source.as_ptr(),
                    target.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                ))?;
                restrict(target, *access)
            })();
            bound.map_err(|e| io::Error::new(e.kind(), format!("{target:?}: {e}")))?;
        }
        check(libc::mount(
            std::ptr::null(),
            prepared.mountpoint.as_ptr(),
            std::ptr::null(),
            libc::MS_REMOUNT | libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NOEXEC,
            std::ptr::null(),
        ))
        .map_err(failed("the jail's tmpfs read-only"))?;

        // the tmpfs the root: the keeper's directory unmounted (the binds are mounts of
        // their own), the tmpfs moved onto the template's root and chrooted into -- not
        // pivot_root (see the module's documentation)
        check(libc::umount2(prepared.state.as_ptr(), libc::MNT_DETACH))
            .map_err(failed("the keeper's directory"))?;
        check(libc::chdir(prepared.mountpoint.as_ptr())).map_err(failed("chdir"))?;
        check(libc::mount(
            c".".as_ptr(),
            c"/".as_ptr(),
            std::ptr::null(),
            libc::MS_MOVE,
            std::ptr::null(),
        ))
        .map_err(failed("the jail's tmpfs onto /"))?;
        check(libc::chroot(c".".as_ptr())).map_err(failed("chroot"))?;
        check(libc::chdir(c"/".as_ptr())).map_err(failed("chdir /"))?;

        for (resource, limit) in [
            (libc::RLIMIT_NOFILE, jail.open_files),
            (libc::RLIMIT_FSIZE, jail.file_size),
            // no core of a VM's memory
            (libc::RLIMIT_CORE, 0),
        ] {
            let limit = libc::rlimit {
                rlim_cur: limit,
                rlim_max: limit,
            };
            check(libc::setrlimit(resource, &limit)).map_err(failed("setrlimit"))?;
        }
        libc::umask(0o007);

        // the bounding set while CAP_SETPCAP is held, then the ids
        let mut cap = 0;
        while libc::prctl(libc::PR_CAPBSET_READ, cap, 0, 0, 0) >= 0 {
            check(libc::prctl(libc::PR_CAPBSET_DROP, cap, 0, 0, 0))
                .map_err(failed("the capability bounding set"))?;
            cap += 1;
        }
        check(libc::setgroups(0, std::ptr::null())).map_err(failed("setgroups"))?;
        check(libc::setresgid(jail.id, jail.id, jail.id)).map_err(failed("setresgid"))?;
        check(libc::setresuid(jail.id, jail.id, jail.id)).map_err(failed("setresuid"))?;
        check(libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0))
            .map_err(failed("no_new_privs"))?;
        // the uid change cleared it
        check(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0))
            .map_err(failed("the parent death signal"))?;
        // the calls Docker's default profile refuses, the jail's among them, which the
        // keeper's filter allows; Firecracker's filters stack on it
        jail.filter
            .install()
            .map_err(failed("the seccomp filter"))?;
        // the keeper ended before it was set? (getppid says 0 in a new PID namespace)
        let mut keeper = libc::pollfd {
            fd: jail.keeper.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        if libc::poll(&mut keeper, 1, 0) != 0 {
            return Err(io::Error::other("the keeper ended before the VM started"));
        }

        // nothing but stdio and the program across the exec (all is close-on-exec
        // already: the received fds too)
        libc::close_range(
            3,
            libc::c_uint::MAX,
            libc::CLOSE_RANGE_CLOEXEC as libc::c_int,
        );
        if let Some(disk) = jail.disk {
            check(libc::fcntl(disk.as_raw_fd(), libc::F_SETFD, 0)).map_err(failed("the disk"))?;
        }
        let argv: Vec<*const libc::c_char> = prepared
            .argv
            .iter()
            .map(|arg| arg.as_ptr())
            .chain(std::iter::once(std::ptr::null()))
            .collect();
        let envp: [*const libc::c_char; 1] = [std::ptr::null()];
        libc::syscall(
            libc::SYS_execveat,
            jail.firecracker.as_raw_fd(),
            c"".as_ptr(),
            argv.as_ptr(),
            envp.as_ptr(),
            libc::AT_EMPTY_PATH,
        );
        Err(failed("execveat firecracker")(io::Error::last_os_error()))
    }
}

/// A sealed, close-on-exec memfd holding `program`'s bytes: the route's copy of
/// `firecracker`, which no jail can change.
pub fn sealed_copy(program: &Path) -> io::Result<OwnedFd> {
    let name = cstring(b"firecracker")?;
    // MFD_EXEC (Linux 6.3) where the kernel knows it: vm.memfd_noexec=1 would make the
    // memfd non-executable otherwise
    let flags = libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING;
    // SAFETY: memfd_create with a valid name
    let mut fd = unsafe { libc::memfd_create(name.as_ptr(), flags | libc::MFD_EXEC) };
    if fd == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::EINVAL) {
        // SAFETY: as above
        fd = unsafe { libc::memfd_create(name.as_ptr(), flags) };
    }
    check(fd).map_err(failed("memfd_create"))?;
    // SAFETY: a new fd of ours
    let memfd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut file = std::fs::File::from(memfd);
    let mut source =
        std::fs::File::open(program).map_err(failed(&program.display().to_string()))?;
    io::copy(&mut source, &mut file)?;
    // SAFETY: fcntl on our fd
    check(unsafe {
        libc::fcntl(
            file.as_raw_fd(),
            libc::F_ADD_SEALS,
            libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE,
        )
    })
    .map_err(failed("sealing the copy"))?;
    Ok(file.into())
}

/// Why a file of the wicket's cannot be bound into the jails, if it cannot: a regular
/// file the VMs' ids (others to it) may read. Its directory may stay private: the bind
/// skips it.
pub fn unshareable(mode: u32) -> Option<&'static str> {
    if mode & libc::S_IFMT != libc::S_IFREG {
        Some("not a regular file")
    } else if mode & libc::S_IROTH == 0 {
        Some(
            "the VMs read it as ids of their own: it needs others' read permission (o+r; \
             its directory may stay private)",
        )
    } else {
        None
    }
}

/// `fd`'s mode.
pub fn mode(fd: BorrowedFd<'_>) -> io::Result<u32> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat into a buffer of its type
    check(unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) })?;
    // SAFETY: filled by the successful call
    Ok(unsafe { stat.assume_init() }.st_mode)
}

/// The VMs' uids (and gids): `first-last`, one per VM alive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ids {
    pub first: u32,
    pub last: u32,
}

impl Ids {
    pub fn parse(range: &str) -> Result<Ids, String> {
        let invalid = || format!("jail-ids {range}: expected <first>-<last>, e.g. 900000-900127");
        let (first, last) = range.split_once('-').ok_or_else(invalid)?;
        let (first, last): (u32, u32) = (
            first.trim().parse().map_err(|_| invalid())?,
            last.trim().parse().map_err(|_| invalid())?,
        );
        // 0: root; u32::MAX: -1, "no change" to setresuid
        if first == 0 || last < first || last == u32::MAX {
            return Err(invalid());
        }
        Ok(Ids { first, last })
    }

    /// How many: never none.
    pub fn count(&self) -> u32 {
        self.last - self.first + 1
    }

    pub fn contains(&self, id: u32) -> bool {
        (self.first..=self.last).contains(&id)
    }
}

/// A pidfd of this process (`pidfd_open`, close-on-exec), for [`Jail::keeper`].
pub fn own_pidfd() -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open of our own pid, no flags
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::getpid(), 0) } as libc::c_int;
    check(fd).map_err(failed("pidfd_open"))?;
    // SAFETY: a new fd of ours
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What Firecracker reads of a CPU's sysfs directory: each cache level's files up to
    /// the first level missing, and MIDR_EL1; readable by every id.
    #[test]
    fn the_host_cpus_copy_holds_what_firecracker_reads() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!("sfc-host-cpu-{}", std::process::id()));
        let (from, to) = (dir.join("cpu0"), dir.join("copy"));
        let write = |name: &str, content: &str| {
            let path = from.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        };
        for file in CACHE_FILES {
            write(&format!("cache/index0/{file}"), &format!("{file} 0\n"));
        }
        write("cache/index0/id", "0\n");
        write("cache/index1/level", "1\n");
        // past index2, missing: not copied
        write("cache/index3/level", "2\n");
        write(MIDR_EL1, "0x00000000410fd0c0\n");

        copy_host_cpu(&from, &to).unwrap();
        let read = |name: &str| std::fs::read_to_string(to.join(name)).ok();
        for file in CACHE_FILES {
            let name = format!("cache/index0/{file}");
            assert_eq!(read(&name), Some(format!("{file} 0\n")));
        }
        assert_eq!(read("cache/index0/id"), None);
        assert_eq!(read("cache/index1/level").as_deref(), Some("1\n"));
        assert_eq!(read("cache/index1/size"), None);
        assert!(!to.join("cache/index3").exists());
        assert_eq!(read(MIDR_EL1).as_deref(), Some("0x00000000410fd0c0\n"));
        let mode = |name: &str| {
            let metadata = std::fs::metadata(to.join(name)).unwrap();
            metadata.permissions().mode() & 0o777
        };
        assert_eq!(mode(MIDR_EL1), 0o444);
        assert_eq!(mode("cache/index0"), 0o755);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
