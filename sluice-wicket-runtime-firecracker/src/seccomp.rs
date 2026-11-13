//! The wicket's seccomp filters: Docker's default profile's denials (moby
//! `profiles/seccomp/default.json`, for a process without capabilities), wherever the
//! wicket runs, so that a container may run it unconfined at no loss (jail mode needs
//! `pivot_root`, which the profile refuses).
//!
//! - [`Filter::unprivileged`]: the wicket (the tunnel, the routes, everything reading
//!   what the guests send) and each jailed `firecracker` before its exec (Firecracker's
//!   own filters follow, stacked)
//! - [`Filter::keeper`]: the keeper (jail mode, root), and what it spawns before their
//!   own: the same but for the namespace, mount and root calls of the jails (`jail`)
//! - [`Filter::forker`]: the forker (`forker`, without the jail), started before the wicket
//!   installs its own: the wicket's but for `clone3`, by which it forks a VM into its cgroup,
//!   the flags its own; each VM installs the wicket's before its exec
//!
//! A denylist, not the profile's allowlist: the calls the profile refuses, the ones
//! newer than the kernels it knows aside. Denied calls fail with `EPERM`, `clone3` with
//! `ENOSYS` (its flags are out of the filter's reach: libc falls back to `clone`,
//! whose namespace flags are refused), as in the profile. Unlike it, `ptrace` and
//! `process_vm_*` are denied as well. Another ABI (x86_64's i386 and x32) is denied
//! whole.

use std::io;

/// The architecture the filter checks: its numbers are this one's.
#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xC000_003E;
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xC000_00B7;

/// x32's calls, on x86_64's architecture number.
#[cfg(target_arch = "x86_64")]
const X32_SYSCALL_BIT: u32 = 0x4000_0000;

/// `seccomp_data`'s offsets: the call's number, its architecture, the low 32 bits of
/// its first argument (little-endian).
const NR: u32 = 0;
const ARCH: u32 = 4;
const ARG0_LOW: u32 = 16;

#[cfg(not(target_endian = "little"))]
compile_error!("the filter reads the first argument's low half at its little-endian offset");

/// The calls refused, on every architecture.
const DENIED: &[libc::c_long] = &[
    libc::SYS_acct,
    libc::SYS_add_key,
    libc::SYS_bpf,
    libc::SYS_clock_settime,
    libc::SYS_delete_module,
    libc::SYS_fanotify_init,
    libc::SYS_finit_module,
    libc::SYS_fsconfig,
    libc::SYS_fsmount,
    libc::SYS_fsopen,
    libc::SYS_fspick,
    libc::SYS_get_mempolicy,
    libc::SYS_init_module,
    libc::SYS_io_uring_enter,
    libc::SYS_io_uring_register,
    libc::SYS_io_uring_setup,
    libc::SYS_kcmp,
    libc::SYS_kexec_file_load,
    libc::SYS_kexec_load,
    libc::SYS_keyctl,
    libc::SYS_lookup_dcookie,
    libc::SYS_mbind,
    libc::SYS_migrate_pages,
    libc::SYS_mount_setattr,
    libc::SYS_move_pages,
    libc::SYS_nfsservctl,
    libc::SYS_open_by_handle_at,
    libc::SYS_perf_event_open,
    libc::SYS_pidfd_getfd,
    libc::SYS_process_madvise,
    libc::SYS_process_vm_readv,
    libc::SYS_process_vm_writev,
    libc::SYS_ptrace,
    libc::SYS_quotactl,
    libc::SYS_quotactl_fd,
    libc::SYS_reboot,
    libc::SYS_request_key,
    libc::SYS_set_mempolicy,
    libc::SYS_set_mempolicy_home_node,
    libc::SYS_setdomainname,
    libc::SYS_sethostname,
    libc::SYS_settimeofday,
    libc::SYS_swapoff,
    libc::SYS_swapon,
    libc::SYS_syslog,
    libc::SYS_userfaultfd,
    libc::SYS_vhangup,
    // lsm_get_self_attr, lsm_set_self_attr, lsm_list_modules, open_tree_attr: numbered
    // alike on every architecture, not named by libc
    459,
    460,
    461,
    467,
];

/// The calls refused on x86_64 besides.
#[cfg(target_arch = "x86_64")]
const DENIED_ARCH: &[libc::c_long] = &[
    libc::SYS__sysctl,
    libc::SYS_afs_syscall,
    libc::SYS_getpmsg,
    libc::SYS_ioperm,
    libc::SYS_iopl,
    libc::SYS_putpmsg,
    libc::SYS_security,
    libc::SYS_sysfs,
    libc::SYS_tuxcall,
    libc::SYS_uselib,
    libc::SYS_ustat,
    libc::SYS_vserver,
];
#[cfg(target_arch = "aarch64")]
const DENIED_ARCH: &[libc::c_long] = &[];

/// The namespace, mount and root calls of the jails, refused but to the keeper.
const JAILS: &[libc::c_long] = &[
    libc::SYS_chroot,
    libc::SYS_mount,
    libc::SYS_move_mount,
    libc::SYS_open_tree,
    libc::SYS_pivot_root,
    libc::SYS_setns,
    libc::SYS_umount2,
    libc::SYS_unshare,
];

/// The namespace flags `clone` is refused (`CLONE_NEWNS`, `_NEWCGROUP`, `_NEWUTS`,
/// `_NEWIPC`, `_NEWUSER`, `_NEWPID`, `_NEWNET`).
const CLONE_NAMESPACES: u32 = 0x7E02_0000;

/// The socket families allowed: below 3 (unspec, unix, inet) and these; the others are
/// old or rarely used protocols (and vsock, `AF_ALG`).
const SOCKET_FAMILIES: &[u32] = &[
    7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 21, 22, 24, 26, 27, 28, 29, 30, 31, 32, 33, 35, 36, 39,
    41, 42, 43, 44, 45,
];

/// The personalities allowed: `PER_LINUX`, `PER_LINUX32`, each with `UNAME26`, and the
/// query.
const PERSONALITIES: &[u32] = &[0, 0x8, 0x2_0000, 0x2_0008, 0xFFFF_FFFF];

/// A seccomp filter, built before it is installed: [`Filter::install`] allocates
/// nothing (it runs between fork and exec).
pub struct Filter(Vec<libc::sock_filter>);

impl Filter {
    /// The filter of the wicket and the VMs.
    pub fn unprivileged() -> Filter {
        Filter::build(false, false)
    }

    /// The keeper's: the jails' namespace and mount calls allowed, `clone3` too (it
    /// clones the VMs into their PID namespace and cgroup).
    pub fn keeper() -> Filter {
        Filter::build(true, true)
    }

    /// The forker's: the wicket's, `clone3` allowed (it forks the VMs into their cgroup).
    pub fn forker() -> Filter {
        Filter::build(false, true)
    }

    /// The filter of a process that jails VMs (`jailer`: the jails' calls allowed, `clone`'s
    /// namespace flags too) or not, `clone3` allowed or refused.
    fn build(jailer: bool, clone3: bool) -> Filter {
        let mut program = Program(Vec::new());
        program.load(ARCH);
        program.jump_eq(AUDIT_ARCH, 1, 0);
        program.ret(fail(libc::EPERM));
        program.load(NR);
        #[cfg(target_arch = "x86_64")]
        {
            program.jump(libc::BPF_JGE, X32_SYSCALL_BIT, 0, 1);
            program.ret(fail(libc::EPERM));
        }
        let jails = if jailer { &[] } else { JAILS };
        for nr in DENIED.iter().chain(DENIED_ARCH).chain(jails) {
            program.deny(*nr, libc::EPERM);
        }
        if !clone3 {
            program.deny(libc::SYS_clone3, libc::ENOSYS);
        }
        if !jailer {
            program.on(libc::SYS_clone, |program| {
                program.jump(libc::BPF_JSET, CLONE_NAMESPACES, 0, 1);
                program.ret(fail(libc::EPERM));
                program.ret(libc::SECCOMP_RET_ALLOW);
            });
        }
        program.on(libc::SYS_socket, |program| {
            program.jump(libc::BPF_JGE, 3, 0, 1 + SOCKET_FAMILIES.len() as u8);
            program.allow_one_of(SOCKET_FAMILIES);
        });
        program.on(libc::SYS_personality, |program| {
            program.allow_one_of(PERSONALITIES);
        });
        program.ret(libc::SECCOMP_RET_ALLOW);
        Filter(program.0)
    }

    /// Sets no new privileges and installs the filter on every thread of the process;
    /// async-signal-safe.
    pub fn install(&self) -> io::Result<()> {
        let program = libc::sock_fprog {
            len: self.0.len() as libc::c_ushort,
            filter: self.0.as_ptr().cast_mut(),
        };
        // SAFETY: prctl, then seccomp with a program of its length, alive for the call
        unsafe {
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            match libc::syscall(
                libc::SYS_seccomp,
                libc::SECCOMP_SET_MODE_FILTER,
                libc::SECCOMP_FILTER_FLAG_TSYNC,
                &program as *const libc::sock_fprog,
            ) {
                0 => Ok(()),
                -1 => Err(io::Error::last_os_error()),
                // a thread with a filter of its own (its id): none here
                _ => Err(io::ErrorKind::Other.into()),
            }
        }
    }
}

/// `SECCOMP_RET_ERRNO` of `errno`.
fn fail(errno: libc::c_int) -> u32 {
    libc::SECCOMP_RET_ERRNO | (errno as u32 & libc::SECCOMP_RET_DATA)
}

/// A classic BPF program under construction.
struct Program(Vec<libc::sock_filter>);

impl Program {
    fn push(&mut self, code: u32, k: u32, jt: u8, jf: u8) {
        self.0.push(libc::sock_filter {
            code: code as u16,
            jt,
            jf,
            k,
        });
    }

    /// Loads the word of `seccomp_data` at `offset`.
    fn load(&mut self, offset: u32) {
        self.push(libc::BPF_LD | libc::BPF_W | libc::BPF_ABS, offset, 0, 0);
    }

    fn jump(&mut self, op: u32, k: u32, jt: u8, jf: u8) {
        self.push(libc::BPF_JMP | op | libc::BPF_K, k, jt, jf);
    }

    fn jump_eq(&mut self, k: u32, jt: u8, jf: u8) {
        self.jump(libc::BPF_JEQ, k, jt, jf);
    }

    fn ret(&mut self, action: u32) {
        self.push(libc::BPF_RET | libc::BPF_K, action, 0, 0);
    }

    /// The call `nr` fails with `errno` (the number loaded).
    fn deny(&mut self, nr: libc::c_long, errno: libc::c_int) {
        self.jump_eq(nr as u32, 0, 1);
        self.ret(fail(errno));
    }

    /// The call `nr` is decided by `block` (the number loaded), on its first argument:
    /// every path of the block returns.
    fn on(&mut self, nr: libc::c_long, block: impl FnOnce(&mut Program)) {
        let mut inner = Program(Vec::new());
        inner.load(ARG0_LOW);
        block(&mut inner);
        let skip = u8::try_from(inner.0.len()).expect("a block jumped over in one");
        self.jump_eq(nr as u32, 0, skip);
        self.0.extend(inner.0);
    }

    /// Allowed when the value loaded is one of `values`, else `EPERM`.
    fn allow_one_of(&mut self, values: &[u32]) {
        let count = values.len();
        for (i, value) in values.iter().enumerate() {
            // to the allow after the denial
            self.jump_eq(*value, (count - i) as u8, 0);
        }
        self.ret(fail(libc::EPERM));
        self.ret(libc::SECCOMP_RET_ALLOW);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A call's value, or its errno negated.
    fn call(ret: libc::c_long) -> i64 {
        match ret {
            -1 => -i64::from(io::Error::last_os_error().raw_os_error().unwrap_or(0)),
            ret => ret,
        }
    }

    /// A call, and what it returns under the wicket's filter and the keeper's (None:
    /// allowed, whatever it returns).
    type Check = (&'static str, fn() -> i64, Option<i64>, Option<i64>);

    /// Each filter installed in a child of its own: what the calls it refuses and
    /// allows return there, as an unprivileged process sees them; the forker's as the
    /// wicket's, but for `clone3`.
    #[test]
    fn the_filters_refuse_dockers_denials_and_the_keeper_its_jails_calls() {
        let [eperm, einval, ebadf, enosys, enoent] = [
            libc::EPERM,
            libc::EINVAL,
            libc::EBADF,
            libc::ENOSYS,
            libc::ENOENT,
        ]
        .map(|e| Some(-i64::from(e)));
        // SAFETY (each check): a plain call; a clone that succeeds exits at once
        let checks: [Check; 10] = [
            (
                "getpid",
                || call(unsafe { libc::syscall(libc::SYS_getpid) }),
                None,
                None,
            ),
            (
                "unshare(0)",
                || call(unsafe { libc::syscall(libc::SYS_unshare, 0) }),
                eperm,
                Some(0),
            ),
            (
                "setns(-1)",
                || call(unsafe { libc::syscall(libc::SYS_setns, -1, 0) }),
                eperm,
                ebadf,
            ),
            // the empty path refused before the capability is checked
            (
                "chroot(\"\")",
                || call(unsafe { libc::syscall(libc::SYS_chroot, c"".as_ptr()) }),
                eperm,
                enoent,
            ),
            (
                "keyctl",
                || call(unsafe { libc::syscall(libc::SYS_keyctl, 0, 0, 0, 0, 0) }),
                eperm,
                eperm,
            ),
            (
                "clone3",
                || call(unsafe { libc::syscall(libc::SYS_clone3, std::ptr::null::<u8>(), 0) }),
                enosys,
                einval,
            ),
            (
                "clone(CLONE_NEWUSER)",
                || {
                    let flags = libc::CLONE_NEWUSER | libc::SIGCHLD;
                    let ret = unsafe { libc::syscall(libc::SYS_clone, flags, 0, 0, 0, 0) };
                    if ret == 0 {
                        unsafe { libc::_exit(0) }
                    }
                    call(ret)
                },
                eperm,
                None,
            ),
            (
                "socket(AF_VSOCK)",
                || call(unsafe { libc::syscall(libc::SYS_socket, libc::AF_VSOCK, -1, 0) }),
                eperm,
                eperm,
            ),
            // the type refused, not the family
            (
                "socket(AF_INET)",
                || call(unsafe { libc::syscall(libc::SYS_socket, libc::AF_INET, -1, 0) }),
                einval,
                einval,
            ),
            (
                "personality(ADDR_NO_RANDOMIZE)",
                || call(unsafe { libc::syscall(libc::SYS_personality, 0x0004_0000) }),
                eperm,
                eperm,
            ),
        ];
        for (jailer, clone3, filter) in [
            (false, false, Filter::unprivileged()),
            (true, true, Filter::keeper()),
            (false, true, Filter::forker()),
        ] {
            let mut results = [i64::MIN; 10];
            let mut pipe = [0; 2];
            // SAFETY: a pipe, a fork whose child installs the filter and writes what the
            // checks return (no allocation), and the parent's read and wait
            unsafe {
                assert_eq!(libc::pipe(pipe.as_mut_ptr()), 0);
                match libc::fork() {
                    0 => {
                        let mut got = [i64::MIN; 10];
                        if filter.install().is_ok() {
                            for (i, (_, check, _, _)) in checks.iter().enumerate() {
                                got[i] = check();
                            }
                        }
                        libc::write(pipe[1], got.as_ptr().cast(), size_of_val(&got));
                        libc::_exit(0);
                    }
                    pid => {
                        libc::close(pipe[1]);
                        let size = size_of_val(&results);
                        let read = libc::read(pipe[0], results.as_mut_ptr().cast(), size);
                        assert_eq!(read as usize, size);
                        libc::close(pipe[0]);
                        libc::waitpid(pid, std::ptr::null_mut(), 0);
                    }
                }
            }
            assert_ne!(results[0], i64::MIN, "the filter installed");
            for ((name, _, wicket, keeper), got) in checks.iter().zip(results) {
                let allowed = match *name {
                    "clone3" => clone3,
                    _ => jailer,
                };
                let filtered = format!("{name}, jailer: {jailer}, clone3: {clone3}");
                match if allowed { keeper } else { wicket } {
                    Some(want) => assert_eq!(got, *want, "{filtered}"),
                    None => assert_ne!(Some(got), eperm, "{filtered}"),
                }
            }
        }
    }
}
