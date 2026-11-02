//! One Firecracker microVM: the `firecracker` process, configured through its API
//! socket (HTTP/1.1 over a Unix socket), and the vsock streams to its guest.
//!
//! The guest is reached over vsock only (no TAP device, so no root): Firecracker exposes
//! the device as a Unix socket on the host, and a host-initiated stream is a connect to
//! it followed by `CONNECT <port>\n`, answered `OK <host port>\n`.
//!
//! A VM never outlives the wicket: `firecracker` runs with the parent death signal set
//! (SIGKILL). Setting it between fork and exec would make every spawn a fork of the
//! wicket, whose page tables the fork copies holding the wicket's memory map lock (1-2ms
//! of no page fault, no allocation growing the heap, on any of its threads, for every
//! VM spawned); the wicket instead spawns itself (`posix_spawn`, no copy) as a launcher
//! ([`launch`]) that sets the signal and execs `firecracker`.
//!
//! The launcher also lowers `firecracker`'s soft open file limit to what a VM needs
//! ([`set_open_files`]): Firecracker grows its file table to that limit at its start
//! (`dup2` to the last descriptor), 3ms per VM at 1048576.
//!
//! In jail mode the keeper spawns the VMs, each in a jail ([`Place::Jail`], `keeper`,
//! `jail`): `firecracker` sees its files at the jail's paths (`jail::KERNEL`, ...), the
//! wicket reaches its sockets in the VM's directory.
//!
//! A route with egress has its guests' streams to the host's vsock port
//! `wire::EGRESS_PORT` served by the wicket (`egress`): its VMs get the rules on their
//! kernel command line (`wire::EGRESS_PARAMETER`), and the wicket listens at each one's
//! socket for the port before it starts.
//!
//! A route's environment variables go to each VM it boots on the env stream
//! (`wire::ENV_PORT`), the wicket dialing `/init` before the process starts; a clone has
//! them in its snapshot's memory. No file holds them, so a jail needs nothing more, and
//! the kernel's command line, which any process of the guest reads, says only that they
//! are coming.

use std::ffi::OsString;
use std::os::fd::OwnedFd;
use std::os::unix::fs::DirBuilderExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sluice_wicket_console::env::Env;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, watch};

use crate::BoxError;
use crate::capacity::Slot;
use crate::egress::{self, Egress, Listening};
use crate::jail;
use crate::keeper::{Client, Role, Size, vm_file_size};
use crate::wire::{self, Rule};

/// Guest CID of every VM: each VM has its own vsock device, so they need not differ.
const GUEST_CID: u32 = 3;

/// How long the `firecracker` process may take to open its API socket.
const API_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether a restore advances the guest's clock by the time since its snapshot
/// (`clock_realtime`, kvmclock: x86_64 alone); elsewhere the clone resumes at its
/// snapshot's time, and the restore notice carries the host's (`wire::Notice`).
pub const RESTORE_ADVANCES_CLOCK: bool = cfg!(target_arch = "x86_64");

/// The first argument of the wicket's launcher mode, see [`launch`].
pub const LAUNCH: &str = "--launch-vm";

/// The descriptors a `firecracker` holds besides its vsock streams: KVM's, its
/// vCPUs', event fds, its sockets, files and log.
const VM_OWN_FILES: u64 = 128;

/// How long `/init` may take to read the env stream.
const ENV_TIMEOUT: Duration = Duration::from_secs(10);

/// The bytes of a route's egress rules a VM's kernel command line takes at most (2048 in
/// all, Firecracker's parameters and the wicket's own among them).
pub const MAX_EGRESS: usize = 1024;

/// The soft open file limit of each `firecracker`, see [`set_open_files`].
static VM_OPEN_FILES: AtomicU64 = AtomicU64::new(1024);

/// Raises the wicket's soft open file limit to its hard one (a connection holds a
/// vsock stream, a VM its sockets and log; a container's soft limit is often 1024),
/// and sets each `firecracker`'s to `connections` (its vsock streams at most) plus its
/// egress streams and its own. Returns both limits.
pub fn set_open_files(connections: usize) -> std::io::Result<(u64, u64)> {
    let hard = set_soft_open_files(u64::MAX)?;
    let vm = (connections as u64)
        .saturating_add(egress::MAX_CONNECTIONS as u64 + VM_OWN_FILES)
        .min(hard);
    VM_OPEN_FILES.store(vm, Ordering::Relaxed);
    Ok((hard, vm))
}

/// Sets the process's soft open file limit to `soft`, at most its hard one; returns
/// the hard one.
fn set_soft_open_files(soft: u64) -> std::io::Result<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: plain getrlimit / setrlimit on a valid struct
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    limit.rlim_cur = soft.min(limit.rlim_max);
    // SAFETY: as above
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(limit.rlim_max)
}

/// The threads spawning the launchers, see [`launching`].
const LAUNCHING_THREADS: usize = 4;

/// A spawn on a launching thread.
type Launching = Box<dyn FnOnce() + Send>;

/// Runs `spawn` (a launcher's spawn) on one of the wicket's launching threads, in the
/// caller's runtime: a `posix_spawn` holds its thread until the launcher's exec (1ms), a
/// worker's relays with it. A child's parent death signal fires as the thread that
/// spawned it ends, not its process: the threads live as long as the wicket (a runtime's
/// blocking threads end once idle, and their VMs with them).
async fn launching<R: Send + 'static>(
    spawn: impl FnOnce() -> R + Send + 'static,
) -> Result<R, BoxError> {
    static THREADS: OnceLock<Mutex<std::sync::mpsc::Sender<Launching>>> = OnceLock::new();
    let threads = THREADS.get_or_init(|| {
        let (sender, receiver) = std::sync::mpsc::channel::<Launching>();
        let receiver = Arc::new(Mutex::new(receiver));
        for n in 0..LAUNCHING_THREADS {
            let receiver = receiver.clone();
            let _ = std::thread::Builder::new()
                .name(format!("launching-{n}"))
                .spawn(move || {
                    loop {
                        let next = receiver.lock().unwrap_or_else(|e| e.into_inner()).recv();
                        match next {
                            Ok(spawn) => spawn(),
                            Err(_) => return,
                        }
                    }
                });
        }
        Mutex::new(sender)
    });
    let runtime = tokio::runtime::Handle::current();
    let (done, spawned) = oneshot::channel();
    let job: Launching = Box::new(move || {
        let _entered = runtime.enter();
        let _ = done.send(spawn());
    });
    threads
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .send(job)
        .map_err(|_| "no launching thread")?;
    Ok(spawned.await.map_err(|_| "no launching thread")?)
}

/// The launcher mode (`<wicket> --launch-vm <wicket pid> <open files> <file size>
/// <program> <args>...`), run before anything else: sets the parent death signal
/// (SIGKILL), checks that the parent is still the wicket of `<wicket pid>` (else it
/// died before the signal was set, and exits), sets the soft open file limit, the file
/// size limit (bytes; a guest's output to its VM's log bounded, as a jail's) and no
/// core (of a VM's memory), and execs the program. Returns only when that failed.
pub fn launch(mut args: impl Iterator<Item = OsString>) -> std::io::Error {
    let mut number = || {
        args.next()
            .and_then(|arg| arg.to_str()?.parse::<u64>().ok())
    };
    let (wicket, files, size) = (number(), number(), number());
    let (Some(wicket), Some(files), Some(size), Some(program)) = (wicket, files, size, args.next())
    else {
        return std::io::Error::other(format!(
            "usage: {LAUNCH} <wicket pid> <open files> <file size> <program> <args>..."
        ));
    };
    // SAFETY: a plain prctl; the signal follows the parent thread, a runtime worker
    // living as long as the wicket
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return std::io::Error::last_os_error();
    }
    // SAFETY: a plain getppid
    if u64::try_from(unsafe { libc::getppid() }) != Ok(wicket) {
        return std::io::Error::other("the wicket ended before the VM started");
    }
    if let Err(e) = set_soft_open_files(files) {
        return e;
    }
    // soft and hard, at most the hard one
    for (resource, limit) in [(libc::RLIMIT_FSIZE, size), (libc::RLIMIT_CORE, 0)] {
        let mut current = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: plain getrlimit / setrlimit on a valid struct
        if unsafe { libc::getrlimit(resource, &mut current) } != 0 {
            return std::io::Error::last_os_error();
        }
        let limit = limit.min(current.rlim_max);
        let limit = libc::rlimit {
            rlim_cur: limit,
            rlim_max: limit,
        };
        // SAFETY: as above
        if unsafe { libc::setrlimit(resource, &limit) } != 0 {
            return std::io::Error::last_os_error();
        }
    }
    std::process::Command::new(program).args(args).exec()
}

/// What a route's VMs are booted from.
#[derive(Clone, Debug, PartialEq)]
pub struct Spec {
    pub firecracker: PathBuf,
    pub kernel: PathBuf,
    pub rootfs: PathBuf,
    pub vcpus: u32,
    pub mem_mib: u32,
    /// Jail mode: the host CPU time a VM may take, in percent of one CPU (its cgroup's
    /// `cpu.max`); None = its vCPUs bound it.
    pub cpu: Option<u32>,
    /// The destinations its guests may connect to, through the wicket (sorted, once
    /// each).
    pub egress: Vec<Rule>,
    /// The environment variables of its process, over the image's.
    pub env: Env,
}

/// `rules` as a VM's kernel command line and a route's target name them: `,` between.
pub fn egress_list(rules: &[Rule]) -> String {
    let rules: Vec<String> = rules.iter().map(Rule::to_string).collect();
    rules.join(",")
}

impl Spec {
    /// What a VM of the spec is given, as the keeper limits it.
    fn size(&self) -> Size {
        Size {
            vcpus: self.vcpus,
            mem: self.mem_mib,
            cpu: self.cpu,
        }
    }
}

/// A paused VM's state and memory, which [`Vm::restore`] clones VMs from.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub firecracker: PathBuf,
    /// The state and memory files as `firecracker` sees them.
    pub state: PathBuf,
    pub memory: PathBuf,
    /// The memory file on the host.
    pub memory_on_host: PathBuf,
    /// Its template's, its clones'.
    size: Size,
}

/// How the wicket starts VMs, before its runtime does.
pub enum Launcher {
    /// As its children (the launcher mode), their files in the state dir.
    Direct { state: PathBuf },
    /// Through the keeper at `fd` (jail mode), whose `firecracker` is the one named at
    /// startup.
    Jailed { fd: OwnedFd, firecracker: PathBuf },
}

impl Launcher {
    /// Its places; the keeper's client started (in the runtime).
    pub fn start(self) -> std::io::Result<Places> {
        Ok(match self {
            Launcher::Direct { state } => Places::Dirs(state),
            Launcher::Jailed { fd, firecracker } => Places::Jails {
                keeper: Client::start(fd)?,
                firecracker,
            },
        })
    }
}

/// Where the routes' VMs are started.
pub enum Places {
    /// A directory per route in the state dir.
    Dirs(PathBuf),
    /// Jails of the keeper's.
    Jails {
        keeper: Arc<Client>,
        firecracker: PathBuf,
    },
}

impl Places {
    /// The place of the `n`th route started, of `spec`: `route-<n>` in the state dir
    /// (0700: the snapshot is the template's whole memory), or a route of the keeper's
    /// given the spec's kernel and rootfs, opened as the wicket's user.
    pub async fn place(&self, n: u64, spec: &Spec) -> Result<Place, BoxError> {
        match self {
            Places::Dirs(state) => {
                let dir = state.join(format!("route-{n}"));
                std::fs::DirBuilder::new()
                    .mode(0o700)
                    .create(&dir)
                    .map_err(|e| format!("{}: {e}", dir.display()))?;
                Ok(Place::Dir(dir))
            }
            Places::Jails {
                keeper,
                firecracker,
            } => {
                if spec.firecracker != *firecracker {
                    return Err(format!(
                        "firecracker {}: jail mode runs the one of its start ({}), the keeper's",
                        spec.firecracker.display(),
                        firecracker.display()
                    )
                    .into());
                }
                let open = |path: &Path| {
                    let file = std::fs::File::open(path)
                        .map_err(|e| format!("{}: {e}", path.display()))?;
                    let mode = std::os::unix::fs::MetadataExt::mode(&file.metadata()?);
                    match jail::unshareable(mode) {
                        Some(why) => Err(format!("{}: {why}", path.display()).into()),
                        None => Ok::<_, BoxError>(file),
                    }
                };
                let (route, snapshot) = keeper
                    .open(open(&spec.kernel)?, open(&spec.rootfs)?)
                    .await?;
                Ok(Place::Jail {
                    keeper: keeper.clone(),
                    route,
                    snapshot,
                })
            }
        }
    }
}

/// Where a route's VMs are started, and their files are; removed with the route.
pub enum Place {
    /// The route's directory: `boot`, `snapshot`, `clones/vm-<n>`.
    Dir(PathBuf),
    /// The keeper's route, and its snapshot directory.
    Jail {
        keeper: Arc<Client>,
        route: u64,
        snapshot: PathBuf,
    },
}

impl Place {
    /// Makes the route's snapshot read-only to the clones, its template ended (jail
    /// mode).
    pub async fn seal(&self) -> Result<(), BoxError> {
        match self {
            Place::Dir(_) => Ok(()),
            Place::Jail { keeper, route, .. } => keeper.seal(*route).await,
        }
    }
}

impl Drop for Place {
    /// The route ended: its files go (once its VMs did, in jail mode).
    fn drop(&mut self) {
        match self {
            Place::Dir(dir) => {
                let _ = std::fs::remove_dir_all(dir);
            }
            Place::Jail { keeper, route, .. } => keeper.close(*route),
        }
    }
}

/// A VM's `firecracker` process.
enum Process {
    /// The wicket's child, its directory removed with it.
    Child { child: Child, dir: PathBuf },
    /// The keeper's, in a jail; its exit once it ended.
    Jailed {
        keeper: Arc<Client>,
        vm: u64,
        exited: watch::Receiver<Option<String>>,
    },
}

/// A running VM, killed (and its files removed) on drop.
pub struct Vm {
    process: Process,
    /// The VM's share of the budget, given back once it is killed.
    _slot: Slot,
    /// Its API socket, vsock socket and log, on the host.
    api: PathBuf,
    vsock: PathBuf,
    log: PathBuf,
    /// Its vsock socket as `firecracker` sees it.
    vsock_inside: PathBuf,
    /// Its listener for the guest's egress streams, when its route has egress.
    egress: Option<Listening>,
}

impl Vm {
    /// Starts `firecracker` in `dir` (created; its API socket, vsock socket and log go
    /// there), the files it writes at most `file_size` bytes each, and waits for its API
    /// socket.
    async fn spawn(
        firecracker: &Path,
        dir: PathBuf,
        file_size: u64,
        slot: Slot,
    ) -> Result<Vm, BoxError> {
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| format!("{}: {e}", dir.display()))?;
        let api = dir.join("api.sock");
        // sun_path holds 108 bytes, its NUL included; the egress's is the longest socket
        let longest = dir.join(format!("v.sock_{}", wire::EGRESS_PORT));
        if longest.as_os_str().len() > 107 {
            return Err(format!(
                "{}: the VM's sockets need a path below 108 bytes; choose a shorter --state-dir",
                dir.display()
            )
            .into());
        }
        let log = dir.join("firecracker.log");
        let vm_log = log.clone();
        let (firecracker, api_path, dir_path) = (firecracker.to_owned(), api.clone(), dir.clone());
        let child = launching(move || -> Result<Child, BoxError> {
            let file =
                std::fs::File::create(&log).map_err(|e| format!("{}: {e}", log.display()))?;
            // the wicket itself, as the launcher (see `launch`): a posix_spawn, no fork
            Ok(Command::new("/proc/self/exe")
                .arg(LAUNCH)
                .arg(std::process::id().to_string())
                .arg(VM_OPEN_FILES.load(Ordering::Relaxed).to_string())
                .arg(file_size.to_string())
                .arg(&firecracker)
                .arg("--api-sock")
                .arg(&api_path)
                .current_dir(&dir_path)
                // the wicket's environment (its token among it) is no VM's business;
                // PATH finds a bare --firecracker
                .env_clear()
                .envs(std::env::var_os("PATH").map(|path| ("PATH", path)))
                .stdin(Stdio::null())
                .stdout(file.try_clone()?)
                .stderr(file)
                .kill_on_drop(true)
                .spawn()
                .map_err(|e| format!("{LAUNCH} {}: {e}", firecracker.display()))?)
        })
        .await??;
        let vsock = dir.join("v.sock");
        let mut vm = Vm {
            process: Process::Child { child, dir },
            _slot: slot,
            api,
            vsock_inside: vsock.clone(),
            vsock,
            log: vm_log,
            egress: None,
        };
        vm.ready().await?;
        Ok(vm)
    }

    /// Has the keeper spawn a VM of `route` in a jail, and waits for its API socket.
    async fn jailed(
        keeper: &Arc<Client>,
        route: u64,
        role: Role,
        size: Size,
        slot: Slot,
    ) -> Result<Vm, BoxError> {
        let open_files = VM_OPEN_FILES.load(Ordering::Relaxed);
        let spawned = keeper.spawn(route, role, size, open_files).await?;
        let run = spawned.dir.join("run");
        let mut vm = Vm {
            process: Process::Jailed {
                keeper: keeper.clone(),
                vm: spawned.vm,
                exited: spawned.exited,
            },
            _slot: slot,
            api: run.join("api.sock"),
            vsock: run.join("v.sock"),
            log: spawned.dir.join("firecracker.log"),
            vsock_inside: PathBuf::from(jail::VSOCK_SOCKET),
            egress: None,
        };
        vm.ready().await?;
        Ok(vm)
    }

    /// Waits for the API socket, within [`API_TIMEOUT`] and while the VM lives.
    async fn ready(&mut self) -> Result<(), BoxError> {
        let deadline = Instant::now() + API_TIMEOUT;
        while UnixStream::connect(&self.api).await.is_err() {
            let ended = match &mut self.process {
                Process::Child { child, .. } => child
                    .try_wait()?
                    .map(|status| format!("{status}; {}", log_tail(&self.log))),
                // its directory is gone: the keeper sent its log's tail
                Process::Jailed { exited, .. } => exited.borrow().clone(),
            };
            if let Some(ended) = ended {
                return Err(format!("the VM ended: {ended}").into());
            }
            if Instant::now() > deadline {
                return Err(format!("no API socket within {API_TIMEOUT:?}; {}", self.log()).into());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    /// Boots a fresh VM of `spec` at `place` (cold boot), the route's shared VM or its
    /// template (`role`); its egress served by `egress` as the VM named so.
    pub async fn boot(
        spec: &Spec,
        place: &Place,
        role: Role,
        slot: Slot,
        egress: Option<(&Arc<Egress>, String)>,
    ) -> Result<Vm, BoxError> {
        let (mut vm, kernel, rootfs) = match place {
            Place::Dir(dir) => (
                Vm::spawn(
                    &spec.firecracker,
                    dir.join("boot"),
                    vm_file_size(role, u64::from(spec.mem_mib)),
                    slot,
                )
                .await?,
                spec.kernel.clone(),
                spec.rootfs.clone(),
            ),
            Place::Jail { keeper, route, .. } => (
                Vm::jailed(keeper, *route, role, spec.size(), slot).await?,
                PathBuf::from(jail::KERNEL),
                PathBuf::from(jail::ROOTFS),
            ),
        };
        vm.listen(egress)?;
        vm.api(
            "PUT",
            "/machine-config",
            json!({ "vcpu_count": spec.vcpus, "mem_size_mib": spec.mem_mib }),
        )
        .await?;
        vm.api(
            "PUT",
            "/boot-source",
            json!({
                "kernel_image_path": kernel,
                "boot_args": boot_args(&spec.egress, !spec.env.is_empty()),
            }),
        )
        .await?;
        vm.api(
            "PUT",
            "/drives/rootfs",
            json!({
                "drive_id": "rootfs",
                "path_on_host": rootfs,
                "is_root_device": true,
                // shared by every VM of the route, and by the clones of its snapshot
                "is_read_only": true,
            }),
        )
        .await?;
        vm.api(
            "PUT",
            "/vsock",
            json!({ "guest_cid": GUEST_CID, "uds_path": vm.vsock_inside }),
        )
        .await?;
        vm.api("PUT", "/actions", json!({ "action_type": "InstanceStart" }))
            .await?;
        if !spec.env.is_empty() {
            vm.give_env(&spec.env).await?;
        }
        Ok(vm)
    }

    /// Gives `/init`, waiting for it, the variables of the process's environment. An
    /// error names none of them.
    async fn give_env(&self, env: &Env) -> Result<(), BoxError> {
        let mut stream = self.dial_ready(wire::ENV_PORT, ENV_TIMEOUT).await?;
        let json = serde_json::to_vec(env.map())?;
        wire::send_env(&mut stream, &json)
            .await
            .map_err(|e| format!("the guest's env stream: {e}; {}", self.log()))?;
        Ok(())
    }

    /// Clones a running VM from `snapshot` at `place` (the `n`th clone): its memory file
    /// is mapped private (copy on write), so clones share the pages they do not write.
    /// Its egress is served by `egress`.
    pub async fn restore(
        snapshot: &Snapshot,
        place: &Place,
        n: u64,
        slot: Slot,
        egress: Option<&Arc<Egress>>,
    ) -> Result<Vm, BoxError> {
        let mut vm = match place {
            Place::Dir(dir) => {
                let dir = dir.join("clones").join(format!("vm-{n}"));
                let size = vm_file_size(Role::Clone, u64::from(snapshot.size.mem));
                Vm::spawn(&snapshot.firecracker, dir, size, slot).await?
            }
            Place::Jail { keeper, route, .. } => {
                Vm::jailed(keeper, *route, Role::Clone, snapshot.size, slot).await?
            }
        };
        vm.listen(egress.map(|egress| (egress, format!("vm-{n}"))))?;
        vm.api(
            "PUT",
            "/snapshot/load",
            json!({
                "snapshot_path": snapshot.state,
                "mem_backend": { "backend_type": "File", "backend_path": snapshot.memory },
                "vsock_override": { "uds_path": vm.vsock_inside },
                // the clock advanced by the time since the snapshot, not resumed
                // from it (x86_64; elsewhere the restore notice sets it)
                "clock_realtime": RESTORE_ADVANCES_CLOCK,
                "resume_vm": true,
            }),
        )
        .await?;
        Ok(vm)
    }

    /// Pauses the VM (of `spec`) and writes its full snapshot into its place's snapshot
    /// directory; the VM stays paused.
    pub async fn snapshot(&self, place: &Place, spec: &Spec) -> Result<Snapshot, BoxError> {
        let (inside, on_host) = match place {
            Place::Dir(dir) => {
                let dir = dir.join("snapshot");
                tokio::fs::create_dir_all(&dir).await?;
                (dir.clone(), dir)
            }
            Place::Jail { snapshot, .. } => (PathBuf::from(jail::SNAPSHOT), snapshot.clone()),
        };
        let snapshot = Snapshot {
            firecracker: spec.firecracker.clone(),
            state: inside.join("vm.state"),
            memory: inside.join("vm.mem"),
            memory_on_host: on_host.join("vm.mem"),
            size: spec.size(),
        };
        self.api("PATCH", "/vm", json!({ "state": "Paused" }))
            .await?;
        self.api(
            "PUT",
            "/snapshot/create",
            json!({
                "snapshot_type": "Full",
                "snapshot_path": snapshot.state,
                "mem_file_path": snapshot.memory,
            }),
        )
        .await?;
        Ok(snapshot)
    }

    /// Listens for the guest's egress streams, served by `egress` as the VM named so:
    /// before it starts, or is restored.
    fn listen(&mut self, egress: Option<(&Arc<Egress>, String)>) -> Result<(), BoxError> {
        if let Some((egress, name)) = egress {
            self.egress = Some(egress.listen(&self.vsock, name)?);
        }
        Ok(())
    }

    /// Opens a stream to the guest's vsock `port`.
    pub async fn dial(&self, port: u32) -> Result<UnixStream, BoxError> {
        let mut stream = UnixStream::connect(&self.vsock).await?;
        stream
            .write_all(format!("CONNECT {port}\n").as_bytes())
            .await?;
        // what follows the line is the guest's
        let line = wire::read_short_line(&mut stream, 64)
            .await
            .map_err(|e| format!("vsock handshake: {e}"))?;
        if !line.starts_with(b"OK ") {
            return Err(
                format!("vsock handshake: {}", String::from_utf8_lossy(&line).trim()).into(),
            );
        }
        Ok(stream)
    }

    /// [`Vm::dial`] retried until the guest listens on `port`, within `timeout`.
    pub async fn dial_ready(&self, port: u32, timeout: Duration) -> Result<UnixStream, BoxError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.dial(port).await {
                Ok(stream) => return Ok(stream),
                Err(e) if Instant::now() > deadline => {
                    return Err(format!(
                        "guest not listening on vsock port {port} within {timeout:?}: {e}; {}",
                        self.log()
                    )
                    .into());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    }

    /// The tail of the VM's log (Firecracker's and the guest console's): its directory
    /// goes with the VM.
    fn log(&self) -> String {
        log_tail(&self.log)
    }

    /// One request to the API socket; a non-2xx answer is an error naming its body.
    async fn api(&self, method: &str, path: &str, body: Value) -> Result<(), BoxError> {
        let stream = UnixStream::connect(&self.api).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        tokio::spawn(connection);
        let request = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body.to_string())))?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.into_body().collect().await?.to_bytes();
        Err(format!(
            "firecracker {method} {path}: {status} {}",
            String::from_utf8_lossy(&body)
        )
        .into())
    }
}

impl Drop for Vm {
    fn drop(&mut self) {
        match &mut self.process {
            Process::Child { child, dir } => {
                let _ = child.start_kill();
                let _ = std::fs::remove_dir_all(dir);
            }
            // the keeper reaps it and removes its directory
            Process::Jailed { keeper, vm, .. } => keeper.kill(*vm),
        }
    }
}

/// The last lines of a VM's log at `path` (Firecracker's and the guest console's), read
/// from its last 4KiB: for an error.
pub fn log_tail(path: &Path) -> String {
    use std::io::{Read as _, Seek as _};
    let mut tail = Vec::new();
    if let Ok(mut file) = std::fs::File::open(path) {
        let end = file.seek(std::io::SeekFrom::End(0)).unwrap_or(0);
        let _ = file.seek(std::io::SeekFrom::Start(end.saturating_sub(4096)));
        let _ = file.take(4096).read_to_end(&mut tail);
    }
    let tail = String::from_utf8_lossy(&tail);
    let lines: Vec<_> = tail.lines().collect();
    format!(
        "firecracker log:\n{}",
        lines[lines.len().saturating_sub(10)..].join("\n")
    )
}

/// No serial console unless `SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE` is set (the kernel's and
/// `/init`'s own output then goes to the VM's log; the processes' goes to the wicket's
/// stdout / stderr regardless): printing to it is the slowest part of a boot. The route's
/// `egress`, if any, for `/init`, and that its env is coming (`env`).
fn boot_args(egress: &[Rule], env: bool) -> String {
    let console = match std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE") {
        Some(_) => "console=ttyS0",
        None => "console=none 8250.nr_uarts=0",
    };
    // x86_64's (arm64 knows neither): no PCI bus probed; kvm-clock, the clock a restore
    // advances (`clock_realtime`), the TSC it does not
    let arch = match cfg!(target_arch = "x86_64") {
        true => " pci=off clocksource=kvm-clock",
        false => "",
    };
    let egress = match egress {
        [] => String::new(),
        rules => format!(" {}={}", wire::EGRESS_PARAMETER, egress_list(rules)),
    };
    let env = match env {
        true => format!(" {}=1", wire::ENV_PARAMETER),
        false => String::new(),
    };
    format!("{console} reboot=k panic=1 quiet{arch} init=/init{egress}{env}")
}
