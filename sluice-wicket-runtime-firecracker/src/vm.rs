//! One Firecracker microVM: the `firecracker` process, configured through its API
//! socket (HTTP/1.1 over a Unix socket), and the vsock streams to its guest.
//!
//! The guest is reached over vsock only (no TAP device, so no root): Firecracker exposes
//! the device as a Unix socket on the host, and a host-initiated stream is a connect to
//! it followed by `CONNECT <port>\n`, answered `OK <host port>\n`.
//!
//! A VM never outlives the wicket: `firecracker` runs with the parent death signal set
//! (SIGKILL). The wicket forks none itself (a fork copies its page tables holding its memory
//! map lock): without the jail the forker does ([`Place::Dir`], `forker`), the VM the
//! wicket's child, in a cgroup of its own where the wicket has them (`cgroup`). Before the
//! VM's exec it sets that signal and lowers `firecracker`'s soft open file limit to what a VM
//! needs ([`set_open_files`]): Firecracker grows its file table to that limit at its start
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
//!
//! An app's booted VM may have a disk (`disk`), a second drive: its lock is taken before
//! `firecracker`'s exec, whose guest's flushes reach the host's disk (`Writeback`: its journal
//! relies on them across a crash of the host). Its VM is stopped by a notice before it is
//! killed ([`Vm::stop`]), dropped or not: a start given up drops it, and the stop then runs in a
//! task of its own (`StopOnDrop`), the disk held until the VM is gone.

use std::os::fd::OwnedFd;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sluice_wicket_console::env::Env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio_util::task::TaskTracker;

use crate::BoxError;
use crate::capacity::Slot;
use crate::cgroup::Cgroups;
use crate::disk::{self, Disk};
use crate::egress::{self, Egress, Listening};
use crate::forker::{Child, Forker, Launch};
use crate::jail;
use crate::keeper::{Client, Role, Size, VmEnd, vm_file_size};
use crate::usage::{self, Meter};
use crate::wire::{self, Rule};

/// Guest CID of every VM: each VM has its own vsock device, so they need not differ.
const GUEST_CID: u32 = 3;

/// How long the `firecracker` process may take to open its API socket.
const API_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether a restore advances the guest's clock by the time since its snapshot
/// (`clock_realtime`, kvmclock: x86_64 alone); elsewhere the clone resumes at its
/// snapshot's time, and the restore notice carries the host's (`wire::Notice`).
pub const RESTORE_ADVANCES_CLOCK: bool = cfg!(target_arch = "x86_64");

/// Set, the VMs have a serial console (`boot_args`).
pub const SERIAL_CONSOLE: &str = "SLUICE_WICKET_RUNTIME_FIRECRACKER_SERIAL_CONSOLE";

/// The descriptors a `firecracker` holds besides its vsock streams: KVM's, its
/// vCPUs', event fds, its sockets, files and log.
const VM_OWN_FILES: u64 = 128;

/// How long `/init` may take to read the env stream.
const ENV_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a guest may take to answer of its disk: it mounts it first, a journal to replay.
const DISK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a guest may take to write all out once its process ended (a stop).
pub const STOP_WRITES: Duration = Duration::from_secs(10);

/// How long a guest may take to take a vsock stream: its kernel's, its process's business
/// aside. Past it the guest is wedged, and no connection, stop or reload waits on for it.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// How long `/init` may take to drop a stream the process refuses.
pub const REFUSAL_TIMEOUT: Duration = Duration::from_secs(1);

/// The longest answer of a guest's control stream the wicket reads.
const MAX_CONTROL_ANSWER: usize = 1024;

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
pub(crate) fn set_soft_open_files(soft: u64) -> std::io::Result<u64> {
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

/// What a route's VMs are booted from.
#[derive(Clone, Debug, PartialEq)]
pub struct Spec {
    pub firecracker: PathBuf,
    pub kernel: PathBuf,
    pub rootfs: PathBuf,
    pub vcpus: u32,
    pub memory_mib: u32,
    /// The host CPU time a VM may take, in percent of one CPU (its cgroup's `cpu.max`: jail
    /// mode, or the wicket's own cgroup delegated to it); None = its vCPUs bound it.
    pub cpu: Option<u32>,
    /// The destinations its guests may connect to, through the wicket (sorted, once
    /// each).
    pub egress: Vec<Rule>,
    /// The environment variables of its process, over the image's.
    pub env: Env,
    /// Its disk: a booted VM's, an app's.
    pub disk: Option<Disk>,
    /// A build's VM (`stage`): its guest runs a build, no process of the image's.
    pub stage: bool,
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
            mem: self.memory_mib,
            cpu: self.cpu,
            shared: false,
        }
    }

    /// What a VM of the spec holds of the CPU budget, in hundredths of a CPU: its vCPUs, or
    /// its `cpu` where that is less.
    pub fn cpus(&self) -> u64 {
        self.size().cpus()
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

impl Snapshot {
    /// Its clones are each a connection's own: in their cgroups, they share one group of the
    /// cpu controller (`cgroup::Cgroups`).
    pub fn of_connections(mut self) -> Snapshot {
        self.size.shared = true;
        self
    }
}

/// How the wicket starts VMs, before its runtime does.
pub enum Launcher {
    /// Through the forker at `forker` (the wicket's end of it), its children: their files in
    /// the state dir, their cgroups `cgroups` where the wicket's own is delegated to it.
    Direct {
        state: PathBuf,
        forker: OwnedFd,
        cgroups: Option<Cgroups>,
    },
    /// Through the keeper at `fd` (jail mode), whose `firecracker` is the one named at
    /// startup.
    Jailed { fd: OwnedFd, firecracker: PathBuf },
}

impl Launcher {
    /// Its places; the forker asked, or the keeper's client started (in the runtime), from
    /// here on.
    pub fn start(self) -> std::io::Result<Places> {
        Ok(match self {
            Launcher::Direct {
                state,
                forker,
                cgroups,
            } => Places::Dirs {
                state,
                forker: Arc::new(Forker::new(forker, cgroups)?),
            },
            Launcher::Jailed { fd, firecracker } => Places::Jails {
                keeper: Client::start(fd)?,
                firecracker,
            },
        })
    }
}

/// Where the routes' VMs are started.
pub enum Places {
    /// A directory per route in the state dir, its VMs the forker's to fork.
    Dirs { state: PathBuf, forker: Arc<Forker> },
    /// Jails of the keeper's.
    Jails {
        keeper: Arc<Client>,
        firecracker: PathBuf,
    },
}

impl Places {
    /// The place `name` (a route's `route-<n>`, a build's `build-<id>`), of `spec`: that
    /// directory in the state dir (0700: the snapshot is what the template wrote of its
    /// memory), or a route of the keeper's given the spec's kernel and rootfs, opened as the
    /// wicket's user.
    pub async fn place(&self, name: &str, spec: &Spec) -> Result<Place, BoxError> {
        match self {
            Places::Dirs { state, forker } => {
                let dir = state.join(name);
                std::fs::DirBuilder::new()
                    .mode(0o700)
                    .create(&dir)
                    .map_err(|e| format!("{}: {e}", dir.display()))?;
                Ok(Place::Dir {
                    dir,
                    forker: forker.clone(),
                })
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
                // its file the VM's alone to write, once the keeper gave it its id
                let disk = spec
                    .disk
                    .as_ref()
                    .map(|disk| {
                        std::fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(&disk.file)
                            .map_err(|e| format!("{}: {e}", disk.file.display()))
                    })
                    .transpose()?;
                let (route, snapshot) = keeper
                    .open(open(&spec.kernel)?, open(&spec.rootfs)?, disk)
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
    /// The route's directory: `boot` (its template), `snapshot`, `clones/vm-<n>`, `vms/vm-<n>`;
    /// named as its template's cgroup is, where the wicket has them.
    Dir { dir: PathBuf, forker: Arc<Forker> },
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
            Place::Dir { .. } => Ok(()),
            Place::Jail { keeper, route, .. } => keeper.seal(*route).await,
        }
    }
}

impl Drop for Place {
    /// The route ended: its files go (once its VMs did, in jail mode), its template's
    /// cgroup with them.
    fn drop(&mut self) {
        match self {
            Place::Dir { dir, forker } => {
                forker.forget(&place_name(dir));
                let _ = std::fs::remove_dir_all(dir);
            }
            Place::Jail { keeper, route, .. } => keeper.close(*route),
        }
    }
}

/// The name of the place whose directory is `dir`.
fn place_name(dir: &Path) -> String {
    let name = dir.file_name().unwrap_or_default();
    name.to_string_lossy().into_owned()
}

/// A VM's `firecracker` process.
enum Process {
    /// The wicket's child, the forker's fork; its directory removed with it.
    Child { child: Child, dir: PathBuf },
    /// The keeper's, in a jail; its exit once it ended.
    Jailed {
        keeper: Arc<Client>,
        vm: u64,
        exited: watch::Receiver<Option<VmEnd>>,
    },
    /// Handed over to the task of its stop, [`Vm::handed_over`].
    HandedOver,
}

/// What stops a VM on a disk as it is dropped.
struct StopOnDrop {
    /// The disk's file, for the log.
    file: PathBuf,
    /// The VM's sockets are in there: not removed before its stop.
    _place: Arc<Place>,
    /// What the wicket's end waits for.
    stops: TaskTracker,
}

/// What a VM without the jail is started with ([`Vm::spawn`]).
struct Start<'a> {
    forker: &'a Forker,
    /// Its place's directory.
    place: &'a Path,
    firecracker: &'a Path,
    /// Its own directory, in its place's.
    dir: PathBuf,
    /// The files it writes at most, bytes each.
    file_size: u64,
    /// Its disk, whose lock it holds.
    disk: Option<&'a Path>,
    /// Its part in its route and what it is given: its cgroup's.
    role: Role,
    size: Size,
}

/// A running VM, killed (and its files removed) on drop; on a disk, stopped first.
pub struct Vm {
    process: Process,
    /// What it uses, as the sampler reads it of its process.
    meter: Arc<Meter>,
    /// The VM's share of the budget, given back once it is killed.
    _slot: Option<Slot>,
    /// Its API socket, vsock socket and log, on the host.
    api: PathBuf,
    vsock: PathBuf,
    log: PathBuf,
    /// Its vsock socket as `firecracker` sees it.
    vsock_inside: PathBuf,
    /// Its listener for the guest's egress streams, when its route has egress.
    egress: Option<Listening>,
    stop_on_drop: Option<StopOnDrop>,
    /// A stop ended: none more as it is dropped.
    stopped: AtomicBool,
}

impl Vm {
    /// Has the forker start `firecracker` as `start` says, in its directory (created; its API
    /// socket, vsock socket and log go there), and waits for its API socket.
    async fn spawn(start: Start<'_>, slot: Slot) -> Result<Vm, BoxError> {
        let dir = start.dir;
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
        let launch = Launch {
            program: start.firecracker.to_owned(),
            args: vec!["--api-sock".into(), api.clone().into()],
            dir: dir.clone(),
            log: log.clone(),
            open_files: VM_OPEN_FILES.load(Ordering::Relaxed),
            file_size: start.file_size,
            disk: start.disk.map(Path::to_path_buf),
            place: place_name(start.place),
            role: start.role,
            size: start.size,
        };
        let child = start
            .forker
            .fork(launch)
            .await
            .map_err(|e| format!("{}: {e}", start.firecracker.display()))?;
        let vsock = dir.join("v.sock");
        let mut vm = Vm {
            meter: child.meter().clone(),
            process: Process::Child { child, dir },
            _slot: Some(slot),
            api,
            vsock_inside: vsock.clone(),
            vsock,
            log,
            egress: None,
            stop_on_drop: None,
            stopped: AtomicBool::new(false),
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
            meter: Meter::start(spawned.pid, spawned.pidfd.map(Arc::new)),
            _slot: Some(slot),
            api: run.join("api.sock"),
            vsock: run.join("v.sock"),
            log: spawned.dir.join("firecracker.log"),
            vsock_inside: PathBuf::from(jail::VSOCK_SOCKET),
            egress: None,
            stop_on_drop: None,
            stopped: AtomicBool::new(false),
        };
        vm.ready().await?;
        Ok(vm)
    }

    /// Waits for the API socket, within [`API_TIMEOUT`] and while the VM lives.
    async fn ready(&mut self) -> Result<(), BoxError> {
        let deadline = Instant::now() + API_TIMEOUT;
        while UnixStream::connect(&self.api).await.is_err() {
            let ended = match &self.process {
                Process::Child { child, .. } => (child.exited().borrow().as_ref())
                    .map(|end| format!("{}; {}", end.status, log_tail(&self.log))),
                // its directory is gone: the keeper sent its log's tail
                Process::Jailed { exited, .. } => {
                    (exited.borrow().as_ref()).map(|end| format!("{}; {}", end.status, end.log))
                }
                Process::HandedOver => None,
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

    /// Boots a fresh VM of `spec` at `place` (cold boot), one serving the route or its
    /// template (`role`), its files in the directory `name` of a place of directories; its egress
    /// served by `egress` as the VM named so. On a disk, a stop as it is dropped goes to `stops`.
    pub async fn boot(
        spec: &Spec,
        place: &Arc<Place>,
        role: Role,
        name: &str,
        slot: Slot,
        egress: Option<(&Arc<Egress>, String)>,
        stops: &TaskTracker,
    ) -> Result<Vm, BoxError> {
        let (mut vm, kernel, rootfs, disk) = match &**place {
            Place::Dir { dir, forker } => {
                let start = Start {
                    forker,
                    place: dir,
                    firecracker: &spec.firecracker,
                    dir: dir.join(name),
                    file_size: vm_file_size(role, u64::from(spec.memory_mib)).max(
                        spec.disk
                            .as_ref()
                            .map_or(Ok(0), Disk::file_bytes)
                            .map_err(BoxError::from)?,
                    ),
                    disk: spec.disk.as_ref().map(|disk| disk.file.as_path()),
                    role,
                    size: spec.size(),
                };
                (
                    Vm::spawn(start, slot).await?,
                    spec.kernel.clone(),
                    spec.rootfs.clone(),
                    spec.disk.as_ref().map(|disk| disk.file.clone()),
                )
            }
            Place::Jail { keeper, route, .. } => (
                Vm::jailed(keeper, *route, role, spec.size(), slot).await?,
                PathBuf::from(jail::KERNEL),
                PathBuf::from(jail::ROOTFS),
                spec.disk.as_ref().map(|_| PathBuf::from(jail::DISK)),
            ),
        };
        vm.listen(egress)?;
        vm.api(
            "PUT",
            "/machine-config",
            json!({
                "vcpu_count": spec.vcpus,
                "mem_size_mib": spec.memory_mib,
                // a template's snapshot holds what its guest wrote (`Vm::snapshot`)
                "track_dirty_pages": matches!(role, Role::Template),
            }),
        )
        .await?;
        vm.api(
            "PUT",
            "/boot-source",
            json!({
                "kernel_image_path": kernel,
                "boot_args": boot_args(
                    &spec.egress,
                    !spec.env.is_empty(),
                    spec.disk.as_ref().map(|disk| disk.mount.as_str()),
                    spec.stage,
                ),
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
        if let Some(disk) = disk {
            vm.api(
                "PUT",
                "/drives/disk",
                json!({
                    "drive_id": "disk",
                    "path_on_host": disk,
                    "is_root_device": false,
                    "is_read_only": false,
                    "cache_type": "Writeback",
                }),
            )
            .await?;
        }
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
        if let Some(disk) = &spec.disk {
            vm.ask_disk().await?;
            // from here on: no process ran before `/init` answered, nothing to write out
            vm.stop_on_drop = Some(StopOnDrop {
                file: disk.file.clone(),
                _place: place.clone(),
                stops: stops.clone(),
            });
            vm.meter.ask_disk(&vm.vsock);
        }
        Ok(vm)
    }

    /// What it uses, as the sampler read it last ([`usage`]); none before it did.
    pub fn usage(&self) -> Option<sluice_wicket_console::status::Usage> {
        self.meter.usage()
    }

    /// What reads what it uses.
    pub fn meter(&self) -> &Arc<Meter> {
        &self.meter
    }

    /// Why it serves no more, its log stream ended: its `firecracker` ended, how, once that is
    /// known ([`usage::END_TIMEOUT`] at most); the host kernel's OOM killer's kill named so, as
    /// its cgroup tells it (the keeper's, or the wicket's own: `forker::Child`), else as the
    /// host's count of them rose while it lived.
    pub async fn end(&self) -> String {
        let exited = match &self.process {
            Process::Child { child, .. } => Some(child.exited().clone()),
            Process::Jailed { exited, .. } => Some(exited.clone()),
            Process::HandedOver => None,
        };
        let (status, oom) = match exited {
            Some(mut exited) => {
                let ended =
                    tokio::time::timeout(usage::END_TIMEOUT, exited.wait_for(Option::is_some));
                match ended.await {
                    Ok(Ok(end)) => {
                        let end = end.as_ref().expect("waited for");
                        (Some(end.status.clone()), end.oom)
                    }
                    _ => (None, false),
                }
            }
            None => (None, false),
        };
        match (status, oom) {
            (Some(status), true) => format!(
                "its VM ended ({status}): out of memory, the host kernel's OOM killer took it"
            ),
            (Some(status), false) => format!("its VM ended ({status})"),
            (None, _) => "its VM ended".into(),
        }
    }

    /// `line` to `/init` on the control stream `stream`, and its answer within `timeout`:
    /// `OK`, or its error. The stream kept whole: Firecracker takes a write half shut down as
    /// the end of the whole stream.
    async fn control(
        &self,
        mut stream: UnixStream,
        line: &str,
        timeout: Duration,
    ) -> Result<(), BoxError> {
        let answered = async {
            stream.write_all(line.as_bytes()).await?;
            Ok::<_, BoxError>(wire::read_short_line(&mut stream, MAX_CONTROL_ANSWER).await?)
        };
        let answer = tokio::time::timeout(timeout, answered)
            .await
            .map_err(|_| format!("no answer within {timeout:?}; {}", self.log()))??;
        match answer.trim_ascii_end() {
            b"OK" => Ok(()),
            other => {
                let other = String::from_utf8_lossy(other);
                Err(other
                    .strip_prefix("ERR ")
                    .unwrap_or(&other)
                    .to_string()
                    .into())
            }
        }
    }

    /// Whether `/init`, once it runs, mounted the disk: its error, or that it knows of none.
    /// A guest predating disks would serve without one, what it writes gone with the VM.
    async fn ask_disk(&self) -> Result<(), BoxError> {
        let stream = self.dial_ready(wire::CONTROL_PORT, DISK_TIMEOUT).await?;
        let line = format!("{}\n", wire::DISK_QUESTION);
        let asked = self.control(stream, &line, DISK_TIMEOUT).await;
        asked.map_err(|e| {
            let e = e.to_string();
            match e.as_str() {
                "not a restore notice" => {
                    "the guest mounts no disk: its /init predates them, build the rootfs again"
                        .to_string()
                }
                _ => format!("the guest's disk: {e}"),
            }
            .into()
        })
    }

    /// Has `/init` end the process within `grace` and write all out, so that the kill to come
    /// loses nothing of what it wrote; `grace` and [`STOP_WRITES`] at most, a wedged guest's
    /// too. A VM ended already says so at once.
    pub async fn stop(&self, grace: Duration) -> Result<(), BoxError> {
        let within = grace + STOP_WRITES;
        let stopped = async {
            let stream = self.dial(wire::CONTROL_PORT).await?;
            self.control(stream, &wire::stop_line(grace), within).await
        };
        let stopped = tokio::time::timeout(within, stopped).await;
        self.stopped.store(true, Ordering::Relaxed);
        stopped.map_err(|_| format!("no answer within {within:?}"))?
    }

    /// All of it, for a task of its own; `self` left with nothing to end.
    fn handed_over(&mut self) -> Vm {
        Vm {
            process: std::mem::replace(&mut self.process, Process::HandedOver),
            meter: self.meter.clone(),
            _slot: self._slot.take(),
            api: std::mem::take(&mut self.api),
            vsock: std::mem::take(&mut self.vsock),
            log: std::mem::take(&mut self.log),
            vsock_inside: std::mem::take(&mut self.vsock_inside),
            egress: self.egress.take(),
            stop_on_drop: None,
            stopped: AtomicBool::new(false),
        }
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
            Place::Dir { dir, forker } => {
                let start = Start {
                    forker,
                    place: dir,
                    firecracker: &snapshot.firecracker,
                    dir: dir.join("clones").join(format!("vm-{n}")),
                    file_size: vm_file_size(Role::Clone, u64::from(snapshot.size.mem)),
                    disk: None,
                    role: Role::Clone,
                    size: snapshot.size,
                };
                Vm::spawn(start, slot).await?
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

    /// Pauses the VM (of `spec`) and writes its snapshot into its place's snapshot directory; the
    /// VM stays paused. A VM booted tracking what its guest writes: its memory file holds that
    /// alone, the rest holes reading as the zeros it is, so a snapshot takes what the guest
    /// touched rather than all of `memory-mib` (see findings, A snapshot's memory).
    pub async fn snapshot(&self, place: &Place, spec: &Spec) -> Result<Snapshot, BoxError> {
        let (inside, on_host) = match place {
            Place::Dir { dir, .. } => {
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
                "snapshot_type": "Diff",
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

    /// Opens a stream to the guest's vsock `port`, within [`DIAL_TIMEOUT`].
    pub async fn dial(&self, port: u32) -> Result<UnixStream, BoxError> {
        dial_at(&self.vsock, port).await
    }

    /// Whether the guest's process accepts on `port`: a dial alone says nothing, `/init`'s vsock
    /// port stays open once the process listened; `/init` drops a stream the process refuses
    /// at once. A process's first bytes, or no drop in time, are its answer.
    pub async fn accepting(&self, port: u32) -> bool {
        match self.dial(port).await {
            Ok(stream) => Vm::accepted(stream, REFUSAL_TIMEOUT).await,
            Err(_) => false,
        }
    }

    /// Whether the process took `stream`, a stream to the guest's port: not dropped within
    /// `within`, or its first bytes came. Taken, it is closed as by a client.
    pub async fn accepted(mut stream: UnixStream, within: Duration) -> bool {
        let mut first = [0u8];
        let accepted = match tokio::time::timeout(within, stream.read(&mut first)).await {
            Err(_) | Ok(Ok(1..)) => true,
            Ok(Ok(0) | Err(_)) => false,
        };
        if accepted {
            // the process sees a client gone, not a stream cut
            let _ = wire::write_close(&mut stream).await;
        }
        accepted
    }

    /// [`Vm::dial`] retried until the guest listens on `port`, within `timeout`.
    pub async fn dial_ready(&self, port: u32, timeout: Duration) -> Result<UnixStream, BoxError> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let dialed = match tokio::time::timeout(left, self.dial(port)).await {
                Ok(dialed) => dialed,
                Err(_) => Err("vsock handshake: no answer".into()),
            };
            match dialed {
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
        // a runtime ending takes the task with it, the VM killed
        if let Some(on_drop) = self.stop_on_drop.take()
            && !*self.stopped.get_mut()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let vm = self.handed_over();
            let stops = on_drop.stops.clone();
            stops.spawn_on(
                async move {
                    if let Err(e) = vm.stop(disk::STOP_GRACE).await {
                        eprintln!("{}: its VM's stop: {e}; killed", on_drop.file.display());
                    }
                    drop(vm);
                    drop(on_drop);
                },
                &runtime,
            );
        }
        match &mut self.process {
            Process::Child { child, dir } => {
                child.kill();
                let _ = std::fs::remove_dir_all(dir);
            }
            // the keeper reaps it and removes its directory
            Process::Jailed { keeper, vm, .. } => keeper.kill(*vm),
            Process::HandedOver => {}
        }
    }
}

/// Opens a stream to the vsock `port` of the guest whose vsock socket is `vsock`, within
/// [`DIAL_TIMEOUT`].
pub async fn dial_at(vsock: &Path, port: u32) -> Result<UnixStream, BoxError> {
    let dialed = async {
        let mut stream = UnixStream::connect(vsock).await?;
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
        Ok::<_, BoxError>(stream)
    };
    tokio::time::timeout(DIAL_TIMEOUT, dialed)
        .await
        .map_err(|_| format!("vsock handshake: no answer within {DIAL_TIMEOUT:?}"))?
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

/// No serial console unless `SLUICE_WICKET_RUNTIME_FIRECRACKER_SERIAL_CONSOLE` is set (the kernel's and
/// `/init`'s own output then goes to the VM's log; the processes' goes to the wicket's
/// stdout / stderr regardless): printing to it is the slowest part of a boot. The route's
/// `egress`, if any, for `/init`, that its env is coming (`env`), and where its disk goes.
fn boot_args(egress: &[Rule], env: bool, disk: Option<&str>, stage: bool) -> String {
    let console = match std::env::var_os(SERIAL_CONSOLE) {
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
    let disk = disk.map_or(String::new(), |mount| {
        format!(" {}={mount}", wire::DISK_PARAMETER)
    });
    let stage = match stage {
        true => format!(" {}=1", wire::STAGE_PARAMETER),
        false => String::new(),
    };
    format!("{console} reboot=k panic=1 quiet{arch} init=/init{egress}{env}{disk}{stage}")
}
