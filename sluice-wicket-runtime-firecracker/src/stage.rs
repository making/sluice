//! Source staged by Cloud Native Buildpacks, on the wicket: the platform of a build, with
//! the guest's `/init` (its `stage` mode).
//!
//! ```text
//! PUT    /api/layers/<sha256>            keeps the body, a tar, under its digest: a build's
//!                                        source, a droplet built elsewhere
//! GET    /api/layers/<sha256>?at=<n>     the kept tar from byte n on, 8 MiB at most a
//!                                        request (`sluice-layer-bytes`: all of it)
//! POST   /api/builds                     starts the build the body (JSON) asks for
//! GET    /api/builds/<id>?after=<n>      its output from line n on and, once it ended, its
//!                                        droplet or its error; held until there is more
//! DELETE /api/builds/<id>                ends and forgets it
//! DELETE /api/caches/<name>              forgets the cache of that name
//! ```
//!
//! A build names a builder image and a source kept before. The builder's rootfs is made as
//! an image's (`image`) and kept by its manifest's digest; a VM of it is booted with a
//! scratch disk, within the VMs' budget as any other, and `/init` there runs the builder's
//! `detector` and `builder` on the source: no analyzer (the wicket names the run image, the
//! builder's or one of its target, pinned to its digest), no exporter. What comes back is the droplet -- a tar of what goes over the
//! run image's root file system: the launch layers, the app, the launcher -- kept as a layer,
//! and the `launch` it runs with. `PUT /api/images` of the run image with that layer and
//! launch makes the rootfs, on this wicket or any other: a droplet is built once.
//!
//! A buildpack a build names by its image (`docker://<image>`, as `pack build --buildpack`
//! takes it: a buildpackage) is its labels' (`io.buildpacks.buildpackage.metadata`), its layers
//! those of its buildpack and of the buildpacks its order names, which `/init` lays over the
//! builder's `/cnb/buildpacks` before the detector runs (a layer of the builder's buildpacks
//! alone is not). They are kept by their diff IDs (`buildpacks/<sha256>.tar`) for the next
//! build; one not used for a week goes when another is kept.
//!
//! A build naming a cache (an app's) gets the `cache = true` layers its last build of that
//! name left on this wicket, kept here as a tar between them (`caches/<name>.tar`), and leaves
//! its own once built: a build is warm on the wicket that built it before. A cache not used
//! for a week goes when another is kept.
//!
//! Nothing the guest sends is taken on trust: its lines and its result are bounded, the
//! droplet is `limit` bytes at most and unpacked later as a registry's layer is
//! (`rootfs::Tree`).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{Read as _, Seek as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::request::Parts;
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sluice_wicket_console::env::Env;
use sluice_wicket_console::http::{
    Body, Intake, Received, Refusal, Response, error, json, pairs, read, receive,
};
use sluice_wicket_console::store::{UPLOAD_GRACE, UPLOADS, check_digest, regular};
use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;
use tokio_util::task::TaskTracker;

use crate::capacity::Capacity;
use crate::config::Binaries;
use crate::disk::Disk;
use crate::egress::Egress;
use crate::image::{self, Images, Over};
use crate::keeper::Role;
use crate::output::{Hosts, Sink};
use crate::vm::{Places, Spec, Vm};
use crate::wire::{self, LogFrame, Rule, Stream};

/// The extension a layer is kept with.
const TAR: &str = ".tar";

/// The most of a layer one request answers.
const PIECE: u64 = 8 << 20;

/// A build's request (its JSON) at most: an env of the most an env holds fits, escaped.
const MAX_REQUEST: usize = 1 << 20;

/// A build VM's defaults: what a Maven build of a Spring Boot app takes (`.kb`).
const MEMORY_MIB: u32 = 2048;
const VCPUS: u32 = 2;
const DISK_MIB: u32 = 8192;

/// A build's time: its default (Cloud Foundry's staging timeout) and the most.
const TIMEOUT: Duration = Duration::from_secs(15 * 60);
const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// How long a build waits for room in the VMs' budget.
const ROOM_WAIT: Duration = Duration::from_secs(30);

/// How long `/init` of a build's VM may take to listen, and to take the request.
const GUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the last lines of a build's output may take to come.
const SYNC_TIMEOUT: Duration = Duration::from_secs(5);

/// The output kept of a build: its last lines.
const MAX_LINES: usize = 20_000;

/// The lines one request answers at most.
const LINES: usize = 1000;

/// How long a request for a build's output is held without any.
const HELD: Duration = Duration::from_secs(20);

/// The builds kept at once, ended or not.
const MAX_BUILDS: usize = 64;

/// A builder's rootfs, or a cache, not used for this long goes when another is made.
const BUILDER_KEPT: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Where the caches are kept, in the console's directory.
const CACHES: &str = "caches";

/// Where the layers of the buildpacks named by their images are kept, in the console's
/// directory.
const BUILDPACKS: &str = "buildpacks";

/// How a buildpack is named by its image, as `pack build --buildpack` takes it.
const BUILDPACK_IMAGE: &str = "docker://";

/// A cache's name at most.
const MAX_CACHE_NAME: usize = 128;

/// The platform APIs the wicket speaks, the one preferred first: what `/init` writes in the
/// analyzer's place (`analyzed.toml` with the run image's target) is 0.12's.
const PLATFORM_APIS: [&str; 4] = ["0.13", "0.14", "0.15", "0.12"];

/// Where the lifecycle keeps the app and the layers, in a build and in what runs.
const APP_DIR: &str = "/workspace";
const LAYERS_DIR: &str = "/layers";

/// Numbers the builds' places in the state dir.
static NEXT_BUILD: AtomicU64 = AtomicU64::new(0);

/// What the builds need.
pub struct Builds {
    pub images: Arc<Images>,
    pub places: Arc<Places>,
    pub capacity: Arc<Capacity>,
    pub sink: Arc<Sink>,
    pub workers: Arc<Workers>,
    pub stops: TaskTracker,
    pub binaries: Binaries,
    /// The console's directory.
    pub dir: PathBuf,
    /// The builds kept, by their ids.
    pub kept: Kept,
}

/// The builds a wicket keeps: those running, and those ended a while.
#[derive(Default)]
pub struct Kept(Mutex<HashMap<String, Arc<Build>>>);

/// What `POST /api/builds` takes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    /// The builder image, `[registry/]repository[:tag][@sha256:<digest>]`.
    builder: String,
    username: Option<String>,
    password: Option<String>,
    /// The run image, in place of the one the builder names: of the builder's target.
    #[serde(rename = "run-image")]
    run_image: Option<String>,
    /// The digest of the source, a tar kept as a layer.
    source: String,
    /// The app's environment, as the buildpacks see it (`/platform/env`).
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// The destinations the build may connect to, as a route's `egress`.
    #[serde(default)]
    egress: Vec<String>,
    #[serde(rename = "memory-mib")]
    memory_mib: Option<u32>,
    vcpus: Option<u32>,
    #[serde(rename = "disk-mib")]
    disk_mib: Option<u32>,
    #[serde(rename = "timeout-ms")]
    timeout_ms: Option<u64>,
    /// The buildpacks detecting, `<id>[@<version>]` or `docker://<image>`, in one group: none,
    /// the builder's order. Of the builder's, a version left out its latest.
    #[serde(default)]
    buildpacks: Vec<String>,
    /// What the droplet runs: a shell's command line, which the launcher runs in place of
    /// the default process.
    command: Option<String>,
    /// The name of the cache the build restores and leaves: none, no cache.
    cache: Option<String>,
}

/// A build as asked for, checked.
struct Asked {
    builder: String,
    credentials: Option<(String, String)>,
    run_image: Option<String>,
    source: std::fs::File,
    env: Env,
    egress: Vec<Rule>,
    memory_mib: u32,
    vcpus: u32,
    disk_mib: u32,
    timeout: Duration,
    buildpacks: Vec<String>,
    command: Option<String>,
    /// Where the cache is kept, and the last build's, open.
    cache: Option<(PathBuf, Option<std::fs::File>)>,
}

/// What a build made.
#[derive(Serialize, Clone)]
struct Built {
    droplet: Layer,
    /// The run image, pinned to its digest: what `PUT /api/images` names.
    #[serde(rename = "run-image")]
    run_image: String,
    /// What the droplet runs, as `PUT /api/images` takes it.
    launch: Over,
}

/// A layer kept.
#[derive(Serialize, Clone)]
struct Layer {
    sha256: String,
    bytes: u64,
}

/// A line of a build's output.
#[derive(Serialize, Clone)]
struct Line {
    stream: &'static str,
    line: String,
}

/// A build: its output so far and how it ended.
pub struct Build {
    state: Mutex<State>,
    /// Counts what changed: a line, the end.
    changed: watch::Sender<u64>,
    task: Mutex<Option<AbortHandle>>,
}

#[derive(Default)]
struct State {
    /// The number of the first line kept.
    first: u64,
    lines: VecDeque<Line>,
    ended: Option<(Result<Built, String>, Instant)>,
}

/// What `GET /api/builds/<id>` answers.
#[derive(Serialize)]
struct View {
    /// `building`, `built` or `failed`.
    state: &'static str,
    /// The number of the line after the last one here: the next request's `after`.
    next: u64,
    lines: Vec<Line>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(flatten)]
    built: Option<Built>,
}

#[derive(Serialize)]
struct Started {
    id: String,
}

impl Build {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn line(&self, stream: Stream, line: &[u8]) {
        let mut state = self.state();
        if state.lines.len() == MAX_LINES {
            state.lines.pop_front();
            state.first += 1;
        }
        state.lines.push_back(Line {
            stream: stream.name(),
            line: String::from_utf8_lossy(line).into_owned(),
        });
        drop(state);
        self.changed.send_modify(|count| *count += 1);
    }

    /// A line of the wicket's own.
    fn note(&self, line: String) {
        self.line(Stream::Stdout, line.as_bytes());
    }

    fn end(&self, ended: Result<Built, String>) {
        self.state().ended = Some((ended, Instant::now()));
        self.changed.send_modify(|count| *count += 1);
    }

    fn view(&self, after: u64) -> View {
        let state = self.state();
        let skip = after.saturating_sub(state.first) as usize;
        let lines: Vec<Line> = state.lines.iter().skip(skip).take(LINES).cloned().collect();
        let next = state.first + (skip.min(state.lines.len()) + lines.len()) as u64;
        // the end once every line before it was read
        let ended = state
            .ended
            .as_ref()
            .filter(|_| next == state.first + state.lines.len() as u64);
        View {
            state: match ended {
                None => "building",
                Some((Ok(_), _)) => "built",
                Some((Err(_), _)) => "failed",
            },
            next,
            lines,
            error: ended.and_then(|(ended, _)| ended.as_ref().err().cloned()),
            built: ended.and_then(|(ended, _)| ended.as_ref().ok().cloned()),
        }
    }
}

/// A file removed when dropped: a build's scratch disk.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

impl Builds {
    fn builds(&self) -> MutexGuard<'_, HashMap<String, Arc<Build>>> {
        self.kept.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Where the layer of `sha256` is kept.
    pub fn layer(&self, sha256: &str) -> PathBuf {
        self.dir.join(UPLOADS).join(format!("{sha256}{TAR}"))
    }

    /// `PUT /api/layers/<sha256>`: the digest is checked before the body is read.
    pub async fn put_layer(&self, sha256: &str, parts: &Parts, body: Body) -> Response {
        let sha256 = match check_digest(sha256) {
            Ok(sha256) => sha256,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let limit = self.images.limit;
        let too_large = format!("a layer is {} GiB at most", limit >> 30);
        let intake = Intake {
            dir: &self.images.work,
            limit,
            too_large: &too_large,
            empty: "no layer: send the tar as the request body",
            head: 0,
            check: |_: &[u8]| Ok(()),
        };
        let received = match receive(parts, body, intake).await {
            Ok(received) => received,
            Err(refused) => return refused.into(),
        };
        if received.sha256 != sha256 {
            let said = format!("the body's sha256 is {}, not {sha256}", received.sha256);
            return error(StatusCode::BAD_REQUEST, said);
        }
        let bytes = received.bytes;
        // kept anew, for as long again
        let path = self.layer(&sha256);
        let _ = std::fs::remove_file(&path);
        match received.keep(&path) {
            Ok(()) => json(StatusCode::OK, &Layer { sha256, bytes }),
            Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e),
        }
    }

    /// `GET /api/layers/<sha256>?at=<n>`.
    pub async fn get_layer(&self, sha256: &str, query: Option<&str>) -> Response {
        let sha256 = match check_digest(sha256) {
            Ok(sha256) => sha256,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let at = pairs(query)
            .into_iter()
            .find(|(key, _)| key == "at")
            .map_or(Ok(0), |(_, at)| at.parse::<u64>());
        let Ok(at) = at else {
            return error(StatusCode::BAD_REQUEST, "at=<n>: a byte's number");
        };
        let path = self.layer(&sha256);
        let piece = tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(&path)?;
            let bytes = file.metadata()?.len();
            file.seek(std::io::SeekFrom::Start(at.min(bytes)))?;
            let mut piece = Vec::new();
            file.take(PIECE).read_to_end(&mut piece)?;
            Ok::<_, std::io::Error>((bytes, piece))
        })
        .await;
        match piece {
            Ok(Ok((bytes, piece))) => hyper::Response::builder()
                .header("content-type", "application/x-tar")
                .header("sluice-layer-bytes", bytes)
                .body(http_body_util::Full::new(Bytes::from(piece)))
                .expect("a valid response"),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                error(StatusCode::NOT_FOUND, format!("no layer {sha256} is kept"))
            }
            Ok(Err(e)) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    }

    /// Where the cache `name` is kept.
    fn cache(&self, name: &str) -> PathBuf {
        self.dir.join(CACHES).join(format!("{name}{TAR}"))
    }

    /// `DELETE /api/caches/<name>`.
    pub fn delete_cache(&self, name: &str) -> Response {
        if let Err(e) = check_cache_name(name) {
            return error(StatusCode::BAD_REQUEST, e);
        }
        match std::fs::remove_file(self.cache(name)) {
            Ok(()) => json(StatusCode::OK, &serde_json::json!({"cache": name})),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                error(StatusCode::NOT_FOUND, format!("no cache {name}"))
            }
            Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    }

    /// `/api/builds` and what is below it (`rest`: empty, or `/<id>`).
    pub async fn handle(
        self: &Arc<Self>,
        rest: &str,
        parts: &Parts,
        body: Option<Body>,
    ) -> Response {
        let id = rest.strip_prefix('/');
        match (&parts.method, id) {
            (&Method::POST, None) => self.start(parts, body.expect("not read yet")).await,
            (&Method::GET, Some(id)) => self.follow(id, parts.uri.query()).await,
            (&Method::DELETE, Some(id)) => match self.builds().remove(id) {
                Some(build) => {
                    build.abort();
                    json(StatusCode::OK, &Started { id: id.to_string() })
                }
                None => error(StatusCode::NOT_FOUND, format!("no build {id}")),
            },
            _ => error(
                StatusCode::METHOD_NOT_ALLOWED,
                "/api/builds takes POST, a build GET or DELETE",
            ),
        }
    }

    async fn start(self: &Arc<Self>, parts: &Parts, body: Body) -> Response {
        let too_large = format!("a build's request is {} KiB at most", MAX_REQUEST >> 10);
        let body = match read(parts, body, MAX_REQUEST, &too_large).await {
            Ok(body) => body,
            Err(refused) => return refused.into(),
        };
        let asked = match serde_json::from_slice::<Request>(&body) {
            Ok(request) => self.asked(request),
            Err(e) => Err(Refusal::new(
                StatusCode::BAD_REQUEST,
                format!("the build: {e}"),
            )),
        };
        let asked = match asked {
            Ok(asked) => asked,
            Err(refused) => return refused.into(),
        };
        let mut id = [0u8; 12];
        if let Err(e) = getrandom::fill(&mut id) {
            return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        }
        let id: String = id.iter().map(|byte| format!("{byte:02x}")).collect();
        let build = Arc::new(Build {
            state: Mutex::new(State::default()),
            changed: watch::channel(0).0,
            task: Mutex::new(None),
        });
        {
            let mut builds = self.builds();
            builds.retain(|_, build| {
                let ended = build.state().ended.as_ref().map(|(_, at)| at.elapsed());
                ended.is_none_or(|ended| ended < UPLOAD_GRACE)
            });
            if builds.len() >= MAX_BUILDS {
                return error(
                    StatusCode::TOO_MANY_REQUESTS,
                    format!("{MAX_BUILDS} builds are kept at most: remove one"),
                );
            }
            builds.insert(id.clone(), build.clone());
        }
        let task = tokio::spawn({
            let (builds, build, id) = (self.clone(), build.clone(), id.clone());
            async move {
                let ended = builds.run(&build, &id, asked).await;
                build.end(ended);
            }
        });
        *build.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task.abort_handle());
        json(StatusCode::ACCEPTED, &Started { id })
    }

    /// The build `request` asks for, every check of it before a VM is.
    fn asked(&self, request: Request) -> Result<Asked, Refusal> {
        let invalid = |why: String| Refusal::new(StatusCode::BAD_REQUEST, why);
        let credentials = match (request.username, request.password) {
            (Some(username), Some(password)) => Some((username, password)),
            (None, None) => None,
            _ => return Err(invalid("username and password go together".into())),
        };
        if let Some(run_image) = &request.run_image {
            sluice_wicket_registry::Reference::parse(run_image)
                .map_err(|e| invalid(format!("run-image: {e}")))?;
        }
        let source = check_digest(&request.source).map_err(|e| invalid(format!("source: {e}")))?;
        // held open: the build has it however long the builder takes to make
        let source = std::fs::File::open(self.layer(&source)).map_err(|_| {
            let mut refusal = Refusal::new(
                StatusCode::CONFLICT,
                format!("no layer {source} is kept: PUT /api/layers/{source} first"),
            );
            refusal.view.missing = vec![source.clone()];
            refusal
        })?;
        let env = Env::new(request.env).map_err(|e| invalid(format!("env: {e}")))?;
        let mut egress = request
            .egress
            .iter()
            .map(|rule| rule.parse::<Rule>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| invalid(format!("egress: {e}")))?;
        egress.sort();
        egress.dedup();
        let within = |name: &str, value: u32, range: std::ops::RangeInclusive<u32>| match range
            .contains(&value)
        {
            true => Ok(value),
            false => Err(invalid(format!(
                "{name} {value}: expected {} to {}",
                range.start(),
                range.end()
            ))),
        };
        let timeout = request
            .timeout_ms
            .map_or(TIMEOUT, Duration::from_millis)
            .min(MAX_TIMEOUT);
        for buildpack in &request.buildpacks {
            if let Some(image) = buildpack.strip_prefix(BUILDPACK_IMAGE) {
                sluice_wicket_registry::Reference::parse(image)
                    .map_err(|e| invalid(format!("buildpacks: '{buildpack}': {e}")))?;
                continue;
            }
            let (id, version) = split_buildpack(buildpack);
            if id.is_empty() || version == Some("") || buildpack.contains(char::is_whitespace) {
                return Err(invalid(format!(
                    "buildpacks: '{buildpack}' is no <id>[@<version>]"
                )));
            }
        }
        let command = request
            .command
            .map(|command| command.trim().to_string())
            .filter(|command| !command.is_empty());
        if command
            .as_ref()
            .is_some_and(|command| command.contains('\0'))
        {
            return Err(invalid("command: a NUL in it".into()));
        }
        let cache = match request.cache {
            Some(name) => {
                check_cache_name(&name).map_err(|e| invalid(format!("cache: {e}")))?;
                let path = self.cache(&name);
                let kept = std::fs::File::open(&path).ok();
                Some((path, kept))
            }
            None => None,
        };
        Ok(Asked {
            builder: request.builder,
            credentials,
            run_image: request.run_image,
            source,
            env,
            egress,
            memory_mib: within(
                "memory-mib",
                request.memory_mib.unwrap_or(MEMORY_MIB),
                128..=1 << 20,
            )?,
            vcpus: within("vcpus", request.vcpus.unwrap_or(VCPUS), 1..=32)?,
            disk_mib: within(
                "disk-mib",
                request.disk_mib.unwrap_or(DISK_MIB),
                64..=1 << 20,
            )?,
            timeout,
            buildpacks: request.buildpacks,
            command,
            cache,
        })
    }

    async fn follow(&self, id: &str, query: Option<&str>) -> Response {
        let Some(build) = self.builds().get(id).cloned() else {
            return error(StatusCode::NOT_FOUND, format!("no build {id}"));
        };
        let after = pairs(query)
            .into_iter()
            .find(|(key, _)| key == "after")
            .and_then(|(_, after)| after.parse::<u64>().ok())
            .unwrap_or(0);
        let mut changed = build.changed.subscribe();
        let mut view = build.view(after);
        if view.lines.is_empty() && view.state == "building" {
            let _ = tokio::time::timeout(HELD, changed.changed()).await;
            view = build.view(after);
        }
        json(StatusCode::OK, &view)
    }

    /// Runs the build `asked` for, within its timeout once its builder is ready (a rootfs
    /// being made has a bound of its own): its droplet kept as a layer.
    async fn run(&self, build: &Arc<Build>, id: &str, asked: Asked) -> Result<Built, String> {
        let builder = self.builder(build, id, &asked).await?;
        let timeout = asked.timeout;
        let staged = self.staged(build, id, builder, asked);
        match tokio::time::timeout(timeout, staged).await {
            Ok(staged) => staged,
            Err(_) => Err(format!("the build took more than {}s", timeout.as_secs())),
        }
    }

    async fn staged(
        &self,
        build: &Arc<Build>,
        id: &str,
        builder: Builder,
        asked: Asked,
    ) -> Result<Built, String> {
        let started = Instant::now();
        let disk = Disk {
            file: self.images.work.join(format!(".stage-{id}.ext4")),
            mount: wire::STAGE_DISK.into(),
            mib: asked.disk_mib,
        };
        // dropped after the VM: its file is the VM's until then
        let _scratch = Scratch(disk.file.clone());
        let scratch = disk.clone();
        tokio::task::spawn_blocking(move || scratch.make_scratch())
            .await
            .map_err(|e| e.to_string())??;
        let (firecracker, kernel) = self.binaries.read()?;
        let spec = Spec {
            firecracker,
            kernel,
            rootfs: builder.rootfs.clone(),
            vcpus: asked.vcpus,
            memory_mib: asked.memory_mib,
            cpu: None,
            egress: asked.egress.clone(),
            env: Env::default(),
            disk: Some(disk),
            stage: true,
        };
        let need = spec.need(crate::route::next_route());
        let slot = self
            .capacity
            .take(need, ROOM_WAIT)
            .await
            .map_err(|e| e.of("the build").to_string())?;
        // short: the VM's sockets are below it
        let name = format!("build-{}", NEXT_BUILD.fetch_add(1, Ordering::Relaxed));
        let place = Arc::new(
            self.places
                .place(&name, &spec)
                .await
                .map_err(|e| e.to_string())?,
        );
        let egress = (!spec.egress.is_empty()).then(|| {
            let hosts = Hosts::new(std::slice::from_ref(&name));
            Egress::new(
                spec.egress.clone(),
                hosts,
                self.sink.clone(),
                self.workers.clone(),
            )
        });
        let named = egress.as_ref().map(|egress| (egress, "vm".to_string()));
        let vm = Vm::boot(&spec, &place, Role::Boot, "vm", slot, named, &self.stops)
            .await
            .map_err(|e| format!("the build's VM: {e}"))?;
        let log = vm
            .dial_ready(wire::LOG_PORT, GUEST_TIMEOUT)
            .await
            .map_err(|e| e.to_string())?;
        let output = Output::follow(log, build.clone());
        let mut stream = vm
            .dial_ready(wire::STAGE_PORT, GUEST_TIMEOUT)
            .await
            .map_err(|e| {
                format!("the builder's /init stages nothing: it predates builds, or {e}")
            })?;
        let command = asked.command.clone();
        let staged = self
            .stage(&mut stream, id, build, &builder, asked)
            .await
            .map_err(|e| e.to_string());
        // the lines the phases wrote before the result
        output.sync().await;
        let (outcome, droplet) = staged?;
        if let Some(error) = outcome.error {
            return Err(error);
        }
        let droplet = droplet.expect("no error: a droplet");
        let layer = Layer {
            sha256: droplet.sha256.clone(),
            bytes: droplet.bytes,
        };
        let path = self.layer(&layer.sha256);
        let _ = std::fs::remove_file(&path);
        droplet.keep(&path)?;
        build.note(format!(
            "Built in {}s: a droplet of {} MiB on {}",
            started.elapsed().as_secs(),
            layer.bytes >> 20,
            builder.run_image
        ));
        Ok(Built {
            droplet: layer,
            run_image: builder.run_image,
            launch: launch(&outcome, &builder.platform_api, command.as_deref()),
        })
    }

    /// The builder of `asked`, its rootfs made unless kept, the run image it names, and the
    /// layers of the buildpacks named by their images.
    async fn builder(&self, build: &Build, id: &str, asked: &Asked) -> Result<Builder, String> {
        let credentials = asked.credentials.as_ref();
        let inspected = self.images.inspect(&asked.builder, credentials).await?;
        let metadata = inspected.config["config"]["Labels"]["io.buildpacks.builder.metadata"]
            .as_str()
            .and_then(|label| serde_json::from_str::<Value>(label).ok())
            .ok_or_else(|| {
                format!(
                    "{}: no Cloud Native Buildpacks builder (no io.buildpacks.builder.metadata)",
                    asked.builder
                )
            })?;
        let supported: Vec<&str> = metadata["lifecycle"]["apis"]["platform"]["supported"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let platform_api = PLATFORM_APIS
            .into_iter()
            .find(|api| supported.contains(api))
            .ok_or_else(|| {
                format!(
                    "the builder's lifecycle speaks the platform APIs {}: this wicket {}",
                    supported.join(", "),
                    PLATFORM_APIS.join(", ")
                )
            })?;
        // the builder's credentials go to its own registry alone
        let registry = |image: &str| {
            sluice_wicket_registry::Reference::parse(image).map(|reference| reference.registry)
        };
        let builder_registry = registry(&asked.builder)?;
        let listed = listed(&metadata);
        let images: Vec<(&str, sluice_wicket_registry::Reference)> = (asked.buildpacks.iter())
            .filter_map(|buildpack| buildpack.strip_prefix(BUILDPACK_IMAGE))
            .map(|image| Ok((image, sluice_wicket_registry::Reference::parse(image)?)))
            .collect::<Result<_, String>>()?;
        // their labels first: the order checked before a layer is pulled
        let mut pulls = Vec::new();
        for (image, reference) in &images {
            let same = reference.registry == builder_registry;
            let pull = self
                .images
                .open(image, reference, credentials.filter(|_| same))
                .await
                .map_err(|e| format!("the buildpack {image}: {e}"))?;
            let package = package(&pull.inspected.config, &listed)
                .map_err(|e| format!("the buildpack {}: {e}", pull.inspected.image))?;
            pulls.push((pull, package));
        }
        let packages: Vec<&Package> = pulls.iter().map(|(_, package)| package).collect();
        let order = order(
            &metadata,
            inspected.config["config"]["Labels"]["io.buildpacks.buildpack.order"].as_str(),
            &asked.buildpacks,
            &packages,
        )?;
        let added = tokio::time::timeout(asked.timeout, self.added(build, id, pulls))
            .await
            .map_err(|_| {
                format!(
                    "the buildpacks' layers took more than {}s",
                    asked.timeout.as_secs()
                )
            })??;
        let run_image_name = match &asked.run_image {
            Some(named) => named.clone(),
            None => metadata["images"][0]["image"]
                .as_str()
                .or(metadata["stack"]["runImage"]["image"].as_str())
                .ok_or("the builder names no run image")?
                .to_string(),
        };
        let same = registry(&run_image_name)? == builder_registry;
        let run = self
            .images
            .inspect(&run_image_name, credentials.filter(|_| same))
            .await
            .map_err(|e| format!("the run image {run_image_name}: {e}"))?;
        let target = Target::of(&run.config);
        // the builder's own is its author's to match; one named in its place, the platform's
        if asked.run_image.is_some() {
            target
                .fits(&Target::of(&inspected.config))
                .map_err(|e| format!("the run image {run_image_name} {e}"))?;
        }
        let rootfs = self
            .images
            .builder(&asked.builder, &inspected.digest, credentials, |line| {
                build.note(line)
            })
            .await?;
        build.note(format!(
            "Builder {}, run image {}",
            inspected.image, run.image
        ));
        Ok(Builder {
            rootfs,
            run_image: run.image,
            run_image_name,
            target,
            platform_api: platform_api.to_string(),
            order,
            added,
        })
    }

    /// The layers of the buildpack images `pulls` the builder lacks, each kept unless it was:
    /// once each, with the buildpacks it holds.
    async fn added(
        &self,
        build: &Build,
        id: &str,
        pulls: Vec<(image::Pull<'_>, Package)>,
    ) -> Result<Vec<Added>, String> {
        let dir = self.dir.join(BUILDPACKS);
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let mut added: Vec<Added> = Vec::new();
        for (mut pull, package) in pulls {
            build.note(format!(
                "Buildpack {} {} of {}",
                package.id, package.version, pull.inspected.image
            ));
            for (diff_id, held) in package.layers {
                if let Some(known) = added.iter_mut().find(|known| known.diff_id == diff_id) {
                    known.buildpacks.extend(held);
                    known.buildpacks.sort();
                    known.buildpacks.dedup();
                    continue;
                }
                let sha256 = diff_id.trim_start_matches("sha256:");
                let path = dir.join(format!("{sha256}{TAR}"));
                if regular(&path) {
                    // used: kept longer
                    if let Ok(file) = std::fs::File::options().append(true).open(&path) {
                        let _ = file.set_modified(std::time::SystemTime::now());
                    }
                } else {
                    let started = Instant::now();
                    let received = self
                        .images
                        .work
                        .join(format!(".buildpack-{id}-{sha256}.tar"));
                    let bytes = pull.layer(&diff_id, &received, self.images.limit).await?;
                    keep(&received, &path)?;
                    build.note(format!(
                        "Pulled its layer {} in {}s: {}",
                        &sha256[..12],
                        started.elapsed().as_secs(),
                        sized(bytes)
                    ));
                }
                let file =
                    std::fs::File::open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
                added.push(Added {
                    diff_id,
                    file,
                    buildpacks: held,
                });
            }
        }
        Ok(added)
    }

    /// One build on the stage stream of its VM: the result, and when it has no error, the
    /// droplet received and the cache kept (its file and size), if one was asked for.
    #[allow(clippy::type_complexity)]
    async fn stage(
        &self,
        stream: &mut UnixStream,
        id: &str,
        build: &Build,
        builder: &Builder,
        asked: Asked,
    ) -> std::io::Result<(Outcome, Option<Received>)> {
        let order: Vec<Value> = (builder.order.iter())
            .map(|(id, version)| serde_json::json!({"id": id, "version": version}))
            .collect();
        let (cache, kept) = asked.cache.unzip();
        let kept = kept.flatten();
        let request = serde_json::json!({
            "env": asked.env.map(),
            "platform_api": builder.platform_api,
            "run_image": builder.run_image,
            "run_image_name": builder.run_image_name,
            "os": builder.target.os,
            "arch": builder.target.arch,
            "arch_variant": builder.target.variant,
            "distro": builder.target.distro,
            "order": order,
            "layers": builder.added.iter().map(|added| {
                let held: Vec<Value> = (added.buildpacks.iter())
                    .map(|(id, version)| serde_json::json!({"id": id, "version": version}))
                    .collect();
                serde_json::json!({"buildpacks": held})
            }).collect::<Vec<_>>(),
            "cache": cache.is_some(),
            "restore": kept.is_some(),
        });
        let request = serde_json::to_vec(&request).map_err(std::io::Error::other)?;
        tokio::time::timeout(GUEST_TIMEOUT, wire::send_env(stream, &request))
            .await
            .map_err(|_| std::io::Error::other("the builder's /init took no request"))??;
        let mut source = tokio::fs::File::from_std(asked.source);
        stream.write_u64(source.metadata().await?.len()).await?;
        tokio::io::copy(&mut source, stream).await?;
        for added in &builder.added {
            let mut layer = tokio::fs::File::from_std(added.file.try_clone()?);
            stream.write_u64(layer.metadata().await?.len()).await?;
            tokio::io::copy(&mut layer, stream).await?;
        }
        let mut kept = kept.map(tokio::fs::File::from_std);
        let outcome = loop {
            let said = wire::read_json(stream, wire::MAX_STAGE_RESULT)
                .await
                .map_err(|e| {
                    std::io::Error::other(format!(
                        "the build's VM ended before its result ({e}): out of memory-mib?"
                    ))
                })?;
            let said: Value = serde_json::from_slice(&said)
                .map_err(|e| std::io::Error::other(format!("the build's result: {e}")))?;
            // the cache, asked for once the buildpacks are detected: sent once
            if said["restore"] != true {
                break serde_json::from_value::<Outcome>(said)
                    .map_err(|e| std::io::Error::other(format!("the build's result: {e}")))?;
            }
            let Some(kept) = kept.take() else {
                return Err(std::io::Error::other("the build asked for a cache twice"));
            };
            let bytes = kept.metadata().await?.len();
            build.note(format!("Restoring a cache of {}", sized(bytes)));
            // used: kept longer
            let _ = kept.set_modified(std::time::SystemTime::now()).await;
            stream.write_u64(bytes).await?;
            tokio::io::copy(&mut kept.take(bytes), stream).await?;
        };
        if outcome.error.is_some() {
            return Ok((outcome, None));
        }
        let path = self.images.work.join(format!(".droplet-{id}.partial"));
        let (sha256, bytes) = self.receive(stream, &path, "a droplet").await?;
        let droplet = Received::adopt(path, sha256, bytes);
        if let Some(cache) = cache {
            let started = Instant::now();
            let received = self.images.work.join(format!(".cache-{id}.partial"));
            match self.receive_pieces(stream, &received).await? {
                Some(0) => {
                    let _ = std::fs::remove_file(&received);
                    if std::fs::remove_file(&cache).is_ok() {
                        build.note("Nothing cached: the last build's cache is forgotten".into());
                    }
                }
                Some(bytes) => {
                    keep(&received, &cache).map_err(std::io::Error::other)?;
                    build.note(format!(
                        "Kept a cache of {} for the next build in {}s",
                        sized(bytes),
                        started.elapsed().as_secs()
                    ));
                }
                None => {
                    let _ = std::fs::remove_file(&received);
                    build.note("The cache was not packed: the last build's is kept".into());
                }
            }
        }
        Ok((outcome, Some(droplet)))
    }

    /// Receives the pieces of `stream` (`[len: u32 BE][bytes]`, then a length of 0, or
    /// `u32::MAX` for none whole) into the file `path`, [`Images::limit`] bytes at most: their
    /// size, None for none whole.
    async fn receive_pieces(
        &self,
        stream: &mut UnixStream,
        path: &Path,
    ) -> std::io::Result<Option<u64>> {
        let mut file = tokio::fs::File::create(path).await?;
        let mut bytes = 0u64;
        let mut piece = Vec::new();
        let received = loop {
            let len = stream.read_u32().await?;
            match len {
                0 => break Some(bytes),
                u32::MAX => break None,
                len => {
                    bytes += u64::from(len);
                    if bytes > self.images.limit {
                        let _ = std::fs::remove_file(path);
                        return Err(std::io::Error::other(format!(
                            "a cache of more than {} MiB",
                            self.images.limit >> 20
                        )));
                    }
                    piece.resize(len as usize, 0);
                    stream.read_exact(&mut piece).await?;
                    file.write_all(&piece).await?;
                }
            }
        };
        file.flush().await?;
        Ok(received)
    }

    /// Receives `[len: u64 BE][bytes]` of `stream` into the file `path`, `what` of
    /// [`Images::limit`] bytes at most: its sha256 and size. Hidden: an upload once whole.
    async fn receive(
        &self,
        stream: &mut UnixStream,
        path: &Path,
        what: &str,
    ) -> std::io::Result<(String, u64)> {
        let bytes = stream.read_u64().await?;
        let limit = self.images.limit;
        if bytes > limit {
            return Err(std::io::Error::other(format!(
                "{what} of {} MiB: {} MiB at most",
                bytes >> 20,
                limit >> 20
            )));
        }
        let received = async {
            let mut file = tokio::fs::File::create(&path).await?;
            let mut hasher = Sha256::new();
            let mut piece = vec![0u8; 64 * 1024];
            let mut left = bytes;
            while left > 0 {
                let wanted = piece.len().min(usize::try_from(left).unwrap_or(usize::MAX));
                let n = stream.read(&mut piece[..wanted]).await?;
                if n == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
                hasher.update(&piece[..n]);
                file.write_all(&piece[..n]).await?;
                left -= n as u64;
            }
            file.flush().await?;
            let sha256: String = hasher
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            Ok::<_, std::io::Error>(sha256)
        }
        .await;
        match received {
            Ok(sha256) => Ok((sha256, bytes)),
            Err(e) => {
                let _ = std::fs::remove_file(path);
                Err(e)
            }
        }
    }
}

/// Keeps the file received at `received` as `path`, by a rename; the files of its directory
/// not used for a while removed (the caches, the buildpacks' layers).
fn keep(received: &Path, path: &Path) -> Result<(), String> {
    let dir = path.parent().expect("in a directory");
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    for other in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let stale = other.metadata().is_ok_and(|file| {
            file.modified()
                .is_ok_and(|at| at.elapsed().is_ok_and(|age| age > BUILDER_KEPT))
        });
        if stale {
            let _ = std::fs::remove_file(other.path());
        }
    }
    std::fs::rename(received, path).map_err(|e| {
        let _ = std::fs::remove_file(received);
        format!("{}: {e}", path.display())
    })
}

/// `bytes` in MiB, in KiB below one.
fn sized(bytes: u64) -> String {
    match bytes >> 20 {
        0 => format!("{} KiB", bytes.div_ceil(1024)),
        mib => format!("{mib} MiB"),
    }
}

/// A cache's name: letters, digits, `-`, `_` and `.`, not first.
fn check_cache_name(name: &str) -> Result<(), String> {
    let valid = !name.is_empty()
        && name.len() <= MAX_CACHE_NAME
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    match valid {
        true => Ok(()),
        false => Err(format!(
            "'{name}': {MAX_CACHE_NAME} letters, digits, '-', '_' or '.' at most, not '.' first"
        )),
    }
}

/// `<id>[@<version>]`.
fn split_buildpack(buildpack: &str) -> (&str, Option<&str>) {
    match buildpack.split_once('@') {
        Some((id, version)) => (id, Some(version)),
        None => (buildpack, None),
    }
}

/// The buildpacks the builder's `metadata` lists, `(id, version)`.
fn listed(metadata: &Value) -> Vec<(String, String)> {
    metadata["buildpacks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|buildpack| {
            let field = |name: &str| buildpack[name].as_str().map(String::from);
            Some((field("id")?, field("version")?))
        })
        .collect()
}

/// The order of the buildpacks `asked` for, as `(id, version)`: one named by its image that
/// of the next of `packages`, any other of those the builder's `metadata` lists, a version left
/// out the latest of that id. One it lacks fails it, naming those of the builder's order (its
/// `order` label).
fn order(
    metadata: &Value,
    order_label: Option<&str>,
    asked: &[String],
    packages: &[&Package],
) -> Result<Vec<(String, String)>, String> {
    let listed = listed(metadata);
    let listed: Vec<(&str, &str)> = (listed.iter())
        .map(|(id, version)| (id.as_str(), version.as_str()))
        .collect();
    let mut packages = packages.iter();
    let mut order = Vec::new();
    for buildpack in asked {
        if buildpack.starts_with(BUILDPACK_IMAGE) {
            let package = packages.next().expect("a package per image");
            order.push((package.id.clone(), package.version.clone()));
            continue;
        }
        let (id, version) = split_buildpack(buildpack);
        let versions = listed
            .iter()
            .filter(|(listed, _)| *listed == id)
            .map(|(_, v)| *v);
        let found = match version {
            Some(version) => versions.into_iter().find(|listed| *listed == version),
            None => versions.max_by(|a, b| by_version(a, b)),
        };
        let Some(version) = found else {
            let mut named: Vec<String> = order_label
                .and_then(|label| serde_json::from_str::<Value>(label).ok())
                .and_then(|order| order.as_array().cloned())
                .into_iter()
                .flatten()
                .flat_map(|entry| entry["group"].as_array().cloned().unwrap_or_default())
                .filter_map(|group| Some(group["id"].as_str()?.to_string()))
                .collect();
            if named.is_empty() {
                named = listed.iter().map(|(id, _)| id.to_string()).collect();
            }
            named.dedup();
            return Err(format!(
                "the builder has no buildpack {buildpack}: its order names {}",
                named.join(", ")
            ));
        };
        order.push((id.to_string(), version.to_string()));
    }
    Ok(order)
}

/// Versions compared by their numbers, dot by dot; text where one is none.
fn by_version(a: &str, b: &str) -> std::cmp::Ordering {
    let numbers = |version: &str| -> Option<Vec<u64>> {
        version
            .split(['.', '-', '+'])
            .take(3)
            .map(|part| part.parse().ok())
            .collect()
    };
    match (numbers(a), numbers(b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

impl Build {
    fn abort(&self) {
        if let Some(task) = self.task.lock().unwrap_or_else(|e| e.into_inner()).take() {
            task.abort();
        }
    }
}

/// A builder, ready for a build.
struct Builder {
    rootfs: PathBuf,
    /// The run image pinned to its digest, as named (by the builder, else the build), and
    /// its target.
    run_image: String,
    run_image_name: String,
    target: Target,
    platform_api: String,
    /// The buildpacks asked for, `(id, version)`: none, the builder's order.
    order: Vec<(String, String)>,
    /// The layers of the buildpacks named by their images, laid over the builder's.
    added: Vec<Added>,
}

/// A layer of buildpacks a build lays over the builder's: of an image it named.
struct Added {
    diff_id: String,
    /// The tar, kept.
    file: std::fs::File,
    /// The buildpacks it holds, `(id, version)`: `/init` unpacks their directories alone.
    buildpacks: Vec<(String, String)>,
}

/// A buildpack image's buildpack (a buildpackage's), and the layers to lay over the builder's:
/// those holding it and the buildpacks its order names, by their diff IDs in the image's
/// order, each with the buildpacks it holds.
#[derive(Debug, PartialEq)]
struct Package {
    id: String,
    version: String,
    layers: Vec<(String, Vec<(String, String)>)>,
}

/// Whether `id` and `version` are a buildpack's, naming its directory
/// (`/cnb/buildpacks/<id, '/' as '_'>/<version>`): an id of letters, digits, `.`, `/` and `-`,
/// as the spec has it, a version of no `/`; neither empty nor starting with `.`.
fn directory_named(id: &str, version: &str) -> bool {
    let id_char = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'/' | b'-');
    let version_char = |b: u8| b.is_ascii_graphic() && !matches!(b, b'/' | b'\\');
    !id.is_empty()
        && !version.is_empty()
        && !id.starts_with('.')
        && !version.starts_with('.')
        && id.bytes().all(id_char)
        && version.bytes().all(version_char)
}

/// The [`Package`] of the image whose config is `config`, as pack reads one: its buildpack of
/// `io.buildpacks.buildpackage.metadata`, its buildpacks' layers of
/// `io.buildpacks.buildpack.layers` (`{<id>: {<version>: {"layerDiffID": ..}}}`); a layer
/// holding only buildpacks the builder `listed` is left out.
fn package(config: &Value, listed: &[(String, String)]) -> Result<Package, String> {
    let labels = &config["config"]["Labels"];
    let label = |key: &str| {
        labels[key]
            .as_str()
            .and_then(|label| serde_json::from_str::<Value>(label).ok())
            .ok_or_else(|| format!("no buildpack image (no {key})"))
    };
    let metadata = label("io.buildpacks.buildpackage.metadata")?;
    let field = |name: &str| {
        metadata[name]
            .as_str()
            .map(String::from)
            .ok_or_else(|| format!("its io.buildpacks.buildpackage.metadata names no {name}"))
    };
    let (id, version) = (field("id")?, field("version")?);
    let mut held: BTreeMap<&str, Vec<(String, String)>> = BTreeMap::new();
    let layers = label("io.buildpacks.buildpack.layers")?;
    for (named, versions) in layers.as_object().into_iter().flatten() {
        for (numbered, described) in versions.as_object().into_iter().flatten() {
            if !directory_named(named, numbered) {
                return Err(format!(
                    "a buildpack '{named}' '{numbered}': no id and version"
                ));
            }
            let diff_id = described["layerDiffID"]
                .as_str()
                .filter(|diff_id| {
                    diff_id
                        .strip_prefix("sha256:")
                        .is_some_and(|sha256| check_digest(sha256).is_ok())
                })
                .ok_or_else(|| format!("the buildpack {named} {numbered} names no layerDiffID"))?;
            held.entry(diff_id)
                .or_default()
                .push((named.clone(), numbered.clone()));
        }
    }
    if !held
        .values()
        .flatten()
        .any(|held| *held == (id.clone(), version.clone()))
    {
        return Err(format!("its layers hold no buildpack {id} {version}"));
    }
    let diff_ids: Vec<&str> = config["rootfs"]["diff_ids"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    if let Some(missing) = held.keys().find(|diff_id| !diff_ids.contains(diff_id)) {
        return Err(format!("its labels name a layer {missing} it has not"));
    }
    let layers = diff_ids
        .into_iter()
        .filter_map(|diff_id| Some((diff_id.to_string(), held.remove(diff_id)?)))
        .filter(|(_, held)| !held.iter().all(|held| listed.contains(held)))
        .collect();
    Ok(Package {
        id,
        version,
        layers,
    })
}

/// An image's target, as the lifecycle reads a run image's: its config's platform, the
/// distribution its labels name (`io.buildpacks.base.distro.*`, else the older
/// `io.buildpacks.stack.distro.*` Paketo's images have).
#[derive(Debug, PartialEq)]
struct Target {
    os: String,
    arch: String,
    variant: Option<String>,
    distro: Option<Distro>,
}

#[derive(Debug, PartialEq, Serialize)]
struct Distro {
    name: String,
    version: String,
}

impl Target {
    /// The target of the image whose config is `config`.
    fn of(config: &Value) -> Target {
        let text = |value: &Value| {
            value
                .as_str()
                .filter(|text| !text.is_empty())
                .map(String::from)
        };
        let labels = &config["config"]["Labels"];
        let label = |key: &str| {
            text(&labels[format!("io.buildpacks.base.{key}")])
                .or_else(|| text(&labels[format!("io.buildpacks.stack.{key}")]))
        };
        let (name, version) = (label("distro.name"), label("distro.version"));
        Target {
            os: text(&config["os"]).unwrap_or_else(|| "linux".into()),
            arch: text(&config["architecture"]).unwrap_or_else(|| image::ARCHITECTURE.into()),
            variant: text(&config["variant"]),
            distro: (name.is_some() || version.is_some()).then(|| Distro {
                name: name.unwrap_or_default(),
                version: version.unwrap_or_default(),
            }),
        }
    }

    /// Whether what is built on `builder` runs on this one: the same OS and architecture, and
    /// the same variant and distribution where both name one (the buildpacks install what was
    /// built for the builder's).
    fn fits(&self, builder: &Target) -> Result<(), String> {
        let differ = |a: &str, b: &str| !a.is_empty() && !b.is_empty() && a != b;
        let variants = (self.variant.as_deref(), builder.variant.as_deref());
        let distros = (self.distro.as_ref(), builder.distro.as_ref());
        let fits = self.os == builder.os
            && self.arch == builder.arch
            && !matches!(variants, (Some(a), Some(b)) if a != b)
            && !matches!(distros, (Some(a), Some(b))
                if differ(&a.name, &b.name) || differ(&a.version, &b.version));
        match fits {
            true => Ok(()),
            false => Err(format!(
                "is {self}, the builder {builder}: not of its target"
            )),
        }
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.os, self.arch)?;
        if let Some(variant) = &self.variant {
            write!(f, "/{variant}")?;
        }
        if let Some(distro) = &self.distro {
            write!(f, " ({} {})", distro.name, distro.version)?;
        }
        Ok(())
    }
}

/// What `/init` answers a build with (its `stage::Outcome`).
#[derive(Deserialize)]
struct Outcome {
    error: Option<String>,
    #[serde(default)]
    processes: Vec<String>,
    default: Option<String>,
}

/// What a droplet runs, as the platform spec has an exporter set an image's config: the
/// default process's link to the launcher (the buildpacks' default, else `web`, else the only
/// one), else the launcher itself. A `command` is the launcher's to run, in place of any
/// process: through bash (the run image's, which the image made must hold), as CF runs a
/// command, the layers' profile scripts sourced and their env set.
fn launch(outcome: &Outcome, platform_api: &str, command: Option<&str>) -> Over {
    let process = outcome
        .default
        .as_ref()
        .or_else(|| outcome.processes.iter().find(|kind| *kind == "web"))
        .or(match &outcome.processes[..] {
            [only] => Some(only),
            _ => None,
        });
    let entrypoint = match (command, process) {
        (Some(command), _) => vec!["/cnb/lifecycle/launcher".into(), command.to_string()],
        (None, Some(kind)) => vec![format!("/cnb/process/{kind}")],
        (None, None) => vec!["/cnb/lifecycle/launcher".into()],
    };
    Over {
        entrypoint,
        env: vec![
            format!("CNB_LAYERS_DIR={LAYERS_DIR}"),
            format!("CNB_APP_DIR={APP_DIR}"),
            format!("CNB_PLATFORM_API={platform_api}"),
            "CNB_DEPRECATION_MODE=quiet".into(),
        ],
        path: vec!["/cnb/process".into(), "/cnb/lifecycle".into()],
        workdir: APP_DIR.into(),
        // the lifecycle's launcher execs it by this path (`launch/bash.go`)
        needs: match command {
            Some(_) => BTreeMap::from([(
                "/bin/bash".into(),
                "the launcher runs the command with it: a run image without a shell (Paketo's \
                 tiny) runs a buildpack's process alone, no command"
                    .into(),
            )]),
            None => BTreeMap::new(),
        },
    }
}

/// A build VM's output being followed.
struct Output {
    syncs: mpsc::Sender<()>,
    synced: watch::Receiver<u64>,
}

impl Output {
    /// Follows the log stream of a build's VM in a task of its own, until the stream ends:
    /// its lines the build's.
    fn follow(mut stream: UnixStream, build: Arc<Build>) -> Output {
        let (syncs, mut asked) = mpsc::channel::<()>(1);
        let (count, synced) = watch::channel(0u64);
        tokio::spawn(async move {
            // borrowed halves: an owned write half shuts the socket down when dropped,
            // which Firecracker takes as the end of the whole stream
            let (reader, mut writer) = stream.split();
            let mut reader = BufReader::with_capacity(4096, reader);
            let reading = async {
                loop {
                    match wire::read_log_frame(&mut reader).await {
                        Ok(Some(LogFrame::Line(stream, line))) => build.line(stream, &line),
                        Ok(Some(LogFrame::Gap { stdout, stderr })) => {
                            build.note(format!("({} lines of the output dropped)", stdout + stderr))
                        }
                        Ok(Some(LogFrame::Synced)) => count.send_modify(|count| *count += 1),
                        Ok(Some(LogFrame::Exited(_))) => {}
                        Ok(None) | Err(_) => return,
                    }
                }
            };
            let asking = async {
                while asked.recv().await.is_some() {
                    if writer.write_all(&[wire::SYNC]).await.is_err() {
                        break;
                    }
                }
                std::future::pending::<()>().await;
            };
            tokio::select! {
                () = reading => {}
                () = asking => {}
            }
        });
        Output { syncs, synced }
    }

    /// Waits for every line written so far to be the build's, within [`SYNC_TIMEOUT`].
    async fn sync(mut self) {
        let before = *self.synced.borrow_and_update();
        let synced = async {
            self.syncs.send(()).await.ok()?;
            self.synced.wait_for(|count| *count > before).await.ok()?;
            Some(())
        };
        let _ = tokio::time::timeout(SYNC_TIMEOUT, synced).await;
    }
}

impl Images {
    /// The rootfs of the builder `image` whose manifest is `digest`: kept by it and the
    /// guest, made when it is not (`note` told so: minutes, for a large one).
    async fn builder(
        &self,
        image: &str,
        digest: &str,
        credentials: Option<&(String, String)>,
        note: impl Fn(String),
    ) -> Result<PathBuf, String> {
        let guest = tokio::fs::read(&self.guest)
            .await
            .map_err(|e| format!("{}: {e}", self.guest.display()))?;
        let mut key = Sha256::new();
        key.update(digest.as_bytes());
        key.update(&guest);
        let key: String = key.finalize()[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let path = self.builders.join(format!("{key}.ext4"));
        // one at a time: a second build of the same builder finds the first's
        let _turn = self.making.lock().await;
        if regular(&path) {
            // used: kept longer
            if let Ok(file) = std::fs::File::options().append(true).open(&path) {
                let _ = file.set_modified(std::time::SystemTime::now());
            }
            return Ok(path);
        }
        note(format!("Making the rootfs of the builder {image}"));
        let started = Instant::now();
        let built = self
            .build(image::Request {
                image: image.to_string(),
                credentials: credentials.cloned(),
                port: None,
                restore: Vec::new(),
                layers: Vec::new(),
                launch: Some(Over {
                    // never started: /init runs the lifecycle's phases itself
                    entrypoint: vec!["/cnb/lifecycle/builder".into()],
                    ..Over::default()
                }),
            })
            .await?;
        note(format!(
            "Made it in {}s: {} MiB",
            started.elapsed().as_secs(),
            built.received.bytes >> 20
        ));
        for other in std::fs::read_dir(&self.builders)
            .into_iter()
            .flatten()
            .flatten()
        {
            let stale = other.metadata().is_ok_and(|file| {
                file.modified()
                    .is_ok_and(|at| at.elapsed().is_ok_and(|age| age > BUILDER_KEPT))
            });
            if stale {
                let _ = std::fs::remove_file(other.path());
            }
        }
        built.received.keep(&path)?;
        Ok(path)
    }
}

/// The layer `sha256` of `dir`'s uploads for an image to be made with, or the refusal naming
/// it missing.
pub fn kept_layer(builds: &Builds, sha256: &str) -> Result<(PathBuf, String), Refusal> {
    let sha256 = check_digest(sha256)
        .map_err(|e| Refusal::new(StatusCode::BAD_REQUEST, format!("layers: {e}")))?;
    let path: &Path = &builds.layer(&sha256);
    if !regular(path) {
        let mut refusal = Refusal::new(
            StatusCode::CONFLICT,
            format!("no layer {sha256} is kept: PUT /api/layers/{sha256} first"),
        );
        refusal.view.missing = vec![sha256];
        return Err(refusal);
    }
    Ok((path.to_path_buf(), sha256))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A build's output is read from any line on, its end told once the lines before it were;
    /// only the last lines are kept. A droplet runs its default process, else `web`, else
    /// its only one, else the launcher; a command, by the launcher. The buildpacks asked for
    /// are the builder's, of the version named, else the latest, or an image's, by its labels,
    /// with the layers holding it and its order the builder lacks. A run image named is of the
    /// builder's target.
    #[test]
    fn a_build_tells_its_lines_then_its_end_and_what_its_droplet_runs() {
        let build = Build {
            state: Mutex::new(State::default()),
            changed: watch::channel(0).0,
            task: Mutex::new(None),
        };
        for n in 0..LINES + 2 {
            build.line(Stream::Stderr, format!("line {n}").as_bytes());
        }
        build.end(Err("no buildpack".into()));
        let first = build.view(0);
        assert_eq!((first.state, first.next), ("building", LINES as u64));
        assert_eq!(first.lines[0].line, "line 0");
        let rest = build.view(first.next);
        assert_eq!((rest.state, rest.lines.len()), ("failed", 2));
        assert_eq!(rest.error.as_deref(), Some("no buildpack"));
        assert_eq!(rest.lines[1].stream, "stderr");
        for n in 0..MAX_LINES {
            build.line(Stream::Stdout, format!("more {n}").as_bytes());
        }
        // the lines before the first kept are gone: the next ones
        let late = build.view(3);
        assert_eq!(late.lines[0].line, "more 0");
        assert_eq!(late.next, (LINES + 2 + LINES) as u64);

        let outcome = |processes: &[&str], default: Option<&str>| Outcome {
            error: None,
            processes: processes.iter().map(|kind| kind.to_string()).collect(),
            default: default.map(String::from),
        };
        for (processes, default, entrypoint) in [
            (&["task", "web"][..], Some("task"), "/cnb/process/task"),
            (&["task", "web"], None, "/cnb/process/web"),
            (&["worker"], None, "/cnb/process/worker"),
            (&["a", "b"], None, "/cnb/lifecycle/launcher"),
        ] {
            let launch = launch(&outcome(processes, default), "0.13", None);
            assert_eq!(launch.entrypoint, [entrypoint]);
            assert!(launch.needs.is_empty());
            assert!(launch.env.contains(&"CNB_PLATFORM_API=0.13".to_string()));
        }
        let command = "java -jar app.jar --port=$PORT";
        let launch = launch(&outcome(&["web"], None), "0.13", Some(command));
        assert_eq!(launch.entrypoint, ["/cnb/lifecycle/launcher", command]);
        assert_eq!(launch.needs.keys().collect::<Vec<_>>(), ["/bin/bash"]);

        let metadata = serde_json::json!({"buildpacks": [
            {"id": "paketo-buildpacks/java", "version": "22.8.0"},
            {"id": "paketo-buildpacks/maven", "version": "6.9.0"},
            {"id": "paketo-buildpacks/maven", "version": "6.26.0"},
        ]});
        let label = r#"[{"group":[{"id":"paketo-buildpacks/java","version":"22.8.0"}]}]"#;
        let asked = |asked: &[&str]| {
            let asked: Vec<String> = asked.iter().map(|asked| asked.to_string()).collect();
            order(&metadata, Some(label), &asked, &[])
        };
        let named = |id: &str, version: &str| (id.to_string(), version.to_string());
        assert_eq!(
            asked(&["paketo-buildpacks/maven", "paketo-buildpacks/java@22.8.0"]),
            Ok(vec![
                named("paketo-buildpacks/maven", "6.26.0"),
                named("paketo-buildpacks/java", "22.8.0")
            ])
        );
        for missing in ["paketo-buildpacks/go", "paketo-buildpacks/java@1.0.0"] {
            assert_eq!(
                asked(&[missing]),
                Err(format!(
                    "the builder has no buildpack {missing}: its order names paketo-buildpacks/java"
                ))
            );
        }
        // a buildpack image, as pack writes one: a composite in one layer with those of its
        // order, a layer of the builder's buildpack alone left out
        let diff_id = |n: u8| format!("sha256:{}", format!("{n:02x}").repeat(32));
        let held = serde_json::json!({
            "paketo-community/rust": {"0.92.2": {"layerDiffID": diff_id(1), "order": []}},
            "paketo-community/cargo": {"0.16.1": {"layerDiffID": diff_id(1)}},
            "paketo-buildpacks/maven": {"6.26.0": {"layerDiffID": diff_id(2)}},
        });
        let image = |held: &Value, diff_ids: &[String]| {
            serde_json::json!({"rootfs": {"diff_ids": diff_ids}, "config": {"Labels": {
                "io.buildpacks.buildpackage.metadata":
                    r#"{"id":"paketo-community/rust","version":"0.92.2"}"#,
                "io.buildpacks.buildpack.layers": held.to_string(),
            }}})
        };
        let listed = listed(&metadata);
        let rust = package(&image(&held, &[diff_id(2), diff_id(1)]), &listed).unwrap();
        assert_eq!(
            rust,
            Package {
                id: "paketo-community/rust".into(),
                version: "0.92.2".into(),
                layers: vec![(
                    diff_id(1),
                    vec![
                        named("paketo-community/cargo", "0.16.1"),
                        named("paketo-community/rust", "0.92.2")
                    ]
                )],
            }
        );
        let asked: Vec<String> = ["paketo-buildpacks/maven", "docker://r.io/rust:1"]
            .map(String::from)
            .into();
        assert_eq!(
            order(&metadata, Some(label), &asked, &[&rust]),
            Ok(vec![
                named("paketo-buildpacks/maven", "6.26.0"),
                named("paketo-community/rust", "0.92.2")
            ])
        );
        let mut escaping = held.clone();
        escaping["../up"] = serde_json::json!({"1": {"layerDiffID": diff_id(1)}});
        for (config, error) in [
            (serde_json::json!({}), "no buildpack image"),
            (image(&held, &[diff_id(1)]), "a layer"),
            (
                image(&escaping, &[diff_id(1), diff_id(2)]),
                "no id and version",
            ),
            (image(&serde_json::json!({}), &[]), "hold no buildpack"),
        ] {
            let refused = package(&config, &listed).unwrap_err();
            assert!(refused.contains(error), "{refused}");
        }

        // a run image named in the builder's place: of its OS, architecture and distribution
        let config = |arch: &str, labels: Value| serde_json::json!({"os": "linux", "architecture": arch, "config": {"Labels": labels}});
        let stack = |version: &str| {
            serde_json::json!({"io.buildpacks.stack.distro.name": "ubuntu",
                "io.buildpacks.stack.distro.version": version})
        };
        let builder = Target::of(&config("amd64", stack("26.04")));
        assert_eq!(builder.to_string(), "linux/amd64 (ubuntu 26.04)");
        let base = serde_json::json!({"io.buildpacks.base.distro.name": "ubuntu",
            "io.buildpacks.base.distro.version": "26.04",
            "io.buildpacks.stack.distro.version": "24.04"});
        for (run, fits) in [
            (config("amd64", base), true),
            (config("amd64", Value::Null), true),
            (config("amd64", stack("24.04")), false),
            (config("arm64", stack("26.04")), false),
        ] {
            let run = Target::of(&run);
            assert_eq!(run.fits(&builder).is_ok(), fits, "{run}");
        }
        assert_eq!(
            Target::of(&config("arm64", stack("26.04"))).fits(&builder),
            Err(
                "is linux/arm64 (ubuntu 26.04), the builder linux/amd64 (ubuntu 26.04): not of \
                 its target"
                    .into()
            )
        );
        assert!(check_cache_name("b5e0f1a2-0c1d-4e5f-8a9b-0c1d2e3f4a5b").is_ok());
        for name in ["", ".hidden", "../up", "a/b"] {
            assert!(check_cache_name(name).is_err(), "{name}");
        }
    }
}
