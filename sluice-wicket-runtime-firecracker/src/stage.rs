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
//! ```
//!
//! A build names a builder image and a source kept before. The builder's rootfs is made as
//! an image's (`image`) and kept by its manifest's digest; a VM of it is booted with a
//! scratch disk, within the VMs' budget as any other, and `/init` there runs the builder's
//! `detector` and `builder` on the source: no analyzer (the wicket names the run image, pinned
//! to its digest), no exporter. What comes back is the droplet -- a tar of what goes over the
//! run image's root file system: the launch layers, the app, the launcher -- kept as a layer,
//! and the `launch` it runs with. `PUT /api/images` of the run image with that layer and
//! launch makes the rootfs, on this wicket or any other: a droplet is built once.
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

use crate::capacity::{Capacity, Need};
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

/// A builder's rootfs not used for this long goes when another is made.
const BUILDER_KEPT: Duration = Duration::from_secs(7 * 24 * 60 * 60);

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
}

/// A build as asked for, checked.
struct Asked {
    builder: String,
    credentials: Option<(String, String)>,
    source: std::fs::File,
    env: Env,
    egress: Vec<Rule>,
    memory_mib: u32,
    vcpus: u32,
    disk_mib: u32,
    timeout: Duration,
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
        Ok(Asked {
            builder: request.builder,
            credentials,
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
        let builder = self.builder(build, &asked).await?;
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
        let need = Need {
            memory: u64::from(spec.memory_mib),
            route: crate::route::next_route(),
        };
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
        let staged = self
            .stage(&mut stream, id, &builder, asked)
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
            launch: launch(&outcome, &builder.platform_api),
        })
    }

    /// The builder of `asked`, its rootfs made unless kept, and the run image it names.
    async fn builder(&self, build: &Build, asked: &Asked) -> Result<Builder, String> {
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
        let run_image_name = metadata["images"][0]["image"]
            .as_str()
            .or(metadata["stack"]["runImage"]["image"].as_str())
            .ok_or("the builder names no run image")?
            .to_string();
        // the builder's credentials go to its own registry alone
        let registry = |image: &str| {
            sluice_wicket_registry::Reference::parse(image).map(|reference| reference.registry)
        };
        let same = registry(&run_image_name)? == registry(&asked.builder)?;
        let run = self
            .images
            .inspect(&run_image_name, credentials.filter(|_| same))
            .await
            .map_err(|e| format!("the run image {run_image_name}: {e}"))?;
        let text =
            |key: &str, default: &str| run.config[key].as_str().unwrap_or(default).to_string();
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
            os: text("os", "linux"),
            arch: text("architecture", image::ARCHITECTURE),
            platform_api: platform_api.to_string(),
        })
    }

    /// One build on the stage stream of its VM: the result, and the droplet received when
    /// it has no error.
    async fn stage(
        &self,
        stream: &mut UnixStream,
        id: &str,
        builder: &Builder,
        asked: Asked,
    ) -> std::io::Result<(Outcome, Option<Received>)> {
        let request = serde_json::json!({
            "env": asked.env.map(),
            "platform_api": builder.platform_api,
            "run_image": builder.run_image,
            "run_image_name": builder.run_image_name,
            "os": builder.os,
            "arch": builder.arch,
        });
        let request = serde_json::to_vec(&request).map_err(std::io::Error::other)?;
        tokio::time::timeout(GUEST_TIMEOUT, wire::send_env(stream, &request))
            .await
            .map_err(|_| std::io::Error::other("the builder's /init took no request"))??;
        let mut source = tokio::fs::File::from_std(asked.source);
        stream.write_u64(source.metadata().await?.len()).await?;
        tokio::io::copy(&mut source, stream).await?;
        let outcome = wire::read_json(stream, wire::MAX_STAGE_RESULT)
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "the build's VM ended before its result ({e}): out of memory-mib?"
                ))
            })?;
        let outcome: Outcome = serde_json::from_slice(&outcome)
            .map_err(|e| std::io::Error::other(format!("the build's result: {e}")))?;
        if outcome.error.is_some() {
            return Ok((outcome, None));
        }
        let bytes = stream.read_u64().await?;
        let limit = self.images.limit;
        if bytes > limit {
            return Err(std::io::Error::other(format!(
                "a droplet of {} MiB: {} MiB at most",
                bytes >> 20,
                limit >> 20
            )));
        }
        // hidden: an upload once whole
        let path = self.images.work.join(format!(".droplet-{id}.partial"));
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
            Ok(sha256) => Ok((outcome, Some(Received::adopt(path, sha256, bytes)))),
            Err(e) => {
                let _ = std::fs::remove_file(&path);
                Err(e)
            }
        }
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
    /// The run image pinned to its digest, as named by the builder, and its platform.
    run_image: String,
    run_image_name: String,
    os: String,
    arch: String,
    platform_api: String,
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
/// one), else the launcher itself.
fn launch(outcome: &Outcome, platform_api: &str) -> Over {
    let process = outcome
        .default
        .as_ref()
        .or_else(|| outcome.processes.iter().find(|kind| *kind == "web"))
        .or(match &outcome.processes[..] {
            [only] => Some(only),
            _ => None,
        });
    Over {
        entrypoint: vec![match process {
            Some(kind) => format!("/cnb/process/{kind}"),
            None => "/cnb/lifecycle/launcher".into(),
        }],
        env: vec![
            format!("CNB_LAYERS_DIR={LAYERS_DIR}"),
            format!("CNB_APP_DIR={APP_DIR}"),
            format!("CNB_PLATFORM_API={platform_api}"),
            "CNB_DEPRECATION_MODE=quiet".into(),
        ],
        path: vec!["/cnb/process".into(), "/cnb/lifecycle".into()],
        workdir: APP_DIR.into(),
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
                        Ok(Some(LogFrame::Exited)) => {}
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
    /// its only one, else the launcher.
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
            let launch = launch(&outcome(processes, default), "0.13");
            assert_eq!(launch.entrypoint, [entrypoint]);
            assert!(launch.env.contains(&"CNB_PLATFORM_API=0.13".to_string()));
        }
    }
}
