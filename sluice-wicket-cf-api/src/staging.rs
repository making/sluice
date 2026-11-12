//! A build: what is pushed decides the droplet's stack, and so the wickets it runs on. A zip is
//! staged by a [`Buildpack`], the one detecting it ([`detect`]): its file is the one the web
//! process command names, else its only `.wasm`, a component (`wasm`: `\0asm`, layer 1; the stack
//! `wasip3`), or its only `.ext4`, a rootfs (`rootfs`: the stack `firecracker`); a zip of neither
//! with an `index.html` at its root is a static site (`site`: `wasip3`). The buildpacks an app
//! names (`cf push -b`, a manifest's `buildpacks:`) are the only ones detecting, so a site
//! holding `.wasm` files is one by `-b site`. The droplet keeps its buildpack.
//!
//! Where a builder is configured (`--cnb-builder`), any other zip is source its Cloud Native
//! Buildpacks build (`cnb`, detecting last: the stack `firecracker`), in a microVM of a
//! Firecracker wicket ([`build`]): the zip goes there as a tar, the build's output comes back
//! as the staging log, and its droplet -- a tar of what goes over the builder's run image --
//! is kept here with that image's pinned reference and the credentials it is pulled with, so
//! every wicket makes the rootfs of it and none builds it again. A name of an app's buildpacks
//! that is none of these is a buildpack of the builder's (`paketo-buildpacks/java`), or one by
//! its image (`docker://<image>`, as `pack build --buildpack`), the ones named the build's order; a web process command that names no file of the package is what
//! the droplet runs. The wicket keeps the build's cache under the app's guid: the next build
//! there restores it.
//!
//! An image has none: one whose manifest holds a wasm component is pulled (`crate::registry`);
//! any other is made into a rootfs by a Firecracker wicket, which keeps it (the cf-api the
//! image's pinned reference, to have it made again). The manifest is read to tell them apart
//! only where wickets of both stacks run.
//!
//! A stack no wicket runs fails the build, naming those they run, as does a package of another
//! stack than the app's pin (`cf push -s`, a manifest's `stack:`). Anything else fails it too,
//! naming what was found. The droplet is the component or the rootfs, by its digest.
//!
//! A static site's droplet is the component of sluice-wicket-site ([`SITE`], embedded at the
//! build) and a tar of the zip's files, the wicket's read-only at `/` for it ([`pack_site`]):
//! dot files (but `.well-known/`) and the manifest are left out, so a `.git`, a `.env` or the
//! env of a `manifest.yml` never reaches the wicket.
//!
//! A build is staged by the instance reconciling the wickets, in its turn (`Controller::follow`),
//! whichever took the request. It keeps what its staging is asked for, and the build of source
//! its wicket runs, with how far its output was taken (`crate::store::Staging`): a stop of the
//! cf-api or the turn's end cutting it short, the next turn takes it up ([`run`]), staging again
//! from the package (an image's manifest read again, its rootfs asked of the wicket, which keeps
//! it by its digest) but a build of source, followed on, built again where its wicket knows it
//! no more. What a staging changes of the state is its turn's alone (`Store::lead`): one whose
//! turn ended ends no build, and forgets none on its wicket.

use std::io::{Read, Seek, Write};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::blobs::Reading;
use crate::controller::{Controller, no_maker};
use crate::logs::{Logs, Stream};
use crate::registry::Registry;
use crate::store::{
    Blob, Building, Built, DockerImage, Droplet, Files, Package, Spool, Staging, State, Store,
    guid, now,
};
use crate::wicket::{BuildRequest, BuildView, Failure, LayerRef, Runtime, Wicket, stacks_named};

/// A rootfs's file name ends so; a droplet's kind is its file's.
pub const ROOTFS: &str = ".ext4";

/// The component a static site is served by: sluice-wicket-site's (`build.rs`).
pub const SITE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/site.wasm"));

/// Its file name in the wicket.
pub const SITE_FILE: &str = "site.wasm";

/// What tells a zip of a static site: its page at the root.
const SITE_INDEX: &str = "index.html";

/// What a site's files may unpack to, and how many: the wicket's bounds of a tar.
const MAX_SITE_BYTES: u64 = 1 << 30;
const MAX_SITE_FILES: usize = 100_000;

/// What a zip of source may unpack to.
const MAX_SOURCE_BYTES: u64 = 1 << 30;

/// A link's target in a zip of source at most.
const MAX_LINK: u64 = 4096;

/// How often in a row a wicket may not answer for its build before the build is given up.
const FOLLOW_TRIES: u32 = 5;

/// Why a staging goes no further: this instance's turn reconciling the wickets ended, the
/// next one's takes the build up.
const TURN_OVER: &str = "this instance's turn is over";

/// How long a staging's end waits for the CLI following its log to print the last lines
/// ([`Logs::read_out`]): it reads 2s behind (log-cache's walk delay), every 250ms.
const READ_OUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The builder staging source (`--cnb-builder`), and what its builds get.
#[derive(Clone, Debug)]
pub struct Cnb {
    /// The builder image.
    pub builder: String,
    /// The run image in place of the builder's (`--cnb-run-image`).
    pub run_image: Option<String>,
    /// Its registry's `(username, password)`.
    pub credentials: Option<(String, String)>,
    /// The destinations a build may connect to (`--cnb-egress`); none: no network.
    pub egress: Vec<String>,
    /// A build VM's size (`--cnb-memory-mib`, `--cnb-disk-mib`: an app's own if larger, as
    /// CF stages; `--cnb-vcpus`) and time (`--cnb-timeout`).
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub vcpus: u32,
    pub timeout: std::time::Duration,
}

impl Cnb {
    /// A build VM's defaults: what a Maven build of a Spring Boot app takes, as long as CF's
    /// staging may take.
    pub const MEMORY_MIB: u64 = 2048;
    pub const DISK_MIB: u64 = 8192;
    pub const VCPUS: u32 = 2;
    pub const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);
}

impl Cnb {
    /// The credentials `image` is pulled with: the builder's, where it is of its registry.
    pub fn credentials_of(&self, image: &str) -> Option<(&str, &str)> {
        use sluice_wicket_registry::Reference;
        let registry = |image: &str| Reference::parse(image).ok().map(|image| image.registry);
        let (username, password) = self.credentials.as_ref()?;
        (registry(image)? == registry(&self.builder)?)
            .then_some((username.as_str(), password.as_str()))
    }
}

/// What a package's component may unpack to: the largest image a wicket takes. A zip takes
/// any size down to next to nothing.
const MAX_DROPLET_BYTES: u64 = 4 << 30;

/// What a component is told by, before it is unpacked: the superblock's magic of a rootfs.
const HEAD: usize = MAGIC_AT + MAGIC.len();

/// Where the ext2/3/4 superblock's magic is, and what it is (little-endian 0xEF53).
const MAGIC_AT: usize = 1024 + 56;
const MAGIC: [u8; 2] = [0x53, 0xEF];

/// What makes a droplet of a zip, as a buildpack of CF: what it detects, and the stack of what
/// it stages. One more is a variant here, what it detects ([`detect`]) and how it stages
/// ([`unpack`]).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Buildpack {
    /// A wasm component, as it is.
    Wasm,
    /// A rootfs (an ext4 image), as it is.
    Rootfs,
    /// A static site: its files, served by sluice-wicket-site.
    Site,
    /// Source, built by a builder's Cloud Native Buildpacks: where one is configured.
    Cnb,
}

impl Buildpack {
    /// All of them, as they detect: a file one stages as it is, before a site, before source.
    const ALL: [Buildpack; 4] = [
        Buildpack::Wasm,
        Buildpack::Rootfs,
        Buildpack::Site,
        Buildpack::Cnb,
    ];

    /// Those there are, as they detect: [`Buildpack::Cnb`] with a builder (`cnb`) alone.
    pub fn all(cnb: bool) -> Vec<Buildpack> {
        let there = |buildpack: &Buildpack| cnb || *buildpack != Buildpack::Cnb;
        Buildpack::ALL.into_iter().filter(there).collect()
    }

    pub fn name(self) -> &'static str {
        match self {
            Buildpack::Wasm => "wasm",
            Buildpack::Rootfs => "rootfs",
            Buildpack::Site => "site",
            Buildpack::Cnb => "cnb",
        }
    }

    /// The stack of what it stages.
    pub fn stack(self) -> Runtime {
        match self {
            Buildpack::Wasm | Buildpack::Site => Runtime::Wasm,
            Buildpack::Rootfs | Buildpack::Cnb => Runtime::Firecracker,
        }
    }

    /// What it stages, as the `cf buildpacks` listing names its file.
    pub fn takes(self) -> &'static str {
        match self {
            Buildpack::Wasm => "*.wasm",
            Buildpack::Rootfs => "*.ext4",
            Buildpack::Site => SITE_INDEX,
            Buildpack::Cnb => "*",
        }
    }

    /// What a package it stages is.
    fn what(self) -> &'static str {
        match self {
            Buildpack::Wasm => "a wasm component",
            Buildpack::Rootfs => "a rootfs",
            Buildpack::Site => "a static site",
            Buildpack::Cnb => "source for Cloud Native Buildpacks",
        }
    }

    /// What it detects, as the staging log tells it.
    fn detects(self) -> &'static str {
        match self {
            Buildpack::Wasm => "the package's only .wasm is the component",
            Buildpack::Rootfs => "the package's only .ext4 is the rootfs",
            Buildpack::Site => "an index.html at its root makes it a static site",
            Buildpack::Cnb => "it is source the builder's buildpacks build",
        }
    }

    /// What a package it does not detect lacks.
    fn lacking(self) -> &'static str {
        match self {
            Buildpack::Wasm => "no .wasm component",
            Buildpack::Rootfs => "no .ext4 rootfs",
            Buildpack::Site => "no index.html at its root",
            Buildpack::Cnb => "no file",
        }
    }

    /// The one staging the file `file` as it is: a rootfs by its name, else a component.
    fn of_file(file: &str) -> Buildpack {
        match file.ends_with(ROOTFS) {
            true => Buildpack::Rootfs,
            false => Buildpack::Wasm,
        }
    }

    /// Whether `file` is one it detects, to stage as it is.
    fn detects_file(self, file: &str) -> bool {
        match self {
            Buildpack::Wasm => file.ends_with(".wasm"),
            Buildpack::Rootfs => file.ends_with(ROOTFS),
            Buildpack::Site | Buildpack::Cnb => false,
        }
    }

    /// The buildpacks detecting for an app naming `named`: those, else all there are
    /// ([`Buildpack::all`] of `cnb`); and the builder's buildpacks named, `<id>[@<version>]`,
    /// and those by their images, `docker://<image>`, which stage source alone. A name of none
    /// fails it, naming them.
    pub fn named(named: &[String], cnb: bool) -> Result<(Vec<Buildpack>, Vec<String>), String> {
        let all = Buildpack::all(cnb);
        if named.is_empty() {
            return Ok((all, Vec::new()));
        }
        let (mut detecting, mut ids) = (Vec::new(), Vec::new());
        for name in named {
            match all.iter().copied().find(|kept| kept.name() == name) {
                Some(buildpack) => detecting.push(buildpack),
                None if cnb && (is_buildpack_id(name) || is_buildpack_image(name)) => {
                    ids.push(name.clone())
                }
                None => {
                    return Err(format!(
                        "there is no buildpack '{name}' (cf push -b, a manifest's buildpacks): \
                         they are {}{}",
                        names(&all),
                        if cnb {
                            ", the builder's by id, and others by their images (docker://<image>)"
                        } else {
                            ""
                        }
                    ));
                }
            }
        }
        if !ids.is_empty() && !detecting.contains(&Buildpack::Cnb) {
            detecting.push(Buildpack::Cnb);
        }
        Ok((detecting, ids))
    }
}

/// Whether `name` is a builder's buildpack's `<id>[@<version>]`: no URL, no space.
fn is_buildpack_id(name: &str) -> bool {
    !name.is_empty()
        && !name.contains("://")
        && name.bytes().all(|b| b.is_ascii_graphic())
        && !name.starts_with(['@', '/', '.'])
}

/// Whether `name` is a buildpack's image, `docker://<image>`.
fn is_buildpack_image(name: &str) -> bool {
    name.strip_prefix("docker://")
        .is_some_and(|image| sluice_wicket_registry::Reference::parse(image).is_ok())
}

/// The names of `buildpacks`, listed.
fn names(buildpacks: &[Buildpack]) -> String {
    let names: Vec<_> = buildpacks
        .iter()
        .map(|buildpack| buildpack.name())
        .collect();
    names.join(", ")
}

/// The buildpack of `detecting` staging a zip of the files `files` for the web process
/// `command`, and the file it stages as it is (a site and source have none): the one the command
/// names, by its path or its file name, else the only one any of them detects, else a site by
/// its `index.html` at the root, else source, whatever it holds. A command naming no
/// component is source's to run, where source is staged.
pub fn detect(
    files: &[String],
    command: Option<&str>,
    detecting: &[Buildpack],
) -> Result<(Buildpack, Option<String>), String> {
    if let Some(command) = command.map(str::trim).filter(|command| !command.is_empty()) {
        let named = |name: &&String| *name == command || name.rsplit('/').next() == Some(command);
        let entry = files.iter().find(named);
        let component = entry.filter(|entry| {
            let detected = |kept: &Buildpack| kept.detects_file(entry);
            Buildpack::ALL.iter().any(detected)
        });
        if component.is_none() && detecting.contains(&Buildpack::Cnb) && !files.is_empty() {
            return Ok((Buildpack::Cnb, None));
        }
        let Some(entry) = entry else {
            return Err(format!(
                "the command names '{command}', which the package does not hold (found: {})",
                listed(files)
            ));
        };
        let buildpack = Buildpack::of_file(entry);
        if !detecting.contains(&buildpack) {
            return Err(format!(
                "the command names '{entry}', {}: the app's buildpacks are {} (cf push -b)",
                buildpack.what(),
                names(detecting)
            ));
        }
        return Ok((buildpack, Some(entry.clone())));
    }
    let detected = |file: &&String| detecting.iter().any(|kept| kept.detects_file(file));
    let entries: Vec<&String> = files.iter().filter(detected).collect();
    match entries.as_slice() {
        [only] => Ok((Buildpack::of_file(only), Some((*only).clone()))),
        [] if detecting.contains(&Buildpack::Site)
            && files.iter().any(|file| file == SITE_INDEX) =>
        {
            Ok((Buildpack::Site, None))
        }
        [] if detecting.contains(&Buildpack::Cnb) && !files.is_empty() => {
            Ok((Buildpack::Cnb, None))
        }
        [] => {
            let lacking: Vec<_> = detecting.iter().map(|kept| kept.lacking()).collect();
            Err(format!(
                "the package holds {} (found: {})",
                lacking.join(", "),
                listed(files)
            ))
        }
        several => Err(format!(
            "the package holds {} components ({}): name one with -c (the web process command)",
            several.len(),
            listed(several)
        )),
    }
}

/// The component a build picked.
#[derive(Debug)]
pub struct Staged {
    /// Its path in the package; its file name for an image.
    pub entry: String,
    /// A component pulled, to keep; empty when kept already (a zip's), or when the wicket
    /// keeps it (a rootfs it made).
    pub bytes: Vec<u8>,
    pub size: u64,
    pub sha256: String,
    /// The image it was pulled from, pinned to its digest.
    pub image: Option<String>,
    /// A static site's files, kept already.
    pub files: Option<Files>,
    /// What buildpacks built, kept already: the rootfs a wicket made of it.
    pub built: Option<Built>,
    /// What it is staged as: where it runs.
    pub stack: Runtime,
    /// What staged it; none of an image's.
    pub buildpack: Option<Buildpack>,
}

impl Staged {
    /// The blobs its droplet names: its component or rootfs, unless a wicket made and keeps
    /// it (of an image, of what buildpacks built), and its tar.
    fn blobs(&self) -> Vec<Blob> {
        let made = self.built.is_some() || self.image.is_some() && self.stack != Runtime::Wasm;
        let component = (!made).then(|| Blob::Droplet(self.sha256.clone()));
        let files = (self.files.as_ref()).or(self.built.as_ref().map(|built| &built.layer));
        let files = files.map(|files| Blob::Files(files.sha256.clone()));
        component.into_iter().chain(files).collect()
    }
}

/// The stacks a package may be staged as: those the wickets run, the app's pin alone of them.
#[derive(Clone, Debug)]
pub struct Takes {
    /// The app's pin (`cf push -s`, a manifest's `stack:`).
    pub pin: Option<Runtime>,
    /// What the wickets run.
    pub stacks: Vec<Runtime>,
}

impl Takes {
    /// Whether a package that is `what` ("a wasm component"), of the stack `stack`, is staged:
    /// else why not.
    pub fn accept(&self, stack: Runtime, what: &str) -> Result<(), String> {
        let found = format!("the package is {what} (the stack {})", stack.stack());
        if let Some(pin) = self.pin.filter(|pin| *pin != stack) {
            return Err(format!(
                "{found}: the app is pinned to the stack {} (cf push -s, a manifest's stack)",
                pin.stack()
            ));
        }
        if !self.stacks.contains(&stack) {
            return Err(format!(
                "{found}: no wicket runs it, they run {}",
                stacks_named(&self.stacks)
            ));
        }
        Ok(())
    }

    fn may(&self, stack: Runtime) -> bool {
        self.accept(stack, "").is_ok()
    }

    /// Why nothing is staged: the pin is of a stack no wicket runs, or none ever answered.
    fn nothing(&self) -> String {
        match self.pin {
            Some(pin) => format!(
                "the app is pinned to the stack {} (cf push -s, a manifest's stack): no wicket \
                 runs it, they run {}",
                pin.stack(),
                stacks_named(&self.stacks)
            ),
            None => "no wicket has answered yet: the stacks they run are not known".into(),
        }
    }
}

/// Stages the build `build` and records how it went: STAGED with its droplet, or FAILED naming
/// why. What it stages is as the build keeps it ([`Staging`]): its package, the web process
/// `command`, the API's URL `base`, which a build of source is told (`VCAP_APPLICATION`). A
/// zip is staged by the buildpack detecting it, of those its app names ([`detect`]); an image's
/// component is pulled from its registry with the package's credentials, if any, or its rootfs
/// made by a wicket. Taken up after a restart of the cf-api or in another instance's turn, it
/// stages again but a build of source a wicket runs, followed on from the line it got to, built
/// again where that wicket knows it no more. Staged again where a sweep deleted a blob of its
/// droplet before the state named it ([`Store::unswept`]): one of a digest dropped a while ago.
/// This instance's turn over, it ends here, as it is: the next turn's to take up.
pub async fn run(
    store: &Arc<Store>,
    logs: &Logs,
    registry: &Registry,
    controller: &Controller,
    build: &str,
) {
    let begun = |state: &mut State| {
        let app = state.build(build)?.app.clone();
        let staging = state.staging_mut(build)?;
        Some((app, std::mem::replace(&mut staging.taken, true)))
    };
    let Some(Some((app, taken))) = store.lead(begun).await else {
        return;
    };
    if taken {
        let line = "Staging taken up where a restart of the cf-api, or the instance staging it, \
                    left it: its lines before are gone";
        logs.add(&app, "STG", line);
    }
    while !staged_once(store, logs, registry, controller, build).await {}
}

/// [`run`], once; false: to stage again.
async fn staged_once(
    store: &Arc<Store>,
    logs: &Logs,
    registry: &Registry,
    controller: &Controller,
    build: &str,
) -> bool {
    let Some((app, package, staging)) = store.read(|state| {
        let kept = state.build(build)?;
        let package = state
            .package(&kept.package)
            .cloned()
            .ok_or(kept.package.clone());
        Some((kept.app.clone(), package, kept.staging.clone()?))
    }) else {
        return true;
    };
    let app = app.as_str();
    // the build of source it ends with, forgotten on its wicket once the state keeps the end
    let building = || store.read(|state| state.build(build)?.staging.clone()?.building);
    let staged = match &package {
        Ok(package) => staged(store, logs, registry, controller, build, package, staging).await,
        Err(_) => Err("the package is gone".into()),
    };
    let staged = match staged {
        Ok(staged) if !staged.bytes.is_empty() => keep_pulled(store, staged).await,
        staged => staged,
    };
    let at = now();
    match staged {
        Ok(staged) => {
            logs.add(
                app,
                "STG",
                format!(
                    "Staged {} ({} bytes, sha256 {}, the stack {}{}){}{}{}",
                    staged.entry,
                    staged.size,
                    &staged.sha256[..12],
                    staged.stack.stack(),
                    staged.buildpack.map_or(String::new(), |buildpack| format!(
                        ", the buildpack {}",
                        buildpack.name()
                    )),
                    staged
                        .image
                        .as_ref()
                        .map_or(String::new(), |image| format!(" from {image}")),
                    staged.files.as_ref().map_or(String::new(), |files| format!(
                        ", serving the files of a {} byte tar (sha256 {})",
                        files.bytes,
                        &files.sha256[..12]
                    )),
                    staged.built.as_ref().map_or(String::new(), |built| format!(
                        ", the rootfs of a {} byte droplet (sha256 {}) on {}",
                        built.layer.bytes,
                        &built.layer.sha256[..12],
                        built.run_image
                    ))
                ),
            );
            let blobs = staged.blobs();
            let droplet = Droplet {
                guid: guid(),
                app: app.to_string(),
                package: package.map_or_else(|guid| guid, |package| package.guid),
                sha256: staged.sha256,
                file: staged.entry,
                staged_as: Some(staged.stack),
                staged_by: staged.buildpack,
                bytes: staged.size,
                image: staged.image,
                made: Default::default(),
                files: staged.files,
                built: staged.built,
                created_at: at.clone(),
                updated_at: at.clone(),
            };
            logs.read_out(app, READ_OUT).await;
            let building = building();
            loop {
                let rounds = store.unswept().await;
                match kept(store, &blobs).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let line =
                            "A blob of the droplet was swept as it was staged: staging again";
                        logs.add(app, "STG", line);
                        return false;
                    }
                    Err(e) => {
                        eprintln!("BLOBS: a droplet waits for its blobs: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
                let named = |state: &mut State| {
                    if state.sweep.rounds != rounds {
                        return false;
                    }
                    if let Some(staging) = state.builds.iter_mut().find(|kept| kept.guid == build) {
                        staging.state = "STAGED".into();
                        staging.droplet = Some(droplet.guid.clone());
                        staging.staging = None;
                        staging.updated_at = at.clone();
                    }
                    state.droplets.push(droplet.clone());
                    true
                };
                match store.lead(named).await {
                    Some(true) => break,
                    Some(false) => {}
                    // the turn over
                    None => return true,
                }
            }
            forget(controller, building).await;
        }
        // the turn over: the next one's to end
        Err(_) if !store.leads() => {}
        Err(e) => {
            logs.add(app, "STG", format!("Staging failed: {e}"));
            logs.read_out(app, READ_OUT).await;
            let building = building();
            let failed = |state: &mut State| {
                if let Some(staging) = state.builds.iter_mut().find(|kept| kept.guid == build) {
                    staging.state = "FAILED".into();
                    staging.error = Some(format!("StagingError - {e}"));
                    staging.staging = None;
                    staging.updated_at = at.clone();
                }
            };
            if store.lead(failed).await.is_some() {
                forget(controller, building).await;
            }
        }
    }
    true
}

/// Has its wicket forget the build of source `building`, ended: its droplet is the wicket's to
/// keep as any upload.
async fn forget(controller: &Controller, building: Option<Building>) {
    let Some(building) = building else {
        return;
    };
    let named = |wicket: &&Arc<Wicket>| wicket.name == building.wicket;
    if let Some(wicket) = controller.wickets.iter().find(named) {
        let _ = wicket.forget_build(&building.id).await;
    }
}

/// Keeps the component pulled of `staged`.
async fn keep_pulled(store: &Store, staged: Staged) -> Result<Staged, String> {
    let (spool, mut file) = store.spool()?;
    file.write_all(&staged.bytes)
        .map_err(|e| format!("{}: {e}", spool.path().display()))?;
    drop(file);
    let blob = Blob::Droplet(staged.sha256.clone());
    store.keep(&blob, spool, &staged.sha256).await?;
    Ok(staged)
}

/// Whether the `blobs` are all kept.
async fn kept(store: &Store, blobs: &[Blob]) -> Result<bool, String> {
    for blob in blobs {
        if !store.has(blob).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// What the build `build` of `package`, asked for as `staging` has it, staged: see [`run`].
async fn staged(
    store: &Arc<Store>,
    logs: &Logs,
    registry: &Registry,
    controller: &Controller,
    build: &str,
    package: &Package,
    staging: Staging,
) -> Result<Staged, String> {
    let app = package.app.as_str();
    if let Some(building) = &staging.building {
        if let Some(staged) = follow_on(store, logs, controller, build, app, building).await {
            return staged;
        }
        let built_again = |state: &mut State| {
            if let Some(staging) = state.staging_mut(build) {
                staging.building = None;
            }
        };
        store.lead(built_again).await.ok_or(TURN_OVER)?;
    }
    let command = staging.command.as_deref();
    let (pin, named) = store.read(|state| {
        let app = state.app(app);
        let named = app.map(|app| app.buildpacks.clone());
        (app.and_then(|app| app.pin), named.unwrap_or_default())
    });
    let takes = Takes {
        pin,
        stacks: controller.stacks().await,
    };
    let (wasm, rootfs) = (takes.may(Runtime::Wasm), takes.may(Runtime::Firecracker));
    match &package.docker {
        _ if !wasm && !rootfs => Err(takes.nothing()),
        None => {
            let (detecting, ids) = Buildpack::named(&named, controller.cnb.is_some())?;
            // told of those whose stack a wicket runs: one of another is refused, detected
            let told: Vec<Buildpack> = detecting
                .iter()
                .copied()
                .filter(|buildpack| takes.may(buildpack.stack()))
                .collect();
            if !told.is_empty() {
                let detects: Vec<_> = told.iter().map(|kept| kept.detects()).collect();
                let line = format!(
                    "Staging with the buildpack{} {}: the web process command names the file, else {}",
                    if told.len() == 1 { "" } else { "s" },
                    names(&told),
                    detects.join(", else ")
                );
                logs.add(app, "STG", line);
            }
            match unpack(store.clone(), &package.guid, command, detecting, takes).await? {
                Unpacked::Staged(staged) => Ok(*staged),
                Unpacked::Source(source) => {
                    let asked = Asked { ids, command };
                    let base = staging.base.as_str();
                    self::build(store, logs, controller, base, (build, app), source, asked).await
                }
            }
        }
        Some(docker) => image(logs, registry, controller, app, docker, (wasm, rootfs)).await,
    }
}

/// Follows on the build of source `building` of the build `build`, the app `app`'s, which a
/// stop of the cf-api cut short: what it staged; None where it is to be built again (its wicket
/// knows it no more, or is configured no more).
async fn follow_on(
    store: &Arc<Store>,
    logs: &Logs,
    controller: &Controller,
    build: &str,
    app: &str,
    building: &Building,
) -> Option<Result<Staged, String>> {
    let Some(cnb) = &controller.cnb else {
        return Some(Err("no builder is configured (--cnb-builder)".into()));
    };
    let named = |wicket: &&Arc<Wicket>| wicket.name == building.wicket;
    let Some(wicket) = controller.wickets.iter().find(named) else {
        let line = format!(
            "The wicket {} building it is configured no more: built again",
            building.wicket
        );
        logs.add(app, "STG", line);
        return None;
    };
    let line = format!(
        "Following the build on {} from the line {} of its output on",
        wicket.label, building.after
    );
    logs.add(app, "STG", line);
    let staged = built(store, logs, controller, cnb, (build, app), wicket, building).await;
    if staged.is_none() {
        let line = format!("{} knows the build no more: built again", wicket.sentence());
        logs.add(app, "STG", line);
    }
    staged
}

/// Stages the zip of the package `guid`, its droplet made, on a blocking thread: a VM of one
/// vCPU has one worker, which the API and the watch run on. The buildpack is the one of
/// `detecting` detecting it ([`detect`]); what `takes` does not is refused before it is unpacked.
/// Source is packed for its build, not staged yet. The zip is read from this instance's disk
/// (a copy of the bucket's), what is made of it kept from there.
async fn unpack(
    store: Arc<Store>,
    guid: &str,
    command: Option<&str>,
    detecting: Vec<Buildpack>,
    takes: Takes,
) -> Result<Unpacked, String> {
    let package = (store.local(&Blob::Package(guid.to_string())).await)
        .map_err(|e| format!("the package: {e}"))?
        .ok_or("the package is gone")?;
    let (scratch, command) = (store.clone(), command.map(str::to_string));
    let unpacked = tokio::task::spawn_blocking(move || {
        let store = scratch;
        let zip =
            std::fs::File::open(package.path()).map_err(|e| format!("the package is gone: {e}"))?;
        let mut archive = zip::ZipArchive::new(zip).map_err(|e| format!("not a zip: {e}"))?;
        let (buildpack, entry) = detect(&files(&archive), command.as_deref(), &detecting)?;
        takes.accept(buildpack.stack(), buildpack.what())?;
        let (spool, mut file) = store.spool()?;
        match entry {
            None if buildpack == Buildpack::Cnb => {
                let (sha256, bytes) = pack_source(&mut archive, &mut file, MAX_SOURCE_BYTES)?;
                Ok((
                    Unpacked::Source(Source {
                        spool,
                        sha256,
                        bytes,
                    }),
                    None,
                ))
            }
            None => {
                let (staged, files) = stage_site(&mut archive, &mut file)?;
                Ok((Unpacked::Staged(Box::new(staged)), Some((files, spool))))
            }
            Some(entry) => {
                let staged = stage(&mut archive, buildpack, entry, &mut file, MAX_DROPLET_BYTES)?;
                let droplet = Blob::Droplet(staged.sha256.clone());
                Ok((Unpacked::Staged(Box::new(staged)), Some((droplet, spool))))
            }
        }
    });
    let (unpacked, made) = unpacked
        .await
        .map_err(|e| format!("staging: {e}"))
        .flatten()?;
    if let Some((blob, spool)) = made {
        store.keep(&blob, spool, blob.name()).await?;
    }
    Ok(unpacked)
}

/// A zip as [`unpack`] leaves it: staged, or source to build.
enum Unpacked {
    Staged(Box<Staged>),
    Source(Source),
}

/// What the app asks of its build of source: the builder's buildpacks it names, and its web
/// process command.
struct Asked<'a> {
    ids: Vec<String>,
    command: Option<&'a str>,
}

/// The source of a build: the zip's files as a tar, spooled until the wicket has it.
struct Source {
    spool: Spool,
    sha256: String,
    bytes: u64,
}

/// Writes the entries of the zip `archive` to `tar` as a build takes its source: each at its
/// path below the app's directory, with its mode and time, a directory and a link as they
/// are, `max` bytes of files at most. A name out of the root fails it. The tar's digest and
/// size.
pub fn pack_source<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    tar: &mut impl Write,
    max: u64,
) -> Result<(String, u64), String> {
    let mut builder = tar::Builder::new(Hashed::new(Counted::new(tar)));
    let mut left = max;
    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|e| format!("not a zip: {e}"))?;
        let name = file
            .name()
            .map_err(|e| format!("not a zip: {e}"))?
            .to_string();
        let Some(path) = file.enclosed_name() else {
            return Err(format!("'{name}' is no path below the app's directory"));
        };
        if path.as_os_str().is_empty() {
            continue;
        }
        let unpacked = |e: std::io::Error| format!("cannot pack {name}: {e}");
        let mode = file.unix_mode().map(|mode| mode & 0o7777);
        let mut header = tar::Header::new_gnu();
        header.set_mtime(file.last_modified().map_or(0, unix_time));
        header.set_size(0);
        if file.is_dir() {
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(mode.unwrap_or(0o755));
            builder
                .append_data(&mut header, &path, std::io::empty())
                .map_err(unpacked)?;
        } else if file.is_symlink() {
            let mut target = String::new();
            (&mut file)
                .take(MAX_LINK)
                .read_to_string(&mut target)
                .map_err(unpacked)?;
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_mode(0o777);
            builder
                .append_link(&mut header, &path, &target)
                .map_err(unpacked)?;
        } else {
            let size = file.size();
            if size > left {
                return Err(format!("the source is more than {} MiB", max >> 20));
            }
            left -= size;
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(size);
            header.set_mode(mode.unwrap_or(0o644));
            builder
                .append_data(&mut header, &path, &mut file)
                .map_err(unpacked)?;
        }
    }
    let hashed = builder
        .into_inner()
        .map_err(|e| format!("cannot pack the source: {e}"))?;
    let bytes = hashed.out.written;
    Ok((hashed.finish(), bytes))
}

/// Builds `source`, the app `app`'s, with the builder's buildpacks on a Firecracker wicket,
/// the first answering: the source uploaded, the build started with the app's environment
/// (as at a start: the user's and `VCAP_*`, `base` the API's URL), the buildpacks and the
/// command `asked` for, in a VM of the app's memory and disk at least, and followed, each line
/// of its output a line of the staging log as it comes, its droplet fetched and kept
/// ([`Built`]), and its rootfs made by that wicket, as an image's is.
async fn build(
    store: &Arc<Store>,
    logs: &Logs,
    controller: &Controller,
    base: &str,
    (build, app): (&str, &str),
    source: Source,
    asked: Asked<'_>,
) -> Result<Staged, String> {
    let Some(cnb) = &controller.cnb else {
        return Err("no builder is configured (--cnb-builder)".into());
    };
    let (env, memory_mb, disk_mb) = store
        .read(|state| {
            let app = state.app(app)?;
            let env = crate::env::guest(base, state, app);
            Some((env, app.process.memory_mb, app.process.disk_mb))
        })
        .ok_or("the app is gone")?;
    crate::env::check_guest(&env)?;
    let credentials = cnb.credentials_of(&cnb.builder);
    let request = BuildRequest {
        builder: &cnb.builder,
        username: credentials.map(|(username, _)| username),
        password: credentials.map(|(_, password)| password),
        run_image: cnb.run_image.as_deref(),
        source: &source.sha256,
        env: &env,
        egress: &cnb.egress,
        memory_mib: cnb.memory_mib.max(memory_mb),
        disk_mib: cnb.disk_mib.max(disk_mb),
        vcpus: cnb.vcpus,
        timeout_ms: cnb.timeout.as_millis() as u64,
        buildpacks: &asked.ids,
        command: asked.command,
        cache: app,
    };
    let mut started = Err(no_maker());
    for wicket in controller.makers() {
        let tar = async { Reading::of(tokio::fs::File::open(source.spool.path()).await?).await };
        let tar = tar.await.map_err(|e| format!("the source: {e}"))?;
        let uploaded = wicket.upload_layer(&source.sha256, tar).await;
        started = match uploaded {
            Ok(()) => wicket.start_build(&request).await,
            Err(e) => Err(e),
        }
        .map(|id| (wicket, id));
        if !matches!(started, Err(Failure::Unreachable(_))) {
            break;
        }
    }
    drop(source.spool);
    let (wicket, id) = started.map_err(|e| e.to_string())?;
    let line = format!(
        "Building {} bytes of source (sha256 {}) with {}{}{} on {}, {} MiB, {} MiB of disk, {} vCPU{}{}",
        source.bytes,
        &source.sha256[..12],
        cnb.builder,
        match asked.ids.as_slice() {
            [] => String::new(),
            ids => format!(" (its buildpacks {})", ids.join(", ")),
        },
        match &cnb.run_image {
            Some(run_image) => format!(" for the run image {run_image}"),
            None => String::new(),
        },
        wicket.label,
        request.memory_mib,
        request.disk_mib,
        request.vcpus,
        if request.vcpus == 1 { "" } else { "s" },
        match cnb.egress.as_slice() {
            [] => String::new(),
            egress => format!(", reaching {}", egress.join(", ")),
        }
    );
    logs.add(app, "STG", line);
    let building = Building {
        wicket: wicket.name.clone(),
        id,
        after: 0,
    };
    let started = |state: &mut State| {
        if let Some(staging) = state.staging_mut(build) {
            staging.building = Some(building.clone());
        }
    };
    store.lead(started).await.ok_or(TURN_OVER)?;
    built(
        store,
        logs,
        controller,
        cnb,
        (build, app),
        wicket,
        &building,
    )
    .await
    .unwrap_or_else(|| {
        Err(format!(
            "the build is lost: {} knows it no more",
            wicket.label
        ))
    })
}

/// Follows the build of source `building` of the build `build`, the app `app`'s, on `wicket`
/// to its end and stages what it built ([`place`]): None where the wicket knows it no more.
/// The wicket forgets it once the state keeps how the staging ended ([`staged_once`]).
async fn built(
    store: &Arc<Store>,
    logs: &Logs,
    controller: &Controller,
    cnb: &Cnb,
    (build, app): (&str, &str),
    wicket: &Wicket,
    building: &Building,
) -> Option<Result<Staged, String>> {
    match follow(store, logs, wicket, (build, app), building).await {
        Ok(view) => Some(place(store, logs, controller, wicket, app, view, cnb).await),
        Err(Some(e)) => Some(Err(e)),
        Err(None) => None,
    }
}

/// Follows the build of source `building` on `wicket` from its line `after` on to its end,
/// its output the app `app`'s staging log, how far it got kept with the build `build`: what it
/// built, else its error; `Err(None)` where the wicket knows it no more.
async fn follow(
    store: &Store,
    logs: &Logs,
    wicket: &Wicket,
    (build, app): (&str, &str),
    building: &Building,
) -> Result<BuildView, Option<String>> {
    let (mut after, mut failed) = (building.after, 0);
    loop {
        let view = match wicket.follow_build(&building.id, after).await {
            Ok(view) => view,
            Err(Failure::Unreachable(e)) if failed < FOLLOW_TRIES => {
                failed += 1;
                println!(
                    "[{app}] STG: {} did not answer for the build: {e}",
                    wicket.label
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            Err(Failure::Refused { status: 404, .. }) => return Err(None),
            Err(e) => return Err(Some(format!("the build is lost: {e}"))),
        };
        failed = 0;
        for line in &view.lines {
            let stream = match line.stream.as_str() {
                "stderr" => Stream::Err,
                _ => Stream::Out,
            };
            logs.add_as(app, "STG", stream, line.line.as_str());
        }
        if view.next != after {
            after = view.next;
            // written with the next change: lines taken twice at most after a crash
            let followed = |staging: &mut Staging| {
                if let Some(building) = &mut staging.building {
                    building.after = after;
                }
            };
            store.note_staging(build, followed).await;
        }
        match view.state.as_str() {
            "building" => {}
            "built" => return Ok(view),
            _ => {
                return Err(Some(
                    view.error.unwrap_or_else(|| "the build failed".into()),
                ));
            }
        }
    }
}

/// Keeps what the build `view` of `wicket` built, the app `app`'s, with the credentials of
/// `cnb` its run image is pulled with, and has that wicket make its rootfs: the droplet staged.
async fn place(
    store: &Arc<Store>,
    logs: &Logs,
    controller: &Controller,
    wicket: &Wicket,
    app: &str,
    view: BuildView,
    cnb: &Cnb,
) -> Result<Staged, String> {
    let (Some(layer), Some(run_image), Some(launch)) = (view.droplet, view.run_image, view.launch)
    else {
        return Err("the build answered no droplet".into());
    };
    fetch(store, wicket, &layer).await?;
    let credentials = cnb.credentials_of(&run_image);
    let built = Built {
        layer: Files {
            sha256: layer.sha256,
            bytes: layer.bytes,
        },
        launch,
        username: credentials.map(|(username, _)| username.to_string()),
        password: credentials.map(|(_, password)| password.to_string()),
        run_image,
    };
    let line = format!(
        "{} is making a rootfs of the droplet ({} bytes, sha256 {}) on {}",
        wicket.sentence(),
        built.layer.bytes,
        &built.layer.sha256[..12],
        built.run_image
    );
    logs.add(app, "STG", line);
    match controller.make_built(wicket, &built).await {
        Ok(made) => Ok(Staged {
            entry: made.name,
            bytes: Vec::new(),
            size: made.bytes,
            sha256: made.sha256,
            image: None,
            files: None,
            built: Some(built),
            stack: Runtime::Firecracker,
            buildpack: Some(Buildpack::Cnb),
        }),
        Err(e) => {
            // kept for nothing, unless a droplet of the same build names it
            let layer = &built.layer.sha256;
            let named = |kept: &Droplet| {
                (kept.built.as_ref()).is_some_and(|kept| kept.layer.sha256 == *layer)
            };
            if !store.read(|state| state.droplets.iter().any(named)) {
                store.delete(&[Blob::Files(layer.clone())]).await;
            }
            Err(format!("{}: {e}", built.run_image))
        }
    }
}

/// Fetches the droplet `layer` from `wicket`, which built it, a piece at a time, and keeps it
/// under its digest, checked.
async fn fetch(store: &Store, wicket: &Wicket, layer: &LayerRef) -> Result<(), String> {
    use tokio::io::AsyncWriteExt as _;
    if layer.sha256.len() != 64 || !layer.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!("the droplet's digest is none: {}", layer.sha256));
    }
    if layer.bytes > MAX_DROPLET_BYTES {
        return Err(format!(
            "the droplet is more than {} MiB",
            MAX_DROPLET_BYTES >> 20
        ));
    }
    let (spool, file) = store.spool()?;
    let mut file = tokio::fs::File::from_std(file);
    let unwritten = |e: std::io::Error| format!("the droplet: {e}");
    let mut hasher = Sha256::new();
    let mut at = 0;
    while at < layer.bytes {
        let (bytes, piece) = wicket
            .layer_piece(&layer.sha256, at)
            .await
            .map_err(|e| format!("the droplet: {e}"))?;
        if bytes != layer.bytes || piece.is_empty() || at + piece.len() as u64 > bytes {
            return Err(format!(
                "the droplet is not the {} bytes the build answered",
                layer.bytes
            ));
        }
        hasher.update(&piece);
        file.write_all(&piece).await.map_err(unwritten)?;
        at += piece.len() as u64;
    }
    file.flush().await.map_err(unwritten)?;
    drop(file);
    if hex(&hasher.finalize()) != layer.sha256 {
        return Err("the droplet is not of the digest the build answered".into());
    }
    let blob = Blob::Files(layer.sha256.clone());
    store.keep(&blob, spool, &layer.sha256).await
}

/// The files of `archive`, by name.
fn files<R: Read + Seek>(archive: &zip::ZipArchive<R>) -> Vec<String> {
    let names = archive.file_names().filter_map(|name| name.ok());
    names
        .filter(|name| !name.ends_with('/'))
        .map(String::from)
        .collect()
}

/// Stages the static site `archive`: its files packed to `tar`, the blob to keep them as,
/// sluice-wicket-site its component.
fn stage_site<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    tar: &mut impl Write,
) -> Result<(Staged, Blob), String> {
    let packed = pack_site(archive, tar, MAX_SITE_BYTES)?;
    let files = Blob::Files(packed.sha256.clone());
    let staged = Staged {
        entry: SITE_FILE.into(),
        bytes: SITE.to_vec(),
        size: SITE.len() as u64,
        sha256: sha256(SITE),
        image: None,
        files: Some(Files {
            sha256: packed.sha256,
            bytes: packed.bytes,
        }),
        built: None,
        stack: Runtime::Wasm,
        buildpack: Some(Buildpack::Site),
    };
    Ok((staged, files))
}

/// A static site's files as [`pack_site`] packed them.
#[derive(Debug)]
pub struct Packed {
    /// The tar's digest and size.
    pub sha256: String,
    pub bytes: u64,
    /// The files packed, and those left out (dot files, the manifest).
    pub files: usize,
    pub left_out: Vec<String>,
}

/// Writes the files of the zip `archive` to `tar` as the wicket takes them: regular
/// files, sorted by name (the same files make the same digest, however zipped), each with
/// its time (what sluice-wicket-site's `ETag` is made of), `max` bytes of them at most. Dot
/// files and directories (but `.well-known/` at the root) and the manifest at the root
/// are left out; a link fails it, as does a name out of the root.
pub fn pack_site<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    tar: &mut impl Write,
    max: u64,
) -> Result<Packed, String> {
    let mut entries: Vec<(String, usize)> = Vec::new();
    let mut left_out = Vec::new();
    for index in 0..archive.len() {
        let file = archive
            .by_index_raw(index)
            .map_err(|e| format!("not a zip: {e}"))?;
        if file.is_dir() {
            continue;
        }
        let name = file
            .name()
            .map_err(|e| format!("not a zip: {e}"))?
            .to_string();
        if file.is_symlink() {
            return Err(format!(
                "'{name}' is a symbolic link: a site holds files only"
            ));
        }
        let Some(path) = file.enclosed_name() else {
            return Err(format!("'{name}' is no path below the site's root"));
        };
        let path: Vec<String> = path
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect();
        let path = path.join("/");
        if left_out_of_site(&path) {
            left_out.push(path);
            continue;
        }
        entries.push((path, index));
    }
    if entries.len() > MAX_SITE_FILES {
        return Err(format!("more than {MAX_SITE_FILES} files"));
    }
    entries.sort();
    if let Some(twice) = entries.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(format!("'{}' twice", twice[0].0));
    }
    let mut builder = tar::Builder::new(Hashed::new(Counted::new(tar)));
    let mut left = max;
    for (path, index) in &entries {
        let mut file = archive
            .by_index(*index)
            .map_err(|e| format!("cannot read {path}: {e}"))?;
        let size = file.size();
        if size > left {
            return Err(format!("the files are more than {} MiB", max >> 20));
        }
        left -= size;
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(size);
        header.set_mode(0o644);
        header.set_mtime(file.last_modified().map_or(0, unix_time));
        builder
            .append_data(&mut header, path, &mut file)
            .map_err(|e| format!("cannot pack {path}: {e}"))?;
    }
    let hashed = builder
        .into_inner()
        .map_err(|e| format!("cannot pack the site: {e}"))?;
    let bytes = hashed.out.written;
    Ok(Packed {
        sha256: hashed.finish(),
        bytes,
        files: entries.len(),
        left_out,
    })
}

/// Whether the site's file `path` is left out: a dot file or directory (but
/// `.well-known/` at the root), what `cf push` reads at the root (the manifest).
fn left_out_of_site(path: &str) -> bool {
    let mut segments = path.split('/');
    let first = segments.next().unwrap_or_default();
    let manifest = matches!(path, "manifest.yml" | "manifest.yaml");
    let hidden = (first.starts_with('.') && first != ".well-known")
        || segments.any(|segment| segment.starts_with('.'));
    manifest || hidden
}

/// A zip time (local, as MS-DOS has it: taken as UTC) in seconds since the epoch.
fn unix_time(time: zip::DateTime) -> u64 {
    // days from the civil date (Howard Hinnant's days_from_civil)
    let (year, month, day) = (
        i64::from(time.year()),
        i64::from(time.month()),
        i64::from(time.day()),
    );
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let of_era = year - era * 400;
    let of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let of_era_days = of_era * 365 + of_era / 4 - of_era / 100 + of_year;
    let days = era * 146_097 + of_era_days - 719_468;
    let seconds = days * 86_400
        + i64::from(time.hour()) * 3600
        + i64::from(time.minute()) * 60
        + i64::from(time.second());
    u64::try_from(seconds).unwrap_or(0)
}

/// A writer counting what passes it.
pub struct Counted<W> {
    out: W,
    written: u64,
}

impl<W: Write> Counted<W> {
    fn new(out: W) -> Counted<W> {
        Counted { out, written: 0 }
    }
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.out.write(bytes)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

/// Stages `docker`'s image, with the package's credentials, if any: its component pulled from
/// its registry (`wasm`: a wasm wicket takes one), else its rootfs made by a Firecracker wicket
/// (`rootfs`: one takes it). Where both do, the image's manifest tells which it is.
async fn image(
    logs: &Logs,
    registry: &Registry,
    controller: &Controller,
    app: &str,
    docker: &DockerImage,
    (wasm, rootfs): (bool, bool),
) -> Result<Staged, String> {
    let credentials = docker.credentials();
    if wasm {
        logs.add(
            app,
            "STG",
            format!(
                "Staging with the wasm lifecycle: pulling {}{}",
                docker.image,
                if credentials.is_some() {
                    " with the package's credentials"
                } else {
                    ""
                }
            ),
        );
        let pulled = registry
            .pull(&docker.image, credentials, rootfs)
            .await
            .map_err(|e| format!("{}: {e}", docker.image))?;
        if let Some(pulled) = pulled {
            return Ok(Staged {
                entry: pulled.file,
                size: pulled.bytes.len() as u64,
                bytes: pulled.bytes,
                sha256: pulled.sha256,
                image: Some(pulled.image),
                files: None,
                built: None,
                stack: Runtime::Wasm,
                buildpack: None,
            });
        }
        logs.add(
            app,
            "STG",
            "The image holds no wasm component: a rootfs is made of it",
        );
    }
    logs.add(
        app,
        "STG",
        format!("The wicket is making a rootfs of {}", docker.image),
    );
    controller
        .make_image(&docker.image, credentials)
        .await
        .map(|made| Staged {
            entry: made.name,
            bytes: Vec::new(),
            size: made.bytes,
            sha256: made.sha256,
            image: Some(made.image),
            files: None,
            built: None,
            stack: Runtime::Firecracker,
            buildpack: None,
        })
        .map_err(|e| format!("{}: {e}", docker.image))
}

/// Stages the file `entry` of the zip `archive`, which `buildpack` detected: the component, or
/// the rootfs, checked to be one and unpacked to `droplet` as it comes, `max` bytes at most, not
/// a process's memory.
pub fn stage<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    buildpack: Buildpack,
    entry: String,
    droplet: &mut impl Write,
    max: u64,
) -> Result<Staged, String> {
    let unread = |e: std::io::Error| format!("cannot read {entry}: {e}");
    let mut file = archive
        .by_name(&entry)
        .map_err(|e| format!("cannot read {entry}: {e}"))?;
    let mut head = Vec::with_capacity(HEAD);
    (&mut file)
        .take(HEAD as u64)
        .read_to_end(&mut head)
        .map_err(unread)?;
    match buildpack {
        Buildpack::Rootfs => check_rootfs(&entry, &head)?,
        Buildpack::Wasm => check_component(&entry, &head)?,
        Buildpack::Site | Buildpack::Cnb => {
            return Err(format!(
                "{entry} is no file {} is staged of",
                buildpack.what()
            ));
        }
    }
    let unwritten = |e: std::io::Error| format!("writing {entry}: {e}");
    let mut hashed = Hashed::new(droplet);
    hashed.write_all(&head).map_err(unwritten)?;
    let mut size = head.len() as u64;
    let mut chunk = vec![0; 1 << 20];
    loop {
        let read = file.read(&mut chunk).map_err(unread)?;
        if read == 0 {
            break;
        }
        size += read as u64;
        if size > max {
            return Err(format!("{entry} unpacks to more than {} MiB", max >> 20));
        }
        hashed.write_all(&chunk[..read]).map_err(unwritten)?;
    }
    drop(file);
    Ok(Staged {
        sha256: hashed.finish(),
        size,
        entry,
        bytes: Vec::new(),
        image: None,
        files: None,
        built: None,
        stack: buildpack.stack(),
        buildpack: Some(buildpack),
    })
}

/// A writer keeping the digest of what passes it.
pub struct Hashed<W> {
    out: W,
    hasher: Sha256,
}

impl<W: Write> Hashed<W> {
    pub fn new(out: W) -> Hashed<W> {
        Hashed {
            out,
            hasher: Sha256::new(),
        }
    }

    /// The digest, in hex.
    pub fn finish(self) -> String {
        hex(&self.hasher.finalize())
    }
}

impl<W: Write> Write for Hashed<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.out.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

/// The SHA-256 of `bytes`, in hex.
pub fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// `digest` in hex.
fn hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A wasm component: the magic, then the version and the layer (1; a core
/// module is layer 0).
pub(crate) fn check_component(entry: &str, bytes: &[u8]) -> Result<(), String> {
    match bytes {
        [0, b'a', b's', b'm', _, _, 1, 0, ..] => Ok(()),
        [0, b'a', b's', b'm', 1, 0, 0, 0, ..] => Err(format!(
            "{entry} is a core wasm module, not a component (a wasi:http 0.3 handler)"
        )),
        [0, b'a', b's', b'm', ..] => Err(format!("{entry} is wasm of an unknown layer")),
        _ => Err(format!("{entry} is not wasm")),
    }
}

/// An ext2/3/4 image: its superblock's magic.
fn check_rootfs(entry: &str, bytes: &[u8]) -> Result<(), String> {
    match bytes.get(MAGIC_AT..MAGIC_AT + MAGIC.len()) {
        Some(magic) if magic == MAGIC => Ok(()),
        _ => Err(format!("{entry} is not an ext4 image")),
    }
}

fn listed<S: AsRef<str>>(names: &[S]) -> String {
    if names.is_empty() {
        return "nothing".into();
    }
    names
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Cursor;

    use super::*;

    const COMPONENT: &[u8] = b"\0asm\x0d\x00\x01\x00rest";
    const MODULE: &[u8] = b"\0asm\x01\x00\x00\x00rest";

    fn zipped(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let time = zip::DateTime::from_date_and_time(2024, 5, 6, 7, 8, 10).unwrap();
        let options = zip::write::SimpleFileOptions::default().last_modified_time(time);
        for (name, bytes) in entries {
            zip.start_file(*name, options).unwrap();
            zip.write_all(bytes).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    /// `zip` staged as [`unpack`] does, by the buildpack of `detecting` detecting it, where
    /// `takes` does it; the droplet in memory, within `max` bytes. A site is told, not packed.
    fn staged_by(
        zip: &[u8],
        command: Option<&str>,
        max: u64,
        detecting: &[Buildpack],
        takes: Option<&Takes>,
    ) -> Result<(Staged, Vec<u8>), String> {
        let mut archive =
            zip::ZipArchive::new(Cursor::new(zip)).map_err(|e| format!("not a zip: {e}"))?;
        let (buildpack, entry) = detect(&files(&archive), command, detecting)?;
        if let Some(takes) = takes {
            takes.accept(buildpack.stack(), buildpack.what())?;
        }
        let entry = entry.ok_or("a static site")?;
        let mut droplet = Vec::new();
        stage(&mut archive, buildpack, entry, &mut droplet, max).map(|staged| (staged, droplet))
    }

    fn staged(zip: &[u8], command: Option<&str>, max: u64) -> Result<(Staged, Vec<u8>), String> {
        staged_by(zip, command, max, &Buildpack::all(false), None)
    }

    #[test]
    fn picks_the_component_or_packs_a_site() {
        let (hello, droplet) = staged(
            &zipped(&[("hello.wasm", COMPONENT), ("README", b"x")]),
            None,
            1 << 20,
        )
        .unwrap();
        assert_eq!(
            (hello.entry.as_str(), hello.stack, hello.buildpack),
            ("hello.wasm", Runtime::Wasm, Some(Buildpack::Wasm))
        );
        assert_eq!(droplet, COMPONENT);
        assert_eq!(hello.sha256, sha256(COMPONENT));
        assert_eq!(hello.size, COMPONENT.len() as u64);
        let two = zipped(&[("a.wasm", COMPONENT), ("dist/b.wasm", COMPONENT)]);
        let picked = |zip: &[u8], command| staged(zip, command, 1 << 20).map(|(staged, _)| staged);
        assert!(
            picked(&two, None)
                .unwrap_err()
                .contains("2 components (a.wasm, dist/b.wasm)")
        );
        assert_eq!(picked(&two, Some("b.wasm")).unwrap().entry, "dist/b.wasm");
        assert_eq!(
            picked(&two, Some("dist/b.wasm")).unwrap().entry,
            "dist/b.wasm"
        );
        assert!(
            picked(&two, Some("c.wasm"))
                .unwrap_err()
                .contains("names 'c.wasm'")
        );
        assert!(
            picked(&zipped(&[("docs/index.html", b"<p>")]), None)
                .unwrap_err()
                .contains(
                    "no .wasm component, no .ext4 rootfs, no index.html at its root (found: \
                     docs/index.html)"
                )
        );
        let mut rootfs = vec![0; 2048];
        rootfs[MAGIC_AT..MAGIC_AT + 2].copy_from_slice(&MAGIC);
        let rootfs_zip = zipped(&[("app/r.ext4", &rootfs)]);
        let (picked_rootfs, unpacked) = staged(&rootfs_zip, None, 1 << 20).unwrap();
        assert_eq!(picked_rootfs.entry, "app/r.ext4");
        assert_eq!(picked_rootfs.stack, Runtime::Firecracker);
        assert_eq!(picked_rootfs.buildpack, Some(Buildpack::Rootfs));
        assert_eq!(unpacked, rootfs);
        // what is pushed decides the stack, of those the wickets run, the app's pin alone of
        // them: another is refused before it is unpacked, naming what it is and theirs
        use Runtime::{Firecracker, Wasm};
        let hello_zip = zipped(&[("hello.wasm", COMPONENT)]);
        for (pin, stacks, zip, refused) in [
            (None, &[Wasm, Firecracker][..], &rootfs_zip, None),
            (None, &[Wasm, Firecracker], &hello_zip, None),
            (Some(Firecracker), &[Wasm, Firecracker], &rootfs_zip, None),
            (
                None,
                &[Wasm],
                &rootfs_zip,
                Some("is a rootfs (the stack firecracker): no wicket runs it, they run wasip3"),
            ),
            (
                Some(Firecracker),
                &[Wasm, Firecracker],
                &hello_zip,
                Some(
                    "is a wasm component (the stack wasip3): the app is pinned to the stack firecracker",
                ),
            ),
            (
                Some(Firecracker),
                &[Wasm],
                &rootfs_zip,
                Some("no wicket runs it, they run wasip3"),
            ),
            (None, &[], &hello_zip, Some("they run none known yet")),
        ] {
            let takes = Takes {
                pin,
                stacks: stacks.to_vec(),
            };
            let staged = staged_by(zip, None, 1 << 20, &Buildpack::all(false), Some(&takes));
            match refused {
                None => assert!(staged.is_ok(), "{takes:?}: {staged:?}"),
                Some(why) => {
                    let refused = staged.unwrap_err();
                    assert!(refused.contains(why), "{takes:?}: {refused}");
                }
            }
        }
        let pinned = Takes {
            pin: Some(Firecracker),
            stacks: vec![Wasm],
        };
        assert!(pinned.nothing().contains("pinned to the stack firecracker"));
        // what unpacks past the bound, however small its zip
        assert!(
            staged(&rootfs_zip, None, 2047)
                .unwrap_err()
                .contains("app/r.ext4 unpacks to more than")
        );
        assert!(
            picked(&zipped(&[("r.ext4", b"not one")]), None)
                .unwrap_err()
                .contains("not an ext4 image")
        );
        assert!(
            picked(&zipped(&[("m.wasm", MODULE)]), None)
                .unwrap_err()
                .contains("core wasm module")
        );
        assert!(
            picked(&zipped(&[("t.wasm", b"text")]), None)
                .unwrap_err()
                .contains("not wasm")
        );
        assert!(
            picked(b"not a zip", None)
                .unwrap_err()
                .contains("not a zip")
        );

        // a static site: no component, an index.html at the root; its files packed sorted,
        // with their times, but the dot files and the manifest
        let archive = |zip: Vec<u8>| zip::ZipArchive::new(Cursor::new(zip)).unwrap();
        let entries = [
            ("index.html", &b"<p>"[..]),
            ("css/a.css", b"p{}"),
            (".git/config", b"[core]"),
            (".env", b"SECRET=x"),
            (".well-known/security.txt", b"Contact: me"),
            ("docs/.secret", b"x"),
            ("manifest.yml", b"env: {SECRET: x}"),
        ];
        let detected_with = |cnb: bool, entries: &[(&str, &[u8])], command, named: &[&str]| {
            let named: Vec<String> = named.iter().map(|name| name.to_string()).collect();
            let (detecting, _) = Buildpack::named(&named, cnb)?;
            detect(&files(&archive(zipped(entries))), command, &detecting)
        };
        let detected = |entries: &[(&str, &[u8])], command, named: &[&str]| {
            detected_with(false, entries, command, named)
        };
        assert_eq!(detected(&entries, None, &[]), Ok((Buildpack::Site, None)));
        // the buildpacks the app names are the only ones detecting: a site holding a .wasm is
        // one by its name, a component else
        let both = [("index.html", &b"<p>"[..]), ("a.wasm", COMPONENT)];
        let component = Ok((Buildpack::Wasm, Some("a.wasm".to_string())));
        assert_eq!(detected(&both, None, &[]), component);
        assert_eq!(detected(&both, None, &["site", "wasm"]), component);
        assert_eq!(
            detected(&both, None, &["site"]),
            Ok((Buildpack::Site, None))
        );
        for (command, named, refused) in [
            (None, &["rootfs"][..], "the package holds no .ext4 rootfs"),
            (
                Some("a.wasm"),
                &["site"],
                "names 'a.wasm', a wasm component: the app's buildpacks are site",
            ),
            (
                None,
                &["staticfile_buildpack"],
                "no buildpack 'staticfile_buildpack' (cf push -b, a manifest's buildpacks): \
                 they are wasm, rootfs, site",
            ),
        ] {
            let why = detected(&both, command, named).unwrap_err();
            assert!(why.contains(refused), "{named:?}: {why}");
        }
        // with a builder, source is what nothing else detects: last, or by its name; its
        // files a tar at their paths, modes and links kept
        let source = [("pom.xml", &b"<project/>"[..]), ("src/Main.java", b"class")];
        assert!(
            detected(&source, None, &[])
                .unwrap_err()
                .contains("no index.html at its root (found: pom.xml, src/Main.java)")
        );
        assert!(
            detected(&source, None, &["cnb"])
                .unwrap_err()
                .contains("no buildpack 'cnb'")
        );
        assert_eq!(
            detected_with(true, &source, None, &[]),
            Ok((Buildpack::Cnb, None))
        );
        assert_eq!(detected_with(true, &both, None, &[]), component);
        assert_eq!(
            detected_with(true, &entries, None, &[]),
            Ok((Buildpack::Site, None))
        );
        assert_eq!(
            detected_with(true, &entries, None, &["cnb"]),
            Ok((Buildpack::Cnb, None))
        );
        // a command naming no component is what source runs; one naming a component stages it
        let command = Some("java -jar target/app.jar");
        assert_eq!(
            detected_with(true, &source, command, &[]),
            Ok((Buildpack::Cnb, None))
        );
        assert_eq!(detected_with(true, &both, Some("a.wasm"), &[]), component);
        assert!(
            detected_with(false, &source, command, &[])
                .unwrap_err()
                .contains("which the package does not hold")
        );
        // a builder's buildpack by its id: source alone, the ids the build's order
        let named = |names: &[&str], cnb| {
            let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
            Buildpack::named(&names, cnb)
        };
        let java = "paketo-buildpacks/java@22.8.0".to_string();
        assert_eq!(
            named(&["paketo-buildpacks/java@22.8.0"], true),
            Ok((vec![Buildpack::Cnb], vec![java.clone()]))
        );
        // and one by its image
        let rust = "docker://paketocommunity/rust:0.92.2".to_string();
        assert_eq!(
            named(&[&java, &rust], true),
            Ok((vec![Buildpack::Cnb], vec![java.clone(), rust]))
        );
        assert_eq!(
            named(&["cnb", "paketo-buildpacks/java@22.8.0"], true),
            Ok((vec![Buildpack::Cnb], vec![java]))
        );
        for (names, cnb) in [
            (&["paketo-buildpacks/java"][..], false),
            (&["https://github.com/cloudfoundry/java-buildpack"], true),
            (&["docker://No Image"], true),
            (&["docker://paketocommunity/rust"], false),
        ] {
            assert!(
                named(names, cnb)
                    .unwrap_err()
                    .contains("there is no buildpack"),
                "{names:?}"
            );
        }
        assert_eq!(Buildpack::Cnb.stack(), Firecracker);
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        zip.add_directory("bin/", options).unwrap();
        zip.start_file("bin/run", options.unix_permissions(0o755))
            .unwrap();
        zip.write_all(b"#!/bin/sh").unwrap();
        zip.add_symlink("latest", "bin/run", options).unwrap();
        let zip = zip.finish().unwrap().into_inner();
        let mut tar = Vec::new();
        let (digest, bytes) = pack_source(&mut archive(zip.clone()), &mut tar, 1 << 20).unwrap();
        assert_eq!((digest, bytes), (sha256(&tar), tar.len() as u64));
        let mut unpacked = tar::Archive::new(&tar[..]);
        let packed: Vec<(String, u32, Option<String>)> = unpacked
            .entries()
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let link = entry.link_name().unwrap();
                (
                    entry.path().unwrap().to_string_lossy().into_owned(),
                    entry.header().mode().unwrap(),
                    link.map(|link| link.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert_eq!(
            packed,
            [
                ("bin".to_string(), 0o755, None),
                ("bin/run".into(), 0o755, None),
                ("latest".into(), 0o777, Some("bin/run".into()))
            ]
        );
        assert!(
            pack_source(&mut archive(zip), &mut Vec::new(), 4)
                .unwrap_err()
                .contains("the source is more than")
        );

        let pack = |entries: &[(&str, &[u8])], max| {
            let mut tar = Vec::new();
            pack_site(&mut archive(zipped(entries)), &mut tar, max).map(|packed| (packed, tar))
        };
        let (packed, tar) = pack(&entries, 1 << 20).unwrap();
        assert_eq!(
            packed.left_out,
            [".git/config", ".env", "docs/.secret", "manifest.yml"]
        );
        assert_eq!((packed.files, packed.bytes), (3, tar.len() as u64));
        assert_eq!(packed.sha256, sha256(&tar));
        let mut unpacked = tar::Archive::new(&tar[..]);
        let files: Vec<(String, u64)> = unpacked
            .entries()
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let name = entry.path().unwrap().to_string_lossy().into_owned();
                (name, entry.header().mtime().unwrap())
            })
            .collect();
        let at = 1_714_979_290;
        assert_eq!(
            files,
            [
                (".well-known/security.txt".to_string(), at),
                ("css/a.css".into(), at),
                ("index.html".into(), at)
            ]
        );
        let mut reversed = entries;
        reversed.reverse();
        assert_eq!(
            pack(&reversed, 1 << 20).unwrap().0.sha256,
            packed.sha256,
            "however zipped"
        );
        assert!(
            pack(&entries, 4)
                .unwrap_err()
                .contains("the files are more than")
        );
        let mut linked = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        linked.start_file("index.html", options).unwrap();
        linked
            .add_symlink("passwd", "/etc/passwd", options)
            .unwrap();
        let linked = linked.finish().unwrap().into_inner();
        let refused = pack_site(&mut archive(linked), &mut Vec::new(), 1 << 20).unwrap_err();
        assert!(refused.contains("'passwd' is a symbolic link"), "{refused}");
    }

    /// A Firecracker wicket's builds as staging uses them: any build but `lost` (one before a
    /// restart of it) ended, with the lines `one` and `two` and the droplet [`LAYER`], whose
    /// rootfs it makes ([`rootfs_sha`]), or while `held` building still, at its line `one`; one
    /// it starts is `new`. What it was asked, counted.
    #[derive(Default)]
    pub(crate) struct Builder {
        pub(crate) started: std::sync::atomic::AtomicUsize,
        uploaded: std::sync::atomic::AtomicUsize,
        pub(crate) forgotten: std::sync::Mutex<Vec<String>>,
        pub(crate) held: std::sync::atomic::AtomicBool,
    }

    const LAYER: &[u8] = b"what buildpacks built";

    /// The digest of the rootfs the builder makes.
    pub(crate) fn rootfs_sha() -> String {
        "cd".repeat(32)
    }

    impl Builder {
        /// Serves it on a port of its own: its URL.
        async fn start() -> (Arc<Builder>, String) {
            let builder = Arc::new(Builder::default());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let serving = builder.clone();
            tokio::spawn(sluice_wicket_server::accept::run(
                listener,
                16,
                "builder",
                move |stream, _, slot| {
                    let builder = serving.clone();
                    tokio::spawn(sluice_wicket_server::http::serve(stream, slot, move |r| {
                        builder.clone().answer(r)
                    }));
                },
            ));
            (builder, url)
        }

        pub(crate) async fn answer(
            self: Arc<Self>,
            request: hyper::Request<sluice_wicket_server::http::Body>,
        ) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
            use http_body_util::BodyExt as _;
            use serde_json::json;
            use std::sync::atomic::Ordering;
            let (head, body) = request.into_parts();
            body.collect().await.unwrap();
            let path = head.uri.path().to_string();
            let at = |key: &str| {
                let query = head.uri.query().unwrap_or_default();
                let value = query.strip_prefix(key).and_then(|at| at.strip_prefix('='));
                value.map_or(0, |at| at.parse::<usize>().unwrap())
            };
            let run_image = format!("registry.test/run@sha256:{}", "ab".repeat(32));
            let mut answer = hyper::Response::new(http_body_util::Full::default());
            let segments: Vec<&str> = path.split('/').collect();
            let answered = match (&head.method, &segments[..]) {
                (&http::Method::PUT, ["", "api", "layers", _]) => {
                    self.uploaded.fetch_add(1, Ordering::Relaxed);
                    json!({})
                }
                (&http::Method::GET, ["", "api", "layers", _]) => {
                    let bytes = LAYER.len().to_string();
                    let headers = answer.headers_mut();
                    headers.insert("sluice-layer-bytes", bytes.parse().unwrap());
                    *answer.body_mut() = bytes::Bytes::from_static(&LAYER[at("at")..]).into();
                    return answer;
                }
                (&http::Method::POST, ["", "api", "builds"]) => {
                    self.started.fetch_add(1, Ordering::Relaxed);
                    json!({"id": "new"})
                }
                (&http::Method::GET, ["", "api", "builds", "lost"]) => {
                    *answer.status_mut() = http::StatusCode::NOT_FOUND;
                    json!({"error": "no build lost"})
                }
                (&http::Method::GET, ["", "api", "builds", _]) => {
                    let lines =
                        ["one", "two"].map(|line| json!({"stream": "stdout", "line": line}));
                    let held = self.held.load(Ordering::Relaxed);
                    if held {
                        // as the wicket holds a request with no line to answer
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    let lines = &lines[..if held { 1 } else { 2 }];
                    json!({
                        "state": if held { "building" } else { "built" },
                        "next": lines.len(),
                        "lines": lines[at("after").min(lines.len())..],
                        "droplet": {"sha256": sha256(LAYER), "bytes": LAYER.len()},
                        "run-image": run_image,
                        "launch": {"processes": []},
                    })
                }
                (&http::Method::DELETE, ["", "api", "builds", id]) => {
                    self.forgotten.lock().unwrap().push(id.to_string());
                    json!({})
                }
                (&http::Method::PUT, ["", "api", "images"]) => json!({
                    "image": run_image,
                    "name": "rootfs.ext4",
                    "sha256": rootfs_sha(),
                    "bytes": 4096,
                }),
                (method, path) => panic!("{method} {path:?}"),
            };
            *answer.body_mut() = answered.to_string().into();
            answer
        }
    }

    /// Builds a stop of the cf-api cut short, the state opened again, each end STAGED with the
    /// droplet it would have staged: a zip staged again, a build of source a wicket runs followed
    /// on from the line it got to, one the wicket knows no more built again. And a droplet
    /// whose blob a sweep deletes as it is staged is staged again.
    #[tokio::test]
    async fn a_staging_cut_short_is_taken_up() {
        use std::sync::atomic::Ordering;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = std::env::temp_dir().join(format!("cf-api-take-up-{}", guid()));
        let (builder, url) = Builder::start().await;
        let names = ["w".to_string(), "v".to_string()];
        let runs = [
            ("w".to_string(), Runtime::Firecracker),
            ("v".to_string(), Runtime::Wasm),
        ];
        let open = async || {
            let localhost = ["localhost".to_string()];
            let runs = runs.clone().into();
            let store = Store::open(&dir, "o", "s", &localhost, &names, &runs, None, None);
            store.await.unwrap()
        };
        let building = |id: &str, after| crate::store::Building {
            wicket: "w".into(),
            id: id.into(),
            after,
        };
        let source = zipped(&[("sample.app", b"an app")]);
        let cut = [
            ("zip", zipped(&[("hello.wasm", COMPONENT)]), None),
            ("on", source.clone(), Some(building("known", 1))),
            ("lost", source, Some(building("lost", 2))),
        ];
        let store = open().await;
        let mut builds = Vec::new();
        for (name, zip, building) in cut {
            let package = guid();
            let (spool, mut file) = store.spool().unwrap();
            file.write_all(&zip).unwrap();
            let blob = Blob::Package(package.clone());
            store.keep(&blob, spool, &sha256(&zip)).await.unwrap();
            builds.push(
                store
                    .update(|state| {
                        let space = state.spaces[0].guid.clone();
                        let app = state.create_app(name, &space).guid.clone();
                        state.packages.push(Package {
                            guid: package.clone(),
                            app: app.clone(),
                            state: "READY".into(),
                            sha256: None,
                            docker: None,
                            created_at: now(),
                            updated_at: now(),
                        });
                        let build = crate::store::Build {
                            guid: guid(),
                            app: app.clone(),
                            package: package.clone(),
                            state: "STAGING".into(),
                            lifecycle: Default::default(),
                            error: None,
                            droplet: None,
                            staging: Some(Staging {
                                base: "http://api.test".into(),
                                command: None,
                                building: building.clone(),
                                taken: false,
                            }),
                            created_at: now(),
                            updated_at: now(),
                        };
                        state.builds.push(build.clone());
                        (build.guid, app)
                    })
                    .await,
            );
        }
        drop(store);

        let store = Arc::new(open().await);
        let logs = Logs::default();
        let cnb = Cnb {
            builder: "registry.test/builder:1".into(),
            run_image: None,
            credentials: None,
            egress: Vec::new(),
            memory_mib: Cnb::MEMORY_MIB,
            disk_mib: Cnb::DISK_MIB,
            vcpus: Cnb::VCPUS,
            timeout: Cnb::TIMEOUT,
        };
        let controller = Controller::new(
            store.clone(),
            vec![
                Arc::new(Wicket::new("w", 2, &url, None, crate::OWNER)),
                Arc::new(Wicket::new(
                    "v",
                    2,
                    "http://127.0.0.1:1",
                    None,
                    crate::OWNER,
                )),
            ],
            Arc::new(Logs::default()),
            std::time::Duration::ZERO,
            Some(cnb),
            crate::osb::Client::new().unwrap(),
            Registry::new().unwrap(),
        );
        let registry = Registry::new().unwrap();
        let runs =
            (builds.iter()).map(|(build, _)| run(&store, &logs, &registry, &controller, build));
        futures_util::future::join_all(runs).await;

        let staged: Vec<(String, String)> = store.read(|state| {
            let staged = builds.iter().map(|(build, _)| {
                let build = state.build(build).unwrap();
                assert!(build.staging.is_none(), "{build:?}");
                let droplet = build
                    .droplet
                    .as_ref()
                    .and_then(|droplet| state.droplet(droplet));
                let sha256 = droplet.map(|droplet| droplet.sha256.clone());
                (build.state.clone(), sha256.unwrap_or_default())
            });
            staged.collect()
        });
        let built = ("STAGED".to_string(), rootfs_sha());
        assert_eq!(
            staged,
            [("STAGED".into(), sha256(COMPONENT)), built.clone(), built]
        );
        assert_eq!(builder.started.load(Ordering::Relaxed), 1, "lost alone");
        assert_eq!(builder.uploaded.load(Ordering::Relaxed), 1, "its source");
        assert_eq!(*builder.forgotten.lock().unwrap(), ["known", "new"]);
        let lines = |at: usize| {
            let lines = logs.read(&builds[at].1, 0, None, 100, false);
            lines.into_iter().map(|line| line.text).collect::<Vec<_>>()
        };
        let on = lines(1);
        let following = "Following the build on the wicket w from the line 1 of its output on";
        assert!(
            on.iter().any(|line| line == following)
                && on.iter().any(|line| line == "two")
                && !on.iter().any(|line| line == "one"),
            "{on:?}"
        );
        let lost = lines(2);
        let again = "The wicket w knows the build no more: built again";
        assert!(
            lost.iter().any(|line| line == again) && lost.iter().any(|line| line == "one"),
            "{lost:?}"
        );

        // staged once more while another instance's sweep is under way, which deletes the
        // droplet's blob, named no more, after it was made again: staged again at its end
        let (build, app) = &builds[0];
        let droplet = Blob::Droplet(sha256(COMPONENT));
        let file = dir.join(droplet.key());
        let sweeping = |by: Option<&str>| {
            let by = by.map(|instance| crate::store::Sweeping {
                instance: instance.into(),
                since: crate::store::epoch_secs(),
                epoch: None,
            });
            store.update(move |state| state.sweep.by = by.clone())
        };
        sweeping(Some("another")).await;
        let again = |state: &mut State| {
            state.droplets.clear();
            let build = state.builds.iter_mut().find(|kept| kept.guid == *build);
            let build = build.unwrap();
            build.state = "STAGING".into();
            build.droplet = None;
            build.staging = Some(Staging {
                base: "http://api.test".into(),
                command: None,
                building: None,
                taken: false,
            });
        };
        store.update(again).await;
        std::fs::remove_file(&file).unwrap();
        let swept = async {
            while !file.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            std::fs::remove_file(&file).unwrap();
            sweeping(None).await;
        };
        tokio::join!(run(&store, &logs, &registry, &controller, build), swept);
        let state = store.read(|state| state.build(build).unwrap().state.clone());
        assert_eq!(state, "STAGED");
        assert!(store.read(|state| state.names(&droplet)) && file.exists());
        let again = "A blob of the droplet was swept as it was staged: staging again";
        let lines = logs.read(app, 0, None, 100, false);
        assert_eq!(lines.iter().filter(|line| line.text == again).count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
