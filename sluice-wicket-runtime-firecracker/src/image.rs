//! An OCI image (one built by Cloud Native Buildpacks, say) as a rootfs, on the wicket:
//! `PUT /api/images` names a registry reference; its manifest for the wicket's platform is
//! read, its layers are pulled and unpacked (`rootfs`), and the image made holds the
//! wicket's own guest as `/init`, so it is of the wicket's version and arch. It is kept as
//! an upload, for an app to name by its digest. The manifest's digest pins it: the same
//! manifest and guest make the same bytes, a rebuild the digest the first build had.
//!
//! A droplet (`stage`) is served so too: its tar laid over the layers of its run image
//! (`layers`), what it runs over the image's config (`launch`).

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sluice_wicket_console::http::Received;
use sluice_wicket_registry::{INDEX_TYPES, Reference, Registry, Session, descriptor_digest};
use tokio::sync::Semaphore;

use crate::rootfs::{ImageConfig, Tools, Tree};

/// A whole build: the pull, the unpacking, the file system.
const BUILD_TIMEOUT: Duration = Duration::from_secs(600);

/// The largest config of an image.
const MAX_CONFIG_BYTES: u64 = 4 << 20;

/// The port of a process whose image exposes none, as `PORT` tells it.
const DEFAULT_PORT: u16 = 8080;

/// What a config is asked as.
const CONFIG_TYPES: &str =
    "application/vnd.oci.image.config.v1+json, application/vnd.docker.container.image.v1+json";

/// What a layer is asked as.
const LAYER_TYPES: &str = "application/vnd.oci.image.layer.v1.tar+gzip, application/vnd.oci.image.layer.v1.tar, application/vnd.docker.image.rootfs.diff.tar.gzip, application/vnd.docker.image.rootfs.diff.tar";

/// The platform the wicket's VMs run: a VM's arch is its host's.
pub const ARCHITECTURE: &str = if cfg!(target_arch = "x86_64") {
    "amd64"
} else {
    "arm64"
};

/// What a build needs.
pub struct Images {
    pub registry: Registry,
    /// The guest, `/init` of the images made.
    pub guest: PathBuf,
    pub tools: Tools,
    /// One build at a time: each takes the room of the image's files twice.
    pub building: Semaphore,
    /// The most an image's files may take.
    pub limit: u64,
    /// Below which each build has a directory of its own, on the file system the images are
    /// kept on.
    pub work: PathBuf,
    /// Where the builders' rootfs are kept (`stage`).
    pub builders: PathBuf,
    /// One builder's rootfs made at a time.
    pub making: tokio::sync::Mutex<()>,
}

/// What `PUT /api/images` asks for.
pub struct Request {
    pub image: String,
    pub credentials: Option<(String, String)>,
    /// The process's port; None: the one the image exposes, else [`DEFAULT_PORT`].
    pub port: Option<u16>,
    pub restore: Vec<String>,
    /// Tars laid over the image's layers, in order, each with its sha256: a droplet's.
    pub layers: Vec<(PathBuf, String)>,
    /// What runs, over the image's config.
    pub launch: Option<Over>,
}

/// What runs in an image whose layers are not all its own, over its config: a droplet's
/// launch, as a build answers it (`stage`).
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Over {
    pub entrypoint: Vec<String>,
    /// `NAME=value`, over the image's of that name.
    #[serde(default)]
    pub env: Vec<String>,
    /// Directories before the image's `PATH`.
    #[serde(default)]
    pub path: Vec<String>,
    /// Empty: the image's.
    #[serde(default)]
    pub workdir: String,
    /// Files of the image the launch runs, by their absolute paths, each with what runs it:
    /// an image without one is refused (a command's shell, say).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub needs: BTreeMap<String, String>,
}

impl Over {
    /// `config` with what it says over it.
    fn over(&self, mut config: ImageConfig) -> ImageConfig {
        config.entrypoint = self.entrypoint.clone();
        config.cmd.clear();
        let name = |pair: &str| pair.split('=').next().unwrap_or_default().to_string();
        let given: Vec<String> = self.env.iter().map(|pair| name(pair)).collect();
        config.env.retain(|pair| !given.contains(&name(pair)));
        config.env.extend(self.env.iter().cloned());
        if !self.path.is_empty() {
            let mut path = self.path.join(":");
            match config.env.iter_mut().find(|pair| pair.starts_with("PATH=")) {
                Some(pair) => {
                    path.push(':');
                    pair.insert_str("PATH=".len(), &path);
                }
                None => config.env.push(format!("PATH={path}")),
            }
        }
        if !self.workdir.is_empty() {
            config.workdir = self.workdir.clone();
        }
        config.needs = self.needs.clone();
        config
    }
}

/// An image as its registry has it for the wicket's platform.
pub struct Inspected {
    /// The reference given, pinned to the manifest's digest.
    pub image: String,
    /// The manifest's digest.
    pub digest: String,
    pub config: Value,
}

/// An image being pulled: its manifest and config read ([`Images::open`]).
pub struct Pull<'a> {
    session: Session<'a>,
    manifest: Value,
    pub inspected: Inspected,
}

impl Pull<'_> {
    /// Writes the layer whose uncompressed digest (of the config's `rootfs.diff_ids`) is
    /// `diff_id` to `path` as a tar, checked against its blob's size and digest and against
    /// `diff_id`, `limit` bytes at most each: its size.
    pub async fn layer(&mut self, diff_id: &str, path: &Path, limit: u64) -> Result<u64, String> {
        let at = self.inspected.config["rootfs"]["diff_ids"]
            .as_array()
            .into_iter()
            .flatten()
            .position(|id| id.as_str() == Some(diff_id))
            .ok_or_else(|| format!("the image has no layer {diff_id}"))?;
        let layer = layers_of(&self.manifest)?
            .into_iter()
            .nth(at)
            .ok_or_else(|| format!("the manifest has no layer {diff_id}"))?;
        let blob = path.with_extension("blob");
        pull_layer(&mut self.session, &layer, &blob, limit).await?;
        let (diff_id, path) = (diff_id.to_string(), path.to_path_buf());
        tokio::task::spawn_blocking(move || {
            let failed = |e: std::io::Error| format!("{}: {e}", path.display());
            let file = std::fs::File::open(&blob).map_err(failed)?;
            let mut read: Box<dyn std::io::Read> = match layer.gzip {
                true => Box::new(flate2::read::GzDecoder::new(file)),
                false => Box::new(file),
            };
            let mut out = std::fs::File::create(&path).map_err(failed)?;
            let mut hasher = Sha256::new();
            let mut piece = vec![0u8; 64 * 1024];
            let mut bytes = 0u64;
            let copied = loop {
                let n = match std::io::Read::read(&mut read, &mut piece) {
                    Ok(0) => break Ok(()),
                    Ok(n) => n,
                    Err(e) => break Err(format!("the layer {diff_id}: {e}")),
                };
                bytes += n as u64;
                if bytes > limit {
                    break Err(format!(
                        "the layer {diff_id} unpacks to more than {} MiB",
                        limit >> 20
                    ));
                }
                hasher.update(&piece[..n]);
                if let Err(e) = out.write_all(&piece[..n]) {
                    break Err(failed(e));
                }
            };
            let _ = std::fs::remove_file(&blob);
            let checked = copied.and_then(|()| {
                match format!("sha256:{}", hex(&hasher.finalize())) == diff_id {
                    true => Ok(bytes),
                    false => Err(format!("the layer {diff_id} is not what its diff ID says")),
                }
            });
            if checked.is_err() {
                let _ = std::fs::remove_file(&path);
            }
            checked
        })
        .await
        .map_err(|e| e.to_string())?
    }
}

/// An image made, received as an upload is.
pub struct Built {
    /// The reference given, pinned to the manifest's digest.
    pub image: String,
    /// The file name it is kept as: the repository's last segment.
    pub name: String,
    pub received: Received,
}

impl Images {
    /// Builds the image of `request`, within [`BUILD_TIMEOUT`].
    pub async fn build(&self, request: Request) -> Result<Built, String> {
        let _turn = self.building.acquire().await.map_err(|e| e.to_string())?;
        tokio::time::timeout(BUILD_TIMEOUT, self.pull_and_build(request))
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "the build took more than {}s",
                    BUILD_TIMEOUT.as_secs()
                ))
            })
    }

    /// What the registry has as `image`, nothing of its layers pulled.
    pub async fn inspect(
        &self,
        image: &str,
        credentials: Option<&(String, String)>,
    ) -> Result<Inspected, String> {
        let reference = Reference::parse(image)?;
        Ok(self.open(image, &reference, credentials).await?.inspected)
    }

    /// The pull of `image` (`reference`): its manifest and config read, its layers to pull.
    pub async fn open<'a>(
        &'a self,
        image: &str,
        reference: &'a Reference,
        credentials: Option<&'a (String, String)>,
    ) -> Result<Pull<'a>, String> {
        let credentials =
            credentials.map(|(username, password)| (username.as_str(), password.as_str()));
        let mut session = self.registry.session(reference, credentials);
        let (digest, manifest) = manifest(&mut session, reference).await?;
        let config = config(&mut session, &manifest).await?;
        Ok(Pull {
            session,
            manifest,
            inspected: Inspected {
                image: pinned_reference(image, reference, &digest),
                digest,
                config,
            },
        })
    }

    async fn pull_and_build(&self, request: Request) -> Result<Built, String> {
        let reference = Reference::parse(&request.image)?;
        let credentials = request
            .credentials
            .as_ref()
            .map(|(username, password)| (username.as_str(), password.as_str()));
        let mut session = self.registry.session(&reference, credentials);
        let (pinned, manifest) = manifest(&mut session, &reference).await?;
        let digest = pinned.clone();
        let config = config(&mut session, &manifest).await?;
        let port = request.port.unwrap_or_else(|| exposed(&config));
        let config = image_config(&config);
        let config = match &request.launch {
            Some(over) => over.over(config),
            None => config,
        };

        let work = Work::new(&self.work)?;
        let mut layers = Vec::new();
        for (n, layer) in layers_of(&manifest)?.into_iter().enumerate() {
            let path = work.path.join(format!("layer-{n}"));
            pull_layer(&mut session, &layer, &path, self.limit).await?;
            layers.push((path, layer.gzip));
        }
        // what else decides the image's bytes
        let mut pinned = pinned;
        for (path, sha256) in request.layers {
            layers.push((path, false));
            pinned.push_str(&format!("+{sha256}"));
        }
        if let Some(over) = &request.launch {
            let over = serde_json::to_string(over).map_err(|e| e.to_string())?;
            pinned.push_str(&format!("+{over}"));
        }

        let name = format!(
            "{}.ext4",
            reference.repository.rsplit('/').next().unwrap_or_default()
        );
        let guest = self.guest.clone();
        let tools = self.tools.clone();
        let limit = self.limit;
        let restore = request.restore;
        let at = work.path.clone();
        let seed_of = pinned.clone();
        let received = tokio::task::spawn_blocking(move || {
            build(
                &at, &layers, config, port, restore, &guest, &seed_of, limit, &tools,
            )
        })
        .await
        .map_err(|e| e.to_string())??;
        Ok(Built {
            image: pinned_reference(&request.image, &reference, &digest),
            name,
            received: Received::adopt(received.0, received.1, received.2),
        })
    }
}

/// `image` pinned to its manifest's `digest`, as given when it names one.
fn pinned_reference(image: &str, reference: &Reference, digest: &str) -> String {
    match &reference.digest {
        Some(_) => image.to_string(),
        None => format!("{image}@{digest}"),
    }
}

/// A build's directory, removed with whatever it still holds: never through a link
/// (`remove_dir_all`), and its tree's modes are all the wicket's own.
struct Work {
    path: PathBuf,
}

impl Work {
    fn new(below: &Path) -> Result<Work, String> {
        std::fs::create_dir_all(below).map_err(|e| format!("{}: {e}", below.display()))?;
        let mut name = [0u8; 12];
        getrandom::fill(&mut name).map_err(|e| e.to_string())?;
        let name: String = name.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = below.join(format!(".build-{name}"));
        std::fs::create_dir(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Work { path })
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.path) {
            eprintln!("--console-dir: remove {}: {e}", self.path.display());
        }
    }
}

/// The manifest of the wicket's platform: an index followed to its entry. Its digest
/// pins the image: the tag's, or the entry's of an index.
async fn manifest(
    session: &mut Session<'_>,
    reference: &Reference,
) -> Result<(String, Value), String> {
    let (digest, manifest) = session.manifest(reference.by()).await?;
    if let Some(pinned) = &reference.digest
        && *pinned != digest
    {
        return Err(format!("the registry answered the manifest {digest}"));
    }
    let media_type = manifest["mediaType"].as_str().unwrap_or_default();
    if !INDEX_TYPES.contains(&media_type) && !manifest["manifests"].is_array() {
        return Ok((digest, manifest));
    }
    let entries = manifest["manifests"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let platform = |entry: &Value| {
        let field = |name: &str| entry["platform"][name].as_str().unwrap_or("?").to_string();
        format!("{}/{}", field("os"), field("architecture"))
    };
    let wanted = format!("linux/{ARCHITECTURE}");
    let entry = entries
        .iter()
        .find(|entry| platform(entry) == wanted)
        .ok_or_else(|| {
            let found: Vec<String> = entries.iter().map(platform).collect();
            format!(
                "the image is an index of {}: none for this wicket's {wanted}",
                found.join(", ")
            )
        })?;
    let entry = descriptor_digest(entry)?;
    let (got, manifest) = session.manifest(&entry).await?;
    if got != entry {
        return Err(format!(
            "the registry answered the manifest {got} for {entry}"
        ));
    }
    Ok((digest, manifest))
}

/// The image's config, checked of the wicket's platform.
async fn config(session: &mut Session<'_>, manifest: &Value) -> Result<Value, String> {
    let descriptor = &manifest["config"];
    let digest = descriptor_digest(descriptor).map_err(|e| format!("its config: {e}"))?;
    let size = descriptor["size"]
        .as_u64()
        .filter(|size| *size <= MAX_CONFIG_BYTES)
        .ok_or("its config has no size, or is too large")?;
    let bytes = session
        .read_blob(&digest, size, CONFIG_TYPES, "the config")
        .await?;
    let config: Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("its config is no JSON: {e}"))?;
    let architecture = config["architecture"].as_str().unwrap_or(ARCHITECTURE);
    if architecture != ARCHITECTURE {
        return Err(format!(
            "the image is for {architecture}, this wicket's VMs for {ARCHITECTURE}"
        ));
    }
    Ok(config)
}

/// The port the config exposes: the lowest; else [`DEFAULT_PORT`].
fn exposed(config: &Value) -> u16 {
    config["config"]["ExposedPorts"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(port, _)| port.split('/').next()?.parse::<u16>().ok())
        .min()
        .unwrap_or(DEFAULT_PORT)
}

fn image_config(config: &Value) -> ImageConfig {
    let config = &config["config"];
    let strings = |key: &str| -> Vec<String> {
        config[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(String::from))
            .collect()
    };
    let text = |key: &str| config[key].as_str().unwrap_or_default().to_string();
    ImageConfig {
        entrypoint: strings("Entrypoint"),
        cmd: strings("Cmd"),
        env: strings("Env"),
        user: text("User"),
        workdir: text("WorkingDir"),
        needs: Default::default(),
    }
}

/// A layer to pull.
#[derive(Debug, PartialEq)]
struct Layer {
    digest: String,
    size: u64,
    gzip: bool,
}

fn layers_of(manifest: &Value) -> Result<Vec<Layer>, String> {
    let layers = manifest["layers"]
        .as_array()
        .ok_or("the manifest names no layers: no container image")?;
    layers
        .iter()
        .map(|layer| {
            let media_type = layer["mediaType"].as_str().unwrap_or_default();
            let gzip = if media_type.ends_with("+gzip") || media_type.ends_with(".tar.gzip") {
                true
            } else if media_type.ends_with(".tar") {
                false
            } else {
                return Err(format!(
                    "a layer of {media_type}: this wicket unpacks tar and gzip ones"
                ));
            };
            Ok(Layer {
                digest: descriptor_digest(layer)?,
                size: layer["size"].as_u64().ok_or("a layer without a size")?,
                gzip,
            })
        })
        .collect()
}

/// Writes the blob of `layer` to `path`, checked against its size and digest, `limit` bytes
/// at most: a layer may unpack to more, never come to less.
async fn pull_layer(
    session: &mut Session<'_>,
    layer: &Layer,
    path: &Path,
    limit: u64,
) -> Result<(), String> {
    if layer.size > limit {
        return Err(format!(
            "a layer of {} bytes: {} MiB at most",
            layer.size,
            limit >> 20
        ));
    }
    let mut response = session.blob(&layer.digest, LAYER_TYPES).await?;
    let failed = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut file = std::fs::File::create(path).map_err(failed)?;
    let mut hasher = Sha256::new();
    let mut read = 0;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("the layer {}: {e}", layer.digest))?
    {
        read += chunk.len() as u64;
        if read > layer.size {
            break;
        }
        hasher.update(&chunk);
        file.write_all(&chunk).map_err(failed)?;
    }
    if read != layer.size || format!("sha256:{}", hex(&hasher.finalize())) != layer.digest {
        return Err(format!(
            "the layer {} is not what its size ({}) and digest say: {read} bytes came",
            layer.digest, layer.size
        ));
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Unpacks `layers` below `work` and makes the image, seeded by `pinned`, the guest, the
/// port and the hook: where it is (hidden, beside `work`), its digest and size.
#[allow(clippy::too_many_arguments)]
fn build(
    work: &Path,
    layers: &[(PathBuf, bool)],
    config: ImageConfig,
    port: u16,
    restore: Vec<String>,
    guest: &Path,
    pinned: &str,
    limit: u64,
    tools: &Tools,
) -> Result<(PathBuf, String, u64), String> {
    let mut tree = Tree::new(&work.join("root"), limit)?;
    for (path, gzip) in layers {
        let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        if *gzip {
            tree.apply(flate2::read::GzDecoder::new(file))?;
        } else {
            tree.apply(file)?;
        }
    }
    let launch = tree.launch(config, port, restore.clone())?;
    let init = std::fs::read(guest).map_err(|e| format!("{}: {e}", guest.display()))?;
    let mut seed = Sha256::new();
    seed.update(pinned.as_bytes());
    seed.update(port.to_be_bytes());
    seed.update(restore.join("\0").as_bytes());
    seed.update(&init);
    let seed: [u8; 16] = seed.finalize()[..16].try_into().expect("16 of 32 bytes");

    let name = work
        .file_name()
        .expect("named")
        .to_string_lossy()
        .into_owned();
    let out = work
        .parent()
        .expect("below a directory")
        .join(format!("{name}.ext4.partial"));
    tree.build(guest, &launch, &seed, &out, tools)
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&out);
        })?;
    let (sha256, bytes) = digest_of(&out).inspect_err(|_| {
        let _ = std::fs::remove_file(&out);
    })?;
    Ok((out, sha256, bytes))
}

/// The sha256 and the size of the file at `path`, as a [`Received`] has them.
fn digest_of(path: &Path) -> Result<(String, u64), String> {
    let failed = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut file = std::fs::File::open(path).map_err(failed)?;
    let mut hasher = Sha256::new();
    let bytes = std::io::copy(&mut file, &mut hasher).map_err(failed)?;
    Ok((hex(&hasher.finalize()), bytes))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A config's port, its launch's part and its layers' kinds, and what is refused.
    #[test]
    fn what_a_manifest_and_a_config_say() {
        let config = json!({"config": {
            "ExposedPorts": {"9000/tcp": {}, "8443/tcp": {}},
            "Entrypoint": ["/cnb/process/web"],
            "Env": ["CNB_APP_DIR=/workspace"],
            "User": "cnb",
            "WorkingDir": "/workspace",
        }});
        assert_eq!(exposed(&config), 8443);
        assert_eq!(exposed(&json!({"config": {}})), DEFAULT_PORT);
        let launch = image_config(&config);
        assert_eq!(launch.entrypoint, ["/cnb/process/web"]);
        assert!(launch.cmd.is_empty());
        assert_eq!(launch.env, ["CNB_APP_DIR=/workspace"]);
        assert_eq!(
            (&launch.user[..], &launch.workdir[..]),
            ("cnb", "/workspace")
        );

        // a droplet's launch over it: its entrypoint alone, its env over the image's, its
        // directories before the image's PATH
        let mut image = image_config(&config);
        image.cmd = vec!["serve".into()];
        image.env.push("PATH=/usr/bin".into());
        let over = Over {
            entrypoint: vec!["/cnb/process/web".into()],
            env: vec!["CNB_APP_DIR=/app".into(), "CNB_LAYERS_DIR=/layers".into()],
            path: vec!["/cnb/process".into(), "/cnb/lifecycle".into()],
            workdir: String::new(),
            needs: BTreeMap::from([("/bin/bash".into(), "a command's shell".into())]),
        };
        let over = over.over(image);
        assert_eq!(over.needs.keys().collect::<Vec<_>>(), ["/bin/bash"]);
        assert_eq!(
            (&over.entrypoint[..], over.cmd.len()),
            (&["/cnb/process/web".to_string()][..], 0)
        );
        assert_eq!(
            over.env,
            [
                "PATH=/cnb/process:/cnb/lifecycle:/usr/bin",
                "CNB_APP_DIR=/app",
                "CNB_LAYERS_DIR=/layers"
            ]
        );
        assert_eq!(over.workdir, "/workspace");

        let digest = format!("sha256:{}", "ab".repeat(32));
        let layer =
            |media_type: &str| json!({"mediaType": media_type, "digest": digest, "size": 3});
        let layers = layers_of(&json!({"layers": [
            layer("application/vnd.oci.image.layer.v1.tar+gzip"),
            layer("application/vnd.docker.image.rootfs.diff.tar.gzip"),
            layer("application/vnd.oci.image.layer.v1.tar"),
        ]}))
        .unwrap();
        assert_eq!(
            layers.iter().map(|layer| layer.gzip).collect::<Vec<_>>(),
            [true, true, false]
        );
        let zstd =
            layers_of(&json!({"layers": [layer("application/vnd.oci.image.layer.v1.tar+zstd")]}));
        assert!(zstd.unwrap_err().contains("unpacks tar and gzip"));
        assert!(layers_of(&json!({})).unwrap_err().contains("no layers"));
    }
}
