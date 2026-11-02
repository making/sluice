//! An OCI image (one built by Cloud Native Buildpacks, say) as a rootfs, on the wicket:
//! `PUT /api/images` names a registry reference; its manifest for the wicket's platform is
//! read, its layers are pulled and unpacked (`rootfs`), and the image made holds the
//! wicket's own guest as `/init`, so it is of the wicket's version and arch. It is kept as
//! an upload, for an app to name by its digest. The manifest's digest pins it: the same
//! manifest and guest make the same bytes, a rebuild the digest the first build had.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
const ARCHITECTURE: &str = if cfg!(target_arch = "x86_64") {
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
}

/// What `PUT /api/images` asks for.
pub struct Request {
    pub image: String,
    pub credentials: Option<(String, String)>,
    /// The process's port; None: the one the image exposes, else [`DEFAULT_PORT`].
    pub port: Option<u16>,
    pub restore: Vec<String>,
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

    async fn pull_and_build(&self, request: Request) -> Result<Built, String> {
        let reference = Reference::parse(&request.image)?;
        let credentials = request
            .credentials
            .as_ref()
            .map(|(username, password)| (username.as_str(), password.as_str()));
        let mut session = self.registry.session(&reference, credentials);
        let (pinned, manifest) = manifest(&mut session, &reference).await?;
        let config = config(&mut session, &manifest).await?;
        let port = request.port.unwrap_or_else(|| exposed(&config));
        let config = image_config(&config);

        let work = Work::new(&self.work)?;
        let mut layers = Vec::new();
        for (n, layer) in layers_of(&manifest)?.into_iter().enumerate() {
            let path = work.path.join(format!("layer-{n}"));
            pull_layer(&mut session, &layer, &path, self.limit).await?;
            layers.push((path, layer.gzip));
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
        let image = match &reference.digest {
            Some(_) => request.image,
            None => format!("{}@{pinned}", request.image),
        };
        Ok(Built {
            image,
            name,
            received: Received::adopt(received.0, received.1, received.2),
        })
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
