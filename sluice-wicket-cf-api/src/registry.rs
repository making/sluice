//! The wasm component of an OCI image, pulled from its registry
//! (`sluice-wicket-registry`): what `cf push --docker-image <reference>` stages.
//! The reference's tag resolves to the digest of the manifest it names (an
//! image index is followed to its one `wasm` entry), and the manifest's one
//! `application/wasm` layer -- the wasm OCI artifact `wkg oci push` makes, or
//! `oras push <reference> hello.wasm:application/wasm` -- is the component,
//! checked against its digest. Any other image fails, saying what it holds.

use std::time::Duration;

use serde_json::Value;
pub use sluice_wicket_registry::Reference;
use sluice_wicket_registry::{INDEX_TYPES, Session, descriptor_digest as digest};

use crate::staging::sha256;

/// The media type of the component's layer.
const WASM_LAYER: &str = "application/wasm";
/// The largest component: as large as a package upload.
const MAX_COMPONENT_BYTES: u64 = crate::api::MAX_UPLOAD_BYTES as u64;
/// A whole pull: the manifests, a token and the layer.
const PULL_TIMEOUT: Duration = Duration::from_secs(300);

/// A component pulled.
pub struct Pulled {
    /// The reference as given, pinned to the manifest's digest: what the
    /// droplet shows.
    pub image: String,
    /// The component's file name in the wicket: the repository's last
    /// segment.
    pub file: String,
    pub bytes: Vec<u8>,
    pub sha256: String,
}

/// The client of the registries.
#[derive(Clone)]
pub struct Registry {
    registry: sluice_wicket_registry::Registry,
}

impl Registry {
    pub fn new() -> Result<Registry, String> {
        let agent = concat!("sluice-wicket-cf-api/", env!("CARGO_PKG_VERSION"));
        Ok(Registry {
            registry: sluice_wicket_registry::Registry::new(agent)?,
        })
    }

    /// Pulls the component of `image`, with the `(username, password)` of a
    /// private one.
    pub async fn pull(
        &self,
        image: &str,
        credentials: Option<(&str, &str)>,
    ) -> Result<Pulled, String> {
        let reference = Reference::parse(image)?;
        let session = self.registry.session(&reference, credentials);
        tokio::time::timeout(PULL_TIMEOUT, pull(session, &reference, image))
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "the pull took more than {}s",
                    PULL_TIMEOUT.as_secs()
                ))
            })
            // what names the credentials here: the CLI's flag
            .map_err(|e| {
                e.replace(
                    "it needs credentials",
                    "--docker-username, with CF_DOCKER_PASSWORD",
                )
                .replace(
                    "the registry asks for credentials",
                    "the registry asks for credentials: --docker-username, with CF_DOCKER_PASSWORD",
                )
            })
    }
}

async fn pull(
    mut session: Session<'_>,
    reference: &Reference,
    image: &str,
) -> Result<Pulled, String> {
    let (digest, manifest) = session.manifest(reference.by()).await?;
    if let Some(pinned) = &reference.digest
        && *pinned != digest
    {
        return Err(format!("the registry answered the manifest {digest}"));
    }
    let layer = match component(&manifest)? {
        Found::Layer(layer) => layer,
        Found::Entry(entry) => {
            let (got, manifest) = session.manifest(&entry).await?;
            match component(&manifest)? {
                Found::Layer(layer) if got == entry => layer,
                Found::Layer(_) => {
                    return Err(format!(
                        "the registry answered the manifest {got} for {entry}"
                    ));
                }
                Found::Entry(_) => return Err("an index in the index's wasm entry".into()),
            }
        }
    };
    let bytes = session
        .read_blob(&layer.digest, layer.size, WASM_LAYER, "the component")
        .await?;
    let file = format!(
        "{}.wasm",
        reference.repository.rsplit('/').next().unwrap_or_default()
    );
    crate::staging::check_component(&file, &bytes)?;
    Ok(Pulled {
        image: match &reference.digest {
            Some(_) => image.to_string(),
            None => format!("{image}@{digest}"),
        },
        file,
        sha256: sha256(&bytes),
        bytes,
    })
}

/// A layer: its digest and size.
#[derive(Debug, PartialEq)]
struct Layer {
    digest: String,
    size: u64,
}

/// What a manifest points the pull at.
#[derive(Debug, PartialEq)]
enum Found {
    /// The component's layer.
    Layer(Layer),
    /// The digest of an index's wasm entry.
    Entry(String),
}

/// The component's layer of `manifest`, or the `wasm` entry of an index;
/// else what the image holds instead.
fn component(manifest: &Value) -> Result<Found, String> {
    let media_type = manifest["mediaType"].as_str().unwrap_or_default();
    if INDEX_TYPES.contains(&media_type) || manifest["manifests"].is_array() {
        let entries = manifest["manifests"]
            .as_array()
            .map_or(&[][..], Vec::as_slice);
        let platform = |entry: &Value| {
            let field = |name: &str| entry["platform"][name].as_str().unwrap_or("?").to_string();
            format!("{}/{}", field("os"), field("architecture"))
        };
        let wasm: Vec<&Value> = entries
            .iter()
            .filter(|entry| entry["platform"]["architecture"] == "wasm")
            .collect();
        return match wasm.as_slice() {
            [entry] => Ok(Found::Entry(digest(entry)?)),
            [] => Err(format!(
                "the image is an index of {}, no wasm among them: it holds no wasm component",
                listed(entries.iter().map(platform))
            )),
            several => Err(format!(
                "the image is an index of {} wasm entries ({}): name one by its digest",
                several.len(),
                listed(several.iter().map(|entry| platform(entry)))
            )),
        };
    }
    let layers = manifest["layers"].as_array().map_or(&[][..], Vec::as_slice);
    let wasm: Vec<&Value> = layers
        .iter()
        .filter(|layer| layer["mediaType"] == WASM_LAYER)
        .collect();
    match wasm.as_slice() {
        [layer] => {
            let size = layer["size"].as_u64().ok_or("the wasm layer has no size")?;
            if size > MAX_COMPONENT_BYTES {
                return Err(format!(
                    "the component is {size} bytes, more than {} MiB",
                    MAX_COMPONENT_BYTES >> 20
                ));
            }
            Ok(Found::Layer(Layer {
                digest: digest(layer)?,
                size,
            }))
        }
        [] => Err(format!(
            "the image holds no wasm component (a layer of {WASM_LAYER}): its config is {} and its layers {}; push one with wkg oci push, or oras push <reference> <file>.wasm:{WASM_LAYER}",
            manifest["config"]["mediaType"].as_str().unwrap_or("none"),
            listed(
                layers
                    .iter()
                    .map(|layer| layer["mediaType"].as_str().unwrap_or("?").to_string())
            )
        )),
        several => Err(format!(
            "the image holds {} wasm layers: one component is deployed",
            several.len()
        )),
    }
}

/// `items` joined, a repeated one once with its count: `a (3), b`.
fn listed(items: impl Iterator<Item = String>) -> String {
    let mut counted: Vec<(String, usize)> = Vec::new();
    for item in items {
        match counted.iter_mut().find(|(seen, _)| *seen == item) {
            Some((_, count)) => *count += 1,
            None => counted.push((item, 1)),
        }
    }
    if counted.is_empty() {
        return "none".into();
    }
    counted
        .iter()
        .map(|(item, count)| match count {
            1 => item.clone(),
            count => format!("{item} ({count})"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn takes_the_one_wasm_layer_or_says_what_the_image_holds() {
        let digest = |n: u8| format!("sha256:{}", format!("{n:02x}").repeat(32));
        let layer = |media_type: &str, n: u8| json!({"mediaType": media_type, "digest": digest(n), "size": 10});
        let manifest = |config: &str, layers: Vec<Value>| {
            json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {"mediaType": config, "digest": digest(0), "size": 2},
                "layers": layers,
            })
        };
        let wanted = Found::Layer(Layer {
            digest: digest(1),
            size: 10,
        });
        // the wasm OCI artifact (wkg oci push), and oras push's default config
        for config in [
            "application/vnd.wasm.config.v0+json",
            "application/vnd.oci.empty.v1+json",
        ] {
            assert_eq!(
                component(&manifest(config, vec![layer(WASM_LAYER, 1)])).unwrap(),
                wanted
            );
        }
        let tar = "application/vnd.oci.image.layer.v1.tar+gzip";
        let image = manifest(
            "application/vnd.oci.image.config.v1+json",
            vec![layer(tar, 1), layer(tar, 2), layer(tar, 3)],
        );
        let error = component(&image).unwrap_err();
        assert!(
            error.contains(&format!(
                "its config is application/vnd.oci.image.config.v1+json and its layers {tar} (3)"
            )),
            "{error}"
        );
        let two = manifest("x", vec![layer(WASM_LAYER, 1), layer(WASM_LAYER, 2)]);
        assert!(component(&two).unwrap_err().contains("2 wasm layers"));
        let mut large = manifest("x", vec![layer(WASM_LAYER, 1)]);
        large["layers"][0]["size"] = json!(MAX_COMPONENT_BYTES + 1);
        assert!(component(&large).unwrap_err().contains("more than 160 MiB"));
        let mut sha512 = manifest("x", vec![layer(WASM_LAYER, 1)]);
        sha512["layers"][0]["digest"] = json!("sha512:ab");
        assert!(component(&sha512).unwrap_err().contains("no sha256 digest"));

        // an index: its wasm entry, else the platforms it holds
        let entry = |os: &str, architecture: &str, n: u8| json!({"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": digest(n), "size": 100, "platform": {"os": os, "architecture": architecture}});
        let index = |entries: Vec<Value>| json!({"schemaVersion": 2, "mediaType": INDEX_TYPES[0], "manifests": entries});
        assert_eq!(
            component(&index(vec![
                entry("linux", "amd64", 1),
                entry("wasip1", "wasm", 2)
            ]))
            .unwrap(),
            Found::Entry(digest(2))
        );
        let error = component(&index(vec![
            entry("linux", "amd64", 1),
            entry("linux", "arm64", 2),
        ]))
        .unwrap_err();
        assert!(
            error.contains("an index of linux/amd64, linux/arm64, no wasm among them"),
            "{error}"
        );
        assert_eq!(listed(std::iter::empty()), "none");
    }
}
