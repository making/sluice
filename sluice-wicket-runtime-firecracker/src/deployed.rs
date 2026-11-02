//! The routes the console deployed (`--console`): hosts served by uploaded
//! rootfs images, kept in the console's directory (`--console-dir`) so a
//! restart serves them again (`sluice-wicket-console`'s layout).
//!
//! ```text
//! <dir>/routes.yaml                   the routes, in the --config file's route shape
//! <dir>/rootfs/<sha256>/<name>.ext4   the images they name, never rewritten
//! ```
//!
//! An image's path names its digest, so other content is another target: its
//! route's VMs start anew, and the old target drains as a removed one does.
//! The routes are held here, `routes.yaml` being their copy: an edit is
//! reloaded first (`routes::Reloader::edit`) and written once the reload
//! published it, so the file never names routes that did not start. An image
//! nothing names is removed after every edit and at startup.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::Deserialize;
use sluice_wicket_console::http::Received;
use sluice_wicket_console::store::{
    Blobs, check_host, check_kept, file_name, quote, rfc3339, write_by_rename,
};

use crate::config::{FileRoute, Origin};
use crate::route::RouteSpec;
use crate::routes::Refused;

/// The routes' file in the directory.
const MANIFEST: &str = "routes.yaml";

/// The images' directory in the directory.
const ROOTFS: &str = "rootfs";

/// The extension an image is kept with.
const EXT4: &str = ".ext4";

/// A route's options: the keys of a `--vm` flag's query, the values as given
/// (numbers normalized), in [`FileRoute::options`]' order.
pub type Options = Vec<(String, String)>;

/// One of the console's routes.
#[derive(Clone, PartialEq, Debug)]
pub struct Route {
    pub host: String,
    /// The image relative to the directory, `rootfs/<sha256>/<name>.ext4`.
    pub rootfs: String,
    pub options: Options,
    /// When it took this image and these options, RFC 3339.
    pub deployed: String,
}

/// The console's routes in the order deployed, and their directory.
#[derive(Clone, PartialEq, Debug)]
pub struct Deployed {
    dir: PathBuf,
    routes: Vec<Route>,
}

impl Deployed {
    /// The routes kept in `dir`, created when missing; removes the images
    /// nothing names (an upload a crash cut short included).
    pub fn open(dir: &Path) -> Result<Deployed, String> {
        let failed = |e: std::io::Error| format!("--console-dir {}: {e}", dir.display());
        std::fs::create_dir_all(dir.join(ROOTFS)).map_err(failed)?;
        let dir = dir.canonicalize().map_err(failed)?;
        let manifest = dir.join(MANIFEST);
        let routes = match std::fs::read_to_string(&manifest) {
            Ok(text) => read(&text).map_err(|e| format!("{}: {e}", manifest.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(format!("{}: {e}", manifest.display())),
        };
        let deployed = Deployed { dir, routes };
        deployed.collect_garbage(true);
        Ok(deployed)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    /// The images' store.
    pub fn images(&self) -> Blobs<'_> {
        Blobs {
            dir: &self.dir,
            kind: ROOTFS,
        }
    }

    /// The routes as specs of `firecracker` and `kernel`; an error names the
    /// route by its host.
    pub fn specs(
        &self,
        firecracker: &Path,
        kernel: &Path,
    ) -> Result<Vec<(RouteSpec, Origin)>, String> {
        self.routes
            .iter()
            .map(|route| {
                let options = route
                    .options
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()));
                let spec = RouteSpec::new(
                    &format!("console route {}", route.host),
                    &route.host,
                    self.dir.join(&route.rootfs),
                    options,
                    firecracker,
                    kernel,
                )?;
                let origin = Origin::Console {
                    deployed: route.deployed.clone(),
                };
                Ok((spec, origin))
            })
            .collect()
    }

    /// The routes with `host` served by the image `received` (uploaded as
    /// `name`) with `options` since `now`; the image kept. The same image and
    /// options again change nothing.
    pub fn deploy(
        &self,
        host: &str,
        name: &str,
        received: Received,
        options: Options,
        now: SystemTime,
    ) -> Result<Deployed, Refused> {
        let host = check_host(host).map_err(Refused::Invalid)?;
        let rootfs = self
            .images()
            .keep(&file_name(name, &host, EXT4), received)
            .map_err(Refused::Failed)?;
        Ok(self.with(Route {
            host,
            rootfs,
            options,
            deployed: rfc3339(now),
        }))
    }

    /// The routes with the route of `host` taking `options` since `now`.
    pub fn with_options(
        &self,
        host: &str,
        options: Options,
        now: SystemTime,
    ) -> Result<Deployed, Refused> {
        let route = self.route(host)?;
        Ok(self.with(Route {
            options,
            deployed: rfc3339(now),
            ..route.clone()
        }))
    }

    /// The routes without the route of `host`.
    pub fn without(&self, host: &str) -> Result<Deployed, Refused> {
        let host = self.route(host)?.host.clone();
        let mut deployed = self.clone();
        deployed.routes.retain(|route| route.host != host);
        Ok(deployed)
    }

    /// The routes with the route of `host` moved to `to` as it is: its image,
    /// options and time, in its place. A move replaces no route of the
    /// console.
    pub fn moved(&self, host: &str, to: &str) -> Result<Deployed, Refused> {
        let host = self.route(host)?.host.clone();
        let to = check_host(to).map_err(Refused::Invalid)?;
        if to != host && self.routes.iter().any(|route| route.host == to) {
            return Err(Refused::Taken(format!(
                "{to} is a route of the console already"
            )));
        }
        let mut deployed = self.clone();
        for route in &mut deployed.routes {
            if route.host == host {
                route.host = to.clone();
            }
        }
        Ok(deployed)
    }

    fn route(&self, host: &str) -> Result<&Route, Refused> {
        let host = host.to_ascii_lowercase();
        self.routes
            .iter()
            .find(|route| route.host == host)
            .ok_or_else(|| Refused::Unknown(format!("the console routes no host '{host}'")))
    }

    /// The routes with `route` in place of the one of its host -- which stays
    /// as it was, its time included, when the image and the options are the
    /// same -- or added last.
    fn with(&self, route: Route) -> Deployed {
        let mut deployed = self.clone();
        match deployed.routes.iter_mut().find(|r| r.host == route.host) {
            Some(r) if r.rootfs == route.rootfs && r.options == route.options => {}
            Some(r) => *r = route,
            None => deployed.routes.push(route),
        }
        deployed
    }

    /// Writes the routes to `routes.yaml`, by a rename; for its owner's eyes
    /// only.
    pub fn save(&self) -> Result<(), String> {
        write_by_rename(
            &self.dir.join(MANIFEST),
            write(&self.routes).as_bytes(),
            true,
        )
    }

    /// Removes the images nothing names; at `startup` the uploads a crash cut
    /// short too.
    pub fn collect_garbage(&self, startup: bool) {
        let named = self.routes.iter().map(|route| &route.rootfs);
        self.images().collect_garbage(named, startup);
    }
}

/// The options of a query's `pairs` (the `--vm` flag's keys) checked as the
/// route of `host` takes them, but its files: in [`FileRoute::options`]'
/// order, the numbers normalized.
pub fn options(host: &str, pairs: &[(String, String)]) -> Result<Options, String> {
    let mut options = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        let Some(n) = FileRoute::KEYS.iter().position(|known| known == key) else {
            return Err(format!("unknown option '{key}'"));
        };
        if options.iter().any(|(known, _, _)| known == key) {
            return Err(format!("{key} given twice"));
        }
        let value = match key.as_str() {
            "mode" | "egress" => value.clone(),
            _ => value
                .parse::<u32>()
                .map_err(|_| format!("{key}={value} is not a number"))?
                .to_string(),
        };
        options.push((key.clone(), value, n));
    }
    options.sort_by_key(|(_, _, n)| *n);
    let options: Options = options
        .into_iter()
        .map(|(key, value, _)| (key, value))
        .collect();
    RouteSpec::configure(
        "the route",
        host,
        PathBuf::from("/"),
        options
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        Path::new("/"),
        Path::new("/"),
    )?;
    Ok(options)
}

/// `routes.yaml`: the routes in the shape of the `--config` file's, each with
/// the time it was deployed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFile {
    #[serde(default)]
    routes: Vec<FileRoute>,
}

/// The routes of `routes.yaml`; errors name the route. A host is routed once.
fn read(text: &str) -> Result<Vec<Route>, String> {
    let mut parsing = serde_saphyr::Options::default();
    parsing.with_snippet = false;
    let file: ManifestFile = serde_saphyr::from_str_with_options(text, parsing).map_err(|e| {
        let e = e.to_string();
        e.strip_prefix("error: ").unwrap_or(&e).to_string()
    })?;
    let mut routes: Vec<Route> = Vec::with_capacity(file.routes.len());
    for (n, route) in file.routes.into_iter().enumerate() {
        let failed = |e: String| format!("routes[{n}]: {e}");
        let host = check_host(&route.host).map_err(failed)?;
        if routes.iter().any(|known| known.host == host) {
            return Err(failed(format!("host '{host}' twice")));
        }
        let rootfs = route.rootfs.to_string_lossy().to_string();
        check_kept(&rootfs, ROOTFS, EXT4).map_err(|e| failed(format!("rootfs: {e}")))?;
        let deployed = route
            .deployed
            .clone()
            .ok_or_else(|| failed("deployed: missing".into()))?;
        let pairs: Vec<(String, String)> = route
            .options()
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        let options = options(&host, &pairs).map_err(failed)?;
        routes.push(Route {
            host,
            rootfs,
            options,
            deployed,
        });
    }
    Ok(routes)
}

/// `routes` as `routes.yaml`; every string double-quoted, so never expanded.
fn write(routes: &[Route]) -> String {
    let mut yaml = String::from(
        "# The routes the console of sluice-wicket-runtime-firecracker (--console) deployed, \
         written by it:\n# read at its startup, overwritten by its next change.\n",
    );
    if routes.is_empty() {
        yaml.push_str("routes: []\n");
    } else {
        yaml.push_str("routes:\n");
    }
    for route in routes {
        yaml.push_str(&format!("  - host: {}\n", quote(&route.host)));
        yaml.push_str(&format!("    rootfs: {}\n", quote(&route.rootfs)));
        yaml.push_str(&format!("    deployed: {}\n", quote(&route.deployed)));
        for (key, value) in &route.options {
            let value = match key.as_str() {
                "mode" => quote(value),
                "egress" => {
                    let rules: Vec<String> = value
                        .split(',')
                        .filter(|rule| !rule.is_empty())
                        .map(quote)
                        .collect();
                    format!("[{}]", rules.join(", "))
                }
                _ => value.clone(),
            };
            yaml.push_str(&format!("    {key}: {value}\n"));
        }
    }
    yaml
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    fn pairs(query: &str) -> Vec<(String, String)> {
        query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (key.to_string(), value.to_string())
            })
            .collect()
    }

    /// The options are the `--vm` flag's, checked as a route takes them, kept in
    /// one order; the routes file reads back as written, and what the console never
    /// writes -- an image not its own, a host twice, an option a route refuses, no
    /// time -- fails, naming the route. A move keeps a route as it is, in its place,
    /// and replaces none.
    #[test]
    fn the_options_and_the_routes_file() {
        let kept = options(
            "a.local",
            &pairs("egress=db.internal:5432,10.0.0.0/24:6379&warm=02&mode=pool&vms=4"),
        )
        .unwrap();
        assert_eq!(
            kept,
            pairs("mode=pool&warm=2&vms=4&egress=db.internal:5432,10.0.0.0/24:6379"),
            "in the file's order, the numbers normalized"
        );
        for (query, error) in [
            ("fuel=1", "unknown option 'fuel'"),
            ("mem=1&mem=2", "mem given twice"),
            ("mem=lots", "mem=lots is not a number"),
            ("warm=1", "warm needs mode=per-connection"),
            ("mode=pool", "mode=pool needs vms"),
            ("mode=fork", "unknown mode fork"),
            (
                "egress=db.internal",
                "egress db.internal: expected <host>:<port>",
            ),
        ] {
            let refused = options("a.local", &pairs(query));
            assert!(
                matches!(&refused, Err(e) if e.contains(error)),
                "{query}: {refused:?}"
            );
        }

        let image = |sha: &str, name: &str| format!("rootfs/{}/{name}", sha.repeat(64));
        let routes = vec![
            Route {
                host: "a.local".into(),
                rootfs: image("0", "a.ext4"),
                options: Vec::new(),
                deployed: rfc3339(UNIX_EPOCH),
            },
            Route {
                host: "b.local".into(),
                rootfs: image("f", "b.ext4"),
                options: kept,
                deployed: rfc3339(UNIX_EPOCH + Duration::from_secs(1_793_276_096)),
            },
        ];
        let text = write(&routes);
        assert_eq!(read(&text), Ok(routes.clone()), "{text}");
        assert!(
            text.contains("egress: [\"db.internal:5432\", \"10.0.0.0/24:6379\"]"),
            "{text}"
        );
        assert_eq!(read(&write(&[])), Ok(Vec::new()));

        let deployed = Deployed {
            dir: PathBuf::new(),
            routes: routes.clone(),
        };
        let moved = deployed.moved("a.local", "C.local").unwrap();
        assert_eq!(moved.routes[0].host, "c.local");
        assert_eq!(
            (&moved.routes[0].rootfs, &moved.routes[0].deployed),
            (&routes[0].rootfs, &routes[0].deployed)
        );
        assert_eq!(moved.routes[1], routes[1]);
        assert_eq!(
            deployed.moved("a.local", "b.local"),
            Err(Refused::Taken(
                "b.local is a route of the console already".into()
            ))
        );
        assert!(matches!(
            deployed.moved("x.local", "c.local"),
            Err(Refused::Unknown(_))
        ));
        // the same options again keep the route's time
        let again = deployed
            .with_options("b.local", routes[1].options.clone(), SystemTime::now())
            .unwrap();
        assert_eq!(again, deployed);

        let rootfs = image("0", "a.ext4");
        let route = |rest: &str| {
            format!("routes:\n  - {{host: a.local, rootfs: {rootfs}, deployed: x{rest}}}\n")
        };
        for (yaml, error) in [
            (
                route("").replace(&rootfs, "../a.ext4"),
                "routes[0]: rootfs: '../a.ext4' is not one of the console's",
            ),
            (route("").replace(".ext4", ".img"), "routes[0]: rootfs: "),
            (
                route(", mode: pool"),
                "routes[0]: the route: mode=pool needs vms",
            ),
            (
                route("").replace(", deployed: x", ""),
                "routes[0]: deployed: missing",
            ),
            (route(", fuel: 1"), "unknown field `fuel`"),
            (
                format!("{}{}", route(""), route("").replace("routes:\n", "")),
                "routes[1]: host 'a.local' twice",
            ),
        ] {
            let failed = read(&yaml);
            assert!(
                matches!(&failed, Err(e) if e.contains(error)),
                "{yaml}: {failed:?}"
            );
        }
    }
}
