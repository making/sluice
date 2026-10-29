//! The routes the console deployed (`--console`): hosts served by uploaded
//! components, kept in the console's directory (`--console-dir`) so a restart
//! serves them again.
//!
//! ```text
//! <dir>/routes.yaml                      the routes, in the --config file's route shape
//! <dir>/components/<sha256>/<name>.wasm  the uploaded components, never rewritten
//! ```
//!
//! A component's path names its digest, so an upload of other content is
//! another target: its route is retargeted, and the old target drains as a
//! removed one does. The routes are held here, `routes.yaml` being their copy:
//! an edit is reloaded first (`routes::Reloader::apply`) and written once the
//! reload published it, so the file never names routes that did not load. A
//! component file no route names is removed after every edit and at startup.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::config::{SandboxFile, sandbox_yaml};
use crate::keyvalue::Stores;
use crate::routes::{Origin, Refused, Spec};
use crate::wasm_host::{Locator, Sandbox};

/// The routes' file in the directory.
const MANIFEST: &str = "routes.yaml";

/// The components' directory in the directory.
const COMPONENTS: &str = "components";

/// Longest file name a component is kept as, in bytes.
const MAX_NAME: usize = 100;

/// One of the console's routes.
#[derive(Clone, PartialEq, Debug)]
pub struct Route {
    pub host: String,
    /// The component file relative to the directory,
    /// `components/<sha256>/<name>`.
    pub component: String,
    /// Without a keyvalue map: with `wasi=keyvalue` the default one applies
    /// (the default store, the host as the namespace).
    pub sandbox: Sandbox,
    /// When it took this component and sandbox, RFC 3339.
    pub deployed: String,
}

/// The console's routes in the order deployed, and their directory.
#[derive(Clone, PartialEq, Debug)]
pub struct Deployed {
    dir: PathBuf,
    routes: Vec<Route>,
}

impl Deployed {
    /// The routes kept in `dir`, created when missing; removes the component
    /// files no route names (an upload a crash cut short included).
    pub fn open(dir: &Path) -> Result<Deployed, String> {
        let failed = |e: std::io::Error| format!("--console-dir {}: {e}", dir.display());
        std::fs::create_dir_all(dir.join(COMPONENTS)).map_err(failed)?;
        let dir = dir.canonicalize().map_err(failed)?;
        let manifest = dir.join(MANIFEST);
        let routes = match std::fs::read_to_string(&manifest) {
            Ok(text) => read(&text).map_err(|e| format!("{}: {e}", manifest.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(format!("{}: {e}", manifest.display())),
        };
        let deployed = Deployed { dir, routes };
        deployed.collect_garbage();
        Ok(deployed)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The routes as specs; a keyvalue route gets the default map of `stores`.
    pub fn specs(&self, stores: &Stores) -> Vec<Spec> {
        self.routes
            .iter()
            .map(|route| {
                let mut sandbox = route.sandbox.clone();
                if sandbox.caps.keyvalue {
                    sandbox.keyvalue = stores.default_map(&route.host);
                }
                Spec {
                    host: route.host.clone(),
                    component: Locator::File(self.dir.join(&route.component)),
                    sandbox,
                    origin: Origin::Console {
                        deployed: route.deployed.clone(),
                    },
                }
            })
            .collect()
    }

    /// The routes with `host` served by `component`, uploaded as `name`, in
    /// `sandbox` (without a keyvalue map) since `now`; the component's file
    /// written. The same component and sandbox again change nothing.
    pub fn deploy(
        &self,
        host: &str,
        name: &str,
        component: &[u8],
        sandbox: Sandbox,
        now: SystemTime,
    ) -> Result<Deployed, Refused> {
        debug_assert!(sandbox.keyvalue.is_empty(), "the default map applies");
        let host = check_host(host).map_err(Refused::Invalid)?;
        let component = self
            .store(&file_name(name, &host), component)
            .map_err(Refused::Failed)?;
        Ok(self.with(Route {
            host,
            component,
            sandbox,
            deployed: rfc3339(now),
        }))
    }

    /// The routes with the route of `host` in `sandbox` since `now`.
    pub fn with_sandbox(
        &self,
        host: &str,
        sandbox: Sandbox,
        now: SystemTime,
    ) -> Result<Deployed, Refused> {
        debug_assert!(sandbox.keyvalue.is_empty(), "the default map applies");
        let route = self.route(host)?;
        Ok(self.with(Route {
            sandbox,
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

    /// The routes with the route of `host` moved to `to` as it is: its
    /// component, sandbox and time, in its place. A move replaces no route of
    /// the console.
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
    /// as it was, its time included, when the component and the sandbox are
    /// the same -- or added last.
    fn with(&self, route: Route) -> Deployed {
        let mut deployed = self.clone();
        match deployed.routes.iter_mut().find(|r| r.host == route.host) {
            Some(r) if r.component == route.component && r.sandbox == route.sandbox => {}
            Some(r) => *r = route,
            None => deployed.routes.push(route),
        }
        deployed
    }

    /// Writes `component` as `components/<sha256>/<name>` unless it is there
    /// as it is, by a rename (no reader sees it half written); its path
    /// relative to the directory.
    fn store(&self, name: &str, component: &[u8]) -> Result<String, String> {
        use sha2::Digest as _;
        let digest: String = sha2::Sha256::digest(component)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let relative = format!("{COMPONENTS}/{digest}/{name}");
        let path = self.dir.join(&relative);
        if std::fs::read(&path).is_ok_and(|kept| kept == component) {
            return Ok(relative);
        }
        let failed = |e: std::io::Error| format!("write {}: {e}", path.display());
        let parent = path.parent().expect("below the components");
        std::fs::create_dir_all(parent).map_err(failed)?;
        let partial = parent.join(format!(".{name}.partial"));
        std::fs::write(&partial, component).map_err(failed)?;
        std::fs::rename(&partial, &path).map_err(failed)?;
        Ok(relative)
    }

    /// Writes the routes to `routes.yaml`, by a rename.
    pub fn save(&self) -> Result<(), String> {
        let manifest = self.dir.join(MANIFEST);
        let partial = self.dir.join(format!(".{MANIFEST}.partial"));
        let failed = |e: std::io::Error| format!("write {}: {e}", manifest.display());
        std::fs::write(&partial, write(&self.routes)).map_err(failed)?;
        std::fs::rename(&partial, &manifest).map_err(failed)
    }

    /// Removes the files below `components/` no route names, and the
    /// directories they leave empty. Links are removed, never followed.
    pub fn collect_garbage(&self) {
        let named: HashSet<PathBuf> = self
            .routes
            .iter()
            .map(|route| self.dir.join(&route.component))
            .collect();
        let remove = |path: &Path| {
            if let Err(e) = std::fs::remove_file(path) {
                eprintln!("--console-dir: remove {}: {e}", path.display());
            }
        };
        let Ok(digests) = std::fs::read_dir(self.dir.join(COMPONENTS)) else {
            return;
        };
        for digest in digests.flatten() {
            let path = digest.path();
            if !digest.file_type().is_ok_and(|kind| kind.is_dir()) {
                remove(&path);
                continue;
            }
            for file in std::fs::read_dir(&path).into_iter().flatten().flatten() {
                let kind = file.file_type();
                if kind.is_ok_and(|kind| !kind.is_dir()) && !named.contains(&file.path()) {
                    remove(&file.path());
                }
            }
            // only once empty
            let _ = std::fs::remove_dir(&path);
        }
    }
}

/// `host` in lower case when it is a host name: dot-separated labels of
/// `a-z 0-9 -`, each 1 to 63 bytes and neither starting nor ending with `-`,
/// 253 bytes at most.
pub fn check_host(host: &str) -> Result<String, String> {
    let host = host.to_ascii_lowercase();
    let label = |label: &str| {
        (1..=63).contains(&label.len())
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    };
    if host.len() <= 253 && host.split('.').all(label) {
        Ok(host)
    } else {
        Err(format!(
            "'{host}' is no host name (dot-separated labels of a-z, 0-9 and '-')"
        ))
    }
}

/// The file name an upload named `name` is kept as: its last path segment,
/// every char but `A-Z a-z 0-9 . _ -` replaced by `-` and leading dots
/// dropped, ending in `.wasm`, [`MAX_NAME`] bytes at most; `<host>.wasm` for
/// a name left empty.
fn file_name(name: &str, host: &str) -> String {
    let last = name.rsplit(['/', '\\']).next().unwrap_or_default();
    let clean: String = last
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let clean = clean.trim_start_matches('.');
    let stem = clean.strip_suffix(".wasm").unwrap_or(clean);
    let stem = if stem.is_empty() { host } else { stem };
    // ASCII only: any byte is a char boundary
    let stem = &stem[..stem.len().min(MAX_NAME - ".wasm".len())];
    format!("{stem}.wasm")
}

/// `time` as an RFC 3339 UTC instant to the second (`2026-10-29T12:34:56Z`).
pub fn rfc3339(time: SystemTime) -> String {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // civil-from-days (H. Hinnant), valid for every date after the epoch
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

/// `routes.yaml`: the routes in the shape of the `--config` file's, each with
/// the time it was deployed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFile {
    #[serde(default)]
    routes: Vec<ManifestRoute>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRoute {
    host: String,
    component: String,
    deployed: String,
    #[serde(default)]
    sandbox: SandboxFile,
}

/// The routes of `routes.yaml`; errors name the route.
fn read(text: &str) -> Result<Vec<Route>, String> {
    let file: ManifestFile = serde_saphyr::from_str(text).map_err(|e| {
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
        let component = check_component(&route.component).map_err(failed)?;
        let (sandbox, map) = route.sandbox.split().map_err(failed)?;
        if map.is_some() {
            return Err(failed(
                "sandbox.keyvalue: the console's routes take the default map".into(),
            ));
        }
        routes.push(Route {
            host,
            component,
            sandbox,
            deployed: route.deployed,
        });
    }
    Ok(routes)
}

/// `component` when it names a file as [`Deployed::store`] writes it.
fn check_component(component: &str) -> Result<String, String> {
    let parts: Vec<&str> = component.split('/').collect();
    match parts.as_slice() {
        [COMPONENTS, digest, name]
            if digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                && file_name(name, "") == *name =>
        {
            Ok(component.to_string())
        }
        _ => Err(format!(
            "component: '{component}' is not one of the console's \
             ({COMPONENTS}/<sha256>/<name>.wasm)"
        )),
    }
}

/// `routes` as `routes.yaml`; every string double-quoted, so never expanded.
fn write(routes: &[Route]) -> String {
    let mut yaml = String::from(
        "# The routes the console of sluice-wicket-wasi (--console) deployed, written by it:\n\
         # read at its startup, overwritten by its next change.\n",
    );
    if routes.is_empty() {
        yaml.push_str("routes: []\n");
        return yaml;
    }
    yaml.push_str("routes:\n");
    for route in routes {
        yaml.push_str(&format!("  - host: {}\n", quote(&route.host)));
        yaml.push_str(&format!("    component: {}\n", quote(&route.component)));
        yaml.push_str(&format!("    deployed: {}\n", quote(&route.deployed)));
        if let Some(sandbox) = sandbox_yaml(&route.sandbox) {
            yaml.push_str(&format!("    sandbox: {sandbox}\n"));
        }
    }
    yaml
}

/// `text` as a YAML double-quoted scalar.
fn quote(text: &str) -> String {
    let mut quoted = String::from('"');
    for c in text.chars() {
        match c {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            c if c.is_control() => quoted.push_str(&format!("\\u{:04x}", c as u32)),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn hosts_and_file_names_are_checked() {
        assert_eq!(check_host("Demo.LocalHost"), Ok("demo.localhost".into()));
        let longest = [
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61),
        ]
        .join(".");
        assert_eq!(check_host(&longest), Ok(longest.clone()));
        let too_long = format!("{longest}d");
        for bad in [
            "",
            "a..b",
            ".a",
            "a.",
            "-a.b",
            "a-.b",
            "a_b",
            "a b",
            "ä.b",
            &"a".repeat(64),
            &too_long,
        ] {
            assert!(check_host(bad).is_err(), "'{bad}'");
        }
        let long = format!("{}.wasm", "x".repeat(200));
        for (name, kept) in [
            ("hello.wasm", "hello.wasm".to_string()),
            ("hello", "hello.wasm".into()),
            ("../../etc/passwd", "passwd.wasm".into()),
            ("C:\\guests\\my app.wasm", "my-app.wasm".into()),
            (".hidden.wasm", "hidden.wasm".into()),
            (".wasm", "wasm.wasm".into()),
            ("", "demo.local.wasm".into()),
            (&long, format!("{}.wasm", "x".repeat(MAX_NAME - 5))),
        ] {
            assert_eq!(file_name(name, "demo.local"), kept, "'{name}'");
            assert_eq!(file_name(&kept, "other"), kept, "'{kept}' is kept as it is");
        }
    }

    /// The routes file reads back as written; what the console never writes
    /// -- a component not its own, a keyvalue map, a host twice -- fails,
    /// naming the route. A move keeps a route as it is, in its place, and
    /// replaces none.
    #[test]
    fn the_routes_file_reads_back_as_written() {
        let routes = vec![
            Route {
                host: "a.local".into(),
                component: format!("components/{}/a.wasm", "0".repeat(64)),
                sandbox: Sandbox::default(),
                deployed: rfc3339(UNIX_EPOCH),
            },
            Route {
                host: "b.local".into(),
                component: format!("components/{}/b.wasm", "f".repeat(64)),
                sandbox: Sandbox::parse(
                    "budget-ms=250&instances=4&memory-mib=64&pool=2&wasi=cli,inherit-network,keyvalue",
                )
                .unwrap(),
                deployed: rfc3339(UNIX_EPOCH + Duration::from_secs(1_793_276_096)),
            },
        ];
        let deployed = Deployed {
            dir: PathBuf::new(),
            routes: routes.clone(),
        };
        let moved = deployed.moved("a.local", "C.local").unwrap();
        assert_eq!(moved.routes[0].host, "c.local");
        assert_eq!(
            (&moved.routes[0].component, &moved.routes[0].deployed),
            (&routes[0].component, &routes[0].deployed)
        );
        assert_eq!(moved.routes[1], routes[1]);
        assert_eq!(deployed.moved("a.local", "a.local"), Ok(deployed.clone()));
        for (to, refused) in [
            (
                "b.local",
                Refused::Taken("b.local is a route of the console already".into()),
            ),
            (
                "a_b",
                Refused::Invalid(
                    "'a_b' is no host name (dot-separated labels of a-z, 0-9 and '-')".into(),
                ),
            ),
        ] {
            assert_eq!(deployed.moved("a.local", to), Err(refused));
        }
        assert!(matches!(
            deployed.moved("x.local", "c.local"),
            Err(Refused::Unknown(_))
        ));

        let text = write(&routes);
        assert_eq!(read(&text), Ok(routes));
        assert!(
            text.contains("deployed: \"1970-01-01T00:00:00Z\""),
            "{text}"
        );
        assert!(
            text.contains("deployed: \"2026-10-29T12:14:56Z\""),
            "{text}"
        );
        assert_eq!(read(&write(&[])), Ok(Vec::new()));

        let component = format!("components/{}/a.wasm", "0".repeat(64));
        let route = |rest: &str| format!("  - {{host: a.local, deployed: x, {rest}}}\n");
        let cases = [
            (
                route("component: ../a.wasm"),
                "routes[0]: component: '../a.wasm' is not one of the console's".to_string(),
            ),
            (
                route(&format!("component: components/{}/a.wasm", "A".repeat(64))),
                "routes[0]: component: ".into(),
            ),
            (
                route(&format!("component: {component}/x.wasm")),
                "routes[0]: component: ".into(),
            ),
            (
                route(&format!(
                    "component: {component}, sandbox: {{wasi: [keyvalue], keyvalue: {{'': local}}}}"
                )),
                "routes[0]: sandbox.keyvalue: the console's routes take the default map".into(),
            ),
            (
                route(&format!(
                    "component: {component}, sandbox: {{pool: 2, instances: 1}}"
                )),
                "routes[0]: sandbox: instances=1 is below pool=2".into(),
            ),
            (
                route(&format!("component: {component}")).repeat(2),
                "routes[1]: host 'a.local' twice".into(),
            ),
            (
                route(&format!("component: {component}, fuel: 1")),
                "unknown field `fuel`".into(),
            ),
        ];
        for (routes, error) in cases {
            let failed = read(&format!("routes:\n{routes}"));
            assert!(
                matches!(&failed, Err(e) if e.contains(&error)),
                "{routes}: {failed:?}"
            );
        }
    }
}
