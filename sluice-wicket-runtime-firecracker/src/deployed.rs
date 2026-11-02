//! The routes and apps the console deployed (`--console`): hosts served by
//! uploaded rootfs images, kept in the console's directory (`--console-dir`) so
//! a restart serves them again (`sluice-wicket-console`'s layout). A route is
//! the page's: one host, its image uploaded with it. An app is the API's (a
//! controller's): hosts served by one image in one set of options with an env,
//! its image uploaded by digest first, changed by its owner only.
//!
//! ```text
//! <dir>/routes.yaml                   the routes and the apps, in the --config file's route shape
//! <dir>/rootfs/<sha256>/<name>.ext4   the images they name, never rewritten
//! <dir>/uploads/<sha256>.ext4         the images uploaded by digest, or made (`image`), for UPLOAD_GRACE
//! <dir>/builds/                       what the images being made take, removed at startup
//! <dir>/disks/<owner>/<name>.ext4     the apps' disks (`disk`), kept by the app's name
//! ```
//!
//! An image's path names its digest, so other content is another target: its
//! route's VMs start anew, and the old target drains as a removed one does.
//! The routes and apps are held here, `routes.yaml` being their copy: an edit
//! is reloaded first (`routes::Reloader::edit`) and written once the reload
//! published it, so the file never names routes that did not start. It holds
//! the apps' env, so only its owner reads it (0600). An image nothing names is
//! removed after every edit and at startup, an upload once `UPLOAD_GRACE`
//! passed: an app naming it links it below `rootfs/` before. A disk stays as the
//! app goes, its next one of that name and owner finding it, until it is removed
//! ([`Deployed::remove_disk`]).

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::Deserialize;
use sluice_wicket_console::env::Env;
use sluice_wicket_console::http::Received;
use sluice_wicket_console::store::{
    Blobs, UPLOADS, check_digest, check_file, check_host, check_hosts, check_kept, check_name,
    check_tag, collect_uploads, file_name, hidden, quote, rfc3339, write_by_rename,
};

use crate::config::{FileRoute, Origin};
use crate::disk;
use crate::route::RouteSpec;
use crate::routes::Refused;

/// The routes' file in the directory.
const MANIFEST: &str = "routes.yaml";

/// The images' directory in the directory.
const ROOTFS: &str = "rootfs";

/// What the images being made take, in the directory.
pub const BUILDS: &str = "builds";

/// The apps' disks' directory in the directory: apart from the images', which a
/// collection empties of what no route names.
const DISKS: &str = "disks";

/// The extension an image and a disk are kept with.
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

/// One app of the console's API: hosts served by one image in one set of
/// options, put and removed by its owner only.
#[derive(Clone, PartialEq, Debug)]
pub struct App {
    pub name: String,
    pub owner: String,
    /// Sorted, each once.
    pub hosts: Vec<String>,
    /// The image relative to the directory, `rootfs/<sha256>/<name>.ext4`.
    pub rootfs: String,
    pub options: Options,
    /// Its VMs' process's environment, over the image's.
    pub env: Env,
    /// The owner's, kept and listed as given ([`check_tag`]).
    pub tag: Option<String>,
    /// When it took this image, these options and this env, RFC 3339.
    pub deployed: String,
}

impl App {
    /// Whether its options name a disk.
    fn disk(&self) -> bool {
        self.options.iter().any(|(key, _)| key == "disk")
    }
}

/// A disk in the console's directory, as listed.
#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub struct KeptDisk {
    pub owner: String,
    /// Its app's name.
    pub name: String,
    pub mib: u64,
    /// Whether that app is there.
    pub app: bool,
}

/// An app as its PUT has it.
#[derive(Clone, PartialEq, Debug)]
pub struct AppSpec {
    pub owner: String,
    pub hosts: Vec<String>,
    /// The image's digest.
    pub sha256: String,
    /// The file name the image is kept as.
    pub file: String,
    pub options: Options,
    pub env: Env,
    pub tag: Option<String>,
}

/// The console's routes in the order deployed, its apps in the order put, and
/// their directory.
#[derive(Clone, PartialEq, Debug)]
pub struct Deployed {
    dir: PathBuf,
    routes: Vec<Route>,
    apps: Vec<App>,
}

impl Deployed {
    /// The routes and apps kept in `dir`, created when missing; removes the
    /// images nothing names (an upload a crash cut short included).
    pub fn open(dir: &Path) -> Result<Deployed, String> {
        let failed = |e: std::io::Error| format!("--console-dir {}: {e}", dir.display());
        std::fs::create_dir_all(dir.join(ROOTFS)).map_err(failed)?;
        let dir = dir.canonicalize().map_err(failed)?;
        // a build a crash cut short
        if let Err(e) = std::fs::remove_dir_all(dir.join(BUILDS))
            && e.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("--console-dir: remove {BUILDS}: {e}");
        }
        let manifest = dir.join(MANIFEST);
        let (routes, apps) = match std::fs::read_to_string(&manifest) {
            Ok(text) => read(&text).map_err(|e| format!("{}: {e}", manifest.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(e) => return Err(format!("{}: {e}", manifest.display())),
        };
        let deployed = Deployed { dir, routes, apps };
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

    /// The routes, then the apps' (one per host), as specs of `firecracker` and
    /// `kernel`; an error names the route by its host, an app's by its name.
    pub fn specs(
        &self,
        firecracker: &Path,
        kernel: &Path,
    ) -> Result<Vec<(RouteSpec, Origin)>, String> {
        let spec = |named: String, host: &str, rootfs: &str, options: &Options| {
            let options = options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()));
            RouteSpec::new(
                &named,
                host,
                self.dir.join(rootfs),
                options,
                firecracker,
                kernel,
            )
        };
        let mut specs = Vec::new();
        for route in &self.routes {
            let named = format!("console route {}", route.host);
            let origin = Origin::Console {
                deployed: route.deployed.clone(),
            };
            let spec = spec(named, &route.host, &route.rootfs, &route.options)?;
            specs.push((spec, origin));
        }
        for app in &self.apps {
            for host in &app.hosts {
                let named = format!("app {}", app.name);
                let mut spec = spec(named, host, &app.rootfs, &app.options)?;
                spec.app = Some(app.name.clone());
                spec.vm.env = app.env.clone();
                if let Some(disk) = &mut spec.vm.disk {
                    disk.file = self.disk_file(&app.owner, &app.name);
                }
                let origin = Origin::Api {
                    name: app.name.clone(),
                    owner: app.owner.clone(),
                    tag: app.tag.clone(),
                    deployed: app.deployed.clone(),
                };
                specs.push((spec, origin));
            }
        }
        Ok(specs)
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

    /// Keeps `received` as the upload of `sha256`, its digest. An upload again
    /// takes the place of the one kept, so it is kept longer: a file of its own,
    /// as the one kept may be linked below `rootfs/`, whose time a route watches.
    pub fn upload(&self, sha256: &str, received: Received) -> Result<(), Refused> {
        let given = check_digest(sha256).map_err(Refused::Invalid)?;
        if received.sha256 != given {
            return Err(Refused::Invalid(format!(
                "the body's sha256 is {}, not {given}",
                received.sha256
            )));
        }
        let path = self.dir.join(UPLOADS).join(format!("{given}{EXT4}"));
        let _ = std::fs::remove_file(&path);
        received.keep(&path).map_err(Refused::Failed)
    }

    /// The apps with the app `name` as `app` has it since `now`: its image made
    /// from an upload or another file of its digest, in place of the app of
    /// that name -- which keeps its time when its image, options and env stay,
    /// its hosts and tag changing or not -- or added last. Another owner's app
    /// is refused, and so is an image of a digest not kept.
    pub fn with_app(&self, name: &str, app: AppSpec, now: SystemTime) -> Result<Deployed, Refused> {
        check_name("app", name).map_err(Refused::Invalid)?;
        check_name("owner", &app.owner).map_err(Refused::Invalid)?;
        let hosts = check_hosts(&app.hosts).map_err(Refused::Invalid)?;
        let sha256 = check_digest(&app.sha256)
            .map_err(|e| Refused::Invalid(format!("component.sha256: {e}")))?;
        check_file(&app.file, EXT4, "an image")
            .map_err(|e| Refused::Invalid(format!("component.name: {e}")))?;
        if let Some(tag) = &app.tag {
            check_tag(tag).map_err(Refused::Invalid)?;
        }
        if let Some(kept) = self.app(name)
            && kept.owner != app.owner
        {
            return Err(Refused::Taken(format!(
                "the app '{name}' is {}'s, not {}'s",
                kept.owner, app.owner
            )));
        }
        // before its VM is touched: a smaller file system does not start, nor does the old VM
        // go for it
        if let Some(wanted) = disk_mib(&app.options)
            && let Ok(kept) = std::fs::metadata(self.disk_file(&app.owner, name))
            && kept.len() > u64::from(wanted) << 20
        {
            return Err(Refused::Rejected(format!(
                "the disk of the app '{name}' is {} MiB, not {wanted}: a disk only grows",
                kept.len() >> 20
            )));
        }
        let upload = self.dir.join(UPLOADS).join(format!("{sha256}{EXT4}"));
        let rootfs = self
            .images()
            .materialize(&sha256, &app.file, &upload)
            .map_err(Refused::Failed)?
            .ok_or_else(|| Refused::Missing(sha256.clone()))?;
        let app = App {
            name: name.to_string(),
            owner: app.owner,
            hosts,
            rootfs,
            options: app.options,
            env: app.env,
            tag: app.tag,
            deployed: rfc3339(now),
        };
        let mut deployed = self.clone();
        match deployed.apps.iter_mut().find(|kept| kept.name == app.name) {
            Some(kept)
                if kept.rootfs == app.rootfs
                    && kept.options == app.options
                    && kept.env == app.env =>
            {
                kept.hosts = app.hosts;
                kept.tag = app.tag;
            }
            Some(kept) => *kept = app,
            None => deployed.apps.push(app),
        }
        Ok(deployed)
    }

    /// The apps without the app `name`, which is `owner`'s.
    pub fn without_app(&self, name: &str, owner: &str) -> Result<Deployed, Refused> {
        let kept = self
            .app(name)
            .ok_or_else(|| Refused::Unknown(format!("the console has no app '{name}'")))?;
        if kept.owner != owner {
            return Err(Refused::Taken(format!(
                "the app '{name}' is {}'s, not {owner}'s",
                kept.owner
            )));
        }
        let mut deployed = self.clone();
        deployed.apps.retain(|app| app.name != name);
        Ok(deployed)
    }

    fn app(&self, name: &str) -> Option<&App> {
        self.apps.iter().find(|app| app.name == name)
    }

    /// The disk of the app `name` of `owner`, by a path of checked names.
    fn disk_file(&self, owner: &str, name: &str) -> PathBuf {
        self.dir
            .join(DISKS)
            .join(owner)
            .join(format!("{name}{EXT4}"))
    }

    /// Removes the disk of the app `name` of `owner` that was: refused while an app has it,
    /// or a VM of one still holds it (a removed app's, draining).
    pub fn remove_disk(&self, name: &str, owner: &str) -> Result<(), Refused> {
        check_name("app", name).map_err(Refused::Invalid)?;
        check_name("owner", owner).map_err(Refused::Invalid)?;
        if self
            .app(name)
            .is_some_and(|app| app.owner == owner && app.disk())
        {
            return Err(Refused::Taken(format!(
                "the app '{name}' has that disk: remove it first"
            )));
        }
        let file = self.disk_file(owner, name);
        let failed = |e: std::io::Error| Refused::Failed(format!("{}: {e}", file.display()));
        if disk::held(&file).map_err(failed)? {
            return Err(Refused::Taken(format!(
                "a VM of the app '{name}' still holds that disk"
            )));
        }
        match std::fs::remove_file(&file) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Refused::Unknown(format!(
                "no disk of the app '{name}' of {owner}"
            ))),
            removed => removed.map_err(failed),
        }
    }

    /// Writes the routes and apps to `routes.yaml`, by a rename; for its
    /// owner's eyes only, as it holds the apps' env.
    pub fn save(&self) -> Result<(), String> {
        write_by_rename(
            &self.dir.join(MANIFEST),
            write(&self.routes, &self.apps).as_bytes(),
            true,
        )
    }

    /// Removes the images nothing names; at `startup` the uploads a crash cut
    /// short too; and the uploads kept for `UPLOAD_GRACE`.
    pub fn collect_garbage(&self, startup: bool) {
        let named = self
            .routes
            .iter()
            .map(|route| &route.rootfs)
            .chain(self.apps.iter().map(|app| &app.rootfs));
        self.images().collect_garbage(named, startup);
        collect_uploads(&self.dir);
    }
}

/// The size `options` ask of a disk, when they name one.
fn disk_mib(options: &Options) -> Option<u32> {
    let value = |name: &str| options.iter().find(|(key, _)| key == name);
    value("disk")?;
    let mib = value("disk-mib").and_then(|(_, mib)| mib.parse().ok());
    Some(mib.unwrap_or(disk::DEFAULT_MIB))
}

/// The disks kept in the directory `dir`, by owner and name: `app` tells whether an app has
/// each.
pub fn disks(dir: &Path, app: impl Fn(&str, &str) -> bool) -> Vec<KeptDisk> {
    let mut disks = Vec::new();
    for owner in std::fs::read_dir(dir.join(DISKS)).into_iter().flatten() {
        let Ok(owner) = owner else { continue };
        let owner_name = owner.file_name().to_string_lossy().into_owned();
        for file in std::fs::read_dir(owner.path()).into_iter().flatten() {
            let Ok(file) = file else { continue };
            let file_name = file.file_name().to_string_lossy().into_owned();
            let Some(name) = file_name
                .strip_suffix(EXT4)
                .filter(|_| !hidden(&file.path()))
            else {
                continue;
            };
            let Ok(metadata) = file.metadata() else {
                continue;
            };
            disks.push(KeptDisk {
                app: app(&owner_name, name),
                name: name.to_string(),
                owner: owner_name.clone(),
                mib: metadata.len() >> 20,
            });
        }
    }
    disks.sort_by(|a, b| (&a.owner, &a.name).cmp(&(&b.owner, &b.name)));
    disks
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
            "mode" | "egress" | "disk" => value.clone(),
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
/// the time it was deployed, and the apps.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFile {
    #[serde(default)]
    routes: Vec<FileRoute>,
    #[serde(default)]
    apps: Vec<ManifestApp>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestApp {
    name: String,
    owner: String,
    hosts: Vec<String>,
    rootfs: String,
    deployed: String,
    tag: Option<String>,
    /// As a `--vm` flag's query has them.
    #[serde(default)]
    options: BTreeMap<String, String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// The routes and apps of `routes.yaml`; errors name the route or app (and a
/// line, never the file's text: it holds the apps' env). A host is routed once
/// over both.
fn read(text: &str) -> Result<(Vec<Route>, Vec<App>), String> {
    let mut parsing = serde_saphyr::Options::default();
    parsing.with_snippet = false;
    let file: ManifestFile = serde_saphyr::from_str_with_options(text, parsing).map_err(|e| {
        let e = e.to_string();
        e.strip_prefix("error: ").unwrap_or(&e).to_string()
    })?;
    let mut routed = HashSet::new();
    let mut routes: Vec<Route> = Vec::with_capacity(file.routes.len());
    for (n, route) in file.routes.into_iter().enumerate() {
        let failed = |e: String| format!("routes[{n}]: {e}");
        let host = check_host(&route.host).map_err(failed)?;
        if !routed.insert(host.clone()) {
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
    let mut apps: Vec<App> = Vec::with_capacity(file.apps.len());
    for (n, app) in file.apps.into_iter().enumerate() {
        let failed = |e: String| format!("apps[{n}]: {e}");
        check_name("name", &app.name).map_err(failed)?;
        if apps.iter().any(|known| known.name == app.name) {
            return Err(failed(format!("app '{}' twice", app.name)));
        }
        check_name("owner", &app.owner).map_err(failed)?;
        let hosts = check_hosts(&app.hosts).map_err(failed)?;
        if let Some(host) = hosts.iter().find(|host| !routed.insert((*host).clone())) {
            return Err(failed(format!("host '{host}' twice")));
        }
        check_kept(&app.rootfs, ROOTFS, EXT4).map_err(|e| failed(format!("rootfs: {e}")))?;
        if let Some(tag) = &app.tag {
            check_tag(tag).map_err(failed)?;
        }
        let pairs: Vec<(String, String)> = app.options.into_iter().collect();
        let options = options(&hosts[0], &pairs).map_err(failed)?;
        let env = Env::new(app.env).map_err(|e| failed(format!("env: {e}")))?;
        apps.push(App {
            name: app.name,
            owner: app.owner,
            hosts,
            rootfs: app.rootfs,
            options,
            env,
            tag: app.tag,
            deployed: app.deployed,
        });
    }
    Ok((routes, apps))
}

/// `routes` and `apps` as `routes.yaml`; every string double-quoted, so never
/// expanded. The apps' section only with apps.
fn write(routes: &[Route], apps: &[App]) -> String {
    let mut yaml = String::from(
        "# The routes and apps the console of sluice-wicket-runtime-firecracker (--console) deployed, \
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
    if !apps.is_empty() {
        yaml.push_str("apps:\n");
    }
    let map = |pairs: Vec<(&str, &str)>| {
        let pairs: Vec<String> = pairs
            .into_iter()
            .map(|(key, value)| format!("{}: {}", quote(key), quote(value)))
            .collect();
        format!("{{{}}}", pairs.join(", "))
    };
    for app in apps {
        let hosts: Vec<String> = app.hosts.iter().map(|host| quote(host)).collect();
        yaml.push_str(&format!("  - name: {}\n", quote(&app.name)));
        yaml.push_str(&format!("    owner: {}\n", quote(&app.owner)));
        yaml.push_str(&format!("    hosts: [{}]\n", hosts.join(", ")));
        yaml.push_str(&format!("    rootfs: {}\n", quote(&app.rootfs)));
        yaml.push_str(&format!("    deployed: {}\n", quote(&app.deployed)));
        if let Some(tag) = &app.tag {
            yaml.push_str(&format!("    tag: {}\n", quote(tag)));
        }
        if !app.options.is_empty() {
            let options = app
                .options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()));
            yaml.push_str(&format!("    options: {}\n", map(options.collect())));
        }
        if !app.env.is_empty() {
            yaml.push_str(&format!("    env: {}\n", map(app.env.iter().collect())));
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
    /// one order; the routes file reads back as written, routes and apps, an app's
    /// env and tag included, and what the console never writes -- an image not its
    /// own, a host or an app twice, an option a route refuses, no time -- fails,
    /// naming the route or the app, never a value. A move keeps a route as it is, in
    /// its place, and replaces none.
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
            ("disk=data", "disk data: expected an absolute path"),
            ("disk=/proc/x", "disk /proc/x: /proc is the guest's own"),
            ("disk-mib=64", "disk-mib needs disk"),
            ("disk=/data&disk-mib=7", "disk-mib must be at least 8"),
            ("disk-mib=lots&disk=/data", "disk-mib=lots is not a number"),
            (
                "disk=/data&mode=reuse",
                "disk needs mode=shared: one VM writes",
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
        let apps = vec![
            App {
                name: "0b9e2c6a".into(),
                owner: "cf".into(),
                hosts: vec!["c.local".into(), "d.local".into()],
                rootfs: image("0", "c.ext4"),
                options: pairs("mode=pool&vms=2&egress=db.internal:5432,10.0.0.0/24:6379"),
                env: Env::new(BTreeMap::from([
                    ("GREETING".into(), "hello".into()),
                    (
                        "PEM".into(),
                        "-----BEGIN-----\n\"AA==\"\\\n-----END-----".into(),
                    ),
                    ("odd: name".into(), "${HOME}".into()),
                ]))
                .unwrap(),
                tag: Some("0123456789abcdef:\"#{}".into()),
                deployed: rfc3339(UNIX_EPOCH),
            },
            App {
                name: "plain".into(),
                owner: "cf".into(),
                hosts: vec!["e.local".into()],
                rootfs: image("e", "e.ext4"),
                options: Vec::new(),
                env: Env::default(),
                tag: None,
                deployed: rfc3339(UNIX_EPOCH),
            },
            App {
                name: "kept".into(),
                owner: "ops".into(),
                hosts: vec!["f.local".into()],
                rootfs: image("e", "e.ext4"),
                options: pairs("disk=/var/lib/app&disk-mib=64"),
                env: Env::default(),
                tag: None,
                deployed: rfc3339(UNIX_EPOCH),
            },
        ];
        let text = write(&routes, &apps);
        assert_eq!(read(&text), Ok((routes.clone(), apps)), "{text}");
        assert!(
            text.contains("egress: [\"db.internal:5432\", \"10.0.0.0/24:6379\"]"),
            "{text}"
        );
        assert_eq!(read(&write(&[], &[])), Ok(Default::default()));
        assert!(!write(&routes, &[]).contains("\napps:"), "no apps' section");

        let deployed = Deployed {
            dir: PathBuf::new(),
            routes: routes.clone(),
            apps: Vec::new(),
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
        let app = |rest: &str| {
            format!(
                "  - {{name: a, owner: o, hosts: [b.local], rootfs: {rootfs}, deployed: x{rest}}}\n"
            )
        };
        let apps = |rest: &str| format!("apps:\n{}", app(rest));
        for (yaml, error) in [
            (
                format!("{}{}", route(""), apps("").replace("b.local", "a.local")),
                "apps[0]: host 'a.local' twice",
            ),
            (
                format!(
                    "apps:\n{}{}",
                    app(""),
                    app("").replace("b.local", "c.local")
                ),
                "apps[1]: app 'a' twice",
            ),
            (
                apps("").replace("owner: o", "owner: ''"),
                "apps[0]: owner: '' is no name",
            ),
            (
                apps("").replace("[b.local]", "[]"),
                "apps[0]: hosts: an app serves one host at least",
            ),
            (
                apps("").replace(&rootfs, "../a.ext4"),
                "apps[0]: rootfs: '../a.ext4' is not one of the console's",
            ),
            (apps(", tag: 'a b'"), "apps[0]: tag: 'a b' is no tag"),
            (
                apps(", options: {mode: pool}"),
                "apps[0]: the route: mode=pool needs vms",
            ),
            (
                apps(", env: {'A=B': s3cr3t}"),
                "apps[0]: env: \"A=B\" is no variable name",
            ),
            // a line, never the file's text: it holds the apps' env
            (apps(", env: {K: s3cr3t}, fuel: 1"), "unknown field `fuel`"),
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
                matches!(&failed, Err(e) if e.contains(error) && !e.contains("s3cr3t")),
                "{yaml}: {failed:?}"
            );
        }
    }
}
