//! An OCI image's layers as a rootfs, without root: the layers are unpacked into a
//! directory as the wicket's user, every file readable by it, what the image says of each
//! file noted aside ([`Tree`]); `mkfs.ext4 -d` makes the file system from the directory, and
//! `debugfs` then gives each file its owner, mode and time. `mkfs.ext4 -d` alone keeps the
//! uid of the user running it (root's `root_owner` is the root directory's alone), fails on
//! a file it cannot read, and takes no tar before e2fsprogs 1.47.1.
//!
//! The same layers, `/init` and launch make the same bytes, whatever the host's clock
//! (`E2FSPROGS_FAKE_TIME`, a seeded UUID and hash seed, every inode's times set): a rebuild
//! has the digest the first build had, its target and its VMs stay.

use std::collections::BTreeMap;
use std::io::{Read, Write as _};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Serialize;

/// How deep the links a layer's path goes through may nest.
const MAX_LINKS: usize = 40;

/// The file system's own time: nothing of the build's in its bytes.
const FAKE_TIME: &str = "1";

/// The file system's blocks: `mkfs.ext4`'s default for what it makes here.
const BLOCK: u64 = 4096;

/// What the image says of a file.
#[derive(Clone, Debug, PartialEq)]
struct Meta {
    kind: Kind,
    uid: u32,
    gid: u32,
    /// The permission bits, setuid and setgid and sticky among them.
    mode: u32,
    /// Seconds since the epoch.
    mtime: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Directory,
    File,
    Link,
    /// `c`, `b` or `p`: made by `debugfs`, no file in the directory.
    Node(char, u32, u32),
}

/// A root file system being unpacked at `root`, what each file of it should be aside.
pub struct Tree {
    root: PathBuf,
    /// By the path below the root, no leading `/`.
    files: BTreeMap<String, Meta>,
    /// What the files take of the disk.
    bytes: u64,
    /// The most they may take.
    limit: u64,
}

/// What `/init` runs: the guest's `/sluice/init.json`.
#[derive(Serialize, Debug, PartialEq)]
pub struct Launch {
    pub argv: Vec<String>,
    pub env: Vec<String>,
    pub uid: u32,
    pub gid: u32,
    pub workdir: String,
    pub port: u16,
    pub restore: Vec<String>,
}

/// The part of an image's config the launch comes from.
#[derive(Default, Debug)]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub user: String,
    pub workdir: String,
    /// Files the launch runs, by their absolute paths, each with what runs it: an image
    /// without one is refused, as its process would fail at its start.
    pub needs: BTreeMap<String, String>,
}

impl Tree {
    /// An empty tree at `root` (made), its files `limit` bytes at most.
    pub fn new(root: &Path, limit: u64) -> Result<Tree, String> {
        std::fs::create_dir_all(root).map_err(|e| format!("{}: {e}", root.display()))?;
        Ok(Tree {
            root: root.to_path_buf(),
            files: BTreeMap::new(),
            bytes: 0,
            limit,
        })
    }

    /// Unpacks the layer `tar` over the tree: a whiteout (`.wh.<name>`) removes what lower
    /// layers put, an opaque one (`.wh..wh..opq`) all a directory held.
    pub fn apply(&mut self, tar: impl Read) -> Result<(), String> {
        let mut archive = tar::Archive::new(tar);
        let entries = archive.entries().map_err(|e| format!("the layer: {e}"))?;
        for entry in entries {
            let mut entry = entry.map_err(|e| format!("the layer: {e}"))?;
            let path = entry.path_bytes();
            let path = std::str::from_utf8(&path)
                .map_err(|_| "a name of the layer is no UTF-8".to_string())?
                .to_string();
            let path = relative(&path)?;
            if path.is_empty() {
                continue;
            }
            let (parent, name) = path.rsplit_once('/').unwrap_or(("", &path));
            if name == ".wh..wh..opq" {
                let dir = self.resolve(parent)?;
                let dir_name = self.relative_name(&dir);
                for child in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                    self.remove(&child.path(), &join(&dir_name, &child.file_name()))?;
                }
                self.forget_below(&dir_name);
                continue;
            }
            if let Some(hidden) = name.strip_prefix(".wh.") {
                let dir = self.resolve(parent)?;
                let dir_name = self.relative_name(&dir);
                self.remove(&dir.join(hidden), &join(&dir_name, hidden.as_ref()))?;
                continue;
            }
            self.unpack(&mut entry, parent, name)?;
        }
        Ok(())
    }

    fn unpack(
        &mut self,
        entry: &mut tar::Entry<'_, impl Read>,
        parent: &str,
        name: &str,
    ) -> Result<(), String> {
        // every entry takes room, millions of empty ones too
        self.bytes += BLOCK;
        self.within_limit()?;
        let header = entry.header();
        let meta = |kind| -> Result<Meta, String> {
            let number = |value: std::io::Result<u64>, what: &str| {
                value.map_err(|e| format!("{parent}/{name}: its {what}: {e}"))
            };
            Ok(Meta {
                kind,
                uid: number(header.uid(), "uid")? as u32,
                gid: number(header.gid(), "gid")? as u32,
                mode: header
                    .mode()
                    .map_err(|e| format!("{parent}/{name}: its mode: {e}"))?
                    & 0o7777,
                mtime: number(header.mtime(), "time")?,
            })
        };
        let dir = self.resolve(parent)?;
        let path = dir.join(name);
        let named = join(&self.relative_name(&dir), name.as_ref());
        let on_disk = std::fs::symlink_metadata(&path).ok();
        let failed = |e: std::io::Error| format!("{named}: {e}");
        let kind = header.entry_type();
        if kind.is_dir() {
            let meta = meta(Kind::Directory)?;
            if !on_disk.as_ref().is_some_and(|known| known.is_dir()) {
                self.remove(&path, &named)?;
                std::fs::create_dir(&path).map_err(failed)?;
                set_mode(&path, 0o700).map_err(failed)?;
            }
            self.files.insert(named, meta);
            return Ok(());
        }
        // anything else takes the place of what is there
        self.remove(&path, &named)?;
        // a sparse entry reads as the file it is, its holes zeros
        if kind.is_file() || kind.is_contiguous() || kind.is_gnu_sparse() {
            let meta = meta(Kind::File)?;
            let size = entry.size();
            self.bytes += size.div_ceil(BLOCK) * BLOCK;
            self.within_limit()?;
            let mut file = std::fs::File::create(&path).map_err(failed)?;
            set_mode(&path, 0o600).map_err(failed)?;
            std::io::copy(entry, &mut file).map_err(failed)?;
            self.files.insert(named, meta);
        } else if kind.is_symlink() {
            let meta = meta(Kind::Link)?;
            let target = entry
                .link_name()
                .map_err(failed)?
                .ok_or_else(|| format!("{named}: a link to nothing"))?;
            std::os::unix::fs::symlink(&target, &path).map_err(failed)?;
            self.files.insert(named, meta);
        } else if kind.is_hard_link() {
            let target = entry
                .link_name()
                .map_err(failed)?
                .ok_or_else(|| format!("{named}: a link to nothing"))?;
            let target = relative(&target.to_string_lossy())?;
            let (target_dir, target_name) = target.rsplit_once('/').unwrap_or(("", &target));
            let from = self.resolve(target_dir)?;
            let from_name = join(&self.relative_name(&from), target_name.as_ref());
            std::fs::hard_link(from.join(target_name), &path).map_err(failed)?;
            let meta = self
                .files
                .get(&from_name)
                .cloned()
                .ok_or_else(|| format!("{named}: a link to {from_name}, not in the layers"))?;
            self.files.insert(named, meta);
        } else if kind.is_character_special() || kind.is_block_special() || kind.is_fifo() {
            let letter = match () {
                () if kind.is_character_special() => 'c',
                () if kind.is_block_special() => 'b',
                () => 'p',
            };
            let number = |value: std::io::Result<Option<u32>>| {
                value.map(Option::unwrap_or_default).map_err(failed)
            };
            // a fifo's fields may be blank
            let (major, minor) = match letter {
                'p' => (0, 0),
                _ => (
                    number(header.device_major())?,
                    number(header.device_minor())?,
                ),
            };
            let meta = meta(Kind::Node(letter, major, minor))?;
            self.files.insert(named, meta);
        }
        Ok(())
    }

    /// The directory of the tree `path` names, through its links as they resolve below the
    /// root, made where missing (root's, 0755, as a runtime makes one).
    fn resolve(&mut self, path: &str) -> Result<PathBuf, String> {
        Ok(self.walk(path, true)?.expect("made where missing"))
    }

    /// Whether `path` names a file of the tree the user `uid` (of the group `gid`) may
    /// execute, through its links as they resolve below the root: nothing made.
    fn runs(&mut self, path: &str, uid: u32, gid: u32) -> bool {
        let Ok(Some(found)) = self.walk(path, false) else {
            return false;
        };
        let named = self.relative_name(&found);
        self.files.get(&named).is_some_and(|meta| {
            let bit = match () {
                () if uid == 0 => 0o111,
                () if meta.uid == uid => 0o100,
                () if meta.gid == gid => 0o010,
                () => 0o001,
            };
            meta.kind == Kind::File && meta.mode & bit != 0
        })
    }

    /// Where `path` leads in the tree, through its links as they resolve below the root: with
    /// `make` a directory, made where missing; else what is there, None where nothing is.
    fn walk(&mut self, path: &str, make: bool) -> Result<Option<PathBuf>, String> {
        let mut at = self.root.clone();
        let mut pending: Vec<String> = path
            .split('/')
            .filter(|part| !part.is_empty())
            .rev()
            .map(String::from)
            .collect();
        let mut links = 0;
        while let Some(part) = pending.pop() {
            if part == ".." {
                if at != self.root {
                    at.pop();
                }
                continue;
            }
            if part == "." {
                continue;
            }
            let next = at.join(&part);
            match std::fs::symlink_metadata(&next) {
                Ok(known) if known.is_symlink() => {
                    links += 1;
                    if links > MAX_LINKS {
                        return Err(format!("{path}: more than {MAX_LINKS} links"));
                    }
                    let target = std::fs::read_link(&next).map_err(|e| format!("{path}: {e}"))?;
                    if target.is_absolute() {
                        at = self.root.clone();
                    }
                    let parts: Vec<String> = target
                        .components()
                        .filter_map(|part| match part {
                            Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
                            Component::ParentDir => Some("..".to_string()),
                            _ => None,
                        })
                        .collect();
                    pending.extend(parts.into_iter().rev());
                }
                Ok(known) if known.is_dir() => at = next,
                Ok(_) if !make && pending.is_empty() => at = next,
                Ok(_) => return Err(format!("{path}: {part} is no directory")),
                Err(_) if !make => return Ok(None),
                Err(_) => {
                    std::fs::create_dir(&next).map_err(|e| format!("{path}: {e}"))?;
                    set_mode(&next, 0o700).map_err(|e| format!("{path}: {e}"))?;
                    let named = self.relative_name(&next);
                    self.files.insert(named, directory(0o755));
                    at = next;
                }
            }
        }
        Ok(Some(at))
    }

    /// `path`, below the root, as the notes name it.
    fn relative_name(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .expect("resolved below the root")
            .to_string_lossy()
            .into_owned()
    }

    /// Removes what `path` (`named` in the notes) holds, its notes too.
    fn remove(&mut self, path: &Path, named: &str) -> Result<(), String> {
        let failed = |e: std::io::Error| format!("{named}: {e}");
        self.files.remove(named);
        match std::fs::symlink_metadata(path) {
            // never followed
            Ok(known) if known.is_dir() => {
                std::fs::remove_dir_all(path).map_err(failed)?;
                self.forget_below(named);
                Ok(())
            }
            Ok(_) => std::fs::remove_file(path).map_err(failed),
            Err(_) => Ok(()),
        }
    }

    /// Forgets the notes of the files below the directory `named`, all but itself.
    fn forget_below(&mut self, named: &str) {
        let prefix = format!("{named}/");
        self.files.retain(|path, _| !path.starts_with(&prefix));
    }

    fn within_limit(&self) -> Result<(), String> {
        if self.bytes > self.limit {
            return Err(format!(
                "the image unpacks to more than {} MiB",
                self.limit >> 20
            ));
        }
        Ok(())
    }

    /// What `/init` runs for the image's `config`, its user and group looked up in the
    /// tree, listening on `port`.
    pub fn launch(
        &mut self,
        config: ImageConfig,
        port: u16,
        restore: Vec<String>,
    ) -> Result<Launch, String> {
        let mut argv = config.entrypoint;
        argv.extend(config.cmd);
        if argv.is_empty() {
            return Err("the image names nothing to run: no Entrypoint, no Cmd".into());
        }
        let etc = self.resolve("etc")?;
        // a regular file alone: a link of the image's may lead off the tree, on this host
        let table = |name: &str| {
            let path = etc.join(name);
            std::fs::symlink_metadata(&path)
                .is_ok_and(|file| file.is_file())
                .then(|| std::fs::read_to_string(&path).ok())
                .flatten()
                .unwrap_or_default()
        };
        let (passwd, group) = (table("passwd"), table("group"));
        let (user, group_name) = match config.user.split_once(':') {
            Some((user, group)) => (user, Some(group)),
            None => (config.user.as_str(), None),
        };
        let user = if user.is_empty() { "0" } else { user };
        let id = |table: &str, file: &str, name: &str, field: usize| -> Result<u32, String> {
            if let Ok(id) = name.parse() {
                return Ok(id);
            }
            table
                .lines()
                .map(|line| line.split(':').collect::<Vec<_>>())
                .find(|fields| fields[0] == name)
                .and_then(|fields| fields.get(field)?.parse().ok())
                .ok_or_else(|| format!("{name}: not in the image's /etc/{file}"))
        };
        let uid = id(&passwd, "passwd", user, 2)?;
        let of_uid = passwd
            .lines()
            .map(|line| line.split(':').collect::<Vec<_>>())
            .find(|fields| fields.get(2).and_then(|id| id.parse().ok()) == Some(uid));
        let gid = match group_name {
            Some(name) => id(&group, "group", name, 2)?,
            None => of_uid
                .as_ref()
                .and_then(|fields| fields.get(3)?.parse().ok())
                .unwrap_or(0),
        };
        for (path, why) in &config.needs {
            if !self.runs(path, uid, gid) {
                return Err(format!("the image has no {path} its user may run: {why}"));
            }
        }
        let mut env = config.env;
        if !env.iter().any(|pair| pair.starts_with("HOME=")) {
            let home = of_uid.as_ref().and_then(|fields| fields.get(5).copied());
            env.push(format!("HOME={}", home.unwrap_or("/")));
        }
        Ok(Launch {
            argv,
            env,
            uid,
            gid,
            workdir: if config.workdir.is_empty() {
                "/".into()
            } else {
                config.workdir
            },
            port,
            restore,
        })
    }

    /// Makes `out` an ext4 image of the tree, with `guest` as `/init` and `launch` as
    /// `/sluice/init.json`; its UUIDs from `seed`, and what else it holds from the tree
    /// alone. `mkfs` and `debugfs` are the tools.
    pub fn build(
        mut self,
        guest: &Path,
        launch: &Launch,
        seed: &[u8; 16],
        out: &Path,
        tools: &Tools,
    ) -> Result<(), String> {
        for dir in ["dev", "proc", "sys", "tmp", "etc", "sluice"] {
            self.resolve(dir)?;
        }
        let write = |tree: &mut Tree, dir: &str, name: &str, bytes: &[u8], mode: u32| {
            let at = tree.resolve(dir)?;
            let named = join(&tree.relative_name(&at), name.as_ref());
            let path = at.join(name);
            tree.remove(&path, &named)?;
            std::fs::write(&path, bytes).map_err(|e| format!("{named}: {e}"))?;
            tree.bytes += (bytes.len() as u64).div_ceil(BLOCK) * BLOCK + BLOCK;
            tree.files.insert(
                named,
                Meta {
                    kind: Kind::File,
                    uid: 0,
                    gid: 0,
                    mode,
                    mtime: 0,
                },
            );
            Ok::<(), String>(())
        };
        let init = std::fs::read(guest).map_err(|e| format!("{}: {e}", guest.display()))?;
        write(&mut self, "", "init", &init, 0o755)?;
        let json = serde_json::to_vec_pretty(launch).map_err(|e| e.to_string())?;
        write(&mut self, "sluice", "init.json", &json, 0o644)?;
        // /init puts the names of a route's egress before these lines; an image's own
        // is a runtime's to write
        let hosts = b"127.0.0.1 localhost sluice\n";
        write(&mut self, "etc", "hosts", hosts, 0o644)?;
        // empty: no name server, the names are in /etc/hosts. A program may need the file
        // (the JVM buildpack's link-local-dns helper fails the launch without it)
        write(&mut self, "etc", "resolv.conf", b"", 0o644)?;
        self.within_limit()?;

        // room for what the file system itself takes
        let size = (self.bytes + self.bytes * 3 / 10 + (32 << 20)).div_ceil(1 << 20) << 20;
        let inodes = self.files.len() as u64 * 5 / 4 + 1024;
        let file = std::fs::File::create(out).map_err(|e| format!("{}: {e}", out.display()))?;
        file.set_len(size)
            .map_err(|e| format!("{}: {e}", out.display()))?;
        drop(file);
        let uuid = uuid(seed);
        let mkfs = Command::new(&tools.mkfs)
            .args(["-q", "-F", "-b", "4096", "-m", "0", "-O", "^has_journal"])
            .args(["-L", "sluice-rootfs", "-U", &uuid])
            .args(["-N", &inodes.to_string()])
            .arg("-E")
            .arg(format!("hash_seed={uuid},root_owner=0:0"))
            .arg("-d")
            .arg(&self.root)
            .arg(out)
            .env("E2FSPROGS_FAKE_TIME", FAKE_TIME)
            .output()
            .map_err(|e| format!("{}: {e}", tools.mkfs.display()))?;
        if !mkfs.status.success() {
            return Err(format!(
                "mkfs.ext4: {}",
                String::from_utf8_lossy(&mkfs.stderr).trim()
            ));
        }
        let script = script(&self.files)?;
        let mut debugfs = Command::new(&tools.debugfs)
            .args(["-w", "-f", "-"])
            .arg(out)
            .env("E2FSPROGS_FAKE_TIME", FAKE_TIME)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("{}: {e}", tools.debugfs.display()))?;
        let mut stdin = debugfs.stdin.take().expect("piped");
        let writer = std::thread::spawn(move || stdin.write_all(script.as_bytes()));
        let output = debugfs
            .wait_with_output()
            .map_err(|e| format!("debugfs: {e}"))?;
        writer
            .join()
            .expect("no panic")
            .map_err(|e| format!("debugfs: {e}"))?;
        // it exits 0 whatever a command did: what it says of them is its stderr, past
        // its banner
        let said = String::from_utf8_lossy(&output.stderr);
        let errors: Vec<&str> = said.lines().skip(1).collect();
        if !output.status.success() || !errors.is_empty() {
            return Err(format!("debugfs: {}", errors.join("; ")));
        }
        // the VMs' ids read it in jail mode (`jail::unshareable`)
        set_mode(out, 0o644).map_err(|e| format!("{}: {e}", out.display()))
    }
}

/// The tools a build runs.
#[derive(Clone, Debug)]
pub struct Tools {
    pub mkfs: PathBuf,
    pub debugfs: PathBuf,
}

impl Tools {
    /// `name` of e2fsprogs: on the PATH, or where a distribution keeps it away from a user's.
    fn find_tool(name: &str) -> Result<PathBuf, String> {
        std::env::var_os("PATH")
            .iter()
            .flat_map(std::env::split_paths)
            .chain(["/usr/sbin".into(), "/sbin".into()])
            .map(|dir: PathBuf| dir.join(name))
            .find(|path| path.is_file())
            .ok_or_else(|| format!("{name} (e2fsprogs) is not on the PATH"))
    }

    /// `mkfs.ext4`, what a disk is made with.
    pub fn mkfs() -> Result<PathBuf, String> {
        Tools::find_tool("mkfs.ext4")
    }

    /// `mkfs.ext4` and `debugfs`.
    pub fn find() -> Result<Tools, String> {
        Ok(Tools {
            mkfs: Tools::mkfs()?,
            debugfs: Tools::find_tool("debugfs")?,
        })
    }
}

/// A directory the layers did not name.
fn directory(mode: u32) -> Meta {
    Meta {
        kind: Kind::Directory,
        uid: 0,
        gid: 0,
        mode,
        mtime: 0,
    }
}

/// The layer's `path` below the root: no leading `/` or `./`, never above it.
fn relative(path: &str) -> Result<String, String> {
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => return Err(format!("{path}: a path above the root")),
            part => parts.push(part),
        }
    }
    Ok(parts.join("/"))
}

fn join(dir: &str, name: &std::ffi::OsStr) -> String {
    let name = name.to_string_lossy();
    match dir {
        "" => name.into_owned(),
        dir => format!("{dir}/{name}"),
    }
}

fn set_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

/// `seed` as a UUID's text.
fn uuid(seed: &[u8; 16]) -> String {
    let hex: String = seed.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// `path` as a `debugfs` argument: quoted, a quote doubled (a backslash is one to it).
fn quoted(path: &str) -> Result<String, String> {
    if path.contains(['\n', '\r']) {
        return Err(format!("{path:?}: a name with a line break"));
    }
    Ok(format!("\"{}\"", path.replace('"', "\"\"")))
}

/// The `debugfs` commands giving each file what the image says of it: `mknod` names an
/// entry of the current directory, `sif` takes a path.
fn script(files: &BTreeMap<String, Meta>) -> Result<String, String> {
    let mut script = String::new();
    for (path, meta) in files {
        let absolute = quoted(&format!("/{path}"))?;
        let type_bits = match meta.kind {
            Kind::Directory => 0o040000,
            Kind::File => 0o100000,
            Kind::Link => 0o120000,
            Kind::Node('c', ..) => 0o020000,
            Kind::Node('b', ..) => 0o060000,
            Kind::Node(..) => 0o010000,
        };
        if let Kind::Node(letter, major, minor) = meta.kind {
            let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
            script.push_str(&format!("cd {}\n", quoted(&format!("/{parent}"))?));
            match letter {
                'p' => script.push_str(&format!("mknod {} p\n", quoted(name)?)),
                _ => script.push_str(&format!(
                    "mknod {} {letter} {major} {minor}\n",
                    quoted(name)?
                )),
            }
            script.push_str("cd /\n");
        }
        let mtime = meta.mtime;
        script.push_str(&format!(
            "sif {absolute} mode 0{:o}\n",
            type_bits | meta.mode
        ));
        script.push_str(&format!("sif {absolute} uid {}\n", meta.uid));
        script.push_str(&format!("sif {absolute} gid {}\n", meta.gid));
        // what -d copied of the unpacked file, its nanoseconds too, is the build's
        for time in ["mtime", "atime", "ctime"] {
            script.push_str(&format!("sif {absolute} {time} @{mtime}\n"));
            script.push_str(&format!("sif {absolute} {time}_extra 0\n"));
        }
    }
    Ok(script)
}

/// What `debugfs` says of the file `path` of `image`.
#[cfg(test)]
fn stat_of(tools: &Tools, image: &Path, path: &str) -> String {
    let output = Command::new(&tools.debugfs)
        .args(["-R", &format!("stat \"{path}\"")])
        .arg(image)
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory of the test's own, removed however it ends.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Scratch {
            let dir =
                std::env::temp_dir().join(format!("sluice-rootfs-{name}-{}", std::process::id()));
            std::fs::create_dir(&dir).unwrap();
            Scratch(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn layer(build: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        build(&mut builder);
        builder.into_inner().unwrap()
    }

    fn header(kind: tar::EntryType, mode: u32, uid: u64, size: u64) -> tar::Header {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(kind);
        header.set_mode(mode);
        header.set_uid(uid);
        header.set_gid(uid + 1);
        header.set_mtime(1_000_000);
        header.set_size(size);
        header
    }

    fn file(builder: &mut tar::Builder<Vec<u8>>, path: &str, mode: u32, uid: u64, body: &[u8]) {
        let mut header = header(tar::EntryType::Regular, mode, uid, body.len() as u64);
        builder.append_data(&mut header, path, body).unwrap();
    }

    fn dir(builder: &mut tar::Builder<Vec<u8>>, path: &str, mode: u32, uid: u64) {
        let mut header = header(tar::EntryType::Directory, mode, uid, 0);
        builder
            .append_data(&mut header, path, std::io::empty())
            .unwrap();
    }

    fn link(builder: &mut tar::Builder<Vec<u8>>, kind: tar::EntryType, path: &str, target: &str) {
        let mut header = header(kind, 0o777, 7, 0);
        builder.append_link(&mut header, path, target).unwrap();
    }

    /// The layers of the test: over the lower one's files, a whiteout, an opaque directory,
    /// a file through a link (merged /usr), an unreadable and a setuid file, a hard link, a
    /// name debugfs must quote, a fifo.
    fn tree(root: &Path) -> Tree {
        let lower = layer(|tar| {
            dir(tar, "etc/", 0o755, 0);
            link(
                tar,
                tar::EntryType::Symlink,
                "etc/resolv.conf",
                "../run/systemd/resolve/stub-resolv.conf",
            );
            file(
                tar,
                "etc/passwd",
                0o644,
                0,
                b"root:x:0:0::/root:/bin/sh\napp:x:1000:1001::/home/app:/bin/sh\n",
            );
            file(
                tar,
                "etc/group",
                0o644,
                0,
                b"root:x:0:\napp:x:1001:\nother:x:2002:\n",
            );
            dir(tar, "usr/", 0o755, 0);
            dir(tar, "usr/bin/", 0o755, 0);
            link(tar, tar::EntryType::Symlink, "bin", "usr/bin");
            dir(tar, "gone/", 0o755, 0);
            file(tar, "gone/file", 0o644, 0, b"x");
            dir(tar, "opaque/", 0o755, 0);
            file(tar, "opaque/old", 0o644, 0, b"x");
        });
        let upper = layer(|tar| {
            file(tar, "gone/.wh.file", 0o644, 0, b"");
            file(tar, "opaque/.wh..wh..opq", 0o644, 0, b"");
            file(tar, "opaque/new", 0o640, 5, b"new");
            file(tar, "bin/tool", 0o4755, 0, b"tool");
            dir(tar, "app/", 0o750, 1000);
            file(tar, "app/secret", 0o000, 1000, b"secret");
            link(tar, tar::EntryType::Link, "app/same", "app/secret");
            file(tar, "app/qu\"ote \\x2d", 0o644, 1000, b"odd");
            // a file with a hole, as a tar written from a disk has it: a sparse entry
            let holed = root.with_extension("holed");
            let file = std::fs::File::create(&holed).unwrap();
            file.set_len(1 << 20).unwrap();
            std::os::unix::fs::FileExt::write_all_at(&file, b"end", (1 << 20) - 3).unwrap();
            file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000))
                .unwrap();
            tar.append_path_with_name(&holed, "app/holed").unwrap();
            std::fs::remove_file(&holed).unwrap();
            let mut fifo = header(tar::EntryType::Fifo, 0o600, 1000, 0);
            tar.append_data(&mut fifo, "app/pipe", std::io::empty())
                .unwrap();
        });
        let mut tree = Tree::new(root, 1 << 30).unwrap();
        tree.apply(&lower[..]).unwrap();
        tree.apply(&upper[..]).unwrap();
        tree
    }

    /// The layers unpack as a runtime would have them, without root and whatever a file's
    /// mode; the image holds each file's owner, mode and time, passes e2fsck, and comes out
    /// the same from a build of its own. What a layer may not do is refused, and a launch
    /// needing a file its user cannot run.
    #[test]
    fn layers_make_the_image_they_say() {
        let Ok(tools) = Tools::find() else {
            eprintln!("skipped: no e2fsprogs here");
            return;
        };
        let scratch = Scratch::new("build");
        let work = &scratch.0;
        let guest = work.join("guest");
        std::fs::write(&guest, b"#!guest\n").unwrap();
        let config = || ImageConfig {
            entrypoint: vec!["/bin/tool".into()],
            cmd: vec!["serve".into()],
            env: vec!["PATH=/bin".into()],
            user: "app:other".into(),
            workdir: "/app".into(),
            // through the image's link /bin
            needs: BTreeMap::from([("/bin/tool".into(), "it serves".into())]),
        };
        let mut digests = Vec::new();
        for n in 0..2 {
            let mut tree = tree(&work.join(format!("root{n}")));
            let launch = tree.launch(config(), 8080, Vec::new()).unwrap();
            assert_eq!(
                launch,
                Launch {
                    argv: vec!["/bin/tool".into(), "serve".into()],
                    env: vec!["PATH=/bin".into(), "HOME=/home/app".into()],
                    uid: 1000,
                    gid: 2002,
                    workdir: "/app".into(),
                    port: 8080,
                    restore: Vec::new(),
                }
            );
            let out = work.join(format!("{n}.ext4"));
            tree.build(&guest, &launch, &[7; 16], &out, &tools).unwrap();
            digests.push(std::fs::read(&out).unwrap());
            std::thread::sleep(std::time::Duration::from_millis(1100));
        }
        assert!(digests[0] == digests[1], "another build, other bytes");
        // a file the launch needs: missing, not one its user may run, no file; none made
        let mut tree = tree(&work.join("needs"));
        for missing in ["/bin/bash", "/app/secret", "/app", "/nowhere/bash"] {
            let mut needing = config();
            needing.needs = BTreeMap::from([(missing.into(), "a command's shell".into())]);
            assert_eq!(
                tree.launch(needing, 8080, Vec::new()).unwrap_err(),
                format!("the image has no {missing} its user may run: a command's shell")
            );
        }
        assert!(!work.join("needs/nowhere").exists());
        let image = work.join("0.ext4");
        let fsck = Command::new("/usr/sbin/e2fsck")
            .args(["-fn"])
            .arg(&image)
            .output();
        if let Ok(fsck) = fsck {
            assert!(
                fsck.status.success(),
                "{}",
                String::from_utf8_lossy(&fsck.stdout)
            );
        }
        for (path, kind, mode, uid, gid) in [
            ("/usr/bin/tool", "regular", "04755", 0, 1),
            ("/app", "directory", "0750", 1000, 1001),
            ("/app/secret", "regular", "0000", 1000, 1001),
            ("/app/same", "regular", "0000", 1000, 1001),
            ("/app/qu\"\"ote \\x2d", "regular", "0644", 1000, 1001),
            ("/app/pipe", "FIFO", "0600", 1000, 1001),
            ("/bin", "symlink", "0777", 7, 8),
            ("/init", "regular", "0755", 0, 0),
            ("/tmp", "directory", "0755", 0, 0),
            ("/etc/hosts", "regular", "0644", 0, 0),
            // the image's link is replaced; a runtime's file, read by the JVM buildpack's helper
            ("/etc/resolv.conf", "regular", "0644", 0, 0),
        ] {
            let stat = stat_of(&tools, &image, path);
            for expected in [
                format!("Type: {kind} "),
                format!("Mode:  {mode} "),
                format!("User: {uid:>5}   Group: {gid:>5}"),
            ] {
                assert!(stat.contains(&expected), "{path}, {expected}: {stat}");
            }
        }
        assert!(
            stat_of(&tools, &image, "/usr/bin/tool").contains("mtime: 0x000f4240"),
            "the layer's time"
        );
        for gone in ["/gone/file", "/opaque/old", "/opaque/.wh..wh..opq"] {
            assert!(stat_of(&tools, &image, gone).is_empty(), "{gone}");
        }
        assert!(!stat_of(&tools, &image, "/opaque/new").is_empty());
        assert!(
            stat_of(&tools, &image, "/app/holed").contains("Size: 1048576"),
            "a sparse entry is a file"
        );

        for (build, error) in [
            (
                layer(|tar| {
                    let mut header = header(tar::EntryType::Regular, 0o644, 0, 1);
                    // the builder refuses to write such a path: as a hostile layer has it
                    header.as_gnu_mut().unwrap().name[..7].copy_from_slice(b"../evil");
                    header.set_cksum();
                    tar.append(&header, &b"x"[..]).unwrap();
                }),
                "a path above the root",
            ),
            (
                layer(|tar| file(tar, "big", 0o644, 0, &[0; 8192])),
                "unpacks to more than",
            ),
        ] {
            let scratch = Scratch::new("refused");
            let mut tree = Tree::new(&scratch.0, 4096).unwrap();
            let refused = tree.apply(&build[..]).unwrap_err();
            assert!(refused.contains(error), "{refused}");
        }
        // a link cannot lead a layer out of the root
        let scratch = Scratch::new("links");
        let root = &scratch.0;
        let outside = root.with_extension("outside");
        let mut tree = Tree::new(root, 1 << 20).unwrap();
        let escaping = layer(|tar| {
            let target = outside.to_string_lossy();
            link(tar, tar::EntryType::Symlink, "out", &target);
            file(tar, "out/x", 0o644, 0, b"x");
            link(tar, tar::EntryType::Symlink, "up", "../../..");
            file(tar, "up/y", 0o644, 0, b"y");
        });
        tree.apply(&escaping[..]).unwrap();
        // nor is a table read through one
        let tables = layer(|tar| {
            link(tar, tar::EntryType::Symlink, "etc/passwd", "/etc/passwd");
        });
        tree.apply(&tables[..]).unwrap();
        let config = ImageConfig {
            entrypoint: vec!["x".into()],
            user: "root".into(),
            ..ImageConfig::default()
        };
        assert_eq!(
            tree.launch(config, 8080, Vec::new()).unwrap_err(),
            "root: not in the image's /etc/passwd"
        );
        assert!(!outside.exists());
        assert!(
            root.join(outside.strip_prefix("/").unwrap())
                .join("x")
                .exists()
        );
        assert!(root.join("y").exists());
    }
}
