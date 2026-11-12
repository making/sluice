//! The console's directory (`--console-dir`): the routes it deployed in a
//! manifest of the wicket's (`routes.yaml`), the files they name kept under
//! their digest, never rewritten:
//!
//! ```text
//! <dir>/routes.yaml                 the routes (the wicket's shape), for its owner only
//! <dir>/<kind>/<sha256>/<name>      the files they name: components, rootfs images
//! <dir>/uploads/<sha256><ext>       the files uploaded by digest, for UPLOAD_GRACE
//! <dir>/wicket-id                   the wicket's id, made at its first start (`identity`)
//! <dir>/epochs.json                 the highest epoch each owner sent (`fence`)
//! ```
//!
//! A file's path names its digest, so other content is another target: its
//! route is retargeted, and the old target drains as a removed one does.
//! Every write is a rename, so no reader sees a file half written; a partial
//! one is hidden (its name starts with a dot) and removed by the next
//! [`Blobs::collect_garbage`].

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::http::Received;

/// Longest file name a file is kept as, and longest name of an app or an
/// owner, in bytes.
pub const MAX_NAME: usize = 100;

/// Longest tag of an app, in bytes.
const MAX_TAG: usize = 128;

/// The uploads' directory in the directory.
pub const UPLOADS: &str = "uploads";

/// How long an upload is kept for an app to name it: minutes, so the PUT
/// naming it after the upload never races its removal.
pub const UPLOAD_GRACE: Duration = Duration::from_secs(10 * 60);

/// Removes the uploads of `dir` kept for [`UPLOAD_GRACE`], and any partial one
/// a crash left: an app naming one links it below its kind's directory before.
pub fn collect_uploads(dir: &Path) {
    for upload in std::fs::read_dir(dir.join(UPLOADS))
        .into_iter()
        .flatten()
        .flatten()
    {
        // a time ahead of the clock is fresh
        let fresh = upload.metadata().is_ok_and(|metadata| {
            metadata.is_file()
                && metadata
                    .modified()
                    .is_ok_and(|at| at.elapsed().is_ok_and(|age| age <= UPLOAD_GRACE))
        });
        if !fresh || hidden(&upload.path()) {
            remove(&upload.path());
        }
    }
}

/// The files of one kind kept below `<dir>/<kind>/<sha256>/<name>`.
pub struct Blobs<'a> {
    pub dir: &'a Path,
    /// `components`, `rootfs`.
    pub kind: &'static str,
}

impl Blobs<'_> {
    /// Writes `bytes` as `<kind>/<sha256>/<name>` unless it is there as it
    /// is; its path relative to the directory.
    pub fn store(&self, name: &str, bytes: &[u8]) -> Result<String, String> {
        let relative = format!("{}/{}/{name}", self.kind, digest(bytes));
        let path = self.dir.join(&relative);
        if std::fs::read(&path).is_ok_and(|kept| kept == bytes) {
            return Ok(relative);
        }
        write_by_rename(&path, bytes, false)?;
        Ok(relative)
    }

    /// Keeps `received` as `<kind>/<sha256>/<name>` (dropped when a file is
    /// there: of the same digest); its path relative to the directory.
    pub fn keep(&self, name: &str, received: Received) -> Result<String, String> {
        let relative = format!("{}/{}/{name}", self.kind, received.sha256);
        received.keep(&self.dir.join(&relative))?;
        Ok(relative)
    }

    /// Where [`receive`](crate::http::receive) writes an upload: hidden files
    /// of the kind's directory, on the file system they are kept on.
    pub fn intake(&self) -> PathBuf {
        self.dir.join(self.kind)
    }

    /// `<kind>/<sha256>/<file>`, made unless there from `upload` or another
    /// file of that digest: linked (the same content, never rewritten), else
    /// copied, by a rename. None with neither.
    pub fn materialize(
        &self,
        sha256: &str,
        file: &str,
        upload: &Path,
    ) -> Result<Option<String>, String> {
        let relative = format!("{}/{sha256}/{file}", self.kind);
        let path = self.dir.join(&relative);
        if regular(&path) {
            return Ok(Some(relative));
        }
        let parent = path.parent().expect("below the kind's directory");
        let others = std::fs::read_dir(parent)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|other| !hidden(other));
        let Some(source) = std::iter::once(upload.to_path_buf())
            .chain(others)
            .find(|source| regular(source))
        else {
            return Ok(None);
        };
        let failed = |e: std::io::Error| format!("write {}: {e}", path.display());
        std::fs::create_dir_all(parent).map_err(failed)?;
        let partial = parent.join(format!(".{file}.partial"));
        let _ = std::fs::remove_file(&partial);
        if std::fs::hard_link(&source, &partial).is_err() {
            std::fs::copy(&source, &partial).map_err(failed)?;
        }
        std::fs::rename(&partial, &path).map_err(failed)?;
        Ok(Some(relative))
    }

    /// Removes the files below `<kind>/` but `named` (paths relative to the
    /// directory), partial ones included, and the directories they leave
    /// empty; at `startup` the uploads a crash cut short too (else they may
    /// be being received). Links are removed, never followed.
    pub fn collect_garbage<'a>(&self, named: impl IntoIterator<Item = &'a String>, startup: bool) {
        let named: HashSet<PathBuf> = named
            .into_iter()
            .map(|relative| self.dir.join(relative))
            .collect();
        let Ok(digests) = std::fs::read_dir(self.dir.join(self.kind)) else {
            return;
        };
        for digest in digests.flatten() {
            let path = digest.path();
            if !digest.file_type().is_ok_and(|kind| kind.is_dir()) {
                // an upload being received is hidden: removed by its own end
                if !hidden(&path) || startup {
                    remove(&path);
                }
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

/// Removes `path`, logging a failure.
pub fn remove(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        eprintln!("--console-dir: remove {}: {e}", path.display());
    }
}

/// Whether `path` is a regular file, not a link.
pub fn regular(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
}

/// Whether the file name of `path` starts with a dot: a partial write.
pub fn hidden(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with('.'))
}

/// Writes `bytes` to `path` by a rename (no reader sees it half written),
/// its directory made when missing; `private`, readable by its owner only
/// (0600) from its first byte on.
pub fn write_by_rename(path: &Path, bytes: &[u8], private: bool) -> Result<(), String> {
    use std::io::Write as _;
    let failed = |e: std::io::Error| format!("write {}: {e}", path.display());
    let parent = path.parent().expect("a file in a directory");
    std::fs::create_dir_all(parent).map_err(failed)?;
    let name = path.file_name().expect("a file").to_string_lossy();
    let partial = parent.join(format!(".{name}.partial"));
    // one a crash left keeps its mode: made anew
    let _ = std::fs::remove_file(&partial);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    options
        .open(&partial)
        .and_then(|mut file| file.write_all(bytes))
        .map_err(failed)?;
    std::fs::rename(&partial, path).map_err(failed)
}

/// The sha256 of `bytes`, lower case hex.
pub fn digest(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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

/// An app's hosts, sorted: at least one, each a host name ([`check_host`])
/// once.
pub fn check_hosts(hosts: &[String]) -> Result<Vec<String>, String> {
    if hosts.is_empty() {
        return Err("hosts: an app serves one host at least".into());
    }
    let mut checked = hosts
        .iter()
        .map(|host| check_host(host))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("hosts: {e}"))?;
    checked.sort();
    match checked.windows(2).find(|pair| pair[0] == pair[1]) {
        Some(twice) => Err(format!("hosts: '{}' twice", twice[0])),
        None => Ok(checked),
    }
}

/// Whether `name` names an app or an owner (`what`): 1 to [`MAX_NAME`] bytes
/// of `A-Z a-z 0-9 . _ -`, starting with a letter or a digit.
pub fn check_name(what: &str, name: &str) -> Result<(), String> {
    let named = (1..=MAX_NAME).contains(&name.len())
        && name.starts_with(|c: char| c.is_ascii_alphanumeric())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
    if named {
        Ok(())
    } else {
        Err(format!(
            "{what}: '{name}' is no name (1 to {MAX_NAME} of A-Z, a-z, 0-9, '.', '_' and '-', \
             starting with a letter or a digit)"
        ))
    }
}

/// Whether `tag` is an app's tag: 1 to [`MAX_TAG`] printable ASCII chars
/// but the space. Opaque to the wicket: its owner tells by it what it put
/// (the digest of an env it can't read back, say).
pub fn check_tag(tag: &str) -> Result<(), String> {
    if (1..=MAX_TAG).contains(&tag.len()) && tag.bytes().all(|byte| byte.is_ascii_graphic()) {
        Ok(())
    } else {
        Err(format!(
            "tag: '{tag}' is no tag (1 to {MAX_TAG} printable ASCII chars but the space)"
        ))
    }
}

/// `sha256` in lower case when it is a sha256 digest: 64 hex digits.
pub fn check_digest(sha256: &str) -> Result<String, String> {
    if sha256.len() == 64 && sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(sha256.to_ascii_lowercase())
    } else {
        Err(format!("'{sha256}' is no sha256 digest (64 hex digits)"))
    }
}

/// Whether `file` is a file name a file ending in `ext` (`.wasm`) is kept as
/// ([`file_name`]); `what` names the kind in the error.
pub fn check_file(file: &str, ext: &str, what: &str) -> Result<(), String> {
    if file_name(file, "", ext) == file {
        Ok(())
    } else {
        Err(format!(
            "'{file}' is no file name of {what} (A-Z, a-z, 0-9, '.', '_' and '-', \
             ending in {ext}, {MAX_NAME} bytes at most)"
        ))
    }
}

/// The file name an upload named `name` is kept as: its last path segment,
/// every char but `A-Z a-z 0-9 . _ -` replaced by `-` and leading dots
/// dropped, ending in `ext` (`.wasm`), [`MAX_NAME`] bytes at most;
/// `<host><ext>` for a name left empty.
pub fn file_name(name: &str, host: &str, ext: &str) -> String {
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
    let stem = clean.strip_suffix(ext).unwrap_or(clean);
    let stem = if stem.is_empty() { host } else { stem };
    // ASCII only: any byte is a char boundary
    let stem = &stem[..stem.len().min(MAX_NAME - ext.len())];
    format!("{stem}{ext}")
}

/// Whether `file` (relative to the directory) is one kept as a file of
/// `kind`: `<kind>/<sha256>/<name>` ending in `ext`; its digest.
pub fn check_kept(file: &str, kind: &str, ext: &str) -> Result<String, String> {
    let malformed =
        || format!("'{file}' is not one of the console's ({kind}/<sha256>/<name>{ext})");
    let rest = file
        .strip_prefix(kind)
        .and_then(|rest| rest.strip_prefix('/'))
        .ok_or_else(malformed)?;
    let (sha256, name) = rest.split_once('/').ok_or_else(malformed)?;
    let sha256 = check_digest(sha256).map_err(|_| malformed())?;
    if sha256 != rest[..64] || file_name(name, "", ext) != name {
        return Err(malformed());
    }
    Ok(sha256)
}

/// `text` as a YAML double-quoted scalar: never expanded.
pub fn quote(text: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_names_and_file_names_are_checked() {
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
            assert_eq!(file_name(name, "demo.local", ".wasm"), kept, "'{name}'");
            assert_eq!(
                file_name(&kept, "other", ".wasm"),
                kept,
                "'{kept}' is kept as it is"
            );
            assert!(
                check_file(&kept, ".wasm", "a component").is_ok(),
                "'{kept}'"
            );
        }
        for bad in ["hello", "my app.wasm", ".hello.wasm", "a/b.wasm", &long] {
            assert!(check_file(bad, ".wasm", "a component").is_err(), "'{bad}'");
        }
        assert_eq!(file_name("app.ext4", "a.local", ".ext4"), "app.ext4");
        assert_eq!(
            file_name("rootfs.img", "a.local", ".ext4"),
            "rootfs.img.ext4"
        );

        let sha256 = "0123456789abcdef".repeat(4);
        let kept = format!("rootfs/{sha256}/app.ext4");
        assert_eq!(check_kept(&kept, "rootfs", ".ext4"), Ok(sha256.clone()));
        for bad in [
            "../app.ext4".to_string(),
            format!("rootfs/{}/app.ext4", sha256.to_uppercase()),
            format!("rootfs/{sha256}/app.wasm"),
            format!("rootfs/{sha256}/a/app.ext4"),
            format!("components/{sha256}/app.ext4"),
        ] {
            let refused = check_kept(&bad, "rootfs", ".ext4");
            assert!(
                matches!(&refused, Err(e) if e.contains("is not one of the console's")),
                "'{bad}': {refused:?}"
            );
        }

        let guid = "0b9e2c6a-6c5e-4c1e-9d3f-2a1b0c9d8e7f";
        for name in [guid, "a", "cf-api", "Hello_World.2", &"x".repeat(MAX_NAME)] {
            assert_eq!(check_name("app", name), Ok(()), "'{name}'");
        }
        for bad in ["", "-a", ".a", "a b", "a/b", "ä", &"x".repeat(MAX_NAME + 1)] {
            assert!(check_name("app", bad).is_err(), "'{bad}'");
        }
        assert_eq!(
            check_hosts(&["B.local".into(), "a.local".into()]),
            Ok(vec!["a.local".into(), "b.local".into()]),
            "sorted, in lower case"
        );
        for (hosts, error) in [
            (vec![], "hosts: an app serves one host at least"),
            (
                vec!["a.local".into(), "A.local".into()],
                "hosts: 'a.local' twice",
            ),
            (vec!["a_b".into()], "hosts: 'a_b' is no host name"),
        ] {
            let checked = check_hosts(&hosts);
            assert!(
                matches!(&checked, Err(e) if e.starts_with(error)),
                "{hosts:?}: {checked:?}"
            );
        }
        assert_eq!(check_digest(&sha256.to_uppercase()), Ok(sha256.clone()));
        for bad in [&sha256[1..], &format!("{}g", &sha256[1..]), ""] {
            assert!(check_digest(bad).is_err(), "'{bad}'");
        }
        for tag in ["v1", &sha256, "a:b/c=\"d\"~", &"x".repeat(MAX_TAG)] {
            assert_eq!(check_tag(tag), Ok(()), "'{tag}'");
        }
        for bad in ["", "a b", "a\nb", "ä", &"x".repeat(MAX_TAG + 1)] {
            assert!(check_tag(bad).is_err(), "'{bad}'");
        }
    }
}
