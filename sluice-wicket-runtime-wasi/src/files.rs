//! An app's files: a tar of regular files and directories, uploaded by digest
//! (`PUT /api/files/<sha256>`) and unpacked below the console's directory,
//! never rewritten; its instances see them read-only at `/`
//! (`wasi:filesystem`, `Sandbox::files`).
//!
//! ```text
//! <dir>/files/<sha256>/...                      the files of the tar of that digest
//! <dir>/files/.<sha256>.<n>.partial/            one being unpacked
//! ```
//!
//! A tar's entries are checked as they are unpacked: relative paths of plain
//! names (no `..`, no root), each once, regular files and directories only
//! (a link, a device or a fifo fails it), [`MAX_ENTRIES`] at most, unpacking
//! to [`MAX_BYTES`] at most. A file keeps its modification time, so what a
//! guest derives from it (an `ETag`) is the tar's. The files nothing names
//! are removed once [`UPLOAD_GRACE`] passed since their upload.

use std::collections::HashSet;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sluice_wicket_console::http::Received;
use sluice_wicket_console::store::UPLOAD_GRACE;

/// The files' directory in the console's directory.
pub const FILES: &str = "files";

/// Largest tar an upload may carry, and the most its files may unpack to.
pub const MAX_BYTES: u64 = 1 << 30;

/// The most entries (files and directories) a tar may hold.
pub const MAX_ENTRIES: usize = 100_000;

/// Unpacks `received`, a tar, as `<files>/<sha256>` unless it is there: then
/// it is kept as it is, its time renewed (the grace of an upload starts
/// again). Errors name what of the tar is refused.
pub fn keep(files: &Path, received: Received) -> Result<(), String> {
    static PARTIAL: AtomicU64 = AtomicU64::new(0);
    let sha256 = received.sha256.clone();
    let to = files.join(&sha256);
    if is_dir(&to) {
        return touch(&to);
    }
    let n = PARTIAL.fetch_add(1, Ordering::Relaxed);
    let partial = files.join(format!(".{sha256}.{n}.partial"));
    let archive = files.join(format!(".{sha256}.{n}.tar"));
    received.keep(&archive)?;
    let unpacked = std::fs::File::open(&archive)
        .map_err(|e| format!("read {}: {e}", archive.display()))
        .and_then(|tar| unpack(tar, &partial));
    let _ = std::fs::remove_file(&archive);
    if let Err(e) = unpacked {
        let _ = std::fs::remove_dir_all(&partial);
        return Err(e);
    }
    if let Err(e) = std::fs::rename(&partial, &to) {
        let _ = std::fs::remove_dir_all(&partial);
        // another upload of the digest came first
        if !is_dir(&to) {
            return Err(format!("write {}: {e}", to.display()));
        }
    }
    touch(&to)
}

/// Unpacks the tar `archive` into `to`, which it makes: none may be there.
pub fn unpack(archive: impl Read, to: &Path) -> Result<(), String> {
    std::fs::create_dir(to).map_err(|e| format!("write {}: {e}", to.display()))?;
    let mut archive = tar::Archive::new(archive);
    let entries = archive.entries().map_err(|e| format!("not a tar: {e}"))?;
    let mut count = 0;
    let mut left = MAX_BYTES;
    for entry in entries {
        let mut entry = entry.map_err(|e| format!("not a tar: {e}"))?;
        count += 1;
        if count > MAX_ENTRIES {
            return Err(format!("more than {MAX_ENTRIES} entries"));
        }
        let name = entry
            .path()
            .map_err(|e| format!("an entry's name: {e}"))?
            .into_owned();
        let shown = name.display().to_string();
        let relative = relative(&name).ok_or_else(|| {
            format!("'{shown}' is no relative path of plain names (no '..', no root)")
        })?;
        let path = to.join(&relative);
        let failed = |e: std::io::Error| format!("write {shown}: {e}");
        match entry.header().entry_type() {
            tar::EntryType::Directory => std::fs::create_dir_all(&path).map_err(failed)?,
            tar::EntryType::Regular | tar::EntryType::Continuous => {
                let size = entry.size();
                if size > left {
                    return Err(format!(
                        "the files unpack to more than {} MiB",
                        MAX_BYTES >> 20
                    ));
                }
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(failed)?;
                }
                // each once: a second entry of a name would replace what a guest saw
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(|e| match e.kind() {
                        std::io::ErrorKind::AlreadyExists => format!("'{shown}' twice"),
                        _ => failed(e),
                    })?;
                let copied = std::io::copy(&mut (&mut entry).take(size), &mut file)
                    .map_err(|e| format!("read {shown}: {e}"))?;
                if copied != size {
                    return Err(format!("{shown}: the tar ends within it"));
                }
                left -= size;
                let modified = entry.header().mtime().unwrap_or(0);
                file.set_modified(UNIX_EPOCH + Duration::from_secs(modified))
                    .map_err(failed)?;
            }
            kind => {
                return Err(format!(
                    "'{shown}' is a {kind:?}: only files and directories"
                ));
            }
        }
    }
    Ok(())
}

/// `name` when it is relative and of plain names only, at least one.
fn relative(name: &Path) -> Option<PathBuf> {
    let mut relative = PathBuf::new();
    for component in name.components() {
        match component {
            Component::Normal(part) => relative.push(part),
            Component::CurDir => {}
            _ => return None,
        }
    }
    relative.to_str()?;
    (!relative.as_os_str().is_empty()).then_some(relative)
}

/// Removes the files below `files` but those `named` (digests) and those
/// uploaded within [`UPLOAD_GRACE`]; at `startup` the ones a crash cut short
/// too (else they may be being unpacked).
pub fn collect_garbage(files: &Path, named: &HashSet<&str>, startup: bool) {
    let Ok(entries) = std::fs::read_dir(files) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let removed = if name.starts_with('.') {
            startup
        } else {
            !named.contains(name.as_str()) && !fresh(&path)
        };
        if !removed {
            continue;
        }
        let gone = match entry.file_type() {
            Ok(kind) if kind.is_dir() => std::fs::remove_dir_all(&path),
            _ => std::fs::remove_file(&path),
        };
        if let Err(e) = gone {
            eprintln!("--console-dir: remove {}: {e}", path.display());
        }
    }
}

/// Whether `path` is a directory, not a link.
pub fn is_dir(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir())
}

/// Whether `path` was uploaded within [`UPLOAD_GRACE`]: a time ahead of the
/// clock is.
fn fresh(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|at| at.elapsed().is_ok_and(|age| age <= UPLOAD_GRACE))
}

/// Renews the time of the directory `path`.
fn touch(path: &Path) -> Result<(), String> {
    std::fs::File::open(path)
        .and_then(|dir| dir.set_modified(SystemTime::now()))
        .map_err(|e| format!("write {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tar of `entries`: a directory where the content is None.
    fn tar(entries: &[(&str, Option<&[u8]>)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_mtime(1_700_000_000);
            header.set_mode(0o644);
            match content {
                Some(content) => {
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_size(content.len() as u64);
                    builder.append_data(&mut header, name, *content).unwrap();
                }
                None => {
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_size(0);
                    builder.append_data(&mut header, name, &[][..]).unwrap();
                }
            }
        }
        builder.into_inner().unwrap()
    }

    /// A tar's files and directories unpack with their times; what is no
    /// plain file or directory, a path out of the root, a name twice and
    /// what is no tar fail it, naming the entry.
    #[test]
    fn unpacks_files_and_directories_only() {
        let dir = std::env::temp_dir().join(format!("files-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let site = dir.join("site");
        let long = format!("assets/{}.js", "a".repeat(150));
        let archive = tar(&[
            ("css", None),
            ("index.html", Some(b"<p>hi")),
            ("./css/site.css", Some(b"p{}")),
            ("deep/er/a.js", Some(b"1")),
            // past the 100 bytes of a header's name: a GNU long name entry before it
            (&long, Some(b"long")),
        ]);
        unpack(&archive[..], &site).unwrap();
        assert_eq!(std::fs::read(site.join(&long)).unwrap(), b"long");
        assert_eq!(std::fs::read(site.join("index.html")).unwrap(), b"<p>hi");
        assert_eq!(std::fs::read(site.join("css/site.css")).unwrap(), b"p{}");
        assert_eq!(std::fs::read(site.join("deep/er/a.js")).unwrap(), b"1");
        let modified = std::fs::metadata(site.join("index.html"))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(modified, UNIX_EPOCH + Duration::from_secs(1_700_000_000));

        let refused = |archive: Vec<u8>| {
            let to = dir.join("refused");
            let _ = std::fs::remove_dir_all(&to);
            unpack(&archive[..], &to).unwrap_err()
        };
        let mut link = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        link.append_link(&mut header, "passwd", "/etc/passwd")
            .unwrap();
        assert!(refused(link.into_inner().unwrap()).contains("'passwd' is a Symlink"));
        // the builder refuses to write such a name: written into the header as is
        let mut escaping = tar(&[("xx/a", Some(b"x"))]);
        escaping[..4].copy_from_slice(b"../a");
        let mut header = tar::Header::from_byte_slice(&escaping[..512]).clone();
        header.set_cksum();
        escaping[..512].copy_from_slice(header.as_bytes());
        assert!(refused(escaping).contains("'../a' is no relative path"));
        assert!(refused(tar(&[("a", Some(b"1")), ("a", Some(b"2"))])).contains("'a' twice"));
        assert!(refused(b"not a tar, not even 512 bytes".to_vec()).contains("not a tar"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
