//! The VMs' files: the state dir (`--state-dir`) and the tmpfs it should be on.

use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

/// The directory of the VMs' files: the process's own (0700) under
/// `$XDG_RUNTIME_DIR`, else the system's temp dir, removed at the end; or
/// `--state-dir`, where the routes' directories (`route-*`, 0700) are created
/// and removed at the end.
pub struct StateDir {
    pub dir: PathBuf,
    owned: bool,
}

impl StateDir {
    pub fn open(given: Option<PathBuf>) -> std::io::Result<StateDir> {
        let (dir, owned) = match given {
            // absolute: each firecracker runs in its VM's directory
            Some(dir) => {
                let dir = std::path::absolute(dir)?;
                std::fs::create_dir_all(&dir)?;
                (dir, false)
            }
            None => {
                // a tmpfs (systemd's per-user one) where there is one
                let base = std::env::var_os("XDG_RUNTIME_DIR")
                    .map(PathBuf::from)
                    .filter(|dir| dir.is_absolute() && dir.is_dir())
                    .unwrap_or_else(std::env::temp_dir);
                let dir = base.join(format!(
                    "sluice-wicket-runtime-firecracker-{}",
                    std::process::id()
                ));
                std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
                (dir, true)
            }
        };
        if !on_tmpfs(&dir) {
            eprintln!(
                "warning: {} is not on a tmpfs: a cloned route's snapshot (what its template \
                 wrote of its memory) is written to disk",
                dir.display()
            );
        }
        if let Some(stale) = routes(&dir)?.into_iter().next() {
            return Err(std::io::Error::other(format!(
                "{} exists: another wicket's, or one that did not end; remove it or choose another --state-dir",
                stale.display()
            )));
        }
        Ok(StateDir { dir, owned })
    }

    pub fn close(self) {
        if self.owned {
            let _ = std::fs::remove_dir_all(&self.dir);
            return;
        }
        for route in routes(&self.dir).unwrap_or_default() {
            let _ = std::fs::remove_dir_all(route);
        }
    }
}

/// Whether `dir` is on a tmpfs (or a ramfs): its files never reach a disk.
pub fn on_tmpfs(dir: &Path) -> bool {
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes()) else {
        return false;
    };
    let mut fs = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: a NUL-terminated path and a statfs buffer to fill
    if unsafe { libc::statfs(path.as_ptr(), fs.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: filled by the successful call
    let kind = unsafe { fs.assume_init() }.f_type;
    // ramfs: 0x858458f6, which libc does not name
    kind == libc::TMPFS_MAGIC || kind == 0x858458f6
}

/// The routes' directories in `dir`.
fn routes(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut routes = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with("route-") {
            routes.push(entry.path());
        }
    }
    Ok(routes)
}
