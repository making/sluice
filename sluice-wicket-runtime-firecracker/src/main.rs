use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use sluice_wicket_runtime_firecracker::config::{self, Command};
use sluice_wicket_runtime_firecracker::vm;

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    if args.next().is_some_and(|first| first == vm::LAUNCH) {
        let error = vm::launch(args);
        eprintln!(
            "{} {}: {error}",
            vm::LAUNCH,
            std::env::args().nth(4).unwrap_or_default()
        );
        return ExitCode::from(127);
    }
    let settings = match config::parse(std::env::args().skip(1), |name| std::env::var(name).ok()) {
        Ok(Command::Help) => {
            println!("{}", config::USAGE);
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("{}", sluice_wicket_runtime_firecracker::describe_build());
            return ExitCode::SUCCESS;
        }
        Ok(Command::Run(settings)) => *settings,
        Err(message) => {
            eprintln!("{message}\n\n{}", config::USAGE);
            return ExitCode::from(2);
        }
    };
    let state = match StateDir::open(settings.state_dir.clone()) {
        Ok(state) => state,
        Err(e) => {
            eprintln!("--state-dir: {e}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ran = runtime.block_on(sluice_wicket_runtime_firecracker::run(
        settings,
        state.dir.clone(),
    ));
    // the tasks left are dropped, and with them the VMs they hold (killed)
    runtime.shutdown_timeout(Duration::from_secs(5));
    state.close();
    match ran {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sluice-wicket-runtime-firecracker failed: {e}");
            ExitCode::FAILURE
        }
    }
}

/// The directory of the VMs' files: the process's own (0700) under
/// `$XDG_RUNTIME_DIR`, else the system's temp dir, removed at the end; or
/// `--state-dir`, where the routes' directories (`route-*`, 0700) are created
/// and removed at the end.
struct StateDir {
    dir: PathBuf,
    owned: bool,
}

impl StateDir {
    fn open(given: Option<PathBuf>) -> std::io::Result<StateDir> {
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
                "warning: {} is not on a tmpfs: a per-connection route's snapshot (its template's \
                 whole memory) is written to disk",
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

    fn close(self) {
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
fn on_tmpfs(dir: &Path) -> bool {
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
