//! Build identification, computed by `build.rs` (which includes this file)
//! and embedded as `SLUICE_WICKET_COMMIT` / `SLUICE_WICKET_DIRTY` /
//! `SLUICE_WICKET_BUILD_TIME`. Nothing here may fail the build: without git
//! (or outside a work tree) the commit is empty and the tree counts as clean.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The git state of the work tree a build is made from.
#[derive(Debug, PartialEq)]
pub struct GitState {
    /// Full commit id of HEAD; empty when unknown.
    pub commit: String,
    /// Whether tracked files had uncommitted changes.
    pub dirty: bool,
}

/// Trimmed stdout of `<program> <args>` run in `dir`; `None` when the
/// program is missing, fails or prints nothing. `GIT_OPTIONAL_LOCKS=0` keeps
/// `git status` from rewriting the index, which would re-trigger the build
/// script through its `rerun-if-changed` on the index.
pub fn git(program: &str, dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(dir)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// HEAD's commit and the dirty flag (tracked files only, as `git.dirty` of
/// the Java modules' git.properties).
pub fn git_state(program: &str, dir: &Path) -> GitState {
    match git(program, dir, &["rev-parse", "HEAD"]) {
        Some(commit) => GitState {
            commit,
            dirty: git(
                program,
                dir,
                &["status", "--porcelain", "--untracked-files=no"],
            )
            .is_some(),
        },
        None => GitState {
            commit: String::new(),
            dirty: false,
        },
    }
}

/// The existing git files whose change means a new commit or a changed
/// index (`HEAD`, its reflog, the index, packed refs and the branch ref).
pub fn git_watch_paths(program: &str, dir: &Path) -> Vec<PathBuf> {
    let mut names = vec![
        "HEAD".to_string(),
        "logs/HEAD".to_string(),
        "index".to_string(),
        "packed-refs".to_string(),
    ];
    if let Some(branch) = git(program, dir, &["symbolic-ref", "-q", "HEAD"]) {
        names.push(branch);
    }
    names
        .iter()
        .filter_map(|name| git(program, dir, &["rev-parse", "--git-path", name]))
        .map(|path| dir.join(path))
        .filter(|path| path.exists())
        .collect()
}

/// `secs` since the Unix epoch as an RFC 3339 UTC instant
/// (`2026-10-12T01:04:33Z`).
pub fn rfc3339(secs: u64) -> String {
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
