//! The build identification of sluice-flume and sluice-wicket-wasi, computed by
//! their build scripts ([`embed`]). Nothing here may fail the build: without git
//! (or outside a work tree) the commit is empty and the tree counts as clean.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Names the commit of a build without a work tree (a Docker image).
pub const GIT_COMMIT_ENV: &str = "SLUICE_GIT_COMMIT";

/// Embeds the build identification into the crate whose build script calls it, as
/// `SLUICE_BUILD_COMMIT` (the full id; empty when unknown), `SLUICE_BUILD_DIRTY`
/// (`true` / `false`) and `SLUICE_BUILD_TIME` (RFC 3339 UTC) for `env!`.
///
/// `sources` (relative to the crate) rerun the build script on a change: any
/// `rerun-if-changed` replaces cargo's default (rerun on any package file), so the
/// crate's sources must be among them. A commit or checkout (HEAD, its reflog, the
/// index, the branch ref) reruns it too, and so does [`GIT_COMMIT_ENV`], which wins
/// over git when set.
pub fn embed(sources: &[&str]) {
    let dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    for path in sources {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-env-changed={GIT_COMMIT_ENV}");
    for path in git_watch_paths("git", &dir) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    let git = resolve(std::env::var(GIT_COMMIT_ENV).ok(), "git", &dir);
    if git.commit.is_empty() {
        println!("cargo:warning=git commit unavailable; the build reports it as unknown");
    }
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=SLUICE_BUILD_COMMIT={}", git.commit);
    println!("cargo:rustc-env=SLUICE_BUILD_DIRTY={}", git.dirty);
    println!("cargo:rustc-env=SLUICE_BUILD_TIME={}", rfc3339(secs));
}

/// The git state of the work tree a build is made from.
#[derive(Debug, PartialEq)]
struct GitState {
    /// Full commit id of HEAD; empty when unknown.
    commit: String,
    /// Whether tracked files had uncommitted changes.
    dirty: bool,
}

/// The commit `given` (clean) when non-empty, else the work tree's state.
fn resolve(given: Option<String>, program: &str, dir: &Path) -> GitState {
    match given.filter(|commit| !commit.is_empty()) {
        Some(commit) => GitState {
            commit,
            dirty: false,
        },
        None => git_state(program, dir),
    }
}

/// Trimmed stdout of `<program> <args>` run in `dir`; `None` when the
/// program is missing, fails or prints nothing. `GIT_OPTIONAL_LOCKS=0` keeps
/// `git status` from rewriting the index, which would re-trigger the build
/// script through its `rerun-if-changed` on the index.
fn git(program: &str, dir: &Path, args: &[&str]) -> Option<String> {
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
fn git_state(program: &str, dir: &Path) -> GitState {
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
fn git_watch_paths(program: &str, dir: &Path) -> Vec<PathBuf> {
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
fn rfc3339(secs: u64) -> String {
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

    /// A build without git (missing program, or outside a work tree) still
    /// gets an identification: an empty commit, reported as unknown. A given
    /// commit wins, clean.
    #[test]
    fn git_state_without_git_is_unknown_unless_given() {
        let dir = std::env::temp_dir();
        let unknown = GitState {
            commit: String::new(),
            dirty: false,
        };
        assert_eq!(resolve(None, "sluice-no-such-git", &dir), unknown);
        assert_eq!(
            resolve(Some(String::new()), "sluice-no-such-git", &dir),
            unknown
        );
        assert_eq!(
            resolve(Some("4880f55".into()), "sluice-no-such-git", &dir),
            GitState {
                commit: "4880f55".into(),
                dirty: false
            }
        );
        assert!(git_watch_paths("sluice-no-such-git", &dir).is_empty());
    }

    #[test]
    fn git_state_reads_the_commit_of_the_work_tree() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let Some(head) = git("git", dir, &["rev-parse", "HEAD"]) else {
            return; // no git on this machine: covered by git_state_without_git_is_unknown_unless_given
        };
        assert_eq!(resolve(None, "git", dir).commit, head);
        assert!(
            git_watch_paths("git", dir)
                .iter()
                .any(|path| path.ends_with("HEAD")),
            "HEAD is watched"
        );
    }

    #[test]
    fn rfc3339_formats_utc_instants() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339(1_760_000_000), "2025-10-09T08:53:20Z");
    }
}
