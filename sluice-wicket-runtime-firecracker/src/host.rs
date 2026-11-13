//! What the host has for the VMs: the memory their budget (`capacity`) gives out.
//!
//! The VMs are charged to the host's memory (`MemTotal`), or to what the cgroup they are in
//! and those above it allow, where that is less ([`Memory::read`]: a container's `--memory`,
//! a unit's `MemoryMax=`). Of that a part is kept for the wicket itself and the rest of the
//! host ([`Memory::reserve`]); the rest is the budget unless one is asked for
//! ([`Budget::resolve`]). So many MiB asked for fit the host or are refused; so many times
//! what the host has (`1.5x`) is its operator's overcommit, stated.

use std::path::{Path, PathBuf};

use crate::cgroup::{self, CGROUP_ROOT};

/// The least kept of the VMs' memory for the wicket itself, in MiB: what it holds with every
/// connection of `--max-connections` open (see findings, The memory budget).
const RESERVE_MIB: u64 = 64;

/// The part kept of the VMs' memory for the wicket itself and the rest of the host: one in
/// so many.
const RESERVE_PART: u64 = 20;

/// The memory the VMs are charged to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Memory {
    /// MiB: the host's, or what `cgroup` allows.
    pub mib: u64,
    /// The cgroup whose limit it is; None: the host's memory.
    pub cgroup: Option<PathBuf>,
}

impl Memory {
    /// The memory the VMs in the cgroup `vms` (under the hierarchy's root; None: the
    /// wicket's own, which its VMs are in or beside) are charged to; None where the host
    /// does not tell (no `/proc/meminfo`).
    pub fn read(vms: Option<&str>) -> Option<Memory> {
        let host = mem_total_mib(&std::fs::read_to_string("/proc/meminfo").ok()?)?;
        let root = Path::new(CGROUP_ROOT);
        let dir = match vms {
            Some(path) => Some(root.join(path.trim_start_matches('/'))),
            None => cgroup::own(root).ok(),
        };
        let limit = dir.and_then(|dir| limit(&dir, root));
        Some(Memory::of(host, limit))
    }

    /// `host` MiB, or the `limit` (bytes, of a cgroup) where that is less.
    fn of(host: u64, limit: Option<(u64, PathBuf)>) -> Memory {
        match limit {
            Some((bytes, cgroup)) if bytes >> 20 < host => Memory {
                mib: bytes >> 20,
                cgroup: Some(cgroup),
            },
            _ => Memory {
                mib: host,
                cgroup: None,
            },
        }
    }

    /// What is kept of it for the wicket itself and the rest of the host, in MiB: one part in
    /// [`RESERVE_PART`], [`RESERVE_MIB`] at least.
    pub fn reserve(&self) -> u64 {
        (self.mib / RESERVE_PART).max(RESERVE_MIB).min(self.mib)
    }

    /// What it has for the VMs, in MiB.
    pub fn for_vms(&self) -> u64 {
        self.mib - self.reserve()
    }
}

impl std::fmt::Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.cgroup {
            None => write!(f, "the host's {} MiB", self.mib),
            Some(cgroup) => write!(f, "the {} MiB {} allows", self.mib, cgroup.display()),
        }
    }
}

/// `MemTotal` of `/proc/meminfo`'s text, in MiB.
fn mem_total_mib(meminfo: &str) -> Option<u64> {
    let total = meminfo.lines().find_map(|line| {
        line.strip_prefix("MemTotal:")?
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse::<u64>()
            .ok()
    });
    total.map(|kib| kib >> 10)
}

/// The least the cgroup `dir` and those above it, up to `root`, allow their processes, in
/// bytes, and the cgroup allowing it: `memory.max`, and `memory.high` -- past it a cgroup's
/// processes stall while the kernel reclaims, and a guest's memory is not reclaimed. None
/// where none limits (a hierarchy that is not cgroup v2 has no such file).
fn limit(dir: &Path, root: &Path) -> Option<(u64, PathBuf)> {
    let mut least: Option<(u64, PathBuf)> = None;
    for cgroup in dir
        .ancestors()
        .take_while(|cgroup| cgroup.starts_with(root))
    {
        for file in ["memory.max", "memory.high"] {
            let bytes = std::fs::read_to_string(cgroup.join(file))
                .ok()
                .and_then(|limit| limit.trim().parse::<u64>().ok());
            if let Some(bytes) = bytes
                && least.as_ref().is_none_or(|(least, _)| bytes < *least)
            {
                least = Some((bytes, cgroup.to_path_buf()));
            }
        }
    }
    least
}

/// The VMs' memory budget: what they may take of the host's memory, and where that comes
/// from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Budget {
    /// MiB; None: none (nothing asked, and the host does not tell its memory).
    pub mib: Option<u64>,
    /// Where it comes from, for the log and a refusal.
    pub of: String,
    /// Whether it is more than the host has for the VMs: its operator's, stated.
    pub overcommitted: bool,
}

impl Budget {
    /// `asked` (`--max-memory-mib`): so many MiB, which `host` must have for the VMs, or
    /// with `x` so many times what it has for them; none asked, all it has for them.
    pub fn resolve(asked: Option<&str>, host: Option<&Memory>) -> Result<Budget, String> {
        let Some(asked) = asked else {
            return Ok(match host {
                Some(host) => Budget {
                    mib: Some(host.for_vms()),
                    of: format!(
                        "{host} less {} MiB kept for the wicket and the host",
                        host.reserve()
                    ),
                    overcommitted: false,
                },
                None => Budget {
                    mib: None,
                    of: "the host does not tell its memory".into(),
                    overcommitted: false,
                },
            });
        };
        let expected = || {
            format!(
                "--max-memory-mib {asked}: expected a number of MiB (16384) or of times what \
                 the host has for the VMs (1.5x)"
            )
        };
        let Some(ratio) = asked.strip_suffix('x') else {
            let mib = asked.parse::<u64>().ok().filter(|mib| *mib > 0);
            let mib = mib.ok_or_else(expected)?;
            return match host {
                Some(host) if mib > host.for_vms() => Err(format!(
                    "--max-memory-mib {mib}: the VMs have {} MiB at most here, {host} less {} \
                     MiB kept for the wicket and the host; to overcommit it, say by how much: \
                     --max-memory-mib {:.2}x",
                    host.for_vms(),
                    host.reserve(),
                    // rounded up: at least what was asked
                    (mib as f64 * 100.0 / host.for_vms().max(1) as f64).ceil() / 100.0
                )),
                _ => Ok(Budget {
                    mib: Some(mib),
                    of: "--max-memory-mib".into(),
                    overcommitted: false,
                }),
            };
        };
        let plain = !ratio.is_empty() && ratio.chars().all(|c| c.is_ascii_digit() || c == '.');
        let times = ratio.parse::<f64>().ok().filter(|_| plain);
        let times = times.filter(|times| (0.01..=1000.0).contains(times));
        let times = times.ok_or_else(expected)?;
        let host = host.ok_or_else(|| {
            format!("--max-memory-mib {asked}: the host does not tell its memory; say how many MiB")
        })?;
        let mib = (host.for_vms() as f64 * times).round() as u64;
        if mib == 0 {
            return Err(expected());
        }
        Ok(Budget {
            mib: Some(mib),
            of: format!(
                "--max-memory-mib {asked}: of the {} MiB the VMs have here, {host} less {} MiB \
                 kept for the wicket and the host",
                host.for_vms(),
                host.reserve()
            ),
            overcommitted: mib > host.for_vms(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The VMs have the host's memory, or what a cgroup above them allows where that is
    /// less, but a reserve; so many MiB asked for must fit it, so many times it overcommit it.
    #[test]
    fn the_budget_is_what_the_host_or_a_cgroup_above_the_vms_has_less_a_reserve() {
        let meminfo = "MemTotal:       16384000 kB\nMemFree:         1000000 kB\n";
        assert_eq!(mem_total_mib(meminfo), Some(16000));
        assert_eq!(mem_total_mib("MemFree: 1 kB\n"), None);

        // a container's limit above the wicket's leaf, a throttle (memory.high) above that
        let root = std::env::temp_dir().join(format!("sfc-host-{}", std::process::id()));
        let leaf = root.join("unit/wicket");
        std::fs::create_dir_all(&leaf).unwrap();
        assert_eq!(limit(&leaf, &root), None, "no file: no cgroup v2");
        for (file, value) in [
            ("unit/wicket/memory.max", "max\n"),
            ("unit/memory.max", "2147483648\n"),
            ("unit/memory.high", "max\n"),
            ("memory.max", "max\n"),
        ] {
            std::fs::write(root.join(file), value).unwrap();
        }
        assert_eq!(limit(&leaf, &root), Some((2 << 30, root.join("unit"))));
        std::fs::write(root.join("memory.high"), "1073741824\n").unwrap();
        let throttled = limit(&leaf, &root);
        assert_eq!(
            throttled,
            Some((1 << 30, root.clone())),
            "the least of them"
        );
        std::fs::remove_dir_all(&root).unwrap();

        let host = Memory::of(16000, None);
        assert_eq!((host.reserve(), host.for_vms()), (800, 15200));
        let contained = Memory::of(16000, Some((1 << 30, PathBuf::from("/sys/fs/cgroup"))));
        assert_eq!((contained.mib, contained.reserve()), (1024, 64));
        assert_eq!(
            contained.to_string(),
            "the 1024 MiB /sys/fs/cgroup allows",
            "a limit below the host's memory"
        );
        let roomy = Memory::of(16000, Some((64 << 30, PathBuf::from("/sys/fs/cgroup"))));
        assert_eq!(roomy, host, "a limit above it bounds nothing");

        let budget = |asked| Budget::resolve(asked, Some(&host));
        let default = budget(None).unwrap();
        assert_eq!(default.mib, Some(15200));
        let of = "the host's 16000 MiB less 800 MiB kept for the wicket and the host";
        assert_eq!(default.of, of);
        assert_eq!(budget(Some("15200")).unwrap().mib, Some(15200));
        let over = budget(Some("16000")).unwrap_err();
        assert!(
            over.contains("the VMs have 15200 MiB at most here") && over.ends_with("1.06x"),
            "{over}"
        );
        let half = budget(Some("0.5x")).unwrap();
        assert_eq!((half.mib, half.overcommitted), (Some(7600), false));
        let twice = budget(Some("2x")).unwrap();
        assert_eq!((twice.mib, twice.overcommitted), (Some(30400), true));
        for bad in ["0", "x", "-1x", "1.5", "0x", "1e3x", "2 x"] {
            assert!(budget(Some(bad)).is_err(), "{bad}");
        }
        // a host that does not tell: no budget unless stated in MiB
        assert_eq!(Budget::resolve(None, None).unwrap().mib, None);
        assert_eq!(Budget::resolve(Some("64"), None).unwrap().mib, Some(64));
        assert!(Budget::resolve(Some("2x"), None).is_err());
    }
}
