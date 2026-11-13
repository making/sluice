//! What an apps' listing says of the wicket itself, for a controller placing apps on several:
//! which wicket it is ([`Identity`]) and its budget ([`Budget`]).

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::store::write_by_rename;

/// The file of the console's directory holding the wicket's id.
pub const ID_FILE: &str = "wicket-id";

/// Which wicket answers: two URLs reaching one, or one replaced behind its URL, tell apart.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Identity {
    /// Made at its first start, kept in its console's directory ([`ID_FILE`]): another
    /// directory is another wicket.
    pub id: String,
    /// Its node among a flume's (`wicket:<client-id>`), which names the lines it feeds; none
    /// without a flume.
    pub node: Option<String>,
    /// Its build, as its log's first line says it.
    pub build: String,
    /// Its start, in seconds since the epoch: from then on it holds the apps put with `held-ms`
    /// until their owners confirm them (`crate::held`).
    pub since: u64,
}

impl Identity {
    /// The wicket of the console directory `dir`, started now, its id made there when it has
    /// none.
    pub fn open(dir: &Path, node: Option<String>, build: String) -> Result<Identity, String> {
        let file = dir.join(ID_FILE);
        let id = match std::fs::read_to_string(&file) {
            Ok(id) if !id.trim().is_empty() => id.trim().to_string(),
            Ok(_) => made(&file)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => made(&file)?,
            Err(e) => return Err(format!("{}: {e}", file.display())),
        };
        let since = SystemTime::now().duration_since(UNIX_EPOCH);
        Ok(Identity {
            id,
            node,
            build,
            since: since.map_or(0, |since| since.as_secs()),
        })
    }
}

/// A new id, written to `file`.
fn made(file: &Path) -> Result<String, String> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|e| format!("{}: {e}", file.display()))?;
    let id: String = id.iter().map(|byte| format!("{byte:02x}")).collect();
    write_by_rename(file, format!("{id}\n").as_bytes(), false)?;
    Ok(id)
}

/// The instances the wicket keeps at once at most, and what of them its routes keep for good
/// (what refuses an edit for room): a wasm wicket's pools' units, a Firecracker wicket's booted
/// and warm VMs, what they may take of the host's memory (their snapshots' too) and their CPUs.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Budget {
    /// `--max-instances`.
    pub max_instances: usize,
    /// `--max-memory-mib`; None: no memory budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory_mib: Option<u64>,
    /// `--max-cpus`, in CPUs; None: no CPU budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cpus: Option<f64>,
    /// The instances the routes keep.
    pub instances: usize,
    /// What they may take of the host's memory, where there is a memory budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    /// Their CPUs (a VM's its vCPUs, or its `cpu` where that is less), where there is a CPU
    /// budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpus: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Made at the first start, the same from then on; another directory another.
    #[test]
    fn an_id_is_kept_in_the_directory() {
        let base = std::env::temp_dir().join(format!("sluice-console-id-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let open = |dir: &str| Identity::open(&base.join(dir), None, "b".into()).unwrap();
        let first = open("a");
        assert_eq!(first.id.len(), 32);
        assert_eq!(open("a").id, first.id);
        assert_ne!(open("b").id, first.id);
        let _ = std::fs::remove_dir_all(&base);
    }
}
