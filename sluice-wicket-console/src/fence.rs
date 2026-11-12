//! An owner's writes fenced by its epoch: a controller of several instances, one at a time
//! reconciling the wickets (`sluice-wicket-cf-api`'s lease), sends the epoch of its turn with
//! each write of the app API ([`HEADER`]), and the wicket refuses one below the highest that owner
//! sent: an instance that lost its turn (paused, cut off) puts nothing over what the next one put.
//! The highest of each owner is kept in the console's directory ([`FILE`]), so a restart of the
//! wicket refuses them still. A write without the header is taken as before (a controller
//! predating it, or of one instance).
//!
//! A write admitted holds its owner's fence until it is handed to the routes ([`Admitted`]): the
//! writes of an owner reach them in the order they were admitted, none of a lower epoch after one
//! of a higher.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use http::StatusCode;
use http::request::Parts;

use crate::http::Refusal;
use crate::store::{check_name, write_by_rename};

/// The header of a write carrying its owner's epoch.
pub const HEADER: &str = "sluice-epoch";

/// The file of the console's directory keeping the highest epoch of each owner.
pub const FILE: &str = "epochs.json";

/// The highest epoch each owner sent.
pub struct Fence {
    file: PathBuf,
    /// The highest epoch of each owner, as the file has it.
    kept: Mutex<BTreeMap<String, u64>>,
    /// Held by each owner's write being admitted, by owner.
    owners: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// A write admitted: its owner's fence held until dropped, once the write is handed over.
pub struct Admitted {
    _held: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Fence {
    /// The fence of the console directory `dir`: the epochs kept there, none without the file.
    pub fn open(dir: &Path) -> Result<Fence, String> {
        let file = dir.join(FILE);
        let kept = match std::fs::read(&file) {
            Ok(json) => {
                serde_json::from_slice(&json).map_err(|e| format!("{}: {e}", file.display()))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(format!("{}: {e}", file.display())),
        };
        Ok(Fence {
            file,
            kept: Mutex::new(kept),
            owners: Mutex::default(),
        })
    }

    /// Admits a write of `owner` (None: the write names none, which an epoch needs) whose
    /// request is `parts`: refused (409, `fenced`) when its epoch is below the highest the
    /// owner sent, which a higher one becomes, kept before it is admitted. A write without an
    /// epoch is admitted as it is.
    pub async fn admit(&self, owner: Option<&str>, parts: &Parts) -> Result<Admitted, Refusal> {
        let Some(epoch) = parts.headers.get(HEADER) else {
            return Ok(Admitted { _held: None });
        };
        let epoch = (epoch.to_str().ok())
            .and_then(|epoch| epoch.parse::<u64>().ok())
            .ok_or_else(|| {
                let said = String::from_utf8_lossy(epoch.as_bytes());
                Refusal::new(
                    StatusCode::BAD_REQUEST,
                    format!("{HEADER}: '{said}' is no epoch (a number)"),
                )
            })?;
        let Some(owner) = owner else {
            return Err(Refusal::new(
                StatusCode::BAD_REQUEST,
                format!("{HEADER} is an owner's: owner=<owner> names it"),
            ));
        };
        check_name("owner", owner).map_err(|e| Refusal::new(StatusCode::BAD_REQUEST, e))?;
        let lock = {
            let mut owners = self.owners.lock().expect("no panic under the lock");
            owners.entry(owner.to_string()).or_default().clone()
        };
        let held = lock.lock_owned().await;
        let mut kept = self.kept.lock().expect("no panic under the lock");
        match kept.get(owner).copied() {
            Some(highest) if epoch < highest => {
                let mut refusal = Refusal::new(
                    StatusCode::CONFLICT,
                    format!(
                        "the epoch {epoch} of {owner} is below {highest}, the highest it sent: \
                         another of its controllers took over"
                    ),
                );
                refusal.view.fenced = true;
                return Err(refusal);
            }
            Some(highest) if epoch == highest => {}
            _ => {
                let mut next = kept.clone();
                next.insert(owner.to_string(), epoch);
                let json = serde_json::to_vec(&next).expect("a map of numbers");
                write_by_rename(&self.file, &json, false).map_err(|e| {
                    Refusal::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("the epoch of {owner} not kept: {e}"),
                    )
                })?;
                *kept = next;
                println!(
                    "fence: {owner} sends the epoch {epoch}: its writes of a lower one are refused"
                );
            }
        }
        Ok(Admitted { _held: Some(held) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The head of a request carrying `epoch`, if any.
    fn parts(epoch: Option<&str>) -> Parts {
        let mut request = http::Request::builder();
        if let Some(epoch) = epoch {
            request = request.header(HEADER, epoch);
        }
        request.body(()).unwrap().into_parts().0
    }

    /// An owner's write of an epoch below the highest it sent is refused, as a fence opened again
    /// on the directory refuses it; another owner's is its own, one without an epoch is taken as
    /// it is. A write admitted holds its owner's fence until dropped.
    #[tokio::test]
    async fn a_write_below_the_highest_epoch_of_its_owner_is_refused() {
        let dir = std::env::temp_dir().join(format!("sluice-fence-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let fence = Fence::open(&dir).unwrap();
        let admit = async |fence: &Fence, owner: &str, epoch: Option<&str>| {
            let admitted = fence.admit(Some(owner), &parts(epoch)).await;
            admitted.map(drop).map_err(|refused| {
                (
                    refused.status.as_u16(),
                    refused.view.fenced,
                    refused.view.error,
                )
            })
        };
        assert_eq!(admit(&fence, "cf", Some("5")).await, Ok(()));
        assert_eq!(
            admit(&fence, "cf", Some("5")).await,
            Ok(()),
            "the same turn"
        );
        assert_eq!(admit(&fence, "cf", Some("7")).await, Ok(()));
        let below = Err((
            409,
            true,
            "the epoch 5 of cf is below 7, the highest it sent: another of its controllers took \
             over"
                .to_string(),
        ));
        assert_eq!(admit(&fence, "cf", Some("5")).await, below);
        assert_eq!(admit(&fence, "ops", Some("1")).await, Ok(()), "its own");
        assert_eq!(admit(&fence, "cf", None).await, Ok(()), "as before");
        let malformed = admit(&fence, "cf", Some("-1")).await.unwrap_err();
        assert_eq!((malformed.0, malformed.1), (400, false));
        for owner in [None, Some("c f")] {
            let unnamed = fence.admit(owner, &parts(Some("7"))).await;
            assert_eq!(
                unnamed.err().map(|refused| refused.status.as_u16()),
                Some(400)
            );
        }
        assert!(fence.admit(None, &parts(None)).await.is_ok());
        // kept: refused after a restart too
        let again = Fence::open(&dir).unwrap();
        assert_eq!(admit(&again, "cf", Some("5")).await, below);
        assert_eq!(admit(&again, "ops", Some("1")).await, Ok(()));
        // held until handed over: the next write of its owner waits for it, another's not
        let eight = parts(Some("8"));
        let held = again.admit(Some("cf"), &eight).await.ok();
        let next = again.admit(Some("cf"), &eight);
        let next = tokio::time::timeout(std::time::Duration::from_millis(100), next);
        assert!(next.await.is_err(), "held");
        assert!(again.admit(Some("ops"), &parts(Some("2"))).await.is_ok());
        drop(held);
        assert!(again.admit(Some("cf"), &eight).await.is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
