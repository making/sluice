//! The instances on one store (`--state`), each a row of it (`sluice_cf_instances`) while it
//! says so, every renewal of the lease's time (`crate::lease`): its build, the rows' layout it
//! reads, its flags' topology and what of its settings the others are to have alike. An update
//! runs two builds and two configurations side by side; what keeps them one cf-api:
//!
//! - the one reconciling the wickets is of the oldest build there is ([`standing`]): what it
//!   puts to the wickets and writes to the store, every instance reads, and the apps do not go
//!   back and forth between two builds' bodies. The newer ones take over once the last older one
//!   left, and then move the store's layout to theirs (`crate::postgres`), which keeps an older
//!   build off it from then on
//! - the topology (the wickets, the domains) is the store's, not each instance's flags': made
//!   the flags' by the one taking the lease, which only an instance does whose flags are the
//!   store's already, or every instance's. So the apps are placed again once, when the last
//!   instance of the flags before left
//! - several instances share the blobs' bucket and the flume: one beside another without them,
//!   or with another bucket, does not start ([`refused`]). The rest of what they should have
//!   alike is said where it differs ([`differences`]): an update is to change it

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::store::State;

/// The wickets the apps are placed on and the shared domains, as flags name them or the state
/// has them.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Topology {
    pub wickets: Vec<String>,
    pub domains: Vec<String>,
}

impl Topology {
    /// The state's.
    pub fn of(state: &State) -> Topology {
        Topology {
            wickets: state.wickets.configured.clone(),
            domains: (state.domains.iter())
                .map(|domain| domain.name.clone())
                .collect(),
        }
    }

    /// Whether a state of this topology is as the flags `flags` make it: their wickets, and
    /// their domains first (one gone from the flags stays behind them).
    pub fn follows(&self, flags: &Topology) -> bool {
        self.wickets == flags.wickets && self.domains.starts_with(&flags.domains)
    }
}

/// The setting of the blobs' bucket among [`Member::settings`], and of the flume.
pub const BLOBS: &str = "blobs";
pub const FLUME: &str = "flume";

/// What an instance without a setting says of it.
pub const NONE: &str = "none";

/// This instance, as it says itself to the others.
#[derive(Clone, Debug, Default)]
pub struct Member {
    pub build: String,
    /// The newest rows' layout it reads: its build's age among the others'.
    pub reads: i32,
    /// Where the others reach it (`--instance-url`).
    pub address: Option<String>,
    /// Its flags'.
    pub topology: Topology,
    /// What the instances are to have alike, by option: every one this build knows, [`NONE`]
    /// where it is not given, a secret or a long one as a digest.
    pub settings: BTreeMap<String, String>,
}

/// Another instance on the store.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Peer {
    pub id: String,
    pub build: String,
    pub reads: i32,
    /// None: not one this build reads.
    pub topology: Option<Topology>,
    pub settings: BTreeMap<String, String>,
}

/// Whether the instance `me` may reconcile the wickets beside the `peers`, the store's topology
/// `kept`; else why not. Of those whose flags are the store's topology, or of all once they
/// have the same flags, the oldest build's.
pub fn standing(me: &Member, peers: &[Peer], kept: &Topology) -> Result<(), String> {
    let alike = (peers.iter()).all(|peer| peer.topology.as_ref() == Some(&me.topology));
    if !alike && !kept.follows(&me.topology) {
        return Err(format!(
            "its --wicket and --domain ({}; {}) are neither the store's ({}; {}) nor every \
             instance's",
            me.topology.wickets.join(", "),
            me.topology.domains.join(", "),
            kept.wickets.join(", "),
            kept.domains.join(", ")
        ));
    }
    let stands =
        |peer: &&Peer| alike || (peer.topology.as_ref()).is_some_and(|of| kept.follows(of));
    let older = (peers.iter().filter(stands))
        .filter(|peer| peer.reads < me.reads)
        .min_by_key(|peer| peer.reads);
    match older {
        Some(older) => Err(format!(
            "the instance {} is of an older build ({})",
            older.id, older.build
        )),
        None => Ok(()),
    }
}

/// Why the instance `me` does not start beside the `peers`, if so: several share one bucket and
/// one flume.
pub fn refused(me: &Member, peers: &[Peer]) -> Option<String> {
    let of = |settings: &BTreeMap<String, String>, key: &str| {
        (settings.get(key).cloned()).filter(|value| value != NONE)
    };
    let why = peers.iter().find_map(|peer| {
        let other = format!("the instance {} ({})", peer.id, peer.build);
        let blobs = (of(&me.settings, BLOBS), of(&peer.settings, BLOBS));
        let flume = (of(&me.settings, FLUME), of(&peer.settings, FLUME));
        match (blobs, flume) {
            ((None, _), _) => Some(format!(
                "{other} is on the store, and this one has no --blobs"
            )),
            ((_, None), _) => Some(format!("{other}, on the store, has no --blobs")),
            ((mine, its), _) if mine != its => {
                Some(format!("{other}, on the store, has another --blobs"))
            }
            (_, (None, _)) => Some(format!(
                "{other} is on the store, and this one has no --flume"
            )),
            (_, (_, None)) => Some(format!("{other}, on the store, has no --flume")),
            _ => None,
        }
    })?;
    Some(format!(
        "--state: {why}: several instances share one bucket (--blobs) and one flume (--flume)"
    ))
}

/// What the instance `peer` has that `me` has otherwise, of what both builds know: its build,
/// then the options. Empty: nothing.
pub fn differences(me: &Member, peer: &Peer) -> Vec<String> {
    let mut differing = Vec::new();
    if peer.build != me.build {
        differing.push(format!("its build ({})", peer.build));
    }
    for (option, mine) in &me.settings {
        if (peer.settings.get(option)).is_some_and(|its| its != mine) {
            differing.push(format!("--{option}"));
        }
    }
    differing
}

#[cfg(test)]
mod tests {
    use super::*;

    /// During an update the instance reconciling the wickets is of the oldest build among
    /// those whose flags are the store's topology; flags the store does not have are taken
    /// once every instance has them. One beside another shares its bucket and has a flume, and
    /// says what else it has otherwise, of what both know.
    #[test]
    fn the_oldest_build_of_the_stores_topology_leads_and_several_share_a_bucket_and_a_flume() {
        let topology = |wickets: &[&str]| Topology {
            wickets: wickets.iter().map(|name| name.to_string()).collect(),
            domains: vec!["apps.test".into()],
        };
        let settings = |blobs: &str, flume: &str, token: &str| -> BTreeMap<String, String> {
            let settings = [(BLOBS, blobs), (FLUME, flume), ("wicket-token", token)];
            (settings.into_iter())
                .map(|(option, value)| (option.to_string(), value.to_string()))
                .collect()
        };
        let member = |reads: i32, wickets: &[&str]| Member {
            build: format!("build {reads}"),
            reads,
            address: None,
            topology: topology(wickets),
            settings: settings("s3://b/", "f", "t"),
        };
        let peer = |id: &str, reads: i32, wickets: &[&str]| Peer {
            id: id.into(),
            build: format!("build {reads}"),
            reads,
            topology: Some(topology(wickets)),
            settings: settings("s3://b/", "f", "t"),
        };
        let kept = Topology {
            domains: vec!["apps.test".into(), "gone.test".into()],
            ..topology(&["a", "b"])
        };
        let (old, new) = (member(1, &["a", "b"]), member(2, &["a", "b"]));
        let waits = |me: &Member, peers: &[Peer]| standing(me, peers, &kept).unwrap_err();

        // alone, and beside the same or a newer build: its turn
        assert_eq!(standing(&new, &[], &kept), Ok(()));
        assert_eq!(standing(&old, &[peer("n", 2, &["a", "b"])], &kept), Ok(()));
        assert_eq!(standing(&new, &[peer("n", 2, &["a", "b"])], &kept), Ok(()));
        // beside an older one: that one's
        let why = waits(
            &new,
            &[peer("o", 1, &["a", "b"]), peer("n", 2, &["a", "b"])],
        );
        assert!(why.contains("the instance o is of an older build"), "{why}");

        // flags adding a wicket wait for the instances of the flags before, whatever their
        // build, which lead meanwhile; then they are every instance's
        let added = member(1, &["a", "b", "c"]);
        let why = waits(&added, &[peer("n", 2, &["a", "b"])]);
        assert!(why.contains("(a, b, c; apps.test)"), "{why}");
        assert!(why.contains("the store's (a, b; apps.test, gone.test)"));
        assert_eq!(
            standing(&new, &[peer("o", 1, &["a", "b", "c"])], &kept),
            Ok(())
        );
        assert_eq!(
            standing(&added, &[peer("n", 2, &["a", "b", "c"])], &kept),
            Ok(())
        );
        let why = waits(
            &member(2, &["a", "b", "c"]),
            &[peer("o", 1, &["a", "b", "c"])],
        );
        assert!(why.contains("older build"), "{why}");
        // one whose topology this build does not read has other flags
        let unread = Peer {
            topology: None,
            ..peer("u", 1, &[])
        };
        assert_eq!(standing(&new, std::slice::from_ref(&unread), &kept), Ok(()));
        assert!(waits(&added, &[unread]).contains("nor every instance's"));

        // several share the bucket and the flume
        let with = |blobs: &str, flume: &str| settings(blobs, flume, "t");
        let beside = |mine: BTreeMap<String, String>, its: BTreeMap<String, String>| {
            let me = Member {
                settings: mine,
                ..new.clone()
            };
            let peer = Peer {
                settings: its,
                ..peer("p", 2, &["a", "b"])
            };
            refused(&me, &[peer])
        };
        assert_eq!(refused(&new, &[]), None);
        assert_eq!(beside(with("s3://b/", "f"), with("s3://b/", "g")), None);
        for (mine, its, why) in [
            (
                with(NONE, "f"),
                with("s3://b/", "f"),
                "this one has no --blobs",
            ),
            (with("s3://b/", "f"), with(NONE, "f"), "has no --blobs"),
            (
                with("s3://b/", "f"),
                with("s3://c/", "f"),
                "another --blobs",
            ),
            (
                with("s3://b/", NONE),
                with("s3://b/", "f"),
                "this one has no --flume",
            ),
            (
                with("s3://b/", "f"),
                with("s3://b/", NONE),
                "has no --flume",
            ),
        ] {
            let refused = beside(mine, its).expect(why);
            assert!(refused.contains(why), "{refused}");
        }

        // what differs is said, but an option one of the builds does not know
        let mut other = peer("p", 1, &["a", "b"]);
        other.settings = settings("s3://b/", "g", "t");
        other.settings.remove("wicket-token");
        other.settings.insert("later".into(), "x".into());
        assert_eq!(
            differences(&new, &other),
            ["its build (build 1)", "--flume"]
        );
        assert_eq!(differences(&new, &peer("p", 2, &["a", "b"])), [""; 0]);
    }
}
