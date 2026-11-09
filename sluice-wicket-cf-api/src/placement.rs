//! Where an app's instances run: each index of its web process on a wicket, by name, kept in the
//! state ([`App::placement`]) and brought to the app's count at every change ([`State::place`]).
//! A new index goes to a wicket holding the fewest of the app's -- none while there is one --,
//! the first configured of them; a scale down drops the highest, as CF does. A wicket's share of
//! an app is the app of its indices (`crate::controller`).
//!
//! A state of one wicket has its indices on the first wicket configured, and an index on a wicket
//! no longer configured is placed again at the start ([`State::follow_wickets`]). None is placed
//! on a wicket running apps another way than the first: an index there stays.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use crate::store::{App, State};

/// The most instances an app has: each index is placed, kept in the state and listed.
pub const MAX_INSTANCES: u64 = 1000;

/// The wickets indices are placed on.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Wickets {
    /// As configured: the first takes a state of one wicket.
    pub configured: Vec<String>,
    /// Those running apps another way than the first wicket listed: none placed on.
    pub odd: BTreeSet<String>,
}

/// How many indices `app` has: its instances, no fewer than what its last start keeps warm, which
/// a manifest may leave (`cf push` restarts the app right after, `cf scale` refuses it): the next
/// start takes it. [`MAX_INSTANCES`] at most.
fn count(app: &App) -> usize {
    let warm = app
        .running
        .as_ref()
        .and_then(|running| running.lifetime.warm);
    app.process
        .instances
        .max(warm.unwrap_or(0))
        .min(MAX_INSTANCES) as usize
}

/// `placement` of `count` indices: the highest dropped; an index on a wicket not configured, and
/// each new one, on a wicket holding the fewest of the others, the first configured of them.
fn placed(placement: &[String], count: usize, wickets: &Wickets) -> Vec<String> {
    let mut placed: Vec<Option<&String>> = placement
        .iter()
        .take(count)
        .map(|wicket| wickets.configured.iter().find(|kept| *kept == wicket))
        .collect();
    placed.resize(count, None);
    let candidates: Vec<&String> = wickets
        .configured
        .iter()
        .filter(|wicket| !wickets.odd.contains(*wicket))
        .collect();
    let mut held: Vec<usize> = candidates
        .iter()
        .map(|candidate| {
            let on = |wicket: &&Option<&String>| **wicket == Some(*candidate);
            placed.iter().filter(on).count()
        })
        .collect();
    for wicket in placed.iter_mut().filter(|wicket| wicket.is_none()) {
        // none configured: none placed
        let Some((at, _)) = held.iter().enumerate().min_by_key(|(_, held)| **held) else {
            break;
        };
        held[at] += 1;
        *wicket = Some(candidates[at]);
    }
    placed.into_iter().flatten().cloned().collect()
}

impl State {
    /// The wicket of each index of `app`, as [`State::place`] keeps it.
    pub fn placement<'a>(&self, app: &'a App) -> Cow<'a, [String]> {
        let count = count(app);
        let configured = |wicket: &String| self.wickets.configured.contains(wicket);
        if app.placement.len() == count && app.placement.iter().all(configured) {
            return Cow::Borrowed(&app.placement);
        }
        Cow::Owned(placed(&app.placement, count, &self.wickets))
    }

    /// The indices of `app` each wicket holds, the lowest first.
    pub fn indices(&self, app: &App) -> BTreeMap<String, Vec<u64>> {
        let mut indices: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for (index, wicket) in self.placement(app).iter().enumerate() {
            indices
                .entry(wicket.clone())
                .or_default()
                .push(index as u64);
        }
        indices
    }

    /// Keeps the placement of every app as [`State::placement`] has it: at every change.
    pub fn place(&mut self) {
        let placed: Vec<Option<Vec<String>>> = self
            .apps
            .iter()
            .map(|app| match self.placement(app) {
                Cow::Owned(placed) => Some(placed),
                Cow::Borrowed(_) => None,
            })
            .collect();
        for (app, placed) in self.apps.iter_mut().zip(placed) {
            if let Some(placed) = placed {
                app.placement = placed;
            }
        }
    }

    /// Takes the wickets `configured`: a state of one wicket has its indices and its removals on
    /// the first; an index on a wicket no longer configured is placed again, and a removal from
    /// one dropped. The lines saying so.
    pub fn follow_wickets(&mut self, configured: &[String]) -> Vec<String> {
        self.wickets.configured = configured.to_vec();
        let Some(first) = configured.first() else {
            return Vec::new();
        };
        // written before there were more: no app had a placement, a removal no wicket
        for app in &mut self.apps {
            if app.placement.is_empty() {
                let count = count(app);
                app.placement = vec![first.clone(); count];
            }
        }
        for removal in &mut self.removals {
            if removal.wicket.is_empty() {
                removal.wicket.clone_from(first);
            }
        }
        let mut said = Vec::new();
        self.removals.retain(|removal| {
            let named = configured.contains(&removal.wicket);
            if !named {
                said.push(format!(
                    "[{}] CELL: not removed from the wicket {}: no --wicket names it",
                    removal.app, removal.wicket
                ));
            }
            named
        });
        for app in &self.apps {
            let placement = self.placement(app);
            for (index, (was, is)) in app.placement.iter().zip(placement.iter()).enumerate() {
                if was != is {
                    said.push(format!(
                        "[{}] CELL: index {index} placed on the wicket {is}: no --wicket names {was}",
                        app.guid
                    ));
                }
            }
        }
        self.place();
        said
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    fn wickets(configured: &[&str], odd: &[&str]) -> Wickets {
        Wickets {
            configured: names(configured),
            odd: odd.iter().map(|name| name.to_string()).collect(),
        }
    }

    /// Each new index on a wicket holding the fewest of the app's, the first configured of them:
    /// distinct wickets while there are some. Stable: a scale down drops the highest, a scale up
    /// after it places as before, what is placed stays.
    #[test]
    fn an_index_goes_where_the_app_has_the_fewest() {
        let ab = wickets(&["a", "b"], &[]);
        let abc = wickets(&["a", "b", "c"], &[]);
        // a runs apps another way than the first wicket listed
        let odd = wickets(&["a", "b"], &["a"]);
        let none = wickets(&[], &[]);
        for (placement, count, wickets, expected) in [
            (&[][..], 1, &ab, &["a"][..]),
            (&[], 2, &ab, &["a", "b"]),
            (&[], 3, &ab, &["a", "b", "a"]),
            (&[], 3, &abc, &["a", "b", "c"]),
            (&["a", "b"], 3, &ab, &["a", "b", "a"]),
            (&["a", "b", "a"], 1, &ab, &["a"]),
            (&["a"], 2, &ab, &["a", "b"]),
            (&["b"], 2, &ab, &["b", "a"]),
            // a state of one wicket's, spread from there on
            (&["a", "a"], 3, &ab, &["a", "a", "b"]),
            (&["a", "a", "b"], 4, &ab, &["a", "a", "b", "b"]),
            // a wicket no longer configured: its indices placed again
            (&["x", "b"], 2, &ab, &["a", "b"]),
            (&["x", "x", "a"], 3, &ab, &["b", "a", "a"]),
            (&[], 2, &odd, &["b", "b"]),
            (&["a"], 2, &odd, &["a", "b"]),
            (&[], 2, &none, &[]),
        ] {
            assert_eq!(
                placed(&names(placement), count, wickets),
                names(expected),
                "{placement:?} at {count} on {wickets:?}"
            );
        }
    }
}
