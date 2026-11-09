//! Where an app's instances run: each index of its web process on a wicket, by name, kept in the
//! state ([`App::placement`]) and brought to the app's count at every change ([`State::place`]).
//! A new index goes to a wicket with room for it ([`State::rooms`]) holding the fewest of the
//! app's -- none while there is one --, of them the one with the most room, the first configured
//! of those; a scale down drops the highest, as CF does. A wicket's share of an app is the app of
//! its indices (`crate::controller`).
//!
//! A wicket's room is its budget as it lists it, less what is not the apps' (`crate::controller`)
//! and the indices of the started apps placed on it: an index takes an instance, a VM's its memory
//! too. One refusing a share for room has none until its revision moves; an index of it is then
//! passed to a wicket with room ([`State::pass`]). No wicket has room: the index is placed as if
//! all had, and refused there. A wicket listing no budget (one predating it) has room enough.
//!
//! A state of one wicket has its indices on the first wicket configured, and an index on a wicket
//! no longer configured is placed again at the start ([`State::follow_wickets`]). None is placed
//! on a wicket running apps another way than the first, nor on a second name of one: an index
//! there stays.
//!
//! A keyvalue namespace of a store of each wicket's host (`sqlite`) has its data on one wicket,
//! its home ([`State::homes`]): the wicket of index 0 of the first app whose start maps it. An
//! app whose last start maps one has all its indices there (the first namespace's home), no index
//! passed elsewhere; a store the wickets share pins nothing.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use crate::store::{App, State};

/// The most instances an app has: each index is placed, kept in the state and listed.
pub const MAX_INSTANCES: u64 = 1000;

/// The wickets indices are placed on.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Wickets {
    /// As configured: the first takes a state of one wicket.
    pub configured: Vec<String>,
    /// None placed on: those running apps another way than the first wicket listed, a second
    /// name of a wicket.
    pub odd: BTreeSet<String>,
    /// What each wicket's budget leaves the apps placed on it, as it last listed it.
    pub budgets: BTreeMap<String, Budget>,
    /// Those that refused a share for room, by the revision they listed then: no room until it
    /// moves.
    pub full: BTreeMap<String, u64>,
    /// The wicket of each flume node (`wicket:<client-id>`), as each last listed it.
    pub nodes: BTreeMap<String, String>,
    /// Each keyvalue store the wickets list, by name: whether it is one store of them all, else
    /// each wicket's host's own (`crate::services::Offer::stores`).
    pub stores: BTreeMap<String, bool>,
}

/// Where all the indices of an app go: a namespace of a store of each wicket's host has its
/// data there.
#[derive(Clone, Debug, PartialEq)]
enum Pin {
    /// The namespace's home.
    To(String),
    /// Where index 0 is, or goes: no namespace has a home yet.
    Together,
}

/// What a wicket's budget leaves the apps placed on it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Budget {
    pub instances: u64,
    /// MiB; None: no memory budget.
    pub memory_mib: Option<u64>,
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

/// Whether the indices of `app` take room where they are: started with a droplet.
fn takes_room(app: &App) -> bool {
    app.started && app.running.is_some() && app.process.instances > 0
}

/// The memory an index of `app` takes, in MiB.
fn need(app: &App) -> u64 {
    app.running.as_ref().map_or(0, |running| running.memory_mib)
}

/// `placement` of `count` indices: the highest dropped; an index on a wicket not configured, and
/// each new one, on a wicket with room for it holding the fewest of the others, of them the one
/// with the most room, the first configured of those. `rooms` is the room for the app's indices
/// on the wickets of a known budget, its own aside: the others have room enough. Pinned, every
/// index on the one wicket, room or not.
fn placed(
    placement: &[String],
    count: usize,
    wickets: &Wickets,
    rooms: &BTreeMap<String, i64>,
    pin: Option<&Pin>,
) -> Vec<String> {
    // the one wicket of them all: index 0's, unpinned, without a home
    let home = pin.map(|pin| match pin {
        Pin::To(home) => Some(home.clone()),
        Pin::Together => placed(placement, count, wickets, rooms, None)
            .into_iter()
            .next(),
    });
    let allowed = |wicket: &String| {
        home.as_ref()
            .is_none_or(|home| home.as_ref() == Some(wicket))
    };
    let mut placed: Vec<Option<&String>> = placement
        .iter()
        .take(count)
        .map(|wicket| wickets.configured.iter().find(|kept| *kept == wicket))
        .map(|wicket| wicket.filter(|wicket| allowed(wicket)))
        .collect();
    placed.resize(count, None);
    let candidates: Vec<&String> = wickets
        .configured
        .iter()
        .filter(|wicket| !wickets.odd.contains(*wicket) && allowed(wicket))
        .collect();
    let mut held: Vec<usize> = candidates
        .iter()
        .map(|candidate| {
            let on = |wicket: &&Option<&String>| **wicket == Some(*candidate);
            placed.iter().filter(on).count()
        })
        .collect();
    // the indices each has room left for: none known, as many as asked
    let mut left: Vec<Option<i64>> = (candidates.iter().zip(&held))
        .map(|(candidate, held)| Some(rooms.get(*candidate)? - *held as i64))
        .collect();
    for wicket in placed.iter_mut().filter(|wicket| wicket.is_none()) {
        let key = |at: &usize| {
            let room = left[*at].unwrap_or(i64::MAX);
            (room < 1, held[*at], Reverse(room))
        };
        // none configured: none placed
        let Some(at) = (0..candidates.len()).min_by_key(key) else {
            break;
        };
        held[at] += 1;
        if let Some(room) = &mut left[at] {
            *room -= 1;
        }
        *wicket = Some(candidates[at]);
    }
    placed.into_iter().flatten().cloned().collect()
}

/// The index of an app of `instances` that a wicket's instance `index` is, of a share of
/// `indices` (the lowest first): its `k`th the share's `k`th, those past it the remainder's (a
/// wasm wicket's route runs more at once, the requests of an instance), as `cf app` lists them.
pub fn app_index(indices: &[u64], index: u64, instances: u64) -> u64 {
    let index = match indices.len() as u64 {
        0 => index,
        count => indices[(index % count) as usize],
    };
    // of an app at 0, which lists none, or gone, as it came
    index.checked_rem(instances).unwrap_or(index)
}

impl State {
    /// The wicket of each index of `app`, as [`State::place`] keeps it.
    pub fn placement<'a>(&self, app: &'a App) -> Cow<'a, [String]> {
        let count = count(app);
        let pin = self.pin(app);
        let configured = |wicket: &String| self.wickets.configured.contains(wicket);
        let pinned = |wicket: &String| match &pin {
            Some(Pin::To(home)) => wicket == home,
            Some(Pin::Together) => app.placement.first() == Some(wicket),
            None => true,
        };
        if app.placement.len() == count
            && app
                .placement
                .iter()
                .all(|wicket| configured(wicket) && pinned(wicket))
        {
            return Cow::Borrowed(&app.placement);
        }
        Cow::Owned(placed(
            &app.placement,
            count,
            &self.wickets,
            &self.rooms(app),
            pin.as_ref(),
        ))
    }

    /// The keyvalue namespaces of a store of each wicket's host that the last start of `app`
    /// maps, as `store/namespace`.
    pub fn local_places<'a>(&self, app: &'a App) -> impl Iterator<Item = &'a String> {
        let running = app.running.iter();
        running
            .flat_map(|running| running.keyvalue.values())
            .filter(|place| {
                let store = place
                    .split_once('/')
                    .map_or(place.as_str(), |(store, _)| store);
                self.wickets.stores.get(store) == Some(&false)
            })
    }

    /// Where every index of `app` goes: the home of the first namespace of a store of each
    /// wicket's host its last start maps that has one.
    fn pin(&self, app: &App) -> Option<Pin> {
        let mut places = self.local_places(app).peekable();
        places.peek()?;
        Some(match places.find_map(|place| self.homes.get(place)) {
            Some(home) => Pin::To(home.clone()),
            None => Pin::Together,
        })
    }

    /// The room for the indices of `app` on each wicket of a known budget: the indices of its
    /// that fit beside those of the other started apps placed there (as kept), 0 at most on one
    /// that refused a share for room.
    pub fn rooms(&self, app: &App) -> BTreeMap<String, i64> {
        let mut rooms = BTreeMap::new();
        for wicket in &self.wickets.configured {
            let full = self.wickets.full.contains_key(wicket);
            let mut room = match self.wickets.budgets.get(wicket) {
                Some(budget) => {
                    let (instances, memory) = (self.apps.iter())
                        .filter(|other| other.guid != app.guid && takes_room(other))
                        .map(|other| {
                            let placed = other.placement.iter().take(count(other));
                            let on = placed.filter(|on| *on == wicket).count() as i64;
                            (on, on * need(other) as i64)
                        })
                        .fold((0, 0), |(i, m), (on, mib)| (i + on, m + mib));
                    let mut room = budget.instances as i64 - instances;
                    if let (Some(max), 1..) = (budget.memory_mib, need(app)) {
                        room = room.min((max as i64 - memory).div_euclid(need(app) as i64));
                    }
                    room
                }
                None if full => 0,
                None => continue,
            };
            if full {
                room = room.min(0);
            }
            rooms.insert(wicket.clone(), room);
        }
        rooms
    }

    /// Passes the highest index of the app `guid` on `wicket`, which refused its share for room,
    /// to a wicket with room for it: the line saying so; none without one.
    pub fn pass(&mut self, guid: &str, wicket: &str) -> Option<String> {
        let app = self.app(guid)?;
        if self.pin(app).is_some() {
            // its data's wicket
            return None;
        }
        let rooms = self.rooms(app);
        let placement = self.placement(app).into_owned();
        let at = placement.iter().rposition(|on| on == wicket)?;
        let held = |other: &String| placement.iter().filter(|on| *on == other).count() as i64;
        let elsewhere = (self.wickets.configured.iter())
            .filter(|other| *other != wicket && !self.wickets.odd.contains(*other))
            .any(|other| rooms.get(other).is_none_or(|room| room - held(other) >= 1));
        if !elsewhere {
            return None;
        }
        let app = self.app_mut(guid)?;
        app.placement = placement;
        app.placement[at] = String::new();
        self.place();
        let to = &self.app(guid)?.placement[at];
        Some(format!(
            "Index {at} placed on the wicket {to}: the wicket {wicket} has no room for it"
        ))
    }

    /// The index of `app` that the instance `index` of `wicket` is, as `cf app` lists it
    /// ([`app_index`]): of a wicket not known, that of the one wicket holding the app's indices,
    /// else as an app of one wicket's.
    pub fn instance_of(&self, app: &App, wicket: Option<&str>, index: u64) -> u64 {
        let held = self.indices(app);
        let indices = match wicket {
            Some(wicket) => held.get(wicket),
            None if held.len() == 1 => held.values().next(),
            None => None,
        };
        let indices = indices.map_or(&[][..], |indices| &indices[..]);
        app_index(indices, index, app.process.instances)
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

    /// Keeps the placement of every app as [`State::placement`] has it, in turn: at every
    /// change. A namespace of a store of each wicket's host an app maps without a home has it
    /// where the app's index 0 is.
    pub fn place(&mut self) {
        for at in 0..self.apps.len() {
            if let Cow::Owned(placed) = self.placement(&self.apps[at]) {
                self.apps[at].placement = placed;
            }
            let app = &self.apps[at];
            let Some(home) = app.placement.first() else {
                continue;
            };
            let homeless: Vec<String> = (self.local_places(app))
                .filter(|place| !self.homes.contains_key(*place))
                .cloned()
                .collect();
            for place in homeless {
                self.homes.insert(place, home.clone());
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
        // the rootfs a wicket no --wicket names made: none serves them
        let unnamed = |made: &mut BTreeMap<String, String>| {
            made.retain(|wicket, _| configured.contains(wicket))
        };
        self.droplets
            .iter_mut()
            .for_each(|droplet| unnamed(&mut droplet.made));
        let running = self.apps.iter_mut().filter_map(|app| app.running.as_mut());
        running.for_each(|running| unnamed(&mut running.made));
        let mut said = Vec::new();
        // a namespace's data on a wicket no --wicket names: its apps start on another without it
        self.homes.retain(|place, home| {
            let named = configured.contains(home);
            if !named {
                said.push(format!(
                    "SERVICES: the data of the keyvalue namespace {place} stays on the wicket \
                     {home}, which no --wicket names: its apps run on another without it"
                ));
            }
            named
        });
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
    use crate::store::Running;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    fn wickets(configured: &[&str], odd: &[&str]) -> Wickets {
        Wickets {
            configured: names(configured),
            odd: odd.iter().map(|name| name.to_string()).collect(),
            ..Wickets::default()
        }
    }

    /// Each new index on a wicket with room holding the fewest of the app's, of them the one
    /// with the most room, the first configured of those: distinct wickets while there are some.
    /// Stable: a scale down drops the highest, a scale up after it places as before, what is
    /// placed stays. No room anywhere: placed as if all had.
    #[test]
    fn an_index_goes_where_there_is_room_and_the_app_has_the_fewest() {
        let ab = wickets(&["a", "b"], &[]);
        let abc = wickets(&["a", "b", "c"], &[]);
        // a runs apps another way than the first wicket listed
        let odd = wickets(&["a", "b"], &["a"]);
        let none = wickets(&[], &[]);
        for (placement, count, wickets, rooms, expected) in [
            (&[][..], 1, &ab, &[][..], &["a"][..]),
            (&[], 2, &ab, &[], &["a", "b"]),
            (&[], 3, &ab, &[], &["a", "b", "a"]),
            (&[], 3, &abc, &[], &["a", "b", "c"]),
            (&["a", "b"], 3, &ab, &[], &["a", "b", "a"]),
            (&["a", "b", "a"], 1, &ab, &[], &["a"]),
            (&["a"], 2, &ab, &[], &["a", "b"]),
            (&["b"], 2, &ab, &[], &["b", "a"]),
            // a state of one wicket's, spread from there on
            (&["a", "a"], 3, &ab, &[], &["a", "a", "b"]),
            (&["a", "a", "b"], 4, &ab, &[], &["a", "a", "b", "b"]),
            // a wicket no longer configured: its indices placed again
            (&["x", "b"], 2, &ab, &[], &["a", "b"]),
            (&["x", "x", "a"], 3, &ab, &[], &["b", "a", "a"]),
            (&[], 2, &odd, &[], &["b", "b"]),
            (&["a"], 2, &odd, &[], &["a", "b"]),
            (&[], 2, &none, &[], &[]),
            // ties broken by room, a wicket of none known having room enough
            (&[], 1, &ab, &[("a", 1), ("b", 2)], &["b"]),
            (&[], 3, &abc, &[("a", 1), ("b", 2)], &["c", "b", "a"]),
            // a wicket without room is not placed on while another has some
            (&[], 2, &ab, &[("a", 0), ("b", 5)], &["b", "b"]),
            (&["a"], 3, &ab, &[("a", 1), ("b", 2)], &["a", "b", "b"]),
            // none has: as if all had
            (&[], 3, &ab, &[("a", 0), ("b", -1)], &["a", "b", "a"]),
        ] {
            let rooms = (rooms.iter())
                .map(|(wicket, room)| (wicket.to_string(), *room))
                .collect();
            assert_eq!(
                placed(&names(placement), count, wickets, &rooms, None),
                names(expected),
                "{placement:?} at {count} on {wickets:?}, {rooms:?}"
            );
        }
        // pinned: every index on the namespace's home, room or not; without one, where index 0
        // is or goes
        let to = |home: &str| Pin::To(home.into());
        let full_a = [("a".to_string(), 0), ("b".to_string(), 5)].into();
        let none = BTreeMap::new();
        for (placement, count, pin, rooms, expected) in [
            (&["a", "b"][..], 3, to("b"), &none, &["b", "b", "b"][..]),
            (&[], 2, to("a"), &full_a, &["a", "a"]),
            (&["a", "b"], 2, Pin::Together, &none, &["a", "a"]),
            (&["x", "b"], 2, Pin::Together, &none, &["a", "a"]),
            (&[], 2, Pin::Together, &full_a, &["b", "b"]),
        ] {
            assert_eq!(
                placed(&names(placement), count, &ab, rooms, Some(&pin)),
                names(expected),
                "{placement:?} at {count} pinned {pin:?}, {rooms:?}"
            );
        }
    }

    /// A wicket's room is its budget less the indices of the other started apps placed there, a
    /// VM's memory too, none on a wicket that refused a share for room; such a refusal passes the
    /// app's highest index there to a wicket with room. A wicket's instance is the app's index of
    /// its share.
    #[test]
    fn the_room_is_what_the_budget_leaves_and_a_refusal_passes_an_index() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        state.follow_wickets(&names(&["a", "b"]));
        let space = state.spaces[0].guid.clone();
        let mut started = |name: &str, instances: u64, memory_mib: u64| {
            let app = state.create_app(name, crate::store::STACK, &space);
            app.started = true;
            app.process.instances = instances;
            app.running = Some(Running {
                memory_mib,
                ..Running::default()
            });
            app.guid.clone()
        };
        let big = started("big", 3, 512);
        let small = started("small", 1, 256);
        state.app_mut(&big).unwrap().placement = names(&["a", "b", "a"]);
        state.app_mut(&small).unwrap().placement = names(&["b"]);
        let budget = |instances, memory_mib| Budget {
            instances,
            memory_mib,
        };
        state.wickets.budgets = [
            ("a".to_string(), budget(4, Some(2048))),
            ("b".to_string(), budget(64, None)),
        ]
        .into();
        let rooms = |state: &State, guid: &str| state.rooms(state.app(guid).unwrap());
        // a: 4 - 2 instances, (2048 - 1024) / 256 MiB; b: 64 less big's one
        let expected = [("a".to_string(), 2), ("b".to_string(), 63)].into();
        assert_eq!(rooms(&state, &small), expected);
        state
            .app_mut(&small)
            .unwrap()
            .running
            .as_mut()
            .unwrap()
            .memory_mib = 1024;
        let expected = [("a".to_string(), 1), ("b".to_string(), 63)].into();
        assert_eq!(rooms(&state, &small), expected, "1024 MiB fit once");
        // a stopped app takes none
        state.app_mut(&big).unwrap().started = false;
        assert_eq!(rooms(&state, &small)["a"], 2);
        state.app_mut(&big).unwrap().started = true;

        // b refused small's share for room: none there, its one index passed to a
        state.wickets.full.insert("b".into(), 7);
        assert_eq!(rooms(&state, &small)["b"], 0);
        assert_eq!(
            state.pass(&small, "b").as_deref(),
            Some("Index 0 placed on the wicket a: the wicket b has no room for it")
        );
        assert_eq!(state.app(&small).unwrap().placement, ["a"]);
        // none with room for big's: it stays
        state.wickets.full.insert("a".into(), 3);
        assert_eq!(state.pass(&big, "b"), None);
        assert_eq!(state.app(&big).unwrap().placement, ["a", "b", "a"]);

        // b's instance 0 is big's index 1, a's 1 its 2, past them the remainder's; a wicket not
        // known, as of the one wicket holding them, else as one wicket's
        let big_app = state.app(&big).unwrap();
        for (wicket, index, expected) in [
            (Some("b"), 0, 1),
            (Some("a"), 1, 2),
            (Some("a"), 3, 2),
            (None, 2, 2),
        ] {
            assert_eq!(state.instance_of(big_app, wicket, index), expected);
        }
        let small_app = state.app(&small).unwrap();
        assert_eq!(state.instance_of(small_app, None, 4), 0, "on a alone");
        assert_eq!(app_index(&[], 3, 0), 3, "an app at 0 lists none");

        // a namespace of a store of each wicket's host has its home where index 0 of the first
        // app mapping it is: every index of an app mapping it there, none passed elsewhere; a
        // store the wickets share pins nothing
        state.wickets.stores = [("local".to_string(), false), ("redis".to_string(), true)].into();
        state.wickets.full.clear();
        let mapping = |state: &mut State, guid: &str, places: &[&str]| {
            let running = state.app_mut(guid).unwrap().running.as_mut().unwrap();
            running.keyvalue = (places.iter().enumerate())
                .map(|(at, place)| (at.to_string(), place.to_string()))
                .collect();
            state.place();
        };
        mapping(&mut state, &big, &["redis/cf/r"]);
        assert_eq!(state.app(&big).unwrap().placement, ["a", "b", "a"]);
        mapping(&mut state, &big, &["redis/cf/r", "local/cf/l"]);
        assert_eq!(state.app(&big).unwrap().placement, ["a", "a", "a"]);
        assert_eq!(state.homes, [("local/cf/l".into(), "a".into())].into());
        state.app_mut(&small).unwrap().placement = names(&["b"]);
        mapping(&mut state, &small, &["local/cf/l"]);
        assert_eq!(state.app(&small).unwrap().placement, ["a"]);
        state.wickets.full.insert("a".into(), 9);
        assert_eq!(state.pass(&big, "a"), None);
        // its home no longer named: the data stays there, the apps run on another without it
        let said = state.follow_wickets(&names(&["b"]));
        assert!(
            said.iter()
                .any(|line| line.contains("local/cf/l stays on the wicket a")),
            "{said:?}"
        );
        assert_eq!(state.app(&big).unwrap().placement, ["b", "b", "b"]);
        assert_eq!(state.homes, [("local/cf/l".into(), "b".into())].into());
    }
}
