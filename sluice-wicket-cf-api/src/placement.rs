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
//! on a second name of a wicket: an index there stays.
//!
//! The wickets run one stack each, what each last listed ([`Wickets::runtimes`]), and an app's
//! indices are on wickets of its stack, its last start's droplet's: as Diego places an LRP on the
//! cells of its stack. A new index, one passed, one moving off a wicket leaving or gone and one
//! spreading an app again go to wickets of that stack; an index on a wicket of another is placed
//! again, so a start of a droplet of another stack moves the app. A wicket that never listed
//! what it runs takes no new index and keeps what it has; with no wicket of its stack, an index
//! is not placed.
//!
//! A keyvalue namespace of a store of each wicket's host (`sqlite`) has its data on one wicket,
//! its home ([`State::homes`]): the wicket of index 0 of the first app whose start maps it. An
//! app whose last start maps one has all its indices there (the first namespace's home), no index
//! passed elsewhere; a store the wickets share pins nothing.
//!
//! A running index moves the way Diego evacuates a cell: started on its new wicket first
//! ([`App::moving`]: both wickets' shares hold it, the new one's room taken), placed there once
//! that one serves it, which takes it out of the old one's share ([`State::moved`]). A wicket
//! leaving (told to stop, it serves on until its apps are elsewhere) has no room, and each index
//! of its started apps moves to a wicket with room for it, as a new index goes
//! ([`State::evacuate`]); with none, or its keyvalue data there, it stays until the wicket stops.
//! A wicket back, or a new one, takes the indices that spread an app again, one at a time
//! ([`State::respread`]): from the wicket holding the most of the app's, while that one holds two
//! more than it. A wicket gone ([`Wickets::gone`]: it has not answered for a bound, or answered
//! again for less) has no room either, and the indices on it move the same way, as long as it
//! does not answer: nothing is removed from it, which answers nothing.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::store::{App, Running, State};
use crate::wicket::Runtime;

/// An index of an app and the wicket it moves to.
pub type Move = (u64, String);

/// What [`State::evacuate`] makes of an app's indices on a wicket leaving.
#[derive(Debug, Default, PartialEq)]
pub struct Evacuated {
    pub moves: Vec<Move>,
    /// Those with nowhere to go, and why.
    pub stay: Vec<u64>,
    pub why: &'static str,
}

/// The most instances an app has: each index is placed, kept in the state and listed.
pub const MAX_INSTANCES: u64 = 1000;

/// The wickets indices are placed on.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Wickets {
    /// As configured: the first takes a state of one wicket.
    pub configured: Vec<String>,
    /// None placed on: a second name of a wicket.
    pub odd: BTreeSet<String>,
    /// What each runs, as it last listed it, by name: kept with the state.
    pub runtimes: BTreeMap<String, Runtime>,
    /// What each wicket's budget leaves the apps placed on it, as it last listed it.
    pub budgets: BTreeMap<String, Budget>,
    /// Those that refused a share for room, by the revision they listed then: no room until it
    /// moves.
    pub full: BTreeMap<String, u64>,
    /// The wicket of each flume node (`wicket:<client-id>`), as each last listed it.
    pub nodes: BTreeMap<String, String>,
    /// Those leaving, told to stop: when each drains at the latest, in seconds since the epoch,
    /// as it last listed it. No room there.
    pub leaving: BTreeMap<String, u64>,
    /// Those gone: they have not answered for `--wicket-away-grace`, or answered again for less
    /// (`crate::controller`). No room there.
    pub gone: BTreeSet<String>,
    /// Each keyvalue store the wickets list, by name: whether it is one store of them all, else
    /// each wicket's host's own (`crate::services::Offer::stores`).
    pub stores: BTreeMap<String, bool>,
}

impl Wickets {
    /// Whether none listed what it runs.
    pub fn none_listed(&self) -> bool {
        self.runtimes.is_empty()
    }

    /// Writes what is kept of them with the state: what each runs.
    pub fn write_runtimes<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.runtimes.serialize(serializer)
    }

    /// Reads what [`Wickets::write_runtimes`] wrote.
    pub fn read_runtimes<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Wickets, D::Error> {
        Ok(Wickets {
            runtimes: BTreeMap::deserialize(deserializer)?,
            ..Wickets::default()
        })
    }

    /// Whether an index of an app of `stack` placed on `wicket` stays there: not on one that
    /// lists another stack.
    fn keeps(&self, wicket: &str, stack: Option<Runtime>) -> bool {
        match (stack, self.runtimes.get(wicket)) {
            (Some(stack), Some(runs)) => stack == *runs,
            _ => true,
        }
    }

    /// Whether a new index of an app of `stack` may go to `wicket`: one listing that stack, no
    /// second name of a wicket. An app never started has no stack, and its indices go anywhere
    /// until it has.
    fn takes(&self, wicket: &str, stack: Option<Runtime>) -> bool {
        !self.odd.contains(wicket)
            && stack.is_none_or(|stack| self.runtimes.get(wicket) == Some(&stack))
    }
}

/// The stack of `app`: its last start's droplet's.
fn stack(app: &App) -> Option<Runtime> {
    app.running.as_ref().map(Running::stack)
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

/// `placement` of `count` indices of an app of `stack`: the highest dropped; an index on a wicket
/// not configured or of another stack, and each new one, on a wicket of its stack with room for
/// it holding the fewest of the others, of them the one with the most room, the first configured
/// of those. `rooms` is the room for the app's indices on the wickets of a known budget, its own
/// aside: the others have room enough. Pinned, every index on the one wicket, room or not.
fn placed(
    placement: &[String],
    count: usize,
    wickets: &Wickets,
    rooms: &BTreeMap<String, i64>,
    pin: Option<&Pin>,
    stack: Option<Runtime>,
) -> Vec<String> {
    // the one wicket of them all: index 0's, unpinned, without a home
    let home = pin.map(|pin| match pin {
        Pin::To(home) => Some(home.clone()),
        Pin::Together => placed(placement, count, wickets, rooms, None, stack)
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
        .map(|wicket| wicket.filter(|wicket| allowed(wicket) && wickets.keeps(wicket, stack)))
        .collect();
    placed.resize(count, None);
    let candidates: Vec<&String> = wickets
        .configured
        .iter()
        .filter(|wicket| wickets.takes(wicket, stack) && allowed(wicket))
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
        // none configured, or none of its stack: none placed
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

/// The indices of `app`, placed as `placement` has it, on `wicket`: those of its count placed
/// there and those moving there.
fn held(app: &App, placement: &[String], wicket: &str) -> usize {
    let count = count(app);
    let placed = placement.iter().take(count).filter(|on| *on == wicket);
    let moving = (app.moving.iter()).filter(|(index, to)| **index < count as u64 && *to == wicket);
    placed.count() + moving.count()
}

/// The moves of `app`, placed as `placement` has it, that still are: of a started app, of an
/// index it has, to a configured wicket of its stack other than the one it is on.
fn moving<'a>(
    app: &'a App,
    placement: &'a [String],
    wickets: &'a Wickets,
) -> impl Iterator<Item = (u64, &'a String)> + 'a {
    let running = takes_room(app);
    let stack = stack(app);
    (app.moving.iter())
        .filter(move |_| running)
        .filter(|(index, to)| (placement.get(**index as usize)).is_some_and(|on| on != *to))
        .filter(move |(_, to)| wickets.configured.contains(to) && wickets.takes(to, stack))
        .map(|(index, to)| (*index, to))
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
        let stack = stack(app);
        let configured = |wicket: &String| {
            self.wickets.configured.contains(wicket) && self.wickets.keeps(wicket, stack)
        };
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
            stack,
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
    /// that fit beside those of the other started apps placed there or moving there (as kept), 0
    /// at most on one that refused a share for room, is leaving or is gone.
    pub fn rooms(&self, app: &App) -> BTreeMap<String, i64> {
        let mut rooms = BTreeMap::new();
        for wicket in &self.wickets.configured {
            let full = self.wickets.full.contains_key(wicket)
                || self.wickets.leaving.contains_key(wicket)
                || self.wickets.gone.contains(wicket);
            let mut room = match self.wickets.budgets.get(wicket) {
                Some(budget) => {
                    let (instances, memory) = (self.apps.iter())
                        .filter(|other| other.guid != app.guid && takes_room(other))
                        .map(|other| {
                            let on = held(other, &other.placement, wicket) as i64;
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

    /// The stacks the wickets configured run, as each last listed, in the order configured: the
    /// first is the default.
    pub fn stacks(&self) -> Vec<Runtime> {
        let mut stacks = Vec::new();
        let listed = self.wickets.configured.iter();
        for runs in listed.filter_map(|wicket| self.wickets.runtimes.get(wicket)) {
            if !stacks.contains(runs) {
                stacks.push(*runs);
            }
        }
        stacks
    }

    /// Whether every wicket configured listed what it runs: [`State::stacks`] is all there is.
    pub fn all_listed(&self) -> bool {
        let mut configured = self.wickets.configured.iter();
        configured.all(|wicket| self.wickets.runtimes.contains_key(wicket))
    }

    /// Passes the highest index of the app `guid` on `wicket`, which refused its share for room,
    /// to a wicket of its stack with room for it: the line saying so; none without one.
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
            .filter(|other| *other != wicket && self.wickets.takes(other, stack(app)))
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

    /// The indices of `app` each wicket holds, the lowest first: an index moving on both
    /// wickets.
    pub fn indices(&self, app: &App) -> BTreeMap<String, Vec<u64>> {
        let mut indices: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        let placement = self.placement(app);
        for (index, wicket) in placement.iter().enumerate() {
            indices
                .entry(wicket.clone())
                .or_default()
                .push(index as u64);
        }
        for (index, to) in moving(app, &placement, &self.wickets) {
            let held = indices.entry(to.clone()).or_default();
            held.push(index);
            held.sort();
        }
        indices
    }

    /// Moves the indices of the app `guid` placed on `from`, a wicket leaving or gone, each to a
    /// wicket of its stack with room for it, neither leaving nor gone, holding the fewest of the app's, of them
    /// the one with the most room, the first configured of those ([`App::moving`]): those with
    /// nowhere to go stay, as they all do with its keyvalue data there. Of a started app only, its
    /// indices moving already moving on.
    pub fn evacuate(&mut self, guid: &str, from: &str) -> Evacuated {
        let evacuated = self.evacuation(guid, from);
        if let Some(app) = self.app_mut(guid) {
            app.moving.extend(evacuated.moves.iter().cloned());
        }
        evacuated
    }

    /// What [`State::evacuate`] would make of the indices of the app `guid` on `from`.
    pub fn evacuation(&self, guid: &str, from: &str) -> Evacuated {
        let Some(app) = self.app(guid).filter(|app| takes_room(app)) else {
            return Evacuated::default();
        };
        let placement = self.placement(app).into_owned();
        let leaving: Vec<u64> = (0..placement.len() as u64)
            .filter(|index| placement[*index as usize] == from)
            .filter(|index| !app.moving.contains_key(index))
            .collect();
        if self.pin(app).is_some() {
            return Evacuated {
                moves: Vec::new(),
                stay: leaving,
                why: "its keyvalue data is there",
            };
        }
        let rooms = self.rooms(app);
        let mut candidates: Vec<&String> = (self.wickets.configured.iter())
            .filter(|wicket| *wicket != from && self.takes_more(wicket))
            .collect();
        let others = candidates.len();
        candidates.retain(|wicket| self.wickets.takes(wicket, stack(app)));
        let why = match others > 0 && candidates.is_empty() {
            true => "no other wicket runs its stack",
            false => "no other wicket has room for it",
        };
        let mut held: Vec<i64> = (candidates.iter())
            .map(|wicket| held(app, &placement, wicket) as i64)
            .collect();
        let (mut moves, mut stay) = (Vec::new(), Vec::new());
        for index in leaving {
            let left = |at: usize| rooms.get(candidates[at]).map(|room| room - held[at]);
            let best = (0..candidates.len())
                .filter(|at| left(*at).is_none_or(|left| left >= 1))
                .min_by_key(|at| (held[*at], Reverse(left(*at).unwrap_or(i64::MAX))));
            match best {
                Some(at) => {
                    held[at] += 1;
                    moves.push((index, candidates[at].clone()));
                }
                None => stay.push(index),
            }
        }
        Evacuated { moves, stay, why }
    }

    /// The next index to move to `to`, a wicket back or new, for an app's spread
    /// ([`App::moving`]): of a started app but those `skipped`, not pinned to a keyvalue home,
    /// none of its indices moving, `to` of its stack with room for one more of it and holding two
    /// fewer of its indices than the wicket holding the most (neither leaving nor gone, the
    /// first configured of those), the highest index there. None: no spread is better for one.
    pub fn respread(&mut self, to: &str, skipped: &BTreeSet<String>) -> Option<(String, Move)> {
        if !self.wickets.configured.iter().any(|wicket| wicket == to) || !self.takes_more(to) {
            return None;
        }
        let found = self.apps.iter().find_map(|app| {
            let movable = takes_room(app) && app.moving.is_empty() && self.pin(app).is_none();
            let movable = movable && self.wickets.takes(to, stack(app));
            if skipped.contains(&app.guid) || !movable {
                return None;
            }
            let placement = self.placement(app);
            let on = |wicket: &str| placement.iter().filter(|on| *on == wicket).count() as i64;
            let here = on(to);
            if (self.rooms(app).get(to)).is_some_and(|room| room - here < 1) {
                return None;
            }
            // of the wickets holding the most, the first configured
            let away = |wicket: &String| {
                self.wickets.leaving.contains_key(wicket) || self.wickets.gone.contains(wicket)
            };
            let from = (self.wickets.configured.iter())
                .filter(|wicket| *wicket != to && !away(wicket))
                .rev()
                .max_by_key(|wicket| on(wicket))?;
            if on(from) < here + 2 {
                return None;
            }
            let index = placement.iter().rposition(|on| on == from)? as u64;
            Some((app.guid.clone(), index))
        });
        let (guid, index) = found?;
        let app = self.app_mut(&guid)?;
        app.moving.insert(index, to.to_string());
        Some((guid, (index, to.to_string())))
    }

    /// The index `index` of the app `guid`, moving to `to`, serves there: placed there, out of
    /// the share of the wicket it was on, which is said. None when it moves there no more (the
    /// app stopped, scaled below it).
    pub fn moved(&mut self, guid: &str, index: u64, to: &str) -> Option<String> {
        let app = self.app(guid)?;
        if app.moving.get(&index).map(String::as_str) != Some(to) {
            return None;
        }
        let mut placement = self.placement(app).into_owned();
        let from = std::mem::replace(placement.get_mut(index as usize)?, to.to_string());
        let app = self.app_mut(guid)?;
        app.moving.remove(&index);
        app.placement = placement;
        Some(from)
    }

    /// The index `index` of the app `guid` moves to `to` no more: it stays where it is placed,
    /// which is said. None when it was not moving there.
    pub fn unmoved(&mut self, guid: &str, index: u64, to: &str) -> Option<String> {
        let app = self.app_mut(guid)?;
        if app.moving.get(&index).map(String::as_str) != Some(to) {
            return None;
        }
        app.moving.remove(&index);
        let app = self.app(guid)?;
        self.placement(app).get(index as usize).cloned()
    }

    /// Whether `wicket` takes indices: a second name of none, neither leaving nor gone.
    fn takes_more(&self, wicket: &str) -> bool {
        !self.wickets.odd.contains(wicket)
            && !self.wickets.leaving.contains_key(wicket)
            && !self.wickets.gone.contains(wicket)
    }

    /// Keeps the placement of every app as [`State::placement`] has it, in turn: at every
    /// change, its moves those that still are ([`moving`]). A namespace of a store of each
    /// wicket's host an app maps without a home has it where the app's index 0 is.
    pub fn place(&mut self) {
        for at in 0..self.apps.len() {
            if let Cow::Owned(placed) = self.placement(&self.apps[at]) {
                self.apps[at].placement = placed;
            }
            let app = &self.apps[at];
            if !app.moving.is_empty() {
                let still: BTreeMap<u64, String> = moving(app, &app.placement, &self.wickets)
                    .map(|(index, to)| (index, to.clone()))
                    .collect();
                self.apps[at].moving = still;
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
        (self.wickets.runtimes).retain(|wicket, _| configured.contains(wicket));
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
        let running = self.apps.iter_mut().flat_map(App::snapshots_mut);
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
impl State {
    /// [`State::follow_wickets`] of `configured`, each having listed that it runs `runs`.
    pub fn follow_listed(&mut self, configured: &[&str], runs: Runtime) -> Vec<String> {
        let configured: Vec<String> = configured.iter().map(|name| name.to_string()).collect();
        self.wickets.runtimes = (configured.iter().cloned().map(|name| (name, runs))).collect();
        self.follow_wickets(&configured)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Running;
    use Runtime::{Firecracker, Wasm};

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    /// The wickets `configured`, each listing the stack `runs` names it by (`w`: wasm, `f`:
    /// Firecracker, `?`: none yet), of them `odd` second names.
    fn wickets(configured: &[&str], runs: &str, odd: &[&str]) -> Wickets {
        let runtimes = configured.iter().zip(runs.chars());
        Wickets {
            configured: names(configured),
            odd: odd.iter().map(|name| name.to_string()).collect(),
            runtimes: runtimes
                .filter_map(|(name, runs)| {
                    let runs = match runs {
                        'w' => Wasm,
                        'f' => Firecracker,
                        _ => return None,
                    };
                    Some((name.to_string(), runs))
                })
                .collect(),
            ..Wickets::default()
        }
    }

    /// Each new index on a wicket of the app's stack with room holding the fewest of the app's,
    /// of them the one with the most room, the first configured of those: distinct wickets while
    /// there are some. Stable: a scale down drops the highest, a scale up after it places as
    /// before, what is placed stays. No room anywhere: placed as if all had. An index on a
    /// wicket of another stack is placed again; with no wicket of its stack, it is not placed.
    #[test]
    fn an_index_goes_where_there_is_room_and_the_app_has_the_fewest() {
        let ab = wickets(&["a", "b"], "ww", &[]);
        let abc = wickets(&["a", "b", "c"], "www", &[]);
        // a is a second name of a wicket
        let odd = wickets(&["a", "b"], "ww", &["a"]);
        let none = wickets(&[], "", &[]);
        // wickets of both stacks, c yet to list what it runs
        let mixed = wickets(&["a", "f", "b", "g", "c"], "wfwf?", &[]);
        let vms = wickets(&["f", "g"], "ff", &[]);
        for (placement, count, wickets, rooms, expected) in [
            // an app's indices on the wickets of its stack: a component's here, a rootfs's below
            (&[][..], 3, &mixed, &[][..], &["a", "b", "a"][..]),
            // on a wicket of another stack (the app started with a component, a rootfs before),
            // placed again; on one yet to list, it stays, and none new goes there
            (&["f", "g", "c"], 4, &mixed, &[], &["a", "b", "c", "a"]),
            // no wicket of its stack: not placed
            (&["f"], 2, &vms, &[], &[]),
            (&[], 1, &ab, &[], &["a"]),
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
                placed(&names(placement), count, wickets, &rooms, None, Some(Wasm)),
                names(expected),
                "{placement:?} at {count} on {wickets:?}, {rooms:?}"
            );
        }
        let rootfs = |placement: &[&str], count| {
            let none = BTreeMap::new();
            placed(
                &names(placement),
                count,
                &mixed,
                &none,
                None,
                Some(Firecracker),
            )
        };
        assert_eq!(rootfs(&[], 3), ["f", "g", "f"]);
        assert_eq!(
            rootfs(&["a", "g"], 2),
            ["f", "g"],
            "a start of another stack"
        );
        // an app never started has no stack: anywhere, until it has
        let unstaged = placed(&[], 2, &mixed, &BTreeMap::new(), None, None);
        assert_eq!(unstaged, ["a", "f"]);
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
                placed(&names(placement), count, &ab, rooms, Some(&pin), Some(Wasm)),
                names(expected),
                "{placement:?} at {count} pinned {pin:?}, {rooms:?}"
            );
        }
    }

    /// A wicket's room is its budget less the indices of the other started apps placed there or
    /// moving there, a VM's memory too, none on a wicket that refused a share for room, is
    /// leaving or is gone; such a refusal passes the app's highest index there to a wicket with
    /// room. A wicket leaving or gone has its indices moved to wickets with room, as new ones go,
    /// a wicket back those that spread an app again. A wicket's instance is the app's index of
    /// its share.
    #[test]
    fn the_room_is_what_the_budget_leaves_and_a_refusal_passes_an_index() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        state.follow_listed(&["a", "b"], Wasm);
        let space = state.spaces[0].guid.clone();
        let mut started = |name: &str, instances: u64, memory_mib: u64| {
            let app = state.create_app(name, &space);
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
        // nor moved off it leaving: they stay until it stops
        state.wickets.leaving.insert("a".into(), 60);
        assert_eq!(
            state.evacuate(&big, "a"),
            Evacuated {
                moves: Vec::new(),
                stay: vec![0, 1, 2],
                why: "its keyvalue data is there"
            }
        );
        state.wickets.leaving.clear();
        // its home no longer named: the data stays there, the apps run on another without it
        let said = state.follow_listed(&["b"], Wasm);
        assert!(
            said.iter()
                .any(|line| line.contains("local/cf/l stays on the wicket a")),
            "{said:?}"
        );
        assert_eq!(state.app(&big).unwrap().placement, ["b", "b", "b"]);
        assert_eq!(state.homes, [("local/cf/l".into(), "b".into())].into());

        // b leaving: no room there; each index of a started app on it moves to a wicket with
        // room holding the fewest of the app's (two's 1 to c, which holds none of it), the room
        // of the one it moves to taken (one's 0 to a: c is full); with none, it stays
        let mut state = State::new("o", "s", &["localhost".into()]);
        state.follow_listed(&["a", "b", "c"], Wasm);
        let space = state.spaces[0].guid.clone();
        let mut started = |name: &str, placement: &[&str]| {
            let app = state.create_app(name, &space);
            app.started = true;
            app.process.instances = placement.len() as u64;
            app.running = Some(Running::default());
            app.placement = names(placement);
            app.guid.clone()
        };
        let two = started("two", &["a", "b"]);
        let one = started("one", &["b"]);
        let three = started("three", &["a", "a", "c"]);
        let stopped = started("stopped", &["b"]);
        state.app_mut(&stopped).unwrap().started = false;
        state.wickets.budgets = [("a", 4), ("b", 4), ("c", 2)]
            .map(|(name, instances)| {
                let budget = Budget {
                    instances,
                    memory_mib: None,
                };
                (name.to_string(), budget)
            })
            .into();
        state.wickets.leaving.insert("b".into(), 60);
        assert_eq!(state.rooms(state.app(&one).unwrap())["b"], 0, "leaving");
        let moved = |moves: &[(u64, &str)], stay: &[u64]| Evacuated {
            moves: (moves.iter())
                .map(|(index, to)| (*index, to.to_string()))
                .collect(),
            stay: stay.to_vec(),
            why: "no other wicket has room for it",
        };
        assert_eq!(state.evacuate(&two, "b"), moved(&[(1, "c")], &[]));
        let indices = state.indices(state.app(&two).unwrap());
        assert_eq!(
            indices,
            [("a", vec![0]), ("b", vec![1]), ("c", vec![1])]
                .map(|(wicket, indices)| (wicket.to_string(), indices))
                .into(),
            "both hold it"
        );
        state.wickets.full.insert("a".into(), 1);
        assert_eq!(state.evacuate(&one, "b"), moved(&[], &[0]));
        state.wickets.full.clear();
        assert_eq!(state.evacuate(&one, "b"), moved(&[(0, "a")], &[]));
        assert_eq!(state.evacuate(&one, "b"), moved(&[], &[]), "moving already");
        assert_eq!(state.evacuate(&stopped, "b"), Evacuated::default());
        // two's served on c: placed there, out of b's share; one's not: back on b alone
        assert_eq!(state.moved(&two, 1, "c").as_deref(), Some("b"));
        assert_eq!(state.app(&two).unwrap().placement, ["a", "c"]);
        assert_eq!(state.unmoved(&one, 0, "a").as_deref(), Some("b"));
        assert_eq!(state.moved(&one, 0, "a"), None, "moving no more");
        assert_eq!(state.indices(state.app(&one).unwrap()).len(), 1);
        // a stop ends a move
        state.evacuate(&one, "b");
        state.app_mut(&one).unwrap().started = false;
        state.place();
        assert!(state.app(&one).unwrap().moving.is_empty());

        // b back: of the apps holding two more indices on a wicket than on b, the highest
        // there moves to b, one app at a time; none once spread, nor to a wicket leaving
        state.wickets.leaving.clear();
        let none = BTreeSet::new();
        let respread = state.respread("b", &none);
        assert_eq!(respread, Some((three.clone(), (1, "b".to_string()))));
        assert_eq!(state.respread("b", &none), None, "three moving already");
        assert_eq!(state.moved(&three, 1, "b").as_deref(), Some("a"));
        assert_eq!(state.app(&three).unwrap().placement, ["a", "b", "c"]);
        assert_eq!(state.respread("b", &none), None);
        state.app_mut(&three).unwrap().placement = names(&["a", "a", "c"]);
        assert_eq!(
            state.respread("b", &[three.clone()].into()),
            None,
            "skipped"
        );
        state.wickets.leaving.insert("b".into(), 60);
        assert_eq!(state.respread("b", &none), None, "leaving");

        // b gone (it has not answered for the bound): no room there, its indices moving as off a
        // wicket leaving (three's 1 to a: c has no room left), none spread to it; a gone, none
        // spread from it
        state.wickets.leaving.clear();
        state.wickets.gone.insert("b".into());
        assert_eq!(state.rooms(state.app(&two).unwrap())["b"], 0, "gone");
        assert_eq!(state.respread("b", &none), None, "gone");
        state.app_mut(&three).unwrap().placement = names(&["a", "b", "c"]);
        assert_eq!(state.evacuate(&three, "b"), moved(&[(1, "a")], &[]));
        assert_eq!(state.moved(&three, 1, "a").as_deref(), Some("b"));
        state.wickets.gone = ["a".to_string()].into();
        assert_eq!(state.respread("b", &none), None, "from a wicket gone");
        state.wickets.gone.clear();
        assert!(state.respread("b", &none).is_some());

        // wickets of two stacks: a component's index is passed to a wicket of its stack and
        // spreads to one alone; a rootfs's on the one Firecracker wicket, leaving, stays
        let mut state = State::new("o", "s", &["localhost".into()]);
        state.wickets = wickets(&["a", "f", "b"], "wfw", &[]);
        let space = state.spaces[0].guid.clone();
        let mut started = |name: &str, file: &str, placement: &[&str]| {
            let app = state.create_app(name, &space);
            app.started = true;
            app.process.instances = placement.len() as u64;
            app.running = Some(Running {
                file: file.into(),
                ..Running::default()
            });
            app.placement = names(placement);
            app.guid.clone()
        };
        let component = started("component", "c.wasm", &["a", "a"]);
        let rootfs = started("rootfs", "r.ext4", &["f"]);
        assert_eq!(state.respread("f", &none), None, "of another stack");
        state.wickets.full.insert("a".into(), 1);
        assert_eq!(
            state.pass(&component, "a").as_deref(),
            Some("Index 1 placed on the wicket b: the wicket a has no room for it")
        );
        state.wickets.leaving.insert("f".into(), 60);
        assert_eq!(
            state.evacuate(&rootfs, "f"),
            Evacuated {
                moves: Vec::new(),
                stay: vec![0],
                why: "no other wicket runs its stack"
            }
        );
        // started as a component: placed again, on the wickets of that stack
        state
            .app_mut(&rootfs)
            .unwrap()
            .running
            .as_mut()
            .unwrap()
            .file = "c.wasm".into();
        state.place();
        assert_eq!(state.app(&rootfs).unwrap().placement, ["b"]);
    }
}
