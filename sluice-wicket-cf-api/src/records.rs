//! The state as records: a row per organization, space, app, route, package, build, ... and one
//! per thing kept whole (the removals, the purges, the wickets' ids, the keyvalue homes, the
//! wickets' stacks, the wickets configured, the blobs' sweeps), so a change writes what it
//! changed and another process takes what changed (`crate::postgres`).
//!
//! What the wickets were seen doing (`crate::controller`) is rows of its own, one per thing seen:
//! what each wicket answered of each app's share, what each wicket listed of itself (its budget,
//! leaving, gone, ...), whether each keyvalue store is one of them all. As what is kept whole,
//! each is told from what was written by its body: noted aside from a change (`Store::observe`),
//! it is written with the next one, by an instance whose turn reconciling the wickets did not end
//! meanwhile (`crate::postgres`); one whose turn ended forgets what it did not write
//! ([`Book::forget_observed`]). They are not in `state.json`: one process's are its own.
//!
//! A row is its kind, its id (an entity's guid), its body (the entity's JSON, as `state.json`
//! holds it) and its `ord`: the rows of a kind in the order of their `ord` are the state's list
//! of them, whose order says something (the first domain takes the default routes, the last
//! package of an app is its newest). A new row's `ord` is above all there are, so a list that
//! grows at its end rewrites none; a list put in another order rewrites the `ord` of those that
//! moved back and of all behind them.
//!
//! A row may be of another build, older or newer, on the store with this one: one of a kind
//! this build does not know is left alone, and what a row holds that this build does not read (a
//! field of a build to come) is kept when this build writes the row again ([`Book::as_written`]),
//! so a build's field is not lost to another's write. A row is told from what was written by
//! its body as this build writes it, not as another did.

use std::collections::{HashMap, HashSet};

use std::borrow::Cow;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::placement::Observed;
use crate::store::{
    App, Binding, Broker, Build, Deployment, Droplet, Instance, Job, Named, Outcome, Package, Role,
    Route, SecurityGroup, Space, State, User,
};

/// The kind of the rows kept whole, their id what they are.
pub const SINGLE: &str = "single";

/// The kinds of what the wickets were seen doing: what one answered of an app's share (id
/// `<app guid>/<wicket>`), what one listed of itself (id its name), whether a keyvalue store is
/// one of them all (id its name).
const OUTCOME: &str = "outcome";
const WICKET: &str = "wicket";
const STORE: &str = "store";
const OBSERVED: [&str; 3] = [OUTCOME, WICKET, STORE];

/// A row to write: `body` None removes it.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub kind: String,
    pub id: String,
    pub ord: i64,
    pub body: Option<String>,
}

/// What a process knows of the rows behind its copy of the state.
#[derive(Default)]
pub struct Book {
    /// The revision its copy is of.
    pub rev: i64,
    /// The `ord` of each row, by kind then id.
    ords: HashMap<&'static str, HashMap<String, i64>>,
    /// The body of each row kept whole, as this build writes it.
    singles: HashMap<String, String>,
    /// The rows changed aside from a transaction, to write with the next (`Store::note_kept`).
    deferred: HashSet<(&'static str, String)>,
    /// The body of each row of what the wickets were seen doing, by kind then id, as this build
    /// writes it.
    observed: HashMap<(&'static str, String), String>,
    /// The rows holding what this build does not read, by kind then id: the body as it was
    /// taken, and as this build writes what it read of it.
    unread: HashMap<(&'static str, String), (Value, Value)>,
}

/// Puts into `new`, a body as this build writes it, what `taken` holds that this build did not
/// read (`read`: what it wrote of `taken` then): the fields of an object, at any depth; those of
/// a list's items only while the list is as it was read, its items then the same ones.
fn keep_unread(new: &mut Value, taken: &Value, read: &Value) {
    match (new, taken, read) {
        (Value::Object(new), Value::Object(taken), Value::Object(read)) => {
            for (key, taken) in taken {
                match (new.get_mut(key), read.get(key)) {
                    (None, None) => {
                        new.insert(key.clone(), taken.clone());
                    }
                    (Some(new), Some(read)) => keep_unread(new, taken, read),
                    _ => {}
                }
            }
        }
        (Value::Array(new), Value::Array(taken), Value::Array(read))
            if new == read && taken.len() == read.len() =>
        {
            for (new, (taken, read)) in new.iter_mut().zip(taken.iter().zip(read)) {
                keep_unread(new, taken, read);
            }
        }
        _ => {}
    }
}

/// An entity of a list of the state.
pub trait Entity: Serialize + DeserializeOwned + PartialEq {
    fn id(&self) -> &str;
}

macro_rules! entities {
    ($($type:ty),*) => {
        $(impl Entity for $type {
            fn id(&self) -> &str {
                &self.guid
            }
        })*
    };
}

entities!(
    Named,
    Space,
    Role,
    User,
    App,
    Route,
    Package,
    Build,
    Droplet,
    Deployment,
    Job,
    Broker,
    Instance,
    Binding,
    SecurityGroup
);

/// The lists of the state and the kind of their rows: one place names them for the diff, the
/// take of a row and the kinds there are.
macro_rules! lists {
    ($($field:ident: $kind:literal),* $(,)?) => {
        /// The kind `kind` as this build names it; None: one of a build to come.
        fn known(kind: &str) -> Option<&'static str> {
            match kind {
                $($kind => Some($kind),)*
                SINGLE => Some(SINGLE),
                kind => OBSERVED.into_iter().find(|observed| *observed == kind),
            }
        }

        fn diff_lists(before: &State, after: &State, diff: &mut Diff<'_>) {
            $(diff.list($kind, &before.$field, &after.$field);)*
        }

        /// Whether the lists of both are the same.
        #[cfg(debug_assertions)]
        pub fn same_lists(a: &State, b: &State) -> bool {
            $(a.$field == b.$field)&&*
        }

        fn take_listed(
            state: &mut State,
            book: &mut Book,
            kind: &'static str,
            row: &Row,
        ) -> Result<Option<String>, String> {
            match kind {
                $($kind => take(&mut state.$field, book.ords.entry($kind).or_default(), row),)*
                _ => Ok(None),
            }
        }

        fn clear_lists(state: &mut State) {
            $(state.$field.clear();)*
        }
    };
}

lists!(
    orgs: "org",
    spaces: "space",
    roles: "role",
    users: "user",
    domains: "domain",
    apps: "app",
    routes: "route",
    packages: "package",
    builds: "build",
    droplets: "droplet",
    deployments: "deployment",
    jobs: "job",
    brokers: "broker",
    instances: "instance",
    bindings: "binding",
    security_groups: "security_group",
);

/// What is kept whole, by the id of its row.
macro_rules! singles {
    ($($id:literal: $($field:ident).+),* $(,)?) => {
        fn diff_singles(after: &State, diff: &mut Diff<'_>) {
            $(diff.single($id, &after.$($field).+);)*
        }

        fn take_single(
            state: &mut State,
            book: &mut Book,
            row: &Row,
        ) -> Result<Option<String>, String> {
            match row.id.as_str() {
                $($id => {
                    let Some(body) = &row.body else {
                        state.$($field).+ = Default::default();
                        book.singles.remove($id);
                        return Ok(None);
                    };
                    state.$($field).+ = serde_json::from_str(body).map_err(|e| e.to_string())?;
                    let read = json(&state.$($field).+);
                    book.singles.insert($id.to_string(), read.clone());
                    Ok(Some(read))
                })*
                // of a build to come
                _ => Ok(None),
            }
        }

        fn clear_singles(state: &mut State) {
            $(state.$($field).+ = Default::default();)*
        }
    };
}

singles!(
    "removals": removals,
    "purges": purges,
    "wicket_ids": wicket_ids,
    "homes": homes,
    "wicket_stacks": wickets.runtimes,
    "wickets": wickets.configured,
    "sweep": sweep,
);

/// The rows of what `state` says the wickets were seen doing, by kind and id: their bodies.
fn observed_rows(state: &State) -> HashMap<(&'static str, String), String> {
    let mut rows = HashMap::new();
    for (guid, outcomes) in &state.outcomes {
        for (wicket, outcome) in outcomes {
            rows.insert((OUTCOME, format!("{guid}/{wicket}")), json(outcome));
        }
    }
    for name in state.wickets.observed_names() {
        let observed = state.wickets.observed(&name);
        if observed != Observed::default() {
            rows.insert((WICKET, name), json(&observed));
        }
    }
    for (store, one) in &state.wickets.stores {
        rows.insert((STORE, store.clone()), json(one));
    }
    rows
}

/// What is written of a value of the state: its JSON.
fn json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("a state's value is JSON")
}

/// The body `body` of a row of what the wickets were seen doing, as this build writes it.
fn observed_as_read(kind: &str, body: &str) -> Result<String, String> {
    fn read<T: Serialize + DeserializeOwned>(body: &str) -> Result<String, String> {
        let value: T = serde_json::from_str(body).map_err(|e| e.to_string())?;
        Ok(json(&value))
    }
    match kind {
        OUTCOME => read::<Outcome>(body),
        WICKET => read::<Observed>(body),
        _ => read::<bool>(body),
    }
}

/// Puts what the row `id` of the kind `kind` says the wickets were seen doing into `state`: none,
/// its body None.
fn take_observed(
    state: &mut State,
    kind: &str,
    id: &str,
    body: Option<&str>,
) -> Result<(), String> {
    fn parse<T: DeserializeOwned>(body: &str) -> Result<T, String> {
        serde_json::from_str(body).map_err(|e| e.to_string())
    }
    match kind {
        OUTCOME => {
            let (guid, wicket) = id.split_once('/').ok_or("no <app>/<wicket>")?;
            match body {
                Some(body) => {
                    let outcome: Outcome = parse(body)?;
                    let outcomes = state.outcomes.entry(guid.to_string()).or_default();
                    outcomes.insert(wicket.to_string(), outcome);
                }
                None => {
                    if let Some(outcomes) = state.outcomes.get_mut(guid) {
                        outcomes.remove(wicket);
                        if outcomes.is_empty() {
                            state.outcomes.remove(guid);
                        }
                    }
                }
            }
        }
        WICKET => {
            let observed: Option<Observed> = body.map(parse).transpose()?;
            let observed = observed.unwrap_or_default();
            state.wickets.set_observed(id, observed);
        }
        STORE => match body {
            Some(body) => {
                state.wickets.stores.insert(id.to_string(), parse(body)?);
            }
            None => {
                state.wickets.stores.remove(id);
            }
        },
        _ => {}
    }
    Ok(())
}

/// Clears what `state` says the wickets were seen doing.
fn clear_observed(state: &mut State) {
    state.outcomes.clear();
    state.wickets.clear_observed();
}

/// The rows a change writes, as they are found.
struct Diff<'a> {
    book: &'a Book,
    /// The `ord` of the next new row.
    next: i64,
    rows: Vec<Row>,
}

impl Diff<'_> {
    fn list<T: Entity>(&mut self, kind: &'static str, before: &[T], after: &[T]) {
        let deferred = (self.book.deferred.iter()).any(|(of, _)| *of == kind);
        if before == after && !deferred {
            return;
        }
        let none = HashMap::new();
        let ords = self.book.ords.get(kind).unwrap_or(&none);
        let was: HashMap<&str, &T> = before.iter().map(|item| (item.id(), item)).collect();
        let mut kept = HashSet::new();
        let mut last = i64::MIN;
        for item in after {
            let id = item.id();
            kept.insert(id);
            // where it was, unless it moved back behind one before it
            let stays = ords.get(id).copied().filter(|ord| *ord > last);
            let ord = stays.unwrap_or_else(|| {
                self.next += 1;
                self.next
            });
            last = ord;
            let same = stays.is_some()
                && was.get(id).is_some_and(|was| *was == item)
                && !self.book.deferred.contains(&(kind, id.to_string()));
            if !same {
                self.rows.push(Row {
                    kind: kind.to_string(),
                    id: id.to_string(),
                    ord,
                    body: Some(json(item)),
                });
            }
        }
        for id in before
            .iter()
            .map(Entity::id)
            .filter(|id| !kept.contains(id))
        {
            self.rows.push(Row {
                kind: kind.to_string(),
                id: id.to_string(),
                ord: 0,
                body: None,
            });
        }
    }

    /// What the wickets were seen doing, as `state` says it: told from what was written by its
    /// bodies, as what is kept whole.
    fn observed(&mut self, state: &State) {
        let rows = observed_rows(state);
        for ((kind, id), body) in &rows {
            if self.book.observed.get(&(*kind, id.clone())) != Some(body) {
                self.rows.push(Row {
                    kind: kind.to_string(),
                    id: id.clone(),
                    ord: 0,
                    body: Some(body.clone()),
                });
            }
        }
        for (kind, id) in self.book.observed.keys() {
            if !rows.contains_key(&(*kind, id.clone())) {
                self.rows.push(Row {
                    kind: kind.to_string(),
                    id: id.clone(),
                    ord: 0,
                    body: None,
                });
            }
        }
    }

    /// What is kept whole is small: told from what was written by its body, so a change aside
    /// from a transaction (`Store::note`: what a wicket listed it runs) is
    /// written with the next.
    fn single<T: Serialize>(&mut self, id: &'static str, value: &T) {
        let body = json(value);
        if self.book.singles.get(id) != Some(&body) {
            self.rows.push(Row {
                kind: SINGLE.to_string(),
                id: id.to_string(),
                ord: 0,
                body: Some(body),
            });
        }
    }
}

/// Puts the row in `items`, at its `ord`; removes it when it is gone. Its body as this build
/// writes it.
fn take<T: Entity>(
    items: &mut Vec<T>,
    ords: &mut HashMap<String, i64>,
    row: &Row,
) -> Result<Option<String>, String> {
    let at = items.iter().position(|item| item.id() == row.id);
    let Some(body) = &row.body else {
        if let Some(at) = at {
            items.remove(at);
        }
        ords.remove(&row.id);
        return Ok(None);
    };
    let item: T = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let read = json(&item);
    match at {
        Some(at) if ords.get(&row.id) == Some(&row.ord) => items[at] = item,
        _ => {
            if let Some(at) = at {
                items.remove(at);
            }
            // behind the last one before it: a new row is the last of them all
            let before = |item: &T| ords.get(item.id()).is_some_and(|ord| *ord < row.ord);
            let at = items.iter().rposition(before).map_or(0, |at| at + 1);
            items.insert(at, item);
            ords.insert(row.id.clone(), row.ord);
        }
    }
    Ok(Some(read))
}

/// The most new rows one change gives an `ord`: the revisions' `ord`s are that far apart.
const ORDS: i64 = 1 << 24;

impl Book {
    /// The rows that make the state `after` of `before` (this book's), written as the revision
    /// `rev`: all of them with `before` None, a store's first; with `observed` those of what the
    /// wickets were seen doing too.
    pub fn diff(
        &self,
        before: Option<&State>,
        after: &State,
        rev: i64,
        observed: bool,
    ) -> Vec<Row> {
        let mut diff = Diff {
            book: self,
            next: rev * ORDS,
            rows: Vec::new(),
        };
        let none = State::default();
        diff_lists(before.unwrap_or(&none), after, &mut diff);
        diff_singles(after, &mut diff);
        if observed {
            diff.observed(after);
        }
        debug_assert!(
            diff.next < (rev + 1) * ORDS,
            "more rows than a change orders"
        );
        diff.rows
    }

    /// The `rows` of [`Book::diff`] as the store takes them: each with what the row it writes
    /// over holds that this build does not read.
    pub fn as_written<'a>(&self, rows: &'a [Row]) -> Cow<'a, [Row]> {
        if self.unread.is_empty() {
            return Cow::Borrowed(rows);
        }
        let written = rows.iter().map(|row| {
            let unread = known(&row.kind).and_then(|kind| self.unread.get(&(kind, row.id.clone())));
            let (Some(body), Some((taken, read))) = (&row.body, unread) else {
                return row.clone();
            };
            let mut new: Value = serde_json::from_str(body).expect("a row's body is JSON");
            keep_unread(&mut new, taken, read);
            Row {
                body: Some(new.to_string()),
                ..row.clone()
            }
        });
        Cow::Owned(written.collect())
    }

    /// The row `id` of `kind` was taken with the body `taken`, which this build writes as
    /// `read`: what it holds more is kept for when this build writes it; nothing of a row gone.
    fn note_unread(&mut self, kind: &'static str, id: &str, taken: Option<&str>, read: &str) {
        let key = (kind, id.to_string());
        let unread = taken.filter(|taken| *taken != read).and_then(|taken| {
            let taken: Value = serde_json::from_str(taken).ok()?;
            let read: Value = serde_json::from_str(read).ok()?;
            (taken != read).then_some((taken, read))
        });
        match unread {
            Some(unread) => self.unread.insert(key, unread),
            None => self.unread.remove(&key),
        };
    }

    /// The rows are written: what this book knows of them.
    pub fn written(&mut self, rows: &[Row], rev: i64) {
        for row in rows {
            let Some(kind) = known(&row.kind) else {
                continue;
            };
            if row.body.is_none() {
                self.unread.remove(&(kind, row.id.clone()));
            }
            if kind == SINGLE {
                if let Some(body) = &row.body {
                    self.singles.insert(row.id.clone(), body.clone());
                }
                continue;
            }
            if OBSERVED.contains(&kind) {
                let key = (kind, row.id.clone());
                match &row.body {
                    Some(body) => self.observed.insert(key, body.clone()),
                    None => self.observed.remove(&key),
                };
                continue;
            }
            let ords = self.ords.entry(kind).or_default();
            match row.body {
                Some(_) => ords.insert(row.id.clone(), row.ord),
                None => ords.remove(&row.id),
            };
        }
        self.deferred.clear();
        self.rev = rev;
    }

    /// The row `id` of `kind` changed aside from a transaction: written with the next.
    pub fn defer(&mut self, kind: &str, id: &str) {
        if let Some(kind) = known(kind) {
            self.deferred.insert((kind, id.to_string()));
        }
    }

    /// Takes the rows another process wrote up to the revision `rev` into `state`; `all`: they
    /// are all there are, what `state` kept before is gone. A row this build cannot read is
    /// said and left out.
    pub fn take(&mut self, state: &mut State, rows: &[Row], rev: i64, all: bool) {
        if all {
            clear_lists(state);
            clear_singles(state);
            clear_observed(state);
            self.ords.clear();
            self.singles.clear();
            self.observed.clear();
            self.unread.clear();
        }
        for row in rows {
            // what is there is the store's: nothing of it is written over
            self.deferred
                .retain(|(kind, id)| *kind != row.kind || *id != row.id);
            let Some(kind) = known(&row.kind) else {
                continue;
            };
            let taken = match kind {
                SINGLE => take_single(state, self, row),
                kind if OBSERVED.contains(&kind) => {
                    let key = (kind, row.id.clone());
                    let read = (row.body.as_deref())
                        .map(|body| observed_as_read(kind, body))
                        .transpose();
                    read.and_then(|read| {
                        match &read {
                            Some(read) => self.observed.insert(key, read.clone()),
                            None => self.observed.remove(&key),
                        };
                        take_observed(state, kind, &row.id, read.as_deref())?;
                        Ok(read)
                    })
                }
                kind => take_listed(state, self, kind, row),
            };
            match taken {
                Ok(read) => {
                    let read = read.as_deref().unwrap_or_default();
                    self.note_unread(kind, &row.id, row.body.as_deref(), read);
                }
                Err(e) => eprintln!("STATE: cannot read the {} {}: {e}", row.kind, row.id),
            }
            if kind == "app" && row.body.is_none() {
                state.outcomes.remove(&row.id);
            }
        }
        self.rev = rev;
    }

    /// Puts back in `state` what the wickets were seen doing as written: what this process saw
    /// and did not write is forgotten (its turn reconciling them ended).
    pub fn forget_observed(&self, state: &mut State) {
        clear_observed(state);
        for ((kind, id), body) in &self.observed {
            if let Err(e) = take_observed(state, kind, id, Some(body)) {
                eprintln!("STATE: cannot read the {kind} {id}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// What a row holds that this build does not read is kept as this build writes it again:
    /// in an object at any depth, in a list's items while the list is as it was read; what
    /// this build reads is as it writes it, a field it left out included.
    #[test]
    fn what_a_build_does_not_read_of_a_row_is_kept_as_it_writes_it() {
        let taken = json!({
            "name": "a", "later": {"of": 1}, "unset": 2,
            "process": {"instances": 1, "later": true},
            "rules": [{"port": 1, "later": "x"}, {"port": 2}],
            "moved": [{"port": 1, "later": "y"}],
        });
        let read = json!({
            "name": "a", "unset": 2,
            "process": {"instances": 1},
            "rules": [{"port": 1}, {"port": 2}],
            "moved": [{"port": 1}],
        });
        let mut new = json!({
            "name": "b",
            "process": {"instances": 3},
            "rules": [{"port": 1}, {"port": 2}],
            "moved": [{"port": 9}, {"port": 1}],
        });
        keep_unread(&mut new, &taken, &read);
        let kept = json!({
            "name": "b", "later": {"of": 1},
            "process": {"instances": 3, "later": true},
            "rules": [{"port": 1, "later": "x"}, {"port": 2}],
            "moved": [{"port": 9}, {"port": 1}],
        });
        assert_eq!(new, kept);
    }
}
