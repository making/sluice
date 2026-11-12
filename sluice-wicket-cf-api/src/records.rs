//! The state as records: a row per organization, space, app, route, package, build, ... and one
//! per thing kept whole (the removals, the purges, the flume's cursor, the wickets' ids, the
//! keyvalue homes, the wickets' stacks), so a change writes what it changed and another process
//! takes what changed (`crate::postgres`).
//!
//! A row is its kind, its id (an entity's guid), its body (the entity's JSON, as `state.json`
//! holds it) and its `ord`: the rows of a kind in the order of their `ord` are the state's list
//! of them, whose order says something (the first domain takes the default routes, the last
//! package of an app is its newest). A new row's `ord` is above all there are, so a list that
//! grows at its end rewrites none; a list put in another order rewrites the `ord` of those that
//! moved back and of all behind them.

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::store::{
    App, Binding, Broker, Build, Deployment, Droplet, Instance, Job, Named, Package, Role, Route,
    SecurityGroup, Space, State, User,
};

/// The kind of the rows kept whole, their id what they are.
pub const SINGLE: &str = "single";

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
    /// The body of each row kept whole, as written.
    singles: HashMap<String, String>,
    /// The rows changed aside from a transaction, to write with the next (`Store::note_kept`).
    deferred: HashSet<(&'static str, String)>,
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
                _ => None,
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
        ) -> Result<(), String> {
            match kind {
                $($kind => take(&mut state.$field, book.ords.entry($kind).or_default(), row),)*
                _ => Ok(()),
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

        fn take_single(state: &mut State, book: &mut Book, row: &Row) -> Result<(), String> {
            match row.id.as_str() {
                $($id => {
                    state.$($field).+ = match &row.body {
                        Some(body) => serde_json::from_str(body).map_err(|e| e.to_string())?,
                        None => Default::default(),
                    };
                    match &row.body {
                        Some(body) => book.singles.insert($id.to_string(), body.clone()),
                        None => book.singles.remove($id),
                    };
                })*
                // of a build to come
                _ => {}
            }
            Ok(())
        }

        fn clear_singles(state: &mut State) {
            $(state.$($field).+ = Default::default();)*
        }
    };
}

singles!(
    "removals": removals,
    "purges": purges,
    "flume": flume,
    "wicket_ids": wicket_ids,
    "homes": homes,
    "wicket_stacks": wickets.runtimes,
);

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
                    body: Some(serde_json::to_string(item).expect("a state's entity is JSON")),
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

    /// What is kept whole is small: told from what was written by its body, so a change aside
    /// from a transaction (`Store::note`: the flume's cursor, what a wicket listed it runs) is
    /// written with the next.
    fn single<T: Serialize>(&mut self, id: &'static str, value: &T) {
        let body = serde_json::to_string(value).expect("a state's value is JSON");
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

/// Puts the row in `items`, at its `ord`; removes it when it is gone.
fn take<T: Entity>(
    items: &mut Vec<T>,
    ords: &mut HashMap<String, i64>,
    row: &Row,
) -> Result<(), String> {
    let at = items.iter().position(|item| item.id() == row.id);
    let Some(body) = &row.body else {
        if let Some(at) = at {
            items.remove(at);
        }
        ords.remove(&row.id);
        return Ok(());
    };
    let item: T = serde_json::from_str(body).map_err(|e| e.to_string())?;
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
    Ok(())
}

/// The most new rows one change gives an `ord`: the revisions' `ord`s are that far apart.
const ORDS: i64 = 1 << 24;

impl Book {
    /// The rows that make the state `after` of `before` (this book's), written as the revision
    /// `rev`: all of them with `before` None, a store's first.
    pub fn diff(&self, before: Option<&State>, after: &State, rev: i64) -> Vec<Row> {
        let mut diff = Diff {
            book: self,
            next: rev * ORDS,
            rows: Vec::new(),
        };
        let none = State::default();
        diff_lists(before.unwrap_or(&none), after, &mut diff);
        diff_singles(after, &mut diff);
        debug_assert!(
            diff.next < (rev + 1) * ORDS,
            "more rows than a change orders"
        );
        diff.rows
    }

    /// The rows are written: what this book knows of them.
    pub fn written(&mut self, rows: &[Row], rev: i64) {
        for row in rows {
            let Some(kind) = known(&row.kind) else {
                continue;
            };
            if kind == SINGLE {
                if let Some(body) = &row.body {
                    self.singles.insert(row.id.clone(), body.clone());
                }
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
            self.ords.clear();
            self.singles.clear();
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
                kind => take_listed(state, self, kind, row),
            };
            if let Err(e) = taken {
                eprintln!("STATE: cannot read the {} {}: {e}", row.kind, row.id);
            }
            if kind == "app" && row.body.is_none() {
                state.outcomes.remove(&row.id);
            }
        }
        self.rev = rev;
    }
}
