//! The apps' environment variables: the user's (`cf set-env`, a manifest's
//! `env:`, a new app's `environment_variables`), checked as CC checks them
//! and as the wicket takes them, and the guest's, taken at a start as on CF:
//! the user's, `VCAP_APPLICATION` and `VCAP_SERVICES` (JSON). A value may be
//! a secret: no log of the cf-api or of the app holds one.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::store::{App, State};

/// The most the user's variables hold, names and values together: half of
/// what the wicket takes (128 KiB), the rest left to `VCAP_*`.
pub const MAX_BYTES: usize = 64 << 10;

/// Whether the user may set `name`: as on CF, not empty, no `VCAP_` or
/// `VMC` prefix and not `PORT` (in any case); as the wicket takes it, no
/// `=` and no control character.
pub fn check_name(name: &str) -> Result<(), String> {
    let upper = name.to_ascii_uppercase();
    let refused = if name.is_empty() {
        "a variable needs a name"
    } else if upper.starts_with("VCAP_") {
        "cannot start with VCAP_"
    } else if upper.starts_with("VMC") {
        "cannot start with VMC"
    } else if upper == "PORT" {
        "cannot set PORT"
    } else if name.contains(|c: char| c == '=' || c.is_control()) {
        "holds '=' or a control character"
    } else {
        return Ok(());
    };
    Err(format!("environment variable {name:?}: {refused}"))
}

/// Checks the user's variables: the names, the values (no NUL), and
/// [`MAX_BYTES`] at most. An error names a variable, never a value.
pub fn check(env: &BTreeMap<String, String>) -> Result<(), String> {
    let mut bytes = 0;
    for (name, value) in env {
        check_name(name)?;
        if value.contains('\0') {
            return Err(format!(
                "environment variable {name:?}: its value holds a NUL"
            ));
        }
        bytes += name.len() + value.len();
    }
    if bytes > MAX_BYTES {
        return Err(format!(
            "the environment variables hold {bytes} bytes, more than {} KiB",
            MAX_BYTES >> 10
        ));
    }
    Ok(())
}

/// `env` with `var` merged in, as CC's PATCH merges it: a null removes the
/// variable; a string is the value, a number or a boolean its JSON text.
/// Checked ([`check`]).
pub fn merged(
    env: &BTreeMap<String, String>,
    var: &Map<String, Value>,
) -> Result<BTreeMap<String, String>, String> {
    let mut env = env.clone();
    for (name, value) in var {
        match value {
            Value::Null => {
                env.remove(name);
            }
            Value::String(text) => {
                env.insert(name.clone(), text.clone());
            }
            Value::Number(_) | Value::Bool(_) => {
                env.insert(name.clone(), value.to_string());
            }
            Value::Array(_) | Value::Object(_) => {
                return Err(format!(
                    "environment variable {name:?}: a value is a string, a number or a boolean"
                ));
            }
        }
    }
    check(&env)?;
    Ok(env)
}

/// `VCAP_APPLICATION` of the app at `base` (the API's URL), as `cf env`
/// shows it and its guest gets it at a start.
pub fn vcap_application(base: &str, state: &State, app: &App) -> Value {
    let uris: Vec<String> = state
        .routes_of(&app.guid)
        .map(|route| state.url(route))
        .collect();
    json!({
        "application_id": app.guid,
        "application_name": app.name,
        "application_uris": uris,
        "application_version": app.process.guid,
        "cf_api": base,
        "limits": {"fds": 16384, "mem": app.process.memory_mb, "disk": app.process.disk_mb},
        "name": app.name,
        "organization_id": state.org.guid,
        "organization_name": state.org.name,
        "space_id": state.space.guid,
        "space_name": state.space.name,
        "uris": uris,
        "users": null,
        "version": app.process.guid,
    })
}

/// The guest's environment at a start: the user's variables,
/// `VCAP_APPLICATION` and `VCAP_SERVICES`.
pub fn guest(base: &str, state: &State, app: &App) -> BTreeMap<String, String> {
    let mut env = app.env.clone();
    env.insert(
        "VCAP_APPLICATION".into(),
        vcap_application(base, state, app).to_string(),
    );
    env.insert(
        "VCAP_SERVICES".into(),
        crate::services::vcap_services(state, &app.guid).to_string(),
    );
    env
}

/// A tag of a snapshot for the wicket, drawn at random: the wicket lists an
/// env by its names only, so the tag tells the app it serves from another
/// snapshot, and holds nothing of the values.
pub fn tag() -> String {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).expect("the OS random source");
    random.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PATCH merges and removes, takes a number or a boolean as text,
    /// and is refused -- naming the variable, never its value -- for a name
    /// CF or the wicket takes not, a structured value, a NUL or a size past
    /// the limit; the guest's environment adds VCAP_* to the user's.
    #[test]
    fn the_users_variables_are_merged_checked_and_given_with_vcap() {
        let var = |value: Value| value.as_object().unwrap().clone();
        let env = merged(
            &BTreeMap::from([("OLD".into(), "x".into())]),
            &var(json!({"GREETING": "hello", "N": 8080, "ON": true, "OLD": null})),
        )
        .unwrap();
        assert_eq!(
            env,
            BTreeMap::from([
                ("GREETING".into(), "hello".into()),
                ("N".into(), "8080".into()),
                ("ON".into(), "true".into()),
            ])
        );
        for (refused, error) in [
            (json!({"": "s3cr3t"}), "\"\": a variable needs a name"),
            (json!({"vcap_x": "s3cr3t"}), "cannot start with VCAP_"),
            (json!({"VMC_X": "s3cr3t"}), "cannot start with VMC"),
            (json!({"Port": "s3cr3t"}), "cannot set PORT"),
            (json!({"A=B": "s3cr3t"}), "holds '=' or a control character"),
            (
                json!({"A\nB": "s3cr3t"}),
                "holds '=' or a control character",
            ),
            (
                json!({"K": ["s3cr3t"]}),
                "a value is a string, a number or a boolean",
            ),
            (json!({"K": "s3cr3t\0"}), "\"K\": its value holds a NUL"),
            (
                json!({"K": "x".repeat(MAX_BYTES)}),
                "bytes, more than 64 KiB",
            ),
        ] {
            let e = merged(&env, &var(refused)).unwrap_err();
            assert!(e.contains(error) && !e.contains("s3cr3t"), "{e}");
        }

        let mut state = State::new("o", "s", &["localhost".into()]);
        let guid = state.create_app("hello").guid.clone();
        let domain = state.domains[0].guid.clone();
        let route = state.create_route("hello", &domain).guid.clone();
        state.map(&route, &guid);
        state.app_mut(&guid).unwrap().env = env;
        let app = state.app(&guid).unwrap().clone();
        let guest = guest("http://api", &state, &app);
        assert_eq!(guest["GREETING"], "hello");
        let vcap: Value = serde_json::from_str(&guest["VCAP_APPLICATION"]).unwrap();
        assert_eq!(
            (&vcap["application_name"], &vcap["application_uris"]),
            (&json!("hello"), &json!(["hello.localhost"]))
        );
        assert_eq!(vcap["cf_api"], "http://api");
        assert_eq!(guest["VCAP_SERVICES"], "{}");
        assert_ne!(tag(), tag());
    }
}
