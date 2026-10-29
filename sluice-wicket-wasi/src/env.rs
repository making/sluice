//! A route's environment variables (`sandbox.env`): what the guest's
//! `wasi:cli/environment` answers. They may hold secrets, so their values
//! are never shown: the advertised target carries a digest of them, keyed
//! by a random key of the process (it tells nothing of the values, and
//! another process has another), the listings their names only, and so does
//! `Debug`.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use sha2::{Digest as _, Sha256};

/// The most an env holds: its names and values together, in bytes.
pub const MAX_ENV_BYTES: usize = 128 << 10;

/// The variables, in name order; their values never shown.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Env(BTreeMap<String, String>);

impl Env {
    /// The variables of `map`: a name is not empty and holds no `=` and no
    /// control character, a value no NUL; [`MAX_ENV_BYTES`] at most in all.
    /// Errors name the variable, never its value.
    pub fn new(map: BTreeMap<String, String>) -> Result<Env, String> {
        let mut bytes = 0;
        for (name, value) in &map {
            if name.is_empty() || name.contains(|c: char| c == '=' || c.is_control()) {
                return Err(format!(
                    "{name:?} is no variable name (not empty, no '=' and no control character)"
                ));
            }
            if value.contains('\0') {
                return Err(format!("the value of {name} holds a NUL"));
            }
            bytes += name.len() + value.len();
        }
        if bytes > MAX_ENV_BYTES {
            return Err(format!(
                "{bytes} bytes of names and values, more than {} KiB",
                MAX_ENV_BYTES >> 10
            ));
        }
        Ok(Env(map))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The names, in order.
    pub fn names(&self) -> Vec<String> {
        self.0.keys().cloned().collect()
    }

    /// The variables, in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// The variables as a map: what a file keeping the env writes.
    pub fn map(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    /// HMAC-SHA256 under the process's key of `<name>=<value>\0` of each
    /// variable in name order (unambiguous: a name holds no `=`, a value no
    /// NUL), its first 16 bytes in hex: the same variables, the same digest
    /// in this process; other variables, another.
    pub fn digest(&self) -> String {
        let mut text = Vec::new();
        for (name, value) in self.iter() {
            text.extend_from_slice(name.as_bytes());
            text.push(b'=');
            text.extend_from_slice(value.as_bytes());
            text.push(0);
        }
        hmac_sha256(key(), &text)[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

/// The names only.
impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

/// The process's random key of [`Env::digest`].
fn key() -> &'static [u8; 32] {
    static KEY: OnceLock<[u8; 32]> = OnceLock::new();
    KEY.get_or_init(|| {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).expect("the OS random source");
        key
    })
}

/// RFC 2104 with SHA-256; the key is 32 bytes, below the block size.
fn hmac_sha256(key: &[u8; 32], data: &[u8]) -> [u8; 32] {
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for (i, byte) in key.iter().enumerate() {
        ipad[i] ^= byte;
        opad[i] ^= byte;
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(data)
        .finalize();
    Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Result<Env, String> {
        Env::new(
            pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        )
    }

    /// A variable is checked, its value never named; the digest follows the
    /// variables and holds none of them, and `Debug` names them only.
    #[test]
    fn the_values_are_checked_digested_and_never_shown() {
        let secret = env(&[("GREETING", "hello"), ("TOKEN", "s3cr3t-value")]).unwrap();
        assert_eq!(secret.names(), ["GREETING", "TOKEN"]);
        let digest = secret.digest();
        assert_eq!(digest.len(), 32);
        assert_eq!(digest, secret.clone().digest(), "the same in this process");
        for other in [
            env(&[("GREETING", "hello"), ("TOKEN", "s3cr3t-valuf")]),
            env(&[("GREETING", "hello")]),
            // the encoding is unambiguous: no shift between name and value
            env(&[("GREETING", "hello"), ("TOKEN", "s3cr3t-value\n")]),
            env(&[("GREETINGTOKEN", "hellos3cr3t-value")]),
        ] {
            assert_ne!(other.unwrap().digest(), digest);
        }
        assert_eq!(format!("{secret:?}"), r#"{"GREETING", "TOKEN"}"#);
        assert_eq!(
            env(&[("PEM", "-----BEGIN-----\nAA==\n-----END-----")])
                .unwrap()
                .names(),
            ["PEM"],
            "a value may span lines"
        );

        for (pairs, error) in [
            (vec![("", "x")], "\"\" is no variable name"),
            (vec![("A=B", "x")], "\"A=B\" is no variable name"),
            (vec![("A\nB", "x")], "\"A\\nB\" is no variable name"),
            (vec![("K", "s3cr3t\0")], "the value of K holds a NUL"),
        ] {
            let refused = env(&pairs).unwrap_err();
            assert!(refused.starts_with(error), "{refused}");
            assert!(!refused.contains("s3cr3t"), "{refused}");
        }
        let large = "x".repeat(MAX_ENV_BYTES);
        assert!(env(&[("K", &large[1..])]).is_ok());
        assert_eq!(
            env(&[("K", &large)]).unwrap_err(),
            format!(
                "{} bytes of names and values, more than 128 KiB",
                MAX_ENV_BYTES + 1
            )
        );
    }
}
