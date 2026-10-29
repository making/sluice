//! A build: the package's zip is read and the component picked -- the one the
//! web process command names, else the package's only `.wasm` -- and it must
//! be a component (`\0asm`, layer 1). Anything else fails the build, naming
//! what was found.

use std::io::{Cursor, Read as _};

use sha2::{Digest, Sha256};

/// The component a build picked.
#[derive(Debug)]
pub struct Staged {
    /// Its path in the package.
    pub entry: String,
    pub bytes: Vec<u8>,
    pub sha256: String,
}

/// Stages `package` (a zip) for the web process `command`.
pub fn stage(package: &[u8], command: Option<&str>) -> Result<Staged, String> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(package)).map_err(|e| format!("not a zip: {e}"))?;
    let names: Vec<String> = archive
        .file_names()
        .filter_map(|name| name.ok())
        .filter(|name| !name.ends_with('/'))
        .map(String::from)
        .collect();
    let components: Vec<&String> = names
        .iter()
        .filter(|name| name.ends_with(".wasm"))
        .collect();
    let entry = match command.map(str::trim).filter(|command| !command.is_empty()) {
        Some(command) => names
            .iter()
            .find(|name| *name == command || name.rsplit('/').next() == Some(command))
            .ok_or_else(|| {
                format!(
                    "the command names '{command}', which the package does not hold (found: {})",
                    listed(&names)
                )
            })?
            .clone(),
        None => match components.as_slice() {
            [only] => (*only).clone(),
            [] => {
                return Err(format!(
                    "the package holds no .wasm component (found: {})",
                    listed(&names)
                ));
            }
            several => {
                return Err(format!(
                    "the package holds {} components ({}): name one with -c (the web process command)",
                    several.len(),
                    listed(several)
                ));
            }
        },
    };
    let mut bytes = Vec::new();
    archive
        .by_name(&entry)
        .and_then(|mut file| file.read_to_end(&mut bytes).map_err(Into::into))
        .map_err(|e| format!("cannot read {entry}: {e}"))?;
    check_component(&entry, &bytes)?;
    let sha256 = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(Staged {
        entry,
        bytes,
        sha256,
    })
}

/// A wasm component: the magic, then the version and the layer (1; a core
/// module is layer 0).
fn check_component(entry: &str, bytes: &[u8]) -> Result<(), String> {
    match bytes {
        [0, b'a', b's', b'm', _, _, 1, 0, ..] => Ok(()),
        [0, b'a', b's', b'm', 1, 0, 0, 0, ..] => Err(format!(
            "{entry} is a core wasm module, not a component (a wasi:http 0.3 handler)"
        )),
        [0, b'a', b's', b'm', ..] => Err(format!("{entry} is wasm of an unknown layer")),
        _ => Err(format!("{entry} is not wasm")),
    }
}

fn listed<S: AsRef<str>>(names: &[S]) -> String {
    if names.is_empty() {
        return "nothing".into();
    }
    names
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    const COMPONENT: &[u8] = b"\0asm\x0d\x00\x01\x00rest";
    const MODULE: &[u8] = b"\0asm\x01\x00\x00\x00rest";

    fn zipped(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, bytes) in entries {
            zip.start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            zip.write_all(bytes).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn picks_the_only_component_or_the_one_the_command_names() {
        let staged = stage(
            &zipped(&[("hello.wasm", COMPONENT), ("README", b"x")]),
            None,
        )
        .unwrap();
        assert_eq!(staged.entry, "hello.wasm");
        assert_eq!(staged.bytes, COMPONENT);
        assert_eq!(staged.sha256.len(), 64);
        let two = zipped(&[("a.wasm", COMPONENT), ("dist/b.wasm", COMPONENT)]);
        assert!(
            stage(&two, None)
                .unwrap_err()
                .contains("2 components (a.wasm, dist/b.wasm)")
        );
        assert_eq!(stage(&two, Some("b.wasm")).unwrap().entry, "dist/b.wasm");
        assert_eq!(
            stage(&two, Some("dist/b.wasm")).unwrap().entry,
            "dist/b.wasm"
        );
        assert!(
            stage(&two, Some("c.wasm"))
                .unwrap_err()
                .contains("names 'c.wasm'")
        );
        assert!(
            stage(&zipped(&[("index.html", b"<p>")]), None)
                .unwrap_err()
                .contains("no .wasm component (found: index.html)")
        );
        assert!(
            stage(&zipped(&[("m.wasm", MODULE)]), None)
                .unwrap_err()
                .contains("core wasm module")
        );
        assert!(
            stage(&zipped(&[("t.wasm", b"text")]), None)
                .unwrap_err()
                .contains("not wasm")
        );
        assert!(stage(b"not a zip", None).unwrap_err().contains("not a zip"));
    }
}
