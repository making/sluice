//! The `multipart/form-data` body of a package upload, read as it comes, a package being
//! larger than what a process's memory is sure to hold: the `bits` part is the zip,
//! `resources` the matched resources (none).

use std::io::{Read, Write};

/// What is read of the body at a time.
const CHUNK: usize = 64 * 1024;

/// The longest headers of a part.
const MAX_HEAD: usize = 8 * 1024;

/// Copies the part `wanted` of `body`, whose boundary `content_type` names, to `out`: its
/// bytes, None when there is no such part.
pub fn copy_part(
    content_type: &str,
    body: impl Read,
    wanted: &str,
    out: &mut impl Write,
) -> Result<Option<u64>, String> {
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|parameter| parameter.strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"'))
        .ok_or("multipart/form-data without a boundary")?;
    let delimiter = format!("--{boundary}");
    // the CRLF before a delimiter is the delimiter's
    let next_part = format!("\r\n{delimiter}");
    let mut scan = Scan::new(body);
    let mut found = None;
    if !scan.until(delimiter.as_bytes(), &mut |_| Ok(()))? {
        return Err("no part".into());
    }
    loop {
        if scan.at(b"--")? {
            return Ok(found);
        }
        scan.at(b"\r\n")?;
        let mut head = Vec::new();
        let closed = scan.until(b"\r\n\r\n", &mut |bytes| {
            head.extend_from_slice(bytes);
            match head.len() <= MAX_HEAD {
                true => Ok(()),
                false => Err("a part's headers run too long".to_string()),
            }
        })?;
        if !closed {
            return Err("a part without headers".into());
        }
        let head = std::str::from_utf8(&head).map_err(|_| "headers not UTF-8")?;
        let name = head
            .lines()
            .find_map(|line| {
                let (field, value) = line.split_once(':')?;
                field
                    .eq_ignore_ascii_case("content-disposition")
                    .then_some(value)
            })
            .and_then(|value| {
                value
                    .split(';')
                    .map(str::trim)
                    .find_map(|parameter| parameter.strip_prefix("name="))
            })
            .map(|name| name.trim_matches('"'))
            .ok_or("a part without a name")?;
        let mut bytes = 0;
        let closed = match name == wanted {
            true => scan.until(next_part.as_bytes(), &mut |part| {
                bytes += part.len() as u64;
                out.write_all(part)
                    .map_err(|e| format!("writing the part: {e}"))
            })?,
            false => scan.until(next_part.as_bytes(), &mut |_| Ok(()))?,
        };
        if !closed {
            return Err("an unterminated part".into());
        }
        if name == wanted {
            found = Some(bytes);
        }
    }
}

/// A body, read forward in chunks.
struct Scan<R> {
    body: R,
    buffer: Vec<u8>,
    /// How much of the buffer was taken.
    at: usize,
}

impl<R: Read> Scan<R> {
    fn new(body: R) -> Scan<R> {
        Scan {
            body,
            buffer: Vec::with_capacity(CHUNK),
            at: 0,
        }
    }

    /// One more chunk behind what is left of the buffer; false at the body's end.
    fn fill(&mut self) -> Result<bool, String> {
        self.buffer.drain(..self.at);
        self.at = 0;
        let held = self.buffer.len();
        self.buffer.resize(held + CHUNK, 0);
        let read = self
            .body
            .read(&mut self.buffer[held..])
            .map_err(|e| format!("reading the body: {e}"))?;
        self.buffer.truncate(held + read);
        Ok(read > 0)
    }

    /// Takes `bytes` if the body goes on with them: whether it does.
    fn at(&mut self, bytes: &[u8]) -> Result<bool, String> {
        while self.buffer.len() - self.at < bytes.len() {
            if !self.fill()? {
                return Ok(false);
            }
        }
        let goes_on = self.buffer[self.at..].starts_with(bytes);
        if goes_on {
            self.at += bytes.len();
        }
        Ok(goes_on)
    }

    /// Hands what comes before `needle` to `sink`, and takes `needle`: false when the body
    /// ends first.
    fn until(
        &mut self,
        needle: &[u8],
        sink: &mut impl FnMut(&[u8]) -> Result<(), String>,
    ) -> Result<bool, String> {
        loop {
            let held = &self.buffer[self.at..];
            if let Some(found) = held
                .windows(needle.len())
                .position(|window| window == needle)
            {
                sink(&held[..found])?;
                self.at += found + needle.len();
                return Ok(true);
            }
            // all but what may be the start of the needle
            let safe = held.len().saturating_sub(needle.len() - 1);
            sink(&held[..safe])?;
            self.at += safe;
            if !self.fill()? {
                return Ok(false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A body handing out one byte at a time: a delimiter comes across reads.
    struct Slowly<'a>(&'a [u8]);

    impl Read for Slowly<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let Some((first, rest)) = self.0.split_first() else {
                return Ok(0);
            };
            buffer[0] = *first;
            self.0 = rest;
            Ok(1)
        }
    }

    #[test]
    fn copies_the_part_up_to_the_boundary() {
        let body = b"--xyz\r\nContent-Disposition: form-data; name=\"resources\"\r\n\r\n[]\r\n--xyz\r\nContent-Disposition: form-data; name=\"bits\"; filename=\"application.zip\"\r\nContent-Type: application/zip\r\n\r\nPK\r\n\x00\x01\r\n--xyz--\r\n";
        let kind = "multipart/form-data; boundary=xyz";
        let part = |wanted: &str, body: &[u8]| {
            let mut out = Vec::new();
            copy_part(kind, body, wanted, &mut out).map(|copied| (copied, out))
        };
        assert_eq!(part("resources", body).unwrap(), (Some(2), b"[]".to_vec()));
        let bits = (Some(6), b"PK\r\n\x00\x01".to_vec());
        assert_eq!(part("bits", body).unwrap(), bits);
        assert_eq!(part("other", body).unwrap(), (None, Vec::new()));
        let mut out = Vec::new();
        let copied = copy_part(kind, Slowly(body), "bits", &mut out).unwrap();
        assert_eq!((copied, out), bits);
        // larger than a read, its delimiter across two
        let large = [
            b"--xyz\r\nContent-Disposition: form-data; name=\"bits\"\r\n\r\n".as_slice(),
            &vec![7; 2 * CHUNK - 60],
            b"\r\n--xyz--\r\n",
        ]
        .concat();
        let (copied, out) = part("bits", &large).unwrap();
        assert_eq!(copied, Some(2 * CHUNK as u64 - 60));
        assert!(out.iter().all(|byte| *byte == 7));

        assert!(copy_part("text/plain", &body[..], "bits", &mut Vec::new()).is_err());
        assert!(part("bits", b"nothing").is_err());
        assert!(part("bits", b"--xyz\r\nno headers").is_err());
        assert!(
            part(
                "bits",
                b"--xyz\r\nContent-Disposition: form-data; name=\"bits\"\r\n\r\nPK"
            )
            .unwrap_err()
            .contains("unterminated")
        );
    }
}
