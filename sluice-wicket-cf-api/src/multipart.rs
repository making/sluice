//! The `multipart/form-data` body of a package upload, read whole: the
//! `bits` part is the zip, `resources` the matched resources (none).

pub struct Part<'a> {
    pub name: String,
    pub body: &'a [u8],
}

/// The parts of `body`, whose boundary `content_type` names.
pub fn parts<'a>(content_type: &str, body: &'a [u8]) -> Result<Vec<Part<'a>>, String> {
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|parameter| parameter.strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"'))
        .ok_or("multipart/form-data without a boundary")?;
    let delimiter = format!("--{boundary}");
    let mut parts = Vec::new();
    let mut rest = body;
    // the preamble up to the first delimiter
    let Some(start) = find(rest, delimiter.as_bytes()) else {
        return Err("no part".into());
    };
    rest = &rest[start + delimiter.len()..];
    loop {
        if rest.starts_with(b"--") {
            return Ok(parts);
        }
        rest = rest.strip_prefix(b"\r\n").unwrap_or(rest);
        let head_end = find(rest, b"\r\n\r\n").ok_or("a part without headers")?;
        let head = std::str::from_utf8(&rest[..head_end]).map_err(|_| "headers not UTF-8")?;
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
            .map(|name| name.trim_matches('"').to_string())
            .ok_or("a part without a name")?;
        rest = &rest[head_end + 4..];
        let next = find(rest, delimiter.as_bytes()).ok_or("an unterminated part")?;
        // the CRLF before the delimiter is the delimiter's
        let body_end = next.saturating_sub(2);
        parts.push(Part {
            name,
            body: &rest[..body_end],
        });
        rest = &rest[next + delimiter.len()..];
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_the_parts_at_the_boundary() {
        let body = b"--xyz\r\nContent-Disposition: form-data; name=\"resources\"\r\n\r\n[]\r\n--xyz\r\nContent-Disposition: form-data; name=\"bits\"; filename=\"application.zip\"\r\nContent-Type: application/zip\r\n\r\nPK\r\n\x00\x01\r\n--xyz--\r\n";
        let parts = parts("multipart/form-data; boundary=xyz", body).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].name, "resources");
        assert_eq!(parts[0].body, b"[]");
        assert_eq!(parts[1].name, "bits");
        assert_eq!(parts[1].body, b"PK\r\n\x00\x01");
        assert!(super::parts("text/plain", body).is_err());
        assert!(super::parts("multipart/form-data; boundary=xyz", b"--xyz\r\nno headers").is_err());
    }
}
