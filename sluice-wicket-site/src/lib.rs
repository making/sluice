// Sluice guest handler serving a static site: the files of the directory its
// route preopens read-only at `/` (wasi:filesystem 0.3), nothing else. GET and
// HEAD only; a directory serves its index.html, `404.html` is the not found
// page; dot files are never served but `/.well-known/`; no listing of a
// directory, no symbolic link followed.
//
// Build: cargo build --release (the component: target/wasm32-wasip3/release/sluice_wicket_site.wasm)
wit_bindgen::generate!({
    path: "../sluice-wicket-runtime-wasi/wit",
    inline: "
        package sluice:site;

        world site {
            import wasi:filesystem/types@0.3.0;
            import wasi:filesystem/preopens@0.3.0;
            import wasi:http/types@0.3.0;
            export wasi:http/handler@0.3.0;
        }
    ",
    features: ["clocks-timezone"],
    generate_all,
});

use exports::wasi::http::handler::Guest;
use wasi::filesystem::types::{Descriptor, DescriptorFlags, DescriptorType, OpenFlags, PathFlags};
use wasi::http::types::{ErrorCode, Headers, Method, Request, Response};

/// The not found page, at the root of the site.
const NOT_FOUND_PAGE: &str = "404.html";

/// A directory's page.
const INDEX: &str = "index.html";

struct Component;

impl Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let head = match request.get_method() {
            Method::Get => false,
            Method::Head => true,
            _ => {
                let allow = [("allow", &b"GET, HEAD"[..])];
                return Ok(text(405, "Method Not Allowed\n", &allow));
            }
        };
        let target = request.get_path_with_query().unwrap_or_default();
        let (path, query) = match target.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (target.as_str(), None),
        };
        let if_none_match = request.get_headers().get("if-none-match");
        drop(request);
        let Some(root) = root() else {
            return Ok(text(
                500,
                "No files: the route preopens no directory\n",
                &[],
            ));
        };
        let file = match resolve(path) {
            Resolved::File(file) => file,
            Resolved::Bad => return Ok(text(400, "Bad Request\n", &[])),
            Resolved::Hidden => return Ok(not_found(&root, head).await),
        };
        let index = file.is_empty() || path.ends_with('/');
        let opened = match open(&root, &file).await {
            Some((descriptor, DescriptorType::Directory, _)) if !index => {
                // its relative links name its files
                drop(descriptor);
                let location = match query {
                    Some(query) => format!("{path}/?{query}"),
                    None => format!("{path}/"),
                };
                let location = [("location", location.as_bytes())];
                return Ok(text(301, "Moved Permanently\n", &location));
            }
            Some((descriptor, DescriptorType::Directory, _)) => {
                drop(descriptor);
                let page = match file.is_empty() {
                    true => INDEX.to_string(),
                    false => format!("{file}/{INDEX}"),
                };
                open(&root, &page)
                    .await
                    .filter(|(_, kind, _)| matches!(kind, DescriptorType::RegularFile))
                    .map(|(descriptor, _, stat)| (descriptor, page, stat))
            }
            Some((descriptor, DescriptorType::RegularFile, stat)) if !index => {
                Some((descriptor, file, stat))
            }
            _ => None,
        };
        match opened {
            Some((descriptor, name, stat)) => Ok(serve(
                descriptor,
                &name,
                stat,
                200,
                head,
                if_none_match.as_slice(),
            )),
            None => Ok(not_found(&root, head).await),
        }
    }
}

export!(Component);

/// What a request's path names.
enum Resolved {
    /// The path relative to the root, its segments percent-decoded, empty for
    /// the root.
    File(String),
    /// A dot file, served as one that is not there.
    Hidden,
    /// No path of a file: not absolute, a bad escape, a NUL, a backslash, a
    /// `.` or `..` segment.
    Bad,
}

/// What the request path `path` names below the root.
fn resolve(path: &str) -> Resolved {
    let Some(path) = path.strip_prefix('/') else {
        return Resolved::Bad;
    };
    let Some(decoded) = percent_decode(path) else {
        return Resolved::Bad;
    };
    if decoded.contains(['\0', '\\']) {
        return Resolved::Bad;
    }
    let segments: Vec<&str> = decoded
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments
        .iter()
        .any(|segment| *segment == "." || *segment == "..")
    {
        return Resolved::Bad;
    }
    let hidden = segments
        .iter()
        .enumerate()
        .any(|(at, segment)| segment.starts_with('.') && !(at == 0 && *segment == ".well-known"));
    if hidden {
        return Resolved::Hidden;
    }
    Resolved::File(segments.join("/"))
}

/// `text` with its `%XX` escapes decoded; None for a bad escape or bytes
/// that are no UTF-8.
fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            let hex = std::str::from_utf8(bytes.get(at + 1..at + 3)?).ok()?;
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            at += 3;
        } else {
            decoded.push(bytes[at]);
            at += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

/// The directory preopened at `/`.
fn root() -> Option<Descriptor> {
    wasi::filesystem::preopens::get_directories()
        .into_iter()
        .find_map(|(descriptor, path)| (path == "/").then_some(descriptor))
}

/// What a file's headers are made of.
struct Stat {
    size: u64,
    /// The modification time, seconds and nanoseconds since the epoch.
    modified: Option<(i64, u32)>,
}

/// `file` (relative to `root`, the root itself when empty) opened to read, its
/// type and its stat; None when it is not there, or not to be opened (a
/// symbolic link: none is followed).
async fn open(root: &Descriptor, file: &str) -> Option<(Descriptor, DescriptorType, Stat)> {
    let path = match file.is_empty() {
        true => ".",
        false => file,
    };
    let descriptor = root
        .open_at(
            PathFlags::empty(),
            path.to_string(),
            OpenFlags::empty(),
            DescriptorFlags::READ,
        )
        .await
        .ok()?;
    let stat = descriptor.stat().await.ok()?;
    let modified = stat
        .data_modification_timestamp
        .map(|instant| (instant.seconds, instant.nanoseconds));
    Some((
        descriptor,
        stat.type_,
        Stat {
            size: stat.size,
            modified,
        },
    ))
}

/// The not found page, `404.html` if the site has one.
async fn not_found(root: &Descriptor, head: bool) -> Response {
    match open(root, NOT_FOUND_PAGE).await {
        Some((descriptor, DescriptorType::RegularFile, stat)) => {
            serve(descriptor, NOT_FOUND_PAGE, stat, 404, head, &[])
        }
        _ => text(404, "Not Found\n", &[]),
    }
}

/// The response of the file `descriptor` (named `name`, for its type) with
/// `status`: its body streamed from the file but with `head`, or 304 when
/// `if_none_match` names its tag.
fn serve(
    descriptor: Descriptor,
    name: &str,
    stat: Stat,
    status: u16,
    head: bool,
    if_none_match: &[Vec<u8>],
) -> Response {
    let headers = Headers::new();
    let _ = headers.append("x-content-type-options", b"nosniff");
    // every use asks whether it changed: a deploy shows at once
    let _ = headers.append("cache-control", b"no-cache");
    let tag = stat
        .modified
        .map(|(seconds, nanoseconds)| format!("W/\"{:x}-{seconds:x}-{nanoseconds:x}\"", stat.size));
    if let Some(tag) = &tag {
        let _ = headers.append("etag", tag.as_bytes());
        if status == 200 && matches(if_none_match, tag) {
            return respond(headers, 304, None);
        }
    }
    if let Some((seconds, _)) = stat.modified {
        let _ = headers.append("last-modified", http_date(seconds).as_bytes());
    }
    let _ = headers.append("content-type", content_type(name).as_bytes());
    let _ = headers.append("content-length", stat.size.to_string().as_bytes());
    if head {
        return respond(headers, status, None);
    }
    let (body, read) = descriptor.read_via_stream(0);
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));
    let (response, _transmit) = Response::new(headers, Some(body), trailers_rx);
    let _ = response.set_status_code(status);
    wit_bindgen::spawn_local(async move {
        let outcome = read.await;
        // the file is read to its end: let go of it
        drop(descriptor);
        let trailers = outcome
            .map(|()| None)
            .map_err(|e| ErrorCode::InternalError(Some(format!("reading the file: {e:?}"))));
        let _ = trailers_tx.write(trailers).await;
    });
    response
}

/// Whether the `If-None-Match` values name `tag`, weakly compared.
fn matches(values: &[Vec<u8>], tag: &str) -> bool {
    let bare = |tag: &str| tag.strip_prefix("W/").unwrap_or(tag).to_string();
    values
        .iter()
        .filter_map(|value| std::str::from_utf8(value).ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|given| given == "*" || bare(given) == bare(tag))
}

/// A response of `headers` with `status` and no body.
fn respond(headers: Headers, status: u16, body: Option<Vec<u8>>) -> Response {
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));
    let response = match body {
        Some(payload) => {
            let (mut body_tx, body_rx) = wit_stream::new();
            let (response, _transmit) = Response::new(headers, Some(body_rx), trailers_rx);
            wit_bindgen::spawn_local(async move {
                let _ = body_tx.write_all(payload).await;
                drop(body_tx);
                let _ = trailers_tx.write(Ok(None)).await;
            });
            response
        }
        None => {
            let (response, _transmit) = Response::new(headers, None, trailers_rx);
            drop(trailers_tx);
            response
        }
    };
    let _ = response.set_status_code(status);
    response
}

/// A `text/plain` response of `payload` with `status` and the headers
/// `extra` (a response's are immutable once made).
fn text(status: u16, payload: &str, extra: &[(&str, &[u8])]) -> Response {
    let headers = Headers::new();
    for (name, value) in extra {
        let _ = headers.append(name, value);
    }
    let _ = headers.append("x-content-type-options", b"nosniff");
    let _ = headers.append("content-type", b"text/plain; charset=utf-8");
    respond(headers, status, Some(payload.as_bytes().to_vec()))
}

/// The media type of the file `name`, by its extension.
fn content_type(name: &str) -> &'static str {
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" | "map" => "application/json",
        "webmanifest" => "application/manifest+json",
        "txt" => "text/plain; charset=utf-8",
        "md" => "text/markdown; charset=utf-8",
        "csv" => "text/csv; charset=utf-8",
        "xml" => "application/xml",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "wasm" => "application/wasm",
        "pdf" => "application/pdf",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",
        _ => "application/octet-stream",
    }
}

/// `seconds` since the epoch as an HTTP date (`Sun, 06 Nov 1994 08:49:37 GMT`).
fn http_date(seconds: i64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = seconds.div_euclid(86_400);
    let time = seconds.rem_euclid(86_400);
    // the civil date of a day count (Howard Hinnant's days_from_civil, inverted)
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let of_era = shifted.rem_euclid(146_097);
    let year_of_era = (of_era - of_era / 1460 + of_era / 36_524 - of_era / 146_096) / 365;
    let of_year = of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * of_year + 2) / 153;
    let day = of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!(
        "{}, {day:02} {} {year} {:02}:{:02}:{:02} GMT",
        DAYS[days.rem_euclid(7) as usize],
        MONTHS[(month - 1) as usize],
        time / 3600,
        time % 3600 / 60,
        time % 60
    )
}
