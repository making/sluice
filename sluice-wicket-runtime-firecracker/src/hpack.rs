//! The HPACK decoding (RFC 7541) local mode needs: the host of an h2
//! connection's first header block. The decoder's state is the connection's
//! (its dynamic table), so the block is decoded for its host alone and relayed
//! as read, for the guest to decode again.

use std::collections::VecDeque;
use std::sync::OnceLock;

/// The static table's names (Appendix A); every value that matters here (of
/// `:authority` and `host`) is empty.
const STATIC_NAMES: [&str; 61] = [
    ":authority",
    ":method",
    ":method",
    ":path",
    ":path",
    ":scheme",
    ":scheme",
    ":status",
    ":status",
    ":status",
    ":status",
    ":status",
    ":status",
    ":status",
    "accept-charset",
    "accept-encoding",
    "accept-language",
    "accept-ranges",
    "accept",
    "access-control-allow-origin",
    "age",
    "allow",
    "authorization",
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-length",
    "content-location",
    "content-range",
    "content-type",
    "cookie",
    "date",
    "etag",
    "expect",
    "expires",
    "from",
    "host",
    "if-match",
    "if-modified-since",
    "if-none-match",
    "if-range",
    "if-unmodified-since",
    "last-modified",
    "link",
    "location",
    "max-forwards",
    "proxy-authenticate",
    "proxy-authorization",
    "range",
    "referer",
    "refresh",
    "retry-after",
    "server",
    "set-cookie",
    "strict-transport-security",
    "transfer-encoding",
    "user-agent",
    "vary",
    "via",
    "www-authenticate",
];

/// The dynamic table's size before the peer's SETTINGS (and its most the
/// decoder follows).
const TABLE_SIZE: usize = 4096;

/// The host a header block names: its `:authority`, else its `host`; None
/// when it names neither.
pub fn host(block: &[u8]) -> Result<Option<String>, String> {
    let mut decoder = Decoder {
        block,
        at: 0,
        table: VecDeque::new(),
        size: 0,
        max: TABLE_SIZE,
    };
    let mut host = None;
    while let Some((name, value)) = decoder.field()? {
        let text = || String::from_utf8(value.clone()).map_err(|_| "a host not UTF-8".to_string());
        match &name[..] {
            b":authority" => return text().map(Some),
            b"host" if host.is_none() => host = Some(text()?),
            _ => {}
        }
    }
    Ok(host)
}

/// A header field: its name and value.
type Field = (Vec<u8>, Vec<u8>);

struct Decoder<'a> {
    block: &'a [u8],
    at: usize,
    /// The dynamic table, newest first.
    table: VecDeque<Field>,
    size: usize,
    max: usize,
}

impl Decoder<'_> {
    /// The next field of the block; None at its end.
    fn field(&mut self) -> Result<Option<Field>, String> {
        loop {
            let Some(&first) = self.block.get(self.at) else {
                return Ok(None);
            };
            if first & 0x80 != 0 {
                // indexed
                let index = self.integer(7)?;
                return self.entry(index).map(Some);
            }
            if first & 0xe0 == 0x20 {
                // a dynamic table size update
                let max = self.integer(5)?;
                if max > TABLE_SIZE {
                    return Err(format!("a dynamic table of {max} bytes"));
                }
                self.max = max;
                self.evict(0);
                continue;
            }
            // a literal: with incremental indexing (01), else without (0000 / 0001)
            let indexed = first & 0x40 != 0;
            let index = self.integer(if indexed { 6 } else { 4 })?;
            let name = match index {
                0 => self.string()?,
                index => self.entry(index)?.0,
            };
            let value = self.string()?;
            if indexed {
                self.insert(name.clone(), value.clone());
            }
            return Ok(Some((name, value)));
        }
    }

    /// The entry at `index` of the static table and the dynamic one after it.
    fn entry(&self, index: usize) -> Result<Field, String> {
        match index {
            0 => Err("index 0".into()),
            1..=61 => Ok((STATIC_NAMES[index - 1].as_bytes().to_vec(), Vec::new())),
            index => self
                .table
                .get(index - 62)
                .cloned()
                .ok_or_else(|| format!("index {index} past the tables")),
        }
    }

    fn insert(&mut self, name: Vec<u8>, value: Vec<u8>) {
        let size = name.len() + value.len() + 32;
        self.evict(size);
        if size <= self.max {
            self.size += size;
            self.table.push_front((name, value));
        }
    }

    /// Evicts the oldest entries until `room` more bytes fit, or the table is empty.
    fn evict(&mut self, room: usize) {
        while self.size + room > self.max {
            let Some((name, value)) = self.table.pop_back() else {
                break;
            };
            self.size -= name.len() + value.len() + 32;
        }
    }

    fn byte(&mut self) -> Result<u8, String> {
        let byte = *self.block.get(self.at).ok_or("a header block cut short")?;
        self.at += 1;
        Ok(byte)
    }

    /// An integer of an `n`-bit prefix (5.1).
    fn integer(&mut self, n: u32) -> Result<usize, String> {
        let mask = (1u8 << n) - 1;
        let mut value = (self.byte()? & mask) as usize;
        if value < mask as usize {
            return Ok(value);
        }
        for shift in (0..28).step_by(7) {
            let byte = self.byte()?;
            value += ((byte & 0x7f) as usize) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err("an integer too large".into())
    }

    /// A string literal (5.2), Huffman coded or not.
    fn string(&mut self) -> Result<Vec<u8>, String> {
        let huffman = self
            .block
            .get(self.at)
            .is_some_and(|first| first & 0x80 != 0);
        let len = self.integer(7)?;
        let bytes = self
            .block
            .get(self.at..self.at + len)
            .ok_or("a header block cut short")?;
        self.at += len;
        if huffman {
            huffman_decode(bytes)
        } else {
            Ok(bytes.to_vec())
        }
    }
}

/// The Huffman code's lengths, by symbol (Appendix B; 256 is EOS). The code is
/// canonical: the lengths make the codes.
const CODE_LENGTHS: [u8; 257] = [
    13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 30, 28,
    28, 28, 28, 28, 28, 28, 28, 28, 6, 10, 10, 12, 13, 6, 8, 11, 10, 10, 8, 11, 8, 6, 6, 6, 5, 5,
    5, 6, 6, 6, 6, 6, 6, 6, 7, 8, 15, 6, 12, 10, 13, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 7, 8, 13, 19, 13, 14, 6, 15, 5, 6, 5, 6, 5, 6, 6, 6, 5, 7, 7, 6, 6,
    6, 5, 6, 7, 6, 5, 5, 6, 7, 7, 7, 7, 7, 15, 11, 14, 13, 28, 20, 22, 20, 20, 22, 22, 22, 23, 22,
    23, 23, 23, 23, 23, 24, 23, 24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24, 22,
    21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23, 21, 21, 22, 21, 23, 22, 23, 23, 20,
    22, 22, 22, 23, 22, 22, 23, 26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25, 19,
    21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27, 20, 24, 20, 21, 22, 21, 21, 23, 22,
    22, 25, 25, 24, 24, 26, 23, 26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26, 30,
];

const EOS: u16 = 256;

/// The longest code.
const MAX_LENGTH: usize = 30;

/// The canonical code by length: each length's first code, and the symbols of
/// that length in code order from its offset on.
struct Canonical {
    first: [u32; MAX_LENGTH + 1],
    count: [u32; MAX_LENGTH + 1],
    offset: [usize; MAX_LENGTH + 1],
    symbols: Vec<u16>,
}

fn canonical() -> &'static Canonical {
    static CODE: OnceLock<Canonical> = OnceLock::new();
    CODE.get_or_init(|| {
        let mut count = [0u32; MAX_LENGTH + 1];
        for &len in &CODE_LENGTHS {
            count[len as usize] += 1;
        }
        let (mut first, mut offset) = ([0u32; MAX_LENGTH + 1], [0usize; MAX_LENGTH + 1]);
        let (mut code, mut at) = (0u32, 0usize);
        for len in 1..=MAX_LENGTH {
            code = (code + count[len - 1]) << 1;
            first[len] = code;
            offset[len] = at;
            at += count[len] as usize;
        }
        let mut symbols: Vec<u16> = (0..=EOS).collect();
        symbols.sort_by_key(|&symbol| (CODE_LENGTHS[symbol as usize], symbol));
        Canonical {
            first,
            count,
            offset,
            symbols,
        }
    })
}

/// Decodes a Huffman coded string (5.2): its padding at most 7 bits of EOS's
/// prefix (ones), EOS itself an error.
fn huffman_decode(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let canonical = canonical();
    let mut decoded = Vec::with_capacity(bytes.len() * 8 / 5);
    let (mut code, mut len) = (0u32, 0usize);
    for byte in bytes {
        for bit in (0..8).rev() {
            code = (code << 1) | ((byte >> bit) & 1) as u32;
            len += 1;
            let index = code.wrapping_sub(canonical.first[len]);
            if index < canonical.count[len] {
                match canonical.symbols[canonical.offset[len] + index as usize] {
                    EOS => return Err("EOS in a Huffman string".into()),
                    symbol => decoded.push(symbol as u8),
                }
                (code, len) = (0, 0);
            } else if len == MAX_LENGTH {
                return Err("an invalid Huffman code".into());
            }
        }
    }
    if len > 7 || code != (1 << len) - 1 {
        return Err("an invalid Huffman padding".into());
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(text: &str) -> Vec<u8> {
        let digits: String = text.split_whitespace().collect();
        (0..digits.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&digits[at..at + 2], 16).unwrap())
            .collect()
    }

    /// The RFC's request examples (C.3.1, C.4.1: without and with Huffman
    /// coding); a host field after the pseudo-header fields, its name the
    /// dynamic table's; a block naming no host; malformed blocks.
    #[test]
    fn a_header_block_names_its_host() {
        type Case = (Vec<u8>, Result<Option<&'static str>, &'static str>);
        let cases: [Case; 7] = [
            (
                hex("8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d"),
                Ok(Some("www.example.com")),
            ),
            (
                hex("8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff"),
                Ok(Some("www.example.com")),
            ),
            // host: a.local (incremental, its name a literal), then host: b.local
            // naming the entry (index 62): the first host counts
            (
                [
                    &[0x82, 0x40, 0x04][..],
                    b"host",
                    &[0x07],
                    b"a.local",
                    &[0x7e, 0x07],
                    b"b.local",
                ]
                .concat(),
                Ok(Some("a.local")),
            ),
            (hex("8286 84"), Ok(None)),
            (hex("8286 bf"), Err("index 63 past the tables")),
            (hex("8286 8441 8cf1"), Err("a header block cut short")),
            // the code of EOS (thirty ones)
            (hex("0001 7884 ffff ffff"), Err("EOS in a Huffman string")),
        ];
        for (block, expected) in cases {
            let expected = expected
                .map(|host| host.map(String::from))
                .map_err(String::from);
            assert_eq!(host(&block), expected, "{block:02x?}");
        }
    }
}
