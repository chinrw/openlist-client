//! What the client returns, and the wire shapes it parses them from.

use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant, SystemTime},
};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

use crate::Error;

/// One file or directory in a listing.
#[derive(Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    /// `None` when the provider reports no time (Go's zero time).
    pub modified: Option<SystemTime>,
    /// Signature for OpenList's `/d` and `/p` URLs. Empty when signing is
    /// off. It can change over time, so it is not part of the digest.
    pub sign: String,
    /// Hashes by algorithm name (`md5`, `sha1`, ...), when the provider has
    /// them.
    pub hashes: BTreeMap<String, String>,
}

/// All entries of one directory.
///
/// Names can repeat, because 115 allows two files with the same name in one
/// directory. Callers that key entries by path must decide what to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    /// The normalized path that was listed.
    pub path: String,
    pub entries: Vec<Entry>,
    /// The storage driver name, such as `115 Cloud`.
    pub provider: String,
    /// When this client received the listing from OpenList.
    pub fetched_at: Instant,
}

impl Listing {
    pub fn find(&self, name: &str) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.name == name)
    }

    /// A digest of every entry's name, type, size and modification time.
    ///
    /// Store it after a full scan of a directory. When a later listing has
    /// the same digest, nothing directly inside changed, and because
    /// subdirectory times are included, a provider that updates directory
    /// times on change lets the caller skip the deep walk too.
    pub fn digest(&self) -> ListingDigest {
        let mut entries: Vec<&Entry> = self.entries.iter().collect();
        entries.sort_by(|a, b| {
            (a.name.as_bytes(), a.is_dir, a.size, a.modified).cmp(&(
                b.name.as_bytes(),
                b.is_dir,
                b.size,
                b.modified,
            ))
        });
        let mut hasher = Sha256::new();
        // Bump the version whenever the encoding below changes, so stored
        // digests stop matching instead of matching by accident.
        hasher.update(b"openlist-listing-v1\0");
        for entry in entries {
            hasher.update((entry.name.len() as u64).to_le_bytes());
            hasher.update(entry.name.as_bytes());
            hasher.update([u8::from(entry.is_dir)]);
            hasher.update(entry.size.to_le_bytes());
            let nanos = entry.modified.map(unix_nanos);
            hasher.update([u8::from(nanos.is_some())]);
            hasher.update(nanos.unwrap_or(0).to_le_bytes());
        }
        ListingDigest(hasher.finalize().into())
    }
}

fn unix_nanos(time: SystemTime) -> i128 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(after) => after.as_nanos() as i128,
        Err(before) => -(before.duration().as_nanos() as i128),
    }
}

/// See [`Listing::digest`]. Its text form is lowercase hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListingDigest([u8; 32]);

impl ListingDigest {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses the 64-character hex form written by `Display`.
    pub fn from_hex(text: &str) -> Option<Self> {
        if text.len() != 64 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok()?;
        }
        Some(Self(bytes))
    }
}

impl std::fmt::Display for ListingDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

/// A direct link to a file and the headers the provider requires with it.
#[derive(Clone)]
pub struct Link {
    pub url: Url,
    /// Usually `User-Agent` and `Referer`. 115 binds a link to the user agent
    /// that requested it, so these must be sent unchanged.
    pub headers: HeaderMap,
    /// When the provider said the link stops working, if it said.
    pub expires_at: Option<Instant>,
}

/// The `fs/get` answer for one object.
#[derive(Clone)]
pub struct FileInfo {
    pub entry: Entry,
    /// OpenList's download URL. For direct-link storages, OpenList asked the
    /// provider for it, which is why `fs/get` is paced like `fs/link`.
    pub raw_url: String,
}

/// One entry found by [`crate::Client::walk`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkEntry {
    /// Normalized absolute path.
    pub path: String,
    pub entry: Entry,
}

// The Debug output below leaves out signatures and the query strings of
// signed links, so a value can be logged without handing out access.

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("name", &self.name)
            .field("is_dir", &self.is_dir)
            .field("size", &self.size)
            .field("modified", &self.modified)
            .field("signed", &!self.sign.is_empty())
            .field("hashes", &self.hashes)
            .finish()
    }
}

impl std::fmt::Debug for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Link")
            .field("url", &without_query(self.url.as_str()))
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl std::fmt::Debug for FileInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileInfo")
            .field("entry", &self.entry)
            .field("raw_url", &without_query(&self.raw_url))
            .finish()
    }
}

fn without_query(url: &str) -> &str {
    url.split_once('?').map_or(url, |(base, _)| base)
}

// Wire shapes, following OpenList's server/handles/fsread.go.

#[derive(Deserialize)]
pub(crate) struct Envelope<T> {
    pub code: i64,
    #[serde(default)]
    pub message: String,
    pub data: Option<T>,
}

#[derive(Serialize)]
pub(crate) struct ListRequest<'a> {
    pub path: &'a str,
    pub password: &'a str,
    pub page: u32,
    pub per_page: u32,
    pub refresh: bool,
}

#[derive(Serialize)]
pub(crate) struct PathRequest<'a> {
    pub path: &'a str,
    pub password: &'a str,
}

#[derive(Serialize)]
pub(crate) struct RemoveRequest<'a> {
    pub dir: &'a str,
    pub names: &'a [String],
}

#[derive(Serialize)]
pub(crate) struct LoginRequest<'a> {
    pub username: &'a str,
    pub password: &'a str,
}

#[derive(Deserialize)]
pub(crate) struct LoginData {
    pub token: String,
}

#[derive(Deserialize)]
pub(crate) struct WireListing {
    pub content: Option<Vec<WireEntry>>,
    pub total: u64,
    #[serde(default)]
    pub provider: String,
}

#[derive(Deserialize)]
pub(crate) struct WireEntry {
    pub name: String,
    pub size: i64,
    pub is_dir: bool,
    #[serde(default)]
    pub modified: String,
    #[serde(default)]
    pub sign: String,
    #[serde(default)]
    pub hash_info: Option<HashMap<String, String>>,
}

impl WireEntry {
    pub(crate) fn into_entry(self, path: &str) -> Result<Entry, Error> {
        // A name that cannot be addressed fails the whole listing, so no
        // caller caches or digests an enumeration it cannot walk.
        crate::path::join(path, &self.name)?;
        Ok(Entry {
            size: u64::try_from(self.size)
                .map_err(|_| Error::protocol(path, format!("negative size for {:?}", self.name)))?,
            modified: parse_time(&self.modified)
                .map_err(|()| Error::protocol(path, format!("bad time for {:?}", self.name)))?,
            hashes: self.hash_info.unwrap_or_default().into_iter().collect(),
            name: self.name,
            is_dir: self.is_dir,
            sign: self.sign,
        })
    }
}

#[derive(Deserialize)]
pub(crate) struct WireFileInfo {
    #[serde(flatten)]
    pub entry: WireEntry,
    #[serde(default)]
    pub raw_url: String,
}

#[derive(Deserialize)]
pub(crate) struct WireLink {
    pub url: String,
    #[serde(default)]
    pub header: Option<HashMap<String, Vec<String>>>,
    /// Go's `time.Duration` in nanoseconds. The field has no JSON tag
    /// upstream, so its key is the Go field name.
    #[serde(default, rename = "Expiration", alias = "expiration")]
    pub expiration: Option<i64>,
}

impl WireLink {
    pub(crate) fn into_link(self, path: &str, fetched_at: Instant) -> Result<Link, Error> {
        if self.url.is_empty() {
            // Some drivers only offer a range reader inside OpenList.
            return Err(Error::protocol(path, "the storage offers no direct link"));
        }
        let url = Url::parse(&self.url).map_err(|_| Error::protocol(path, "unparsable link"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::protocol(path, "link is not http(s)"));
        }
        let mut headers = HeaderMap::new();
        for (name, values) in self.header.unwrap_or_default() {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| Error::protocol(path, "bad header name in link"))?;
            for value in values {
                let value = HeaderValue::from_str(&value)
                    .map_err(|_| Error::protocol(path, "bad header value in link"))?;
                headers.append(name.clone(), value);
            }
        }
        let expires_at = self
            .expiration
            .map(|nanos| fetched_at + Duration::from_nanos(u64::try_from(nanos).unwrap_or(0)));
        Ok(Link {
            url,
            headers,
            expires_at,
        })
    }
}

/// Parses Go's RFC 3339 output. Go writes the zero time for unknown values.
fn parse_time(text: &str) -> Result<Option<SystemTime>, ()> {
    if text.is_empty() {
        return Ok(None);
    }
    let time = OffsetDateTime::parse(text, &Rfc3339).map_err(|_| ())?;
    if time.year() <= 1 {
        return Ok(None);
    }
    Ok(Some(time.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, size: u64, secs: u64) -> Entry {
        Entry {
            name: name.into(),
            is_dir: false,
            size,
            modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs)),
            sign: String::new(),
            hashes: BTreeMap::new(),
        }
    }

    fn listing(entries: Vec<Entry>) -> Listing {
        Listing {
            path: "/w".into(),
            entries,
            provider: "test".into(),
            fetched_at: Instant::now(),
        }
    }

    #[test]
    fn digest_ignores_order_sign_and_hashes() {
        let a = listing(vec![entry("1.mp3", 10, 100), entry("2.mp3", 20, 200)]);
        let mut b = listing(vec![entry("2.mp3", 20, 200), entry("1.mp3", 10, 100)]);
        b.entries[0].sign = "changes-every-hour".into();
        b.entries[1].hashes.insert("sha1".into(), "ab".into());
        assert_eq!(a.digest(), b.digest());
    }

    #[test]
    fn digest_changes_with_size_time_type_or_name() {
        let base = listing(vec![entry("1.mp3", 10, 100)]);
        let changed = [
            listing(vec![entry("1.mp3", 11, 100)]),
            listing(vec![entry("1.mp3", 10, 101)]),
            listing(vec![entry("1.flac", 10, 100)]),
            listing(vec![Entry {
                is_dir: true,
                ..entry("1.mp3", 10, 100)
            }]),
            listing(vec![Entry {
                modified: None,
                ..entry("1.mp3", 10, 100)
            }]),
            listing(vec![]),
        ];
        for other in changed {
            assert_ne!(base.digest(), other.digest(), "{other:?}");
        }
    }

    #[test]
    fn digest_does_not_confuse_name_boundaries() {
        let a = listing(vec![entry("ab", 1, 1), entry("c", 1, 1)]);
        let b = listing(vec![entry("a", 1, 1), entry("bc", 1, 1)]);
        assert_ne!(a.digest(), b.digest());
    }

    #[test]
    fn digest_round_trips_through_hex() {
        let digest = listing(vec![entry("1.mp3", 10, 100)]).digest();
        let text = digest.to_string();
        assert_eq!(text.len(), 64);
        assert_eq!(ListingDigest::from_hex(&text), Some(digest));
        assert_eq!(ListingDigest::from_hex("zz"), None);
        assert_eq!(ListingDigest::from_hex(&"g".repeat(64)), None);
        assert_eq!(ListingDigest::from_hex(&"+f".repeat(32)), None);
    }

    #[test]
    fn go_times_parse_with_offsets_and_zero_means_unknown() {
        let utc = parse_time("2024-05-01T12:00:00Z").unwrap().unwrap();
        let cst = parse_time("2024-05-01T20:00:00.5+08:00").unwrap().unwrap();
        assert_eq!(cst.duration_since(utc).unwrap(), Duration::from_millis(500));
        assert_eq!(parse_time("0001-01-01T00:00:00Z"), Ok(None));
        assert_eq!(parse_time(""), Ok(None));
        assert!(parse_time("yesterday").is_err());
    }

    #[test]
    fn link_parses_headers_and_expiration() {
        let wire: WireLink = serde_json::from_value(serde_json::json!({
            "url": "https://cdn.example/f?sig=1",
            "header": {"User-Agent": ["ua"], "Referer": ["https://example/"]},
            "Expiration": 90_000_000_000i64,
            "concurrency": 0,
        }))
        .unwrap();
        let now = Instant::now();
        let link = wire.into_link("/f", now).unwrap();
        assert_eq!(link.headers["user-agent"], "ua");
        assert_eq!(link.expires_at, Some(now + Duration::from_secs(90)));

        let bare: WireLink =
            serde_json::from_value(serde_json::json!({"url": "", "header": null})).unwrap();
        assert!(bare.into_link("/f", now).is_err());
        assert!(!format!("{link:?}").contains("sig=1"));
    }
}
