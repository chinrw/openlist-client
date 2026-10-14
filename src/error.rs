//! Failures and how callers and the retry loop classify them.

use crate::limit::RequestClass;

/// Everything a request can fail with.
///
/// Errors are `Clone` because one upstream request can answer several
/// concurrent callers (see the caches), and each caller gets its own copy.
/// `Display` never includes response bodies, which can echo tokens and
/// signed URLs.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// OpenList reported that the object does not exist.
    #[error("{path} does not exist on OpenList")]
    NotFound { path: String },
    /// OpenList answered with a business error code.
    #[error("OpenList answered code {code} for {path}")]
    Api {
        code: i64,
        path: String,
        message: String,
    },
    /// The HTTP status was not a success. It comes from OpenList, or from
    /// the provider for [`crate::Transfer::error_for_status`].
    #[error("HTTP {status} for {path}")]
    Http {
        status: u16,
        path: String,
        body: String,
    },
    /// The request never produced a complete response.
    #[error("request for {path} failed: {message}")]
    Transport {
        path: String,
        message: String,
        retryable: bool,
    },
    /// OpenList refused the configured credentials: the login failed, or
    /// it rejected a token given with [`crate::Config::with_token`].
    #[error("OpenList login failed (code {code})")]
    Login { code: i64, message: String },
    /// OpenList asked the client to pause for longer than the wait budget
    /// allows.
    #[error("OpenList is throttling requests beyond the wait budget")]
    RateLimited,
    /// Too many link fetches for distinct files are already waiting.
    #[error("too many link requests are waiting for OpenList")]
    Busy,
    /// Pages of one listing did not add up to the reported total.
    #[error("listing of {path} is incomplete: got {got} of {total} entries")]
    Incomplete {
        path: String,
        got: usize,
        total: u64,
    },
    /// The response did not have the expected shape.
    #[error("unexpected response for {path}: {message}")]
    Protocol { path: String, message: String },
    /// A path or entry name that cannot be sent to OpenList.
    #[error("invalid OpenList path {path:?}")]
    InvalidPath { path: String },
}

/// The three outcomes a storage layer must tell apart.
///
/// Only [`ErrorKind::NotFound`] is evidence that something is gone. A scanner
/// must never mark files missing on [`ErrorKind::Unavailable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// OpenList said the object does not exist.
    NotFound,
    /// OpenList or the provider could not answer right now.
    Unavailable,
    /// OpenList answered with an error that retrying will not change.
    Upstream,
}

/// Substrings 115 uses when it throttles a caller (error code 770004).
const THROTTLE_MARKERS: &[&str] = &["770004", "访问上限", "访问频繁"];
/// A provider marker for blocked content, which never succeeds on retry.
const PERMANENT_MARKERS: &[&str] = &["违规内容"];
/// OpenList's message for a missing object (`errs.ObjectNotFound`).
pub(crate) const NOT_FOUND_MARKER: &str = "object not found";

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::NotFound { .. } => ErrorKind::NotFound,
            Self::Api { code, message, .. } => {
                if (*code == 401 || *code >= 500) && !has_marker(message, PERMANENT_MARKERS) {
                    ErrorKind::Unavailable
                } else {
                    ErrorKind::Upstream
                }
            }
            Self::InvalidPath { .. } => ErrorKind::Upstream,
            Self::Http { .. }
            | Self::Transport { .. }
            | Self::Login { .. }
            | Self::RateLimited
            | Self::Busy
            | Self::Incomplete { .. }
            | Self::Protocol { .. } => ErrorKind::Unavailable,
        }
    }

    /// Whether the provider is probably throttling this class of request.
    ///
    /// A link request reaches the provider every time, and OpenList sometimes
    /// turns 115's 770004 into a bare 500, so any transient 5xx on a link
    /// counts. Listings are usually served from OpenList's cache, so only an
    /// explicit marker counts for them.
    pub(crate) fn signals_throttle(&self, class: RequestClass) -> bool {
        let link = class == RequestClass::Link;
        match self {
            Self::Api { code, message, .. } => {
                has_marker(message, THROTTLE_MARKERS)
                    || (link && *code == 500 && self.is_retryable())
            }
            Self::Http { status, body, .. } => {
                has_marker(body, THROTTLE_MARKERS)
                    || (link && *status >= 500 && self.is_retryable())
            }
            _ => false,
        }
    }

    /// Whether the same request may succeed if sent again: a transient 5xx,
    /// 408 or 429, or a connection that failed. A missing object or blocked
    /// content never qualifies, whatever the status.
    ///
    /// The client already retried API calls, so callers use this for what
    /// it does not retry, such as reading a [`crate::Transfer`].
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Api { code, message, .. } => {
                *code == 500
                    && !has_marker(message, PERMANENT_MARKERS)
                    && !message.contains(NOT_FOUND_MARKER)
            }
            Self::Http { status, body, .. } => {
                // 501 and 505 report a missing capability, not a passing fault.
                matches!(status, 408 | 429 | 500 | 502..=504 | 506..=599)
                    && !has_marker(body, PERMANENT_MARKERS)
                    && !body.contains(NOT_FOUND_MARKER)
            }
            Self::Transport { retryable, .. } => *retryable,
            _ => false,
        }
    }

    /// Whether the provider refused the content itself, as 115 does with
    /// 违规内容. Retrying or refreshing the link never helps.
    pub fn is_blocked(&self) -> bool {
        match self {
            Self::Api { message, .. } => has_marker(message, PERMANENT_MARKERS),
            Self::Http { body, .. } => has_marker(body, PERMANENT_MARKERS),
            _ => false,
        }
    }

    /// OpenList answers 401 inside a 200 response when the token expired, was
    /// revoked, or the password changed.
    pub(crate) fn is_auth_expired(&self) -> bool {
        matches!(self, Self::Api { code: 401, .. })
    }

    pub(crate) fn transport(path: &str, error: reqwest::Error) -> Self {
        // A body cut short while streaming surfaces as a decode error.
        let retryable = error.is_timeout()
            || error.is_connect()
            || error.is_body()
            || error.is_decode()
            || error.is_request();
        // Without the URL, which can carry a signed provider link.
        let error = error.without_url();
        let mut message = error.to_string();
        let mut source = std::error::Error::source(&error);
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        Self::Transport {
            path: path.to_owned(),
            message,
            retryable,
        }
    }

    pub(crate) fn protocol(path: &str, message: impl Into<String>) -> Self {
        Self::Protocol {
            path: path.to_owned(),
            message: message.into(),
        }
    }
}

/// Bytes of a failed response read for classification. Error pages can be
/// long HTML whose only signal comes late.
const ERROR_BODY_LIMIT: usize = 64 * 1024;

/// Reads the start of an error response's body, for [`clean_body`].
pub(crate) async fn error_prefix(response: &mut reqwest::Response) -> Vec<u8> {
    let mut prefix = Vec::new();
    while prefix.len() < ERROR_BODY_LIMIT {
        match response.chunk().await {
            Ok(Some(chunk)) => prefix.extend_from_slice(&chunk),
            _ => break,
        }
    }
    prefix.truncate(ERROR_BODY_LIMIT);
    prefix
}

fn has_marker(text: &str, markers: &[&str]) -> bool {
    markers.iter().any(|marker| text.contains(marker))
}

/// Keeps a bounded, single-line copy of an error body for logs.
pub(crate) fn clean_body(bytes: &[u8]) -> String {
    const MAX_CHARS: usize = 1024;
    let text = String::from_utf8_lossy(bytes);
    let mut out = String::new();
    let mut last_space = true;
    for ch in text.chars().take(MAX_CHARS) {
        // Control characters could inject terminal escapes into logs.
        if ch.is_whitespace() || ch.is_control() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(ch);
            last_space = false;
        }
    }
    out.truncate(out.trim_end().len());
    // Error pages and driver messages sometimes quote a signed link, and
    // these strings end up in Debug output and logs.
    let mut out = out
        .split(' ')
        .map(|word| {
            if word.contains("http://") || word.contains("https://") {
                "<url>"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    // Retries and throttling are decided on this text, so a marker past the
    // cut is kept.
    let markers = THROTTLE_MARKERS
        .iter()
        .chain(PERMANENT_MARKERS)
        .chain([&NOT_FOUND_MARKER]);
    for marker in markers {
        if text.contains(marker) && !out.contains(marker) {
            out.push_str(" ... ");
            out.push_str(marker);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api(code: i64, message: &str) -> Error {
        Error::Api {
            code,
            path: "/a".into(),
            message: message.into(),
        }
    }

    fn http(status: u16, body: &str) -> Error {
        Error::Http {
            status,
            path: "/a".into(),
            body: body.into(),
        }
    }

    #[test]
    fn throttle_markers_count_for_every_class_and_bare_5xx_only_for_links() {
        let marked = api(500, "failed get link: 770004 已达到当前访问上限");
        let bare = api(500, "");
        for class in RequestClass::ALL {
            assert!(marked.signals_throttle(class), "{class:?}");
            assert_eq!(
                bare.signals_throttle(class),
                class == RequestClass::Link,
                "{class:?}"
            );
        }
        assert!(!api(500, "object not found").signals_throttle(RequestClass::Link));
        assert!(!api(403, "").signals_throttle(RequestClass::Link));
        assert!(http(502, "").signals_throttle(RequestClass::Link));
        assert!(!http(502, "").signals_throttle(RequestClass::List));
        assert!(!http(404, "").signals_throttle(RequestClass::Link));
        assert!(http(200, "访问频繁").signals_throttle(RequestClass::List));
    }

    #[test]
    fn only_transient_failures_are_retried() {
        assert!(api(500, "upstream overloaded").is_retryable());
        assert!(!api(500, "failed get objs: object not found").is_retryable());
        assert!(!api(500, "违规内容").is_retryable());
        assert!(!api(403, "Refresh without permission").is_retryable());
        assert!(!api(401, "token is expired").is_retryable());
        for status in [408, 429, 500, 502, 503, 504, 507, 520] {
            assert!(http(status, "").is_retryable(), "{status}");
        }
        for status in [301, 400, 403, 404, 501, 505] {
            assert!(!http(status, "").is_retryable(), "{status}");
        }
        assert!(!http(500, "failed link: object not found").is_retryable());
    }

    #[test]
    fn blocked_content_is_recognized_in_messages_and_bodies() {
        assert!(api(500, "failed get link: 违规内容").is_blocked());
        assert!(http(403, "<p>违规内容</p>").is_blocked());
        assert!(!http(500, "upstream overloaded").is_blocked());
        assert!(!Error::RateLimited.is_blocked());
    }

    #[test]
    fn kinds_separate_missing_from_unavailable() {
        assert_eq!(
            Error::NotFound { path: "/a".into() }.kind(),
            ErrorKind::NotFound
        );
        assert_eq!(api(500, "").kind(), ErrorKind::Unavailable);
        assert_eq!(api(401, "").kind(), ErrorKind::Unavailable);
        assert_eq!(api(403, "").kind(), ErrorKind::Upstream);
        assert_eq!(api(500, "违规内容").kind(), ErrorKind::Upstream);
        assert_eq!(Error::RateLimited.kind(), ErrorKind::Unavailable);
        let incomplete = Error::Incomplete {
            path: "/a".into(),
            got: 1,
            total: 2,
        };
        assert_eq!(incomplete.kind(), ErrorKind::Unavailable);
    }

    #[test]
    fn display_never_contains_the_body() {
        let error = http(500, "token=secret");
        assert!(!error.to_string().contains("secret"));
        let error = api(500, "https://cdn.example/signed?k=secret");
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn clean_body_collapses_whitespace_and_controls() {
        assert_eq!(clean_body(b"  a \n\t b\x1b[2K  "), "a b [2K");
        assert_eq!(clean_body(&[b'x'; 5000]).len(), 1024);
        let mut page = vec![b'x'; 5000];
        page.extend_from_slice(b" failed link: object not found");
        assert!(clean_body(&page).ends_with("... object not found"));
        assert!(!http(500, &clean_body(&page)).is_retryable());
        assert_eq!(
            clean_body(b"failed: https://cdn.example/f?sig=secret gone"),
            "failed: <url> gone"
        );
    }
}
