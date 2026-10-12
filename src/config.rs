//! Client settings. The defaults suit OpenList in front of 115, which
//! throttles at roughly one provider call per second.

use std::time::Duration;

use url::Url;

/// Slower rates make the pacing interval overflow `Duration`.
const MIN_RATE: f64 = 0.001;

/// Settings for one [`crate::Client`]. Build with [`Config::new`] and change
/// the public fields as needed.
#[derive(Clone)]
pub struct Config {
    /// OpenList's base URL. A path prefix is kept, for a reverse proxy that
    /// serves OpenList under one.
    pub url: Url,
    pub auth: Auth,
    /// Sent with API requests and file transfers alike. 115 ties a link to
    /// the user agent that asked for it, so the two must match.
    pub user_agent: String,
    /// Listings in flight at once. Halved while the provider throttles.
    pub list_concurrency: usize,
    /// `fs/link` and `fs/get` requests per second. Halved while the provider
    /// throttles. `f64::INFINITY` turns pacing off, and throttling then
    /// leaves it off.
    pub link_rate: f64,
    /// `fs/remove` requests per second, on a budget separate from links.
    /// Halved while the provider throttles. `f64::INFINITY` turns it off.
    pub mutate_rate: f64,
    /// API requests in flight at once across all classes.
    pub max_api_requests: usize,
    /// File transfers open at once. Transfers have their own limit because a
    /// player can hold one open for minutes, which must not block listings.
    pub max_transfers: usize,
    /// Entries per `fs/list` page. 0 asks for the whole directory in one
    /// response. OpenList builds the full listing for every page anyway, so
    /// 0 is the cheapest choice.
    pub per_page: u32,
    /// Largest API response body accepted. A listing entry is roughly 300
    /// bytes, so the default fits a directory of about 200 000 entries.
    pub max_response_bytes: usize,
    pub request_timeout: Duration,
    /// Timeout for a forced refresh, which waits for the provider to list
    /// the directory again.
    pub refresh_timeout: Duration,
    /// How long one call may spend waiting out 429 and 503 pauses before it
    /// fails with [`crate::Error::RateLimited`].
    pub cooldown_budget: Duration,
    /// First retry delay. Later retries double it, with full jitter.
    pub retry_delay: Duration,
    /// How long a listing is served from memory. 0 turns the cache off.
    pub listing_ttl: Duration,
    pub listing_cache_capacity: usize,
    /// When [`crate::Client::stat`] misses in a listing at least this old,
    /// it lists the parent once more before answering `NotFound`. Younger
    /// listings are trusted, so a burst of misses costs at most one relist.
    pub stat_recheck_age: Duration,
    /// Upper bound on how long a link is reused. A shorter expiry from the
    /// provider wins.
    pub link_ttl: Duration,
    pub link_cache_capacity: usize,
    /// Link fetches for distinct files that may wait at once. One more
    /// fails at once with [`crate::Error::Busy`]: at `link_rate` a longer
    /// queue only makes every caller wait longer.
    pub max_pending_links: usize,
}

/// How the client authenticates to OpenList.
#[derive(Clone)]
#[non_exhaustive]
pub enum Auth {
    /// Logs in through `api/auth/login`, and again when OpenList rejects
    /// the token it issued.
    Password { username: String, password: String },
    /// Sends this token unchanged and never logs in. OpenList rejecting it
    /// fails the call with [`crate::Error::Login`], because the client has
    /// nothing to replace it with.
    Token(String),
    /// Sends no token, so OpenList serves the requests as its guest user.
    /// This only works where the guest user is enabled and may read the
    /// paths in question. A rejection fails the call with
    /// [`crate::Error::Login`].
    Anonymous,
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Password { username, .. } => f
                .debug_struct("Password")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Token(_) => f.debug_tuple("Token").field(&"<redacted>").finish(),
            Self::Anonymous => f.write_str("Anonymous"),
        }
    }
}

impl Config {
    /// Settings that log in with a username and password.
    pub fn new(url: Url, username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::with_auth(
            url,
            Auth::Password {
                username: username.into(),
                password: password.into(),
            },
        )
    }

    /// Settings that send a token issued elsewhere, such as the admin token
    /// on OpenList's settings page.
    pub fn with_token(url: Url, token: impl Into<String>) -> Self {
        Self::with_auth(url, Auth::Token(token.into()))
    }

    /// Settings that send no credentials and act as OpenList's guest user.
    pub fn anonymous(url: Url) -> Self {
        Self::with_auth(url, Auth::Anonymous)
    }

    fn with_auth(url: Url, auth: Auth) -> Self {
        Self {
            url,
            auth,
            user_agent: concat!("openlist-client/", env!("CARGO_PKG_VERSION")).to_owned(),
            list_concurrency: 4,
            link_rate: 1.0,
            mutate_rate: 1.0,
            max_api_requests: 8,
            max_transfers: 16,
            per_page: 0,
            max_response_bytes: 64 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            refresh_timeout: Duration::from_secs(300),
            cooldown_budget: Duration::from_secs(120),
            retry_delay: Duration::from_millis(500),
            listing_ttl: Duration::from_secs(300),
            listing_cache_capacity: 4096,
            stat_recheck_age: Duration::from_secs(5),
            link_ttl: Duration::from_secs(600),
            link_cache_capacity: 1024,
            max_pending_links: 32,
        }
    }

    pub(crate) fn check(&self) -> Result<(), ConfigError> {
        let fail = |message: &str| Err(ConfigError(message.to_owned()));
        if !matches!(self.url.scheme(), "http" | "https") {
            return fail("url must be http or https");
        }
        if self.url.cannot_be_a_base() {
            return fail("url cannot be a base URL");
        }
        if !self.url.username().is_empty() || self.url.password().is_some() {
            return fail("put credentials in username and password, not in the url");
        }
        match &self.auth {
            Auth::Password { username, .. } if username.is_empty() => {
                return fail("username is empty");
            }
            Auth::Token(token) if token.is_empty() => return fail("token is empty"),
            _ => {}
        }
        // NaN fails the comparison. Infinity is allowed and means no pacing.
        let usable = |rate: f64| rate >= MIN_RATE;
        if !usable(self.link_rate) || !usable(self.mutate_rate) {
            return fail("link_rate and mutate_rate must be at least 0.001 per second");
        }
        if self.list_concurrency == 0 || self.max_api_requests == 0 || self.max_transfers == 0 {
            return fail("concurrency limits must be at least 1");
        }
        if self.listing_cache_capacity == 0
            || self.link_cache_capacity == 0
            || self.max_pending_links == 0
        {
            return fail("cache capacities must be at least 1");
        }
        Ok(())
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("url", &self.url.as_str())
            .field("auth", &self.auth)
            .field("user_agent", &self.user_agent)
            .field("list_concurrency", &self.list_concurrency)
            .field("link_rate", &self.link_rate)
            .field("mutate_rate", &self.mutate_rate)
            .field("max_api_requests", &self.max_api_requests)
            .field("max_transfers", &self.max_transfers)
            .field("per_page", &self.per_page)
            .field("listing_ttl", &self.listing_ttl)
            .field("link_ttl", &self.link_ttl)
            .finish_non_exhaustive()
    }
}

/// A setting that cannot work.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid OpenList client config: {0}")]
pub struct ConfigError(pub(crate) String);

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config::new("http://openlist.test".parse().unwrap(), "user", "secret")
    }

    #[test]
    fn debug_hides_the_password() {
        assert!(!format!("{:?}", config()).contains("secret"));
    }

    #[test]
    fn debug_hides_the_token() {
        let config = Config::with_token("http://openlist.test".parse().unwrap(), "openlist-abc");
        assert!(!format!("{config:?}").contains("openlist-abc"));
    }

    #[test]
    fn check_rejects_settings_that_cannot_work() {
        assert!(config().check().is_ok());
        let mut bad = config();
        bad.link_rate = 0.0;
        assert!(bad.check().is_err());
        let mut bad = config();
        bad.list_concurrency = 0;
        assert!(bad.check().is_err());
        let mut bad = config();
        bad.url = "ftp://openlist.test".parse().unwrap();
        assert!(bad.check().is_err());
        let mut bad = config();
        bad.url = "http://admin:hunter2@openlist.test".parse().unwrap();
        assert!(bad.check().is_err());
        let mut bad = config();
        bad.link_rate = 1e-20;
        assert!(bad.check().is_err());
        let mut bad = config();
        bad.link_rate = f64::NAN;
        assert!(bad.check().is_err());
        let mut unpaced = config();
        unpaced.link_rate = f64::INFINITY;
        unpaced.mutate_rate = f64::INFINITY;
        assert!(unpaced.check().is_ok());
        let guest = Config::anonymous("http://openlist.test".parse().unwrap());
        assert!(guest.check().is_ok());
        let mut bad = config();
        bad.auth = Auth::Token(String::new());
        assert!(bad.check().is_err());
        let mut bad = config();
        bad.auth = Auth::Password {
            username: String::new(),
            password: "secret".into(),
        };
        assert!(bad.check().is_err());
    }
}
