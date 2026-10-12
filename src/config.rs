//! Client settings. The defaults suit OpenList in front of 115, which
//! throttles at roughly one provider call per second.

use std::time::Duration;

use url::Url;

/// Slower rates make the pacing interval overflow `Duration`.
const MIN_LINK_RATE: f64 = 0.001;

/// Settings for one [`crate::Client`]. Build with [`Config::new`] and change
/// the public fields as needed.
#[derive(Clone)]
pub struct Config {
    /// OpenList's base URL. A path prefix is kept, for a reverse proxy that
    /// serves OpenList under one.
    pub url: Url,
    pub username: String,
    pub password: String,
    /// Sent with API requests and file transfers alike. 115 ties a link to
    /// the user agent that asked for it, so the two must match.
    pub user_agent: String,
    /// Listings in flight at once. Halved while the provider throttles.
    pub list_concurrency: usize,
    /// `fs/link` and `fs/get` requests per second. Halved while the provider
    /// throttles.
    pub link_rate: f64,
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

impl Config {
    pub fn new(url: Url, username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            url,
            username: username.into(),
            password: password.into(),
            user_agent: concat!("openlist-client/", env!("CARGO_PKG_VERSION")).to_owned(),
            list_concurrency: 4,
            link_rate: 1.0,
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
        if self.username.is_empty() {
            return fail("username is empty");
        }
        if !(self.link_rate.is_finite() && self.link_rate >= MIN_LINK_RATE) {
            return fail("link_rate must be at least 0.001 per second");
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
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("user_agent", &self.user_agent)
            .field("list_concurrency", &self.list_concurrency)
            .field("link_rate", &self.link_rate)
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
    }
}
