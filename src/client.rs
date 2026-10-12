//! The client: login, the retry loop and the API calls.

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime},
};

use futures_util::stream::{FuturesUnordered, StreamExt};
use reqwest::{
    StatusCode,
    header::{AUTHORIZATION, HeaderMap, RETRY_AFTER},
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tracing::{debug, warn};
use url::Url;

use crate::{
    Auth, Config, ConfigError, Entry, Error, FileInfo, Link, Listing, Transfer, WalkEntry,
    admission::{Admission, Limits, WaitBudget, retry_after},
    cache::Cache,
    error::{NOT_FOUND_MARKER, clean_body},
    limit::RequestClass,
    path,
    types::{
        Envelope, ListRequest, LoginData, LoginRequest, PathRequest, RemoveRequest, WireFileInfo,
        WireLink, WireListing,
    },
};

/// Total attempts for a retryable request, the first one included.
const MAX_ATTEMPTS: u32 = 3;
/// Pause after a business code 429, which carries no Retry-After.
const ENVELOPE_429_PAUSE: Duration = Duration::from_secs(60);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(10);
/// Bytes of a failed response kept for logs.
const ERROR_BODY_LIMIT: usize = 4096;

/// An OpenList client. Clones share one connection pool, one set of rate
/// limits and one set of caches, so create one per OpenList server.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    config: Config,
    /// `config.url` with a trailing slash, so endpoints join below it.
    base: Url,
    api: reqwest::Client,
    transfer: reqwest::Client,
    admission: Arc<Admission>,
    transfers: Arc<Semaphore>,
    token: Mutex<TokenState>,
    /// Lets one task log in while the others wait for its token.
    login_lock: AsyncMutex<()>,
    listings: Arc<Cache<Arc<Listing>>>,
    links: Arc<Cache<Link>>,
}

#[derive(Default)]
struct TokenState {
    value: Option<String>,
    /// Bumped on every login, so a task holding a rejected token can tell
    /// whether someone already replaced it.
    generation: u64,
}

struct CallSpec<'a> {
    class: RequestClass,
    endpoint: &'static str,
    /// For error messages and logs.
    path: &'a str,
    timeout: Duration,
    retry: bool,
}

impl Client {
    /// # Errors
    /// Returns an error when a setting cannot work or the TLS backend fails
    /// to start.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        config.check()?;
        let mut base = config.url.clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let build_error = |error: reqwest::Error| ConfigError(error.without_url().to_string());
        // API calls never follow redirects, so the token cannot leak to
        // another host, and never retry inside reqwest, so every attempt
        // passes through admission.
        let api = reqwest::Client::builder()
            .user_agent(&config.user_agent)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(build_error)?;
        // Transfers have no total timeout because a player reads at its own
        // pace. A stalled read still fails after `request_timeout`.
        let transfer = reqwest::Client::builder()
            .user_agent(&config.user_agent)
            .redirect(reqwest::redirect::Policy::limited(10))
            .no_proxy()
            .connect_timeout(config.request_timeout)
            .read_timeout(config.request_timeout)
            .build()
            .map_err(build_error)?;
        Ok(Self {
            inner: Arc::new(Inner {
                admission: Arc::new(Admission::new(
                    config.max_api_requests,
                    config.list_concurrency,
                    config.link_rate,
                    config.mutate_rate,
                )),
                transfers: Arc::new(Semaphore::new(config.max_transfers)),
                listings: Cache::new(config.listing_cache_capacity, usize::MAX),
                links: Cache::new(config.link_cache_capacity, config.max_pending_links),
                token: Mutex::new(TokenState {
                    value: match &config.auth {
                        Auth::Token(token) => Some(token.clone()),
                        Auth::Password { .. } => None,
                    },
                    generation: 0,
                }),
                login_lock: AsyncMutex::new(()),
                base,
                api,
                transfer,
                config,
            }),
        })
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// The current adaptive limits.
    pub fn limits(&self) -> Limits {
        self.inner.admission.limits()
    }

    /// Lists a directory, from memory when a listing younger than
    /// `listing_ttl` is cached. Concurrent calls for one directory share a
    /// single request.
    ///
    /// All pages are read. If any page fails, or the pages do not add up to
    /// the reported total, the whole call fails, so a caller never sees a
    /// partial directory.
    ///
    /// # Errors
    /// [`Error::NotFound`] when OpenList says the directory does not exist,
    /// and any other request failure.
    pub async fn list(&self, path: &str) -> Result<Arc<Listing>, Error> {
        let path = path::normalize(path)?;
        let inner = Arc::clone(&self.inner);
        let ttl = inner.config.listing_ttl;
        let key = path.clone();
        self.inner
            .listings
            .get_or_fetch(&path, async move {
                let listing = inner.fetch_listing(&key, false).await?;
                Ok((Arc::new(listing), ttl))
            })
            .await
    }

    /// Makes OpenList list the directory from the provider again, and
    /// caches the result. The request is sent once, never retried, because
    /// each attempt costs a provider call.
    ///
    /// # Errors
    /// As [`Client::list`]. OpenList also refuses a refresh with code 403
    /// when the account lacks write permission there.
    pub async fn refresh(&self, path: &str) -> Result<Arc<Listing>, Error> {
        let path = path::normalize(path)?;
        let inner = Arc::clone(&self.inner);
        let ttl = inner.config.listing_ttl;
        let key = path.clone();
        // Through the cache, so an invalidation while the refresh runs keeps
        // its result from being stored.
        self.inner
            .listings
            .fetch_fresh(&path, async move {
                let listing = inner.fetch_listing(&key, true).await?;
                Ok((Arc::new(listing), ttl))
            })
            .await
    }

    /// Looks an object up in its parent's listing instead of calling
    /// `fs/get`, which asks the provider for a link.
    ///
    /// A miss in a listing older than `stat_recheck_age` is checked once against
    /// a fresh listing, so a file added since the listing was cached is
    /// still found, while a burst of misses costs at most one relist.
    ///
    /// # Errors
    /// [`Error::NotFound`] when neither the parent nor the name exists.
    pub async fn stat(&self, path: &str) -> Result<Entry, Error> {
        let path = path::normalize(path)?;
        let Some((parent, name)) = path::split(&path) else {
            return Ok(Entry {
                name: String::new(),
                is_dir: true,
                size: 0,
                modified: None,
                sign: String::new(),
                hashes: Default::default(),
            });
        };
        let listing = self.list(parent).await?;
        if let Some(entry) = listing.find(name) {
            return Ok(entry.clone());
        }
        if listing.fetched_at.elapsed() >= self.inner.config.stat_recheck_age {
            // Only this listing is dropped, so a newer one or a fetch in
            // flight from another caller is kept and shared.
            self.inner
                .listings
                .remove_if(parent, |cached| Arc::ptr_eq(cached, &listing));
            if let Some(entry) = self.list(parent).await?.find(name) {
                return Ok(entry.clone());
            }
        }
        Err(Error::NotFound { path })
    }

    /// Calls `fs/get`. Paced like [`Client::link`], because OpenList asks the
    /// provider for a download URL. Prefer [`Client::stat`] for metadata.
    ///
    /// # Errors
    /// [`Error::NotFound`] when the object does not exist, and any other
    /// request failure.
    pub async fn get(&self, path: &str) -> Result<FileInfo, Error> {
        let path = path::normalize(path)?;
        let spec = self
            .inner
            .spec(RequestClass::Link, "api/fs/get", &path, true);
        let body = PathRequest {
            path: &path,
            password: "",
        };
        let wire: WireFileInfo = self.inner.call(&spec, &body).await?;
        Ok(FileInfo {
            entry: wire.entry.into_entry(&path)?,
            raw_url: wire.raw_url,
        })
    }

    /// Returns a direct link, from memory while it is younger than both
    /// `link_ttl` and the provider's own expiry. Concurrent calls for one
    /// file share a single request.
    ///
    /// `fs/link` needs an admin account. It ignores the account's base path,
    /// so the account's base path must be `/` for paths to agree with
    /// `fs/list`.
    ///
    /// # Errors
    /// [`Error::NotFound`] when the file does not exist, and any other
    /// request failure.
    pub async fn link(&self, path: &str) -> Result<Link, Error> {
        let path = path::normalize(path)?;
        let inner = Arc::clone(&self.inner);
        let key = path.clone();
        self.inner
            .links
            .get_or_fetch(&path, async move {
                let link = inner.fetch_link(&key).await?;
                let mut ttl = inner.config.link_ttl;
                if let Some(expires_at) = link.expires_at {
                    ttl = ttl.min(expires_at.saturating_duration_since(Instant::now()));
                }
                Ok((link, ttl))
            })
            .await
    }

    /// Deletes `names` from the directory `dir` with one `fs/remove` call.
    ///
    /// The request is paced on its own budget (`mutate_rate`) and sent once,
    /// never retried, because a timeout leaves it unknown whether the
    /// provider deleted anything. Cached listings and links for `dir` and
    /// everything below the removed names are dropped whatever the outcome,
    /// since even a rejected request may have deleted some entries.
    ///
    /// # Errors
    /// [`Error::InvalidPath`] for a name that cannot be sent. Otherwise
    /// [`Error::Transport`] means the outcome is unknown, and an
    /// [`Error::Api`] or [`Error::Http`] rejection may still have had partial
    /// effects.
    pub async fn remove(&self, dir: &str, names: &[String]) -> Result<(), Error> {
        let dir = path::normalize(dir)?;
        let targets = names
            .iter()
            .map(|name| path::join(&dir, name))
            .collect::<Result<Vec<_>, _>>()?;
        if names.is_empty() {
            return Ok(());
        }
        let spec = self
            .inner
            .spec(RequestClass::Mutate, "api/fs/remove", &dir, false);
        let body = RemoveRequest { dir: &dir, names };
        let result = self
            .inner
            .call::<_, serde::de::IgnoredAny>(&spec, &body)
            .await
            .map(drop);
        self.inner.listings.invalidate(&dir);
        for target in &targets {
            self.inner.listings.invalidate_tree(target);
            self.inner.links.invalidate_tree(target);
        }
        result
    }

    /// Forgets a cached link, for example after the provider rejected it.
    pub fn invalidate_link(&self, path: &str) {
        if let Ok(path) = path::normalize(path) {
            self.inner.links.invalidate(&path);
        }
    }

    /// Forgets a cached listing, for example after a write.
    pub fn invalidate_listing(&self, path: &str) {
        if let Ok(path) = path::normalize(path) {
            self.inner.listings.invalidate(&path);
        }
    }

    pub fn clear_caches(&self) {
        self.inner.listings.clear();
        self.inner.links.clear();
    }

    /// Lists everything below `root`, with up to `list_concurrency` listings
    /// in flight. Fails as a whole if any directory fails.
    ///
    /// # Errors
    /// As [`Client::list`], for the root and every directory below it.
    pub async fn walk(&self, root: &str) -> Result<Vec<WalkEntry>, Error> {
        let listing = self.list(root).await?;
        self.walk_from(listing).await
    }

    /// Like [`Client::walk`], starting from a root listing the caller already
    /// has, such as the one it just compared digests on.
    ///
    /// # Errors
    /// As [`Client::list`], for every directory below the root.
    pub async fn walk_from(&self, root: Arc<Listing>) -> Result<Vec<WalkEntry>, Error> {
        let mut visited = HashSet::from([root.path.clone()]);
        let mut queue = VecDeque::new();
        let mut found = Vec::new();
        collect(&root, &mut found, &mut queue, &mut visited)?;
        let mut in_flight = FuturesUnordered::new();
        loop {
            while in_flight.len() < self.inner.config.list_concurrency {
                let Some(dir) = queue.pop_front() else {
                    break;
                };
                let client = self.clone();
                in_flight.push(async move { client.list(&dir).await });
            }
            let Some(listing) = in_flight.next().await else {
                break;
            };
            let listing = listing?;
            collect(&listing, &mut found, &mut queue, &mut visited)?;
        }
        // Listings finish in any order.
        found.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        Ok(found)
    }

    /// Opens a file for reading through its direct link, forwarding
    /// `headers` such as `Range` and `If-Range`.
    ///
    /// When the provider answers 401, 403 or 404 the link has probably
    /// expired: the link is fetched again and the file opened once more.
    /// Transfers are not paced. Only `max_transfers` limits them.
    ///
    /// # Errors
    /// [`Error::Http`] when the second attempt is still rejected, and any
    /// failure of [`Client::link`] or the connection.
    pub async fn open(&self, path: &str, headers: &HeaderMap) -> Result<Transfer, Error> {
        let path = path::normalize(path)?;
        let link = self.link(&path).await?;
        let transfer = self.inner.fetch_file(&path, &link, headers).await?;
        if !link_rejected(transfer.status()) {
            return Ok(transfer);
        }
        drop(transfer);
        debug!(path, "provider rejected a cached link, fetching a new one");
        // Another request may already have replaced the link.
        self.inner
            .links
            .remove_if(&path, |cached| cached.url == link.url);
        let link = self.link(&path).await?;
        let transfer = self.inner.fetch_file(&path, &link, headers).await?;
        let status = transfer.status();
        if link_rejected(status) {
            self.inner
                .links
                .remove_if(&path, |cached| cached.url == link.url);
            return Err(Error::Http {
                status: status.as_u16(),
                path,
                body: String::new(),
            });
        }
        Ok(transfer)
    }

    /// Opens a URL that did not come from [`Client::link`], such as the
    /// `raw_url` of [`Client::get`] or an OpenList `/d` link built from an
    /// entry's `sign`, forwarding `headers` such as `Range`. Unlike
    /// [`Client::open`] it needs no admin account.
    ///
    /// A URL on OpenList's own origin makes OpenList ask the provider for
    /// the file, so it waits for the link budget like [`Client::link`] and
    /// carries the token, which redirects to another host drop. Any other
    /// URL is a plain transfer, limited only by `max_transfers`.
    ///
    /// Nothing is retried. The caller reads the status, including a 403
    /// from an expired link.
    ///
    /// # Errors
    /// [`Error::RateLimited`] when a pause outlasts the wait budget,
    /// [`Error::InvalidPath`] for a URL that is not http(s), and connection
    /// failures.
    pub async fn open_url(&self, url: &Url, headers: &HeaderMap) -> Result<Transfer, Error> {
        self.inner.open_url(url, headers).await
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("url", &self.inner.config.url.origin().ascii_serialization())
            .finish_non_exhaustive()
    }
}

fn too_large(path: &str, limit: usize) -> Error {
    Error::protocol(path, format!("response is larger than {limit} bytes"))
}

fn link_rejected(status: StatusCode) -> bool {
    matches!(status.as_u16(), 401 | 403 | 404)
}

fn collect(
    listing: &Listing,
    found: &mut Vec<WalkEntry>,
    queue: &mut VecDeque<String>,
    visited: &mut HashSet<String>,
) -> Result<(), Error> {
    for entry in &listing.entries {
        let path = path::join(&listing.path, &entry.name)?;
        if entry.is_dir && visited.insert(path.clone()) {
            queue.push_back(path.clone());
        }
        found.push(WalkEntry {
            path,
            entry: entry.clone(),
        });
    }
    Ok(())
}

impl Inner {
    fn spec<'a>(
        &self,
        class: RequestClass,
        endpoint: &'static str,
        path: &'a str,
        retry: bool,
    ) -> CallSpec<'a> {
        CallSpec {
            class,
            endpoint,
            path,
            timeout: self.config.request_timeout,
            retry,
        }
    }

    async fn fetch_listing(&self, path: &str, refresh: bool) -> Result<Listing, Error> {
        let per_page = self.config.per_page;
        let first = self.list_page(path, 1, refresh).await?;
        let total = first.total;
        let provider = first.provider;
        let mut entries = Vec::new();
        for wire in first.content.unwrap_or_default() {
            entries.push(wire.into_entry(path)?);
        }
        let mut page = 1;
        while per_page > 0 && (entries.len() as u64) < total {
            page += 1;
            // Only the first page refreshes, or every page would make
            // OpenList list the directory from the provider again.
            // Page 1 proved the directory exists, so "object not found" now
            // means it changed underneath the listing, not that it is gone.
            let next = match self.list_page(path, page, false).await {
                Ok(next) => next,
                Err(Error::NotFound { .. }) => {
                    return Err(Error::Incomplete {
                        path: path.to_owned(),
                        got: entries.len(),
                        total,
                    });
                }
                Err(error) => return Err(error),
            };
            let content = next.content.unwrap_or_default();
            if next.total != total || content.is_empty() {
                // The directory changed between pages.
                return Err(Error::Incomplete {
                    path: path.to_owned(),
                    got: entries.len(),
                    total,
                });
            }
            for wire in content {
                entries.push(wire.into_entry(path)?);
            }
        }
        if entries.len() as u64 != total {
            return Err(Error::Incomplete {
                path: path.to_owned(),
                got: entries.len(),
                total,
            });
        }
        Ok(Listing {
            path: path.to_owned(),
            entries,
            provider,
            fetched_at: Instant::now(),
        })
    }

    async fn list_page(&self, path: &str, page: u32, refresh: bool) -> Result<WireListing, Error> {
        let mut spec = self.spec(RequestClass::List, "api/fs/list", path, !refresh);
        if refresh {
            spec.timeout = self.config.refresh_timeout;
        }
        let body = ListRequest {
            path,
            password: "",
            page,
            per_page: self.config.per_page,
            refresh,
        };
        self.call(&spec, &body).await
    }

    async fn fetch_link(&self, path: &str) -> Result<Link, Error> {
        let spec = self.spec(RequestClass::Link, "api/fs/link", path, true);
        let body = PathRequest { path, password: "" };
        let wire: WireLink = self.call(&spec, &body).await?;
        wire.into_link(path, Instant::now())
    }

    async fn fetch_file(
        &self,
        path: &str,
        link: &Link,
        headers: &HeaderMap,
    ) -> Result<Transfer, Error> {
        let mut budget = WaitBudget::new(self.config.cooldown_budget);
        let permit = self.transfer_slot(&mut budget).await?;
        let mut merged = headers.clone();
        // The provider's headers win, because the link only works with them.
        for name in link.headers.keys() {
            merged.remove(name);
        }
        for (name, value) in &link.headers {
            merged.append(name, value.clone());
        }
        let response = self
            .transfer
            .get(link.url.clone())
            .headers(merged)
            .send()
            .await
            .map_err(|error| Error::transport(path, error))?;
        if let Some(delay) = self.requested_pause(&response) {
            self.admission.extend_transfer_cooldown(delay);
        }
        Ok(Transfer::new(response, permit))
    }

    /// Opens a URL that did not come from `fs/link`. See [`Client::open_url`].
    async fn open_url(&self, url: &Url, headers: &HeaderMap) -> Result<Transfer, Error> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::InvalidPath {
                path: url.path().to_owned(),
            });
        }
        let label = url.path();
        let on_server = url.origin() == self.base.origin();
        let mut budget = WaitBudget::new(self.config.cooldown_budget);
        // The transfer slot comes first, so a request waiting for one does
        // not hold an API slot that listings need.
        let permit = self.transfer_slot(&mut budget).await?;
        let mut request = self.transfer.get(url.clone()).headers(headers.clone());
        let api_permit = if on_server {
            let (token, _) = self.token(&mut budget).await?;
            // reqwest drops Authorization when a redirect leaves this host,
            // so the provider host never sees the token.
            request = request.header(AUTHORIZATION, token);
            Some(
                self.admission
                    .acquire(RequestClass::Link, &mut budget)
                    .await?,
            )
        } else {
            None
        };
        let response = request
            .send()
            .await
            .map_err(|error| Error::transport(label, error))?;
        // OpenList has answered or redirected, so the provider call it made
        // is over. The body may take minutes and must not hold an API slot.
        drop(api_permit);
        let answered_by_server = response.url().origin() == self.base.origin();
        if let Some(delay) = self.requested_pause(&response) {
            if answered_by_server {
                self.admission.extend_cooldown(delay);
            } else {
                self.admission.extend_transfer_cooldown(delay);
            }
        }
        if on_server {
            let status = response.status();
            if !answered_by_server || status.is_success() {
                self.admission.on_success(RequestClass::Link);
            } else if matches!(status.as_u16(), 500 | 502..=504 | 506..=599) {
                // As for fs/link, OpenList may relay 115's throttling as a
                // bare 5xx.
                self.admission.on_throttle(RequestClass::Link);
            }
        }
        Ok(Transfer::new(response, permit))
    }

    /// Waits for a transfer slot. Transfers honour the client-wide pause
    /// like API calls, checked again after the slot wait for the same
    /// reason as in admission.
    async fn transfer_slot(
        &self,
        budget: &mut WaitBudget,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Error> {
        self.admission.wait_transfer_cooldown(budget).await?;
        let Ok(permit) = Arc::clone(&self.transfers).acquire_owned().await else {
            unreachable!("the transfer semaphore is never closed");
        };
        self.admission.wait_transfer_cooldown(budget).await?;
        Ok(permit)
    }

    /// How long a 429 or 503 response asks the client to pause.
    fn requested_pause(&self, response: &reqwest::Response) -> Option<Duration> {
        if !matches!(response.status().as_u16(), 429 | 503) {
            return None;
        }
        let header = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok());
        Some(retry_after(header, SystemTime::now()).unwrap_or(self.config.retry_delay))
    }

    fn token_state(&self) -> MutexGuard<'_, TokenState> {
        self.token.lock().expect("token mutex poisoned")
    }

    /// Sends one API call with retries, logging in first if needed and once
    /// more if OpenList rejects the token.
    async fn call<B: Serialize, T: DeserializeOwned>(
        &self,
        spec: &CallSpec<'_>,
        body: &B,
    ) -> Result<T, Error> {
        let mut budget = WaitBudget::new(self.config.cooldown_budget);
        let attempts = if spec.retry { MAX_ATTEMPTS } else { 1 };
        let mut attempt = 1;
        let mut logged_in_again = false;
        let mut throttled = false;
        loop {
            let (token, generation) = self.token(&mut budget).await?;
            let error = match self.send(spec, Some(&token), body, &mut budget).await {
                Ok(value) => {
                    self.admission.on_success(spec.class);
                    return Ok(value);
                }
                Err(error) => error,
            };
            // Once per call: a file that keeps failing with a bare 500 must
            // not cut the rate by half on every attempt.
            if !throttled && error.signals_throttle(spec.class) {
                throttled = true;
                self.admission.on_throttle(spec.class);
                warn!(path = spec.path, endpoint = spec.endpoint, limits = ?self.admission.limits(),
                    "provider is throttling, slowing down");
            }
            // A rejected token means OpenList never ran the request, so even
            // a request that is otherwise sent once can be sent again.
            if error.is_auth_expired() && !logged_in_again {
                logged_in_again = true;
                self.login_again(generation, &mut budget).await?;
                continue;
            }
            if !error.is_retryable() || attempt >= attempts {
                return Err(error);
            }
            debug!(path = spec.path, endpoint = spec.endpoint, attempt, %error, "retrying");
            tokio::time::sleep(self.retry_delay(attempt)).await;
            attempt += 1;
        }
    }

    async fn token(&self, budget: &mut WaitBudget) -> Result<(String, u64), Error> {
        let current =
            |state: &TokenState| state.value.clone().map(|token| (token, state.generation));
        if let Some(found) = current(&self.token_state()) {
            return Ok(found);
        }
        let _login = self.login_lock.lock().await;
        if let Some(found) = current(&self.token_state()) {
            return Ok(found);
        }
        let token = self.login(budget).await?;
        let mut state = self.token_state();
        state.value = Some(token.clone());
        state.generation += 1;
        Ok((token, state.generation))
    }

    async fn login_again(&self, rejected: u64, budget: &mut WaitBudget) -> Result<(), Error> {
        let _login = self.login_lock.lock().await;
        if self.token_state().generation != rejected {
            return Ok(());
        }
        debug!("OpenList rejected the token, logging in again");
        let token = self.login(budget).await?;
        let mut state = self.token_state();
        state.value = Some(token);
        state.generation += 1;
        Ok(())
    }

    async fn login(&self, budget: &mut WaitBudget) -> Result<String, Error> {
        let (username, password) = match &self.config.auth {
            Auth::Password { username, password } => (username, password),
            Auth::Token(_) => {
                return Err(Error::Login {
                    code: 401,
                    message: "OpenList rejected the configured token".to_owned(),
                });
            }
        };
        let spec = self.spec(RequestClass::Control, "api/auth/login", "/", true);
        let body = LoginRequest { username, password };
        let mut attempt = 1;
        loop {
            match self.send::<_, LoginData>(&spec, None, &body, budget).await {
                Ok(data) => return Ok(data.token),
                Err(error) if error.is_retryable() && attempt < MAX_ATTEMPTS => {
                    tokio::time::sleep(self.retry_delay(attempt)).await;
                    attempt += 1;
                }
                Err(Error::Api { code, message, .. }) => {
                    return Err(Error::Login { code, message });
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// One HTTP exchange through admission. Parses the envelope and turns a
    /// non-200 `code` into an error.
    async fn send<B: Serialize, T: DeserializeOwned>(
        &self,
        spec: &CallSpec<'_>,
        token: Option<&str>,
        body: &B,
        budget: &mut WaitBudget,
    ) -> Result<T, Error> {
        let path = spec.path;
        let url = self
            .base
            .join(spec.endpoint)
            .map_err(|_| Error::protocol(path, "cannot build the endpoint URL"))?;
        let permit = self.admission.acquire(spec.class, budget).await?;
        let mut request = self.api.post(url).timeout(spec.timeout).json(body);
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, token);
        }
        let mut response = request
            .send()
            .await
            .map_err(|error| Error::transport(path, error))?;
        let status = response.status();
        if let Some(delay) = self.requested_pause(&response) {
            self.admission.extend_cooldown(delay);
        }
        if !status.is_success() {
            let mut prefix = Vec::new();
            while prefix.len() < ERROR_BODY_LIMIT {
                match response.chunk().await {
                    Ok(Some(chunk)) => prefix.extend_from_slice(&chunk),
                    _ => break,
                }
            }
            prefix.truncate(ERROR_BODY_LIMIT);
            return Err(Error::Http {
                status: status.as_u16(),
                path: path.to_owned(),
                body: clean_body(&prefix),
            });
        }
        // A misbehaving server or proxy must not be able to exhaust memory
        // with one endless response.
        let limit = self.config.max_response_bytes;
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(too_large(path, limit));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| Error::transport(path, error))?
        {
            if bytes.len() + chunk.len() > limit {
                return Err(too_large(path, limit));
            }
            bytes.extend_from_slice(&chunk);
        }
        drop(permit);
        let envelope: Envelope<T> = serde_json::from_slice(&bytes).map_err(|error| {
            // The message can quote the body, so only its position is kept.
            Error::protocol(
                path,
                format!(
                    "bad JSON at line {} column {}",
                    error.line(),
                    error.column()
                ),
            )
        })?;
        if envelope.code == 429 {
            // OpenList answers a login lockout inside a 200 response and
            // gives no Retry-After. Further attempts only prolong the
            // lockout, so the whole client pauses as for an HTTP 429.
            self.admission.extend_cooldown(ENVELOPE_429_PAUSE);
            return Err(Error::RateLimited);
        }
        if envelope.code != 200 {
            if envelope.message.contains(NOT_FOUND_MARKER) {
                return Err(Error::NotFound {
                    path: path.to_owned(),
                });
            }
            return Err(Error::Api {
                code: envelope.code,
                path: path.to_owned(),
                message: clean_body(envelope.message.as_bytes()),
            });
        }
        match envelope.data {
            Some(data) => Ok(data),
            // `fs/remove` and other writes answer `data: null`, which only a
            // `T` such as `IgnoredAny` accepts.
            None => serde_json::from_value(serde_json::Value::Null)
                .map_err(|_| Error::protocol(path, "response has no data")),
        }
    }

    /// Exponential backoff with full jitter, so concurrent failures do not
    /// retry in lockstep.
    fn retry_delay(&self, attempt: u32) -> Duration {
        let ceiling = self
            .config
            .retry_delay
            .saturating_mul(1 << attempt.saturating_sub(1).min(16))
            .min(MAX_RETRY_DELAY);
        let entropy = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| u64::from(d.subsec_nanos()))
            .unwrap_or(0);
        let ceiling_micros = u64::try_from(ceiling.as_micros()).unwrap_or(u64::MAX);
        Duration::from_micros(entropy % ceiling_micros.saturating_add(1))
    }
}
