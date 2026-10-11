//! A bounded in-memory cache that also merges concurrent fetches of one key.
//!
//! Every link request costs one provider call, and 115 limits them per
//! minute. Seeking in a player sends many Range requests for one file, and a
//! page of tracks asks about many files in one directory, so most requests
//! repeat a key that is cached or already being fetched.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use tokio::{task::AbortHandle, time::Instant};

use crate::Error;

/// Longer TTLs are cut to this, which also keeps `Instant` from
/// overflowing.
const MAX_TTL: Duration = Duration::from_secs(86_400);

type SharedFetch<V> = Shared<BoxFuture<'static, Result<V, Error>>>;

enum Slot<V> {
    Ready {
        value: V,
        expires_at: Instant,
    },
    Pending {
        id: u64,
        fetch: SharedFetch<V>,
        state: Arc<FetchState>,
    },
}

/// What a fetch's callers share, apart from its slot: an invalidation can
/// drop the slot while callers still wait, and the last of them must still
/// be able to abort it.
struct FetchState {
    /// Callers still waiting. Changed only under the slots lock, so a
    /// caller joining and the last one leaving cannot race.
    waiters: AtomicUsize,
    abort: AbortHandle,
}

pub(crate) struct Cache<V> {
    slots: Mutex<HashMap<String, Slot<V>>>,
    capacity: usize,
    /// Fetches allowed to be in flight at once.
    max_pending: usize,
    /// Fetches in flight, counted by the fetch tasks themselves. Slots are
    /// not counted, because an invalidation removes a slot while its fetch
    /// keeps running for the callers already waiting.
    outstanding: Arc<AtomicUsize>,
    next_id: AtomicU64,
}

/// Held by a fetch task, so the count drops when it finishes or is aborted.
struct Outstanding(Arc<AtomicUsize>);

impl Drop for Outstanding {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One caller waiting on a fetch. Dropping the last one aborts the fetch,
/// which releases its admission permit: a fetch nobody waits for must not
/// keep a place in the provider's rate-limited queue.
struct Waiter<V: Clone + Send + Sync + 'static> {
    cache: Weak<Cache<V>>,
    key: String,
    id: u64,
    state: Arc<FetchState>,
}

impl<V: Clone + Send + Sync + 'static> Drop for Waiter<V> {
    fn drop(&mut self) {
        let cache = self.cache.upgrade();
        let mut slots = cache.as_ref().map(|cache| cache.slots());
        if self.state.waiters.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        self.state.abort.abort();
        if let Some(slots) = slots.as_mut()
            && matches!(slots.get(&self.key), Some(Slot::Pending { id, .. }) if *id == self.id)
        {
            slots.remove(&self.key);
        }
    }
}

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(crate) fn new(capacity: usize, max_pending: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Mutex::new(HashMap::new()),
            capacity,
            max_pending,
            outstanding: Arc::new(AtomicUsize::new(0)),
            next_id: AtomicU64::new(0),
        })
    }

    fn slots(&self) -> MutexGuard<'_, HashMap<String, Slot<V>>> {
        self.slots.lock().expect("cache mutex poisoned")
    }

    /// Returns a cached value that has not expired.
    #[cfg(test)]
    pub(crate) fn get(&self, key: &str) -> Option<V> {
        match self.slots().get(key) {
            Some(Slot::Ready { value, expires_at }) if *expires_at > Instant::now() => {
                Some(value.clone())
            }
            _ => None,
        }
    }

    /// Returns the cached value, joins a fetch already in flight, or starts
    /// `fetch`. The fetch yields the value and how long to keep it.
    ///
    /// All callers waiting on one fetch get the same result, errors
    /// included, so a failure is not retried once per waiter. Errors are
    /// never cached. When every caller has gone, the fetch is aborted.
    ///
    /// # Errors
    /// [`Error::Busy`] when `max_pending` fetches of other keys are in
    /// flight, or the fetch's own error.
    pub(crate) async fn get_or_fetch<F>(self: &Arc<Self>, key: &str, fetch: F) -> Result<V, Error>
    where
        F: Future<Output = Result<(V, Duration), Error>> + Send + 'static,
    {
        let (shared, _waiter) = {
            let mut slots = self.slots();
            match slots.get_mut(key) {
                Some(Slot::Ready { value, expires_at }) if *expires_at > Instant::now() => {
                    return Ok(value.clone());
                }
                Some(Slot::Pending { id, fetch, state }) => {
                    state.waiters.fetch_add(1, Ordering::SeqCst);
                    (fetch.clone(), self.waiter(key, *id, state))
                }
                _ => self.start(&mut slots, key, fetch)?,
            }
        };
        shared.await
    }

    fn waiter(self: &Arc<Self>, key: &str, id: u64, state: &Arc<FetchState>) -> Waiter<V> {
        Waiter {
            cache: Arc::downgrade(self),
            key: key.to_owned(),
            id,
            state: Arc::clone(state),
        }
    }

    /// Starts `fetch` even if a value is cached or a fetch is in flight, and
    /// stores its result unless the key is invalidated meanwhile. Later
    /// callers of [`Cache::get_or_fetch`] join it.
    pub(crate) async fn fetch_fresh<F>(self: &Arc<Self>, key: &str, fetch: F) -> Result<V, Error>
    where
        F: Future<Output = Result<(V, Duration), Error>> + Send + 'static,
    {
        let (shared, _waiter) = self.start(&mut self.slots(), key, fetch)?;
        shared.await
    }

    /// Starts `fetch` as the slot's fetch, with the caller as its first
    /// waiter.
    fn start<F>(
        self: &Arc<Self>,
        slots: &mut HashMap<String, Slot<V>>,
        key: &str,
        fetch: F,
    ) -> Result<(SharedFetch<V>, Waiter<V>), Error>
    where
        F: Future<Output = Result<(V, Duration), Error>> + Send + 'static,
    {
        // Checked and taken under the slots lock, so two callers cannot both
        // take the last place.
        if self.outstanding.load(Ordering::SeqCst) >= self.max_pending {
            return Err(Error::Busy);
        }
        self.outstanding.fetch_add(1, Ordering::SeqCst);
        let outstanding = Outstanding(Arc::clone(&self.outstanding));
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let cache = Arc::downgrade(self);
        let owned_key = key.to_owned();
        // The fetch runs in its own task, so one caller being cancelled does
        // not cancel it for the others. When the last caller goes, the
        // `Waiter` aborts the task, which drops its admission permit; kept
        // in the slot unpolled instead, it would hold the permit forever.
        let task = tokio::spawn(async move {
            let _outstanding = outstanding;
            let result = fetch.await;
            finish(&cache, &owned_key, id, &result);
            result.map(|(value, _)| value)
        });
        let abort = task.abort_handle();
        let path = key.to_owned();
        let shared = async move {
            task.await
                .unwrap_or_else(|_| Err(Error::protocol(&path, "the fetch task panicked")))
        }
        .boxed()
        .shared();
        // A fetch this one replaces (a forced refresh) keeps running for
        // its own callers, and their waiters no longer find its slot.
        let state = Arc::new(FetchState {
            waiters: AtomicUsize::new(1),
            abort,
        });
        let waiter = self.waiter(key, id, &state);
        slots.insert(
            key.to_owned(),
            Slot::Pending {
                id,
                fetch: shared.clone(),
                state,
            },
        );
        Ok((shared, waiter))
    }

    /// Drops the key. A fetch in flight still answers its waiters but its
    /// result is not stored, because it may predate the invalidation.
    pub(crate) fn invalidate(&self, key: &str) {
        self.slots().remove(key);
    }

    /// Drops the key only if its cached value matches, so a value another
    /// task just stored survives.
    pub(crate) fn remove_if(&self, key: &str, matches: impl FnOnce(&V) -> bool) {
        let mut slots = self.slots();
        if let Some(Slot::Ready { value, .. }) = slots.get(key)
            && matches(value)
        {
            slots.remove(key);
        }
    }

    pub(crate) fn clear(&self) {
        self.slots().clear();
    }

    fn evict(&self, slots: &mut HashMap<String, Slot<V>>) {
        if slots.len() <= self.capacity {
            return;
        }
        let now = Instant::now();
        slots.retain(|_, slot| match slot {
            Slot::Ready { expires_at, .. } => *expires_at > now,
            Slot::Pending { .. } => true,
        });
        if slots.len() <= self.capacity {
            return;
        }
        // Dropping an eighth at once keeps the sort off every insert.
        let target = self.capacity - self.capacity / 8;
        let mut ready: Vec<(Instant, String)> = slots
            .iter()
            .filter_map(|(key, slot)| match slot {
                Slot::Ready { expires_at, .. } => Some((*expires_at, key.clone())),
                Slot::Pending { .. } => None,
            })
            .collect();
        ready.sort_unstable();
        for (_, key) in ready {
            if slots.len() <= target {
                break;
            }
            slots.remove(&key);
        }
    }
}

fn finish<V: Clone + Send + Sync + 'static>(
    cache: &Weak<Cache<V>>,
    key: &str,
    id: u64,
    result: &Result<(V, Duration), Error>,
) {
    let Some(cache) = cache.upgrade() else {
        return;
    };
    let mut slots = cache.slots();
    // Only the fetch that owns the slot may fill it. An invalidation or a
    // forced refresh in the meantime replaced or removed it.
    if !matches!(slots.get(key), Some(Slot::Pending { id: current, .. }) if *current == id) {
        return;
    }
    match result {
        Ok((value, ttl)) if !ttl.is_zero() => {
            let expires_at = Instant::now() + (*ttl).min(MAX_TTL);
            let value = value.clone();
            slots.insert(key.to_owned(), Slot::Ready { value, expires_at });
            cache.evict(&mut slots);
        }
        _ => {
            slots.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    fn counted(
        calls: &Arc<AtomicUsize>,
        value: u32,
    ) -> impl Future<Output = Result<(u32, Duration), Error>> + Send + 'static {
        let calls = Arc::clone(calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok((value, TTL))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_callers_share_one_fetch() {
        let cache = Cache::new(16, usize::MAX);
        let calls = Arc::new(AtomicUsize::new(0));
        let (a, b, c) = tokio::join!(
            cache.get_or_fetch("k", counted(&calls, 1)),
            cache.get_or_fetch("k", counted(&calls, 2)),
            cache.get_or_fetch("k", counted(&calls, 3)),
        );
        assert_eq!((a.unwrap(), b.unwrap(), c.unwrap()), (1, 1, 1));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            cache.get_or_fetch("k", counted(&calls, 4)).await.unwrap(),
            1
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn values_expire_after_their_ttl() {
        let cache = Cache::new(16, usize::MAX);
        let calls = Arc::new(AtomicUsize::new(0));
        cache.get_or_fetch("k", counted(&calls, 1)).await.unwrap();
        tokio::time::sleep(TTL).await;
        assert_eq!(cache.get("k"), None);
        assert_eq!(
            cache.get_or_fetch("k", counted(&calls, 2)).await.unwrap(),
            2
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn errors_reach_every_waiter_and_are_not_cached() {
        let cache: Arc<Cache<u32>> = Cache::new(16, usize::MAX);
        let calls = Arc::new(AtomicUsize::new(0));
        let failing = || {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                Err(Error::RateLimited)
            }
        };
        let (a, b) = tokio::join!(
            cache.get_or_fetch("k", failing()),
            cache.get_or_fetch("k", failing())
        );
        assert!(matches!(a, Err(Error::RateLimited)));
        assert!(matches!(b, Err(Error::RateLimited)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.get_or_fetch("k", failing()).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn invalidation_during_a_fetch_discards_its_result() {
        let cache = Cache::new(16, usize::MAX);
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move { cache.get_or_fetch("k", counted(&calls, 1)).await })
        };
        tokio::time::sleep(Duration::from_millis(1)).await;
        cache.invalidate("k");
        assert_eq!(
            fetch.await.unwrap().unwrap(),
            1,
            "the waiter still gets its answer"
        );
        assert_eq!(cache.get("k"), None);
    }

    async fn fixed(value: u32, ttl: Duration) -> Result<(u32, Duration), Error> {
        Ok((value, ttl))
    }

    #[tokio::test(start_paused = true)]
    async fn eviction_keeps_the_newest_entries_within_capacity() {
        let cache = Cache::new(8, usize::MAX);
        for i in 0..20u32 {
            let ttl = TTL + Duration::from_secs(u64::from(i));
            cache
                .get_or_fetch(&i.to_string(), fixed(i, ttl))
                .await
                .unwrap();
        }
        let kept: Vec<u32> = (0..20u32)
            .filter_map(|i| cache.get(&i.to_string()))
            .collect();
        assert!(kept.len() <= 8, "{kept:?}");
        assert!(kept.contains(&19));
        assert!(!kept.contains(&0));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_ttl_is_not_stored_and_huge_ttl_does_not_overflow() {
        let cache = Cache::new(8, usize::MAX);
        cache
            .get_or_fetch("k", fixed(1, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(cache.get("k"), None);
        cache
            .get_or_fetch("k", fixed(2, Duration::MAX))
            .await
            .unwrap();
        assert_eq!(cache.get("k"), Some(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_fetch_abandoned_by_every_caller_is_aborted_and_frees_its_key() {
        let cache = Cache::new(8, usize::MAX);
        let calls = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let slow = {
            let finished = Arc::clone(&finished);
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                finished.fetch_add(1, Ordering::SeqCst);
                Ok((1, TTL))
            }
        };
        let caller = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.get_or_fetch("k", slow).await })
        };
        tokio::time::sleep(Duration::from_millis(1)).await;
        caller.abort();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(finished.load(Ordering::SeqCst), 0, "the fetch was aborted");
        assert_eq!(cache.get("k"), None);
        // The key is free for the next caller, which starts its own fetch.
        assert_eq!(
            cache.get_or_fetch("k", counted(&calls, 2)).await.unwrap(),
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_fetch_keeps_running_while_any_caller_waits() {
        let cache = Cache::new(8, usize::MAX);
        let calls = Arc::new(AtomicUsize::new(0));
        let spawn = |value| {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move { cache.get_or_fetch("k", counted(&calls, value)).await })
        };
        let (first, second) = (spawn(1), spawn(2));
        tokio::time::sleep(Duration::from_millis(1)).await;
        first.abort();
        assert_eq!(second.await.unwrap().unwrap(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_invalidated_fetch_still_counts_until_it_ends() {
        let cache = Cache::new(8, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let first = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move { cache.get_or_fetch("a", counted(&calls, 1)).await })
        };
        tokio::time::sleep(Duration::from_millis(1)).await;
        cache.invalidate("a");
        assert!(matches!(
            cache.get_or_fetch("b", counted(&calls, 2)).await,
            Err(Error::Busy)
        ));
        first.await.unwrap().unwrap();
        assert_eq!(
            cache.get_or_fetch("b", counted(&calls, 2)).await.unwrap(),
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_invalidated_fetch_is_still_aborted_when_its_callers_go() {
        let cache = Cache::new(8, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let first = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move { cache.get_or_fetch("a", counted(&calls, 1)).await })
        };
        tokio::time::sleep(Duration::from_millis(1)).await;
        cache.invalidate("a");
        first.abort();
        tokio::task::yield_now().await;
        // The abandoned fetch gave its place back.
        assert_eq!(
            cache.get_or_fetch("b", counted(&calls, 2)).await.unwrap(),
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pending_fetches_of_distinct_keys_are_capped() {
        let cache = Cache::new(8, 2);
        let calls = Arc::new(AtomicUsize::new(0));
        let spawn = |key: &'static str| {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move { cache.get_or_fetch(key, counted(&calls, 1)).await })
        };
        let (a, b) = (spawn("a"), spawn("b"));
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(matches!(
            cache.get_or_fetch("c", counted(&calls, 1)).await,
            Err(Error::Busy)
        ));
        let joined = spawn("a");
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        joined.await.unwrap().unwrap();
        assert!(cache.get_or_fetch("c", counted(&calls, 1)).await.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_fetch_replaces_the_value_unless_invalidated() {
        let cache = Cache::new(8, usize::MAX);
        let calls = Arc::new(AtomicUsize::new(0));
        cache.get_or_fetch("k", counted(&calls, 1)).await.unwrap();
        assert_eq!(cache.fetch_fresh("k", counted(&calls, 2)).await.unwrap(), 2);
        assert_eq!(cache.get("k"), Some(2));

        let fresh = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move { cache.fetch_fresh("k", counted(&calls, 3)).await })
        };
        tokio::time::sleep(Duration::from_millis(1)).await;
        cache.invalidate("k");
        assert_eq!(fresh.await.unwrap().unwrap(), 3);
        assert_eq!(
            cache.get("k"),
            None,
            "a write after the refresh started wins"
        );
    }
}
