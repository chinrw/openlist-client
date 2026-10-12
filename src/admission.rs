//! Request admission for one client: class budgets, shared slots and the
//! client-wide cooldown that 429 and 503 responses start.

use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use tokio::{
    sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

use crate::{
    Error,
    limit::{Pacer, RequestClass, Window},
};

/// How long one operation, retries included, may spend waiting out
/// cooldowns before it gives up with [`Error::RateLimited`].
pub(crate) struct WaitBudget {
    remaining: Duration,
}

impl WaitBudget {
    pub(crate) fn new(total: Duration) -> Self {
        Self { remaining: total }
    }
}

pub(crate) struct Admission {
    slots: Arc<Semaphore>,
    link: Mutex<Pacer>,
    /// Serializes link admission, so one link request at a time holds a
    /// slot while it waits for the pacer.
    link_gate: AsyncMutex<()>,
    mutate: Mutex<Pacer>,
    mutate_gate: AsyncMutex<()>,
    list: Mutex<Window>,
    cooldown_until: Mutex<Instant>,
    /// A pause started by the storage provider's own 429 or 503 on a file
    /// transfer. It only holds back transfers, so a provider host cannot
    /// stop OpenList API calls.
    transfer_cooldown_until: Mutex<Instant>,
    /// Wakes cooldown waiters when a pause is extended, so a call whose
    /// budget no longer covers the pause fails at once.
    cooldown_changed: Notify,
    /// Wakes list requests waiting for a window place.
    window_changed: Notify,
}

/// Holds one shared slot, and for listings one window place, until dropped.
pub(crate) struct Permit {
    _window: Option<WindowPlace>,
    _slot: OwnedSemaphorePermit,
}

/// A taken window place. Dropping it frees the place even when the acquire
/// is cancelled while waiting for a slot.
struct WindowPlace(Arc<Admission>);

impl Drop for WindowPlace {
    fn drop(&mut self) {
        self.0.window().leave();
        self.0.window_changed.notify_waiters();
    }
}

/// A snapshot of the adaptive limits, for logs and status pages.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limits {
    /// Current link requests per second, below the configured rate while
    /// the provider throttles.
    pub link_rate: f64,
    /// Current `fs/remove` requests per second.
    pub mutate_rate: f64,
    /// Current number of listings allowed in flight.
    pub list_window: usize,
    /// Time left in a client-wide pause after a 429 or 503.
    pub cooldown: Duration,
}

impl Admission {
    pub(crate) fn new(
        slots: usize,
        list_concurrency: usize,
        link_rate: f64,
        mutate_rate: f64,
    ) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(slots)),
            link: Mutex::new(Pacer::new(link_rate)),
            link_gate: AsyncMutex::new(()),
            mutate: Mutex::new(Pacer::new(mutate_rate)),
            mutate_gate: AsyncMutex::new(()),
            list: Mutex::new(Window::new(list_concurrency)),
            cooldown_until: Mutex::new(Instant::now()),
            transfer_cooldown_until: Mutex::new(Instant::now()),
            cooldown_changed: Notify::new(),
            window_changed: Notify::new(),
        }
    }

    fn window(&self) -> MutexGuard<'_, Window> {
        self.list.lock().expect("window mutex poisoned")
    }

    fn pacer(&self, class: RequestClass) -> MutexGuard<'_, Pacer> {
        let pacer = match class {
            RequestClass::Mutate => &self.mutate,
            _ => &self.link,
        };
        pacer.lock().expect("pacer mutex poisoned")
    }

    fn cooldown_left(&self) -> Duration {
        let until = *self.cooldown_until.lock().expect("cooldown mutex poisoned");
        until.saturating_duration_since(Instant::now())
    }

    pub(crate) fn limits(&self) -> Limits {
        Limits {
            link_rate: self.pacer(RequestClass::Link).rate(),
            mutate_rate: self.pacer(RequestClass::Mutate).rate(),
            list_window: self.window().size(),
            cooldown: self.cooldown_left(),
        }
    }

    /// Waits out any cooldown, then the class budget and a shared slot. A
    /// cooldown that starts while the request is queued sends it back to
    /// wait, so queued requests do not fire into a 429 pause.
    pub(crate) async fn acquire(
        self: &Arc<Self>,
        class: RequestClass,
        budget: &mut WaitBudget,
    ) -> Result<Permit, Error> {
        loop {
            self.wait_cooldown(budget).await?;
            let permit = match class {
                RequestClass::Link | RequestClass::Mutate => {
                    let gate = match class {
                        RequestClass::Mutate => &self.mutate_gate,
                        _ => &self.link_gate,
                    };
                    // The slot is taken before the pacer grant, and both under
                    // the gate, so grants cannot pile up while slots are full
                    // and then fire together.
                    let _gate = gate.lock().await;
                    let slot = self.slot().await;
                    loop {
                        let reserved = self.pacer(class).reserve(Instant::now());
                        match reserved {
                            Ok(()) => break,
                            Err(wait) => tokio::time::sleep(wait).await,
                        }
                    }
                    Permit {
                        _window: None,
                        _slot: slot,
                    }
                }
                RequestClass::List => {
                    let window = self.window_place().await;
                    Permit {
                        _window: Some(window),
                        _slot: self.slot().await,
                    }
                }
                RequestClass::Control => Permit {
                    _window: None,
                    _slot: self.slot().await,
                },
            };
            if self.cooldown_left().is_zero() {
                return Ok(permit);
            }
        }
    }

    async fn slot(&self) -> OwnedSemaphorePermit {
        let Ok(slot) = Arc::clone(&self.slots).acquire_owned().await else {
            unreachable!("the slot semaphore is never closed");
        };
        slot
    }

    async fn window_place(self: &Arc<Self>) -> WindowPlace {
        loop {
            let changed = self.window_changed.notified();
            tokio::pin!(changed);
            // Registered before the check so a release in between is not
            // missed.
            changed.as_mut().enable();
            if self.window().try_enter() {
                return WindowPlace(Arc::clone(self));
            }
            changed.await;
        }
    }

    async fn wait_cooldown(&self, budget: &mut WaitBudget) -> Result<(), Error> {
        wait_out(budget, &self.cooldown_changed, || self.cooldown_left()).await
    }

    /// Time left in a pause a provider host started.
    pub(crate) fn transfer_cooldown_left(&self) -> Duration {
        let until = *self
            .transfer_cooldown_until
            .lock()
            .expect("cooldown mutex poisoned");
        until.saturating_duration_since(Instant::now())
    }

    /// Waits out both the client-wide pause and the transfer pause.
    pub(crate) async fn wait_transfer_cooldown(
        &self,
        budget: &mut WaitBudget,
    ) -> Result<(), Error> {
        wait_out(budget, &self.cooldown_changed, || {
            self.cooldown_left().max(self.transfer_cooldown_left())
        })
        .await
    }

    /// Pauses every new request of this client for `delay`.
    pub(crate) fn extend_cooldown(&self, delay: Duration) {
        // Anything past a day fails every budget anyway, and the cap keeps
        // the deadline from overflowing `Instant`.
        extend(&self.cooldown_until, delay.min(Duration::from_secs(86_400)));
        self.cooldown_changed.notify_waiters();
    }

    /// Pauses new transfers for `delay`, at most five minutes, because the
    /// value comes from a provider host rather than from OpenList.
    pub(crate) fn extend_transfer_cooldown(&self, delay: Duration) {
        extend(&self.transfer_cooldown_until, delay.min(MAX_TRANSFER_PAUSE));
        self.cooldown_changed.notify_waiters();
    }

    pub(crate) fn on_success(&self, class: RequestClass) {
        match class {
            RequestClass::Link | RequestClass::Mutate => self.pacer(class).on_success(),
            RequestClass::List => self.window().on_success(),
            RequestClass::Control => {}
        }
    }

    pub(crate) fn on_throttle(&self, class: RequestClass) {
        match class {
            RequestClass::Link | RequestClass::Mutate => {
                self.pacer(class).on_throttle(Instant::now());
            }
            RequestClass::List => self.window().on_throttle(),
            RequestClass::Control => {}
        }
    }
}

const MAX_TRANSFER_PAUSE: Duration = Duration::from_secs(300);

async fn wait_out(
    budget: &mut WaitBudget,
    changed: &Notify,
    left: impl Fn() -> Duration,
) -> Result<(), Error> {
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        // Registered before reading the deadline so an extension in between
        // still wakes this waiter.
        notified.as_mut().enable();
        let wait = left();
        if wait.is_zero() {
            return Ok(());
        }
        if wait > budget.remaining {
            return Err(Error::RateLimited);
        }
        let started = Instant::now();
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = notified => {}
        }
        budget.remaining = budget.remaining.saturating_sub(started.elapsed());
    }
}

fn extend(until: &Mutex<Instant>, delay: Duration) {
    let mut until = until.lock().expect("cooldown mutex poisoned");
    *until = (*until).max(Instant::now() + delay);
}

/// Parses `Retry-After` as seconds or an HTTP date.
pub(crate) fn retry_after(value: Option<&str>, now: SystemTime) -> Option<Duration> {
    let value = value?.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        // An overflowing number asks for more than any budget, so it must not
        // turn into an early retry.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|date| date.duration_since(now).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cooldown_beyond_the_budget_fails_without_waiting() {
        let admission = Arc::new(Admission::new(4, 4, 1.0, 1.0));
        admission.extend_cooldown(Duration::from_secs(30));
        let mut budget = WaitBudget::new(Duration::from_secs(10));
        let start = Instant::now();
        let result = admission.acquire(RequestClass::List, &mut budget).await;
        assert!(matches!(result, Err(Error::RateLimited)));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn cooldown_within_the_budget_is_waited_out_and_charged() {
        let admission = Arc::new(Admission::new(4, 4, 1.0, 1.0));
        admission.extend_cooldown(Duration::from_secs(3));
        let mut budget = WaitBudget::new(Duration::from_secs(10));
        let start = Instant::now();
        let _permit = admission
            .acquire(RequestClass::Control, &mut budget)
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(3));
        assert_eq!(budget.remaining, Duration::from_secs(7));
    }

    #[tokio::test(start_paused = true)]
    async fn link_grants_follow_the_rate() {
        let admission = Arc::new(Admission::new(8, 4, 2.0, 1.0));
        let mut budget = WaitBudget::new(Duration::ZERO);
        let start = Instant::now();
        for _ in 0..5 {
            drop(
                admission
                    .acquire(RequestClass::Link, &mut budget)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn removals_are_paced_apart_from_links() {
        let admission = Arc::new(Admission::new(8, 4, 1.0, 0.5));
        let mut budget = WaitBudget::new(Duration::ZERO);
        let start = Instant::now();
        for class in [RequestClass::Mutate, RequestClass::Link] {
            drop(admission.acquire(class, &mut budget).await.unwrap());
        }
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "each class has its own grant"
        );
        drop(
            admission
                .acquire(RequestClass::Mutate, &mut budget)
                .await
                .unwrap(),
        );
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        admission.on_throttle(RequestClass::Mutate);
        assert_eq!(admission.limits().mutate_rate, 0.25);
        assert_eq!(admission.limits().link_rate, 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn list_window_blocks_until_a_place_frees() {
        let admission = Arc::new(Admission::new(8, 2, 1.0, 1.0));
        let mut budget = WaitBudget::new(Duration::ZERO);
        let first = admission
            .acquire(RequestClass::List, &mut budget)
            .await
            .unwrap();
        let _second = admission
            .acquire(RequestClass::List, &mut budget)
            .await
            .unwrap();
        let waiter = {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move {
                let mut budget = WaitBudget::new(Duration::ZERO);
                admission
                    .acquire(RequestClass::List, &mut budget)
                    .await
                    .map(|_| ())
            })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!waiter.is_finished());
        drop(first);
        waiter.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_cooldown_that_starts_while_queued_is_waited_out() {
        let admission = Arc::new(Admission::new(8, 1, 1.0, 1.0));
        let mut budget = WaitBudget::new(Duration::from_secs(60));
        let first = admission
            .acquire(RequestClass::List, &mut budget)
            .await
            .unwrap();
        let queued = {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move {
                let mut budget = WaitBudget::new(Duration::from_secs(60));
                admission
                    .acquire(RequestClass::List, &mut budget)
                    .await
                    .map(|_| Instant::now())
            })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        let start = Instant::now();
        admission.extend_cooldown(Duration::from_secs(30));
        drop(first);
        let admitted = queued.await.unwrap().unwrap();
        assert!(admitted >= start + Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn an_extension_beyond_the_budget_fails_a_waiting_call_at_once() {
        let admission = Arc::new(Admission::new(4, 4, 1.0, 1.0));
        admission.extend_cooldown(Duration::from_secs(119));
        let waiter = {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move {
                let mut budget = WaitBudget::new(Duration::from_secs(120));
                let result = admission.acquire(RequestClass::List, &mut budget).await;
                (result.map(|_| ()), Instant::now())
            })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        let extended_at = Instant::now();
        admission.extend_cooldown(Duration::from_secs(3600));
        let (result, failed_at) = waiter.await.unwrap();
        assert!(matches!(result, Err(Error::RateLimited)));
        assert_eq!(failed_at, extended_at);
    }

    #[tokio::test(start_paused = true)]
    async fn a_provider_pause_holds_back_transfers_but_not_api_calls() {
        let admission = Arc::new(Admission::new(4, 4, 1.0, 1.0));
        admission.extend_transfer_cooldown(Duration::from_secs(86_400));
        let mut budget = WaitBudget::new(Duration::from_secs(10));
        admission
            .acquire(RequestClass::List, &mut budget)
            .await
            .unwrap();
        let result = admission.wait_transfer_cooldown(&mut budget).await;
        assert!(matches!(result, Err(Error::RateLimited)));

        let mut budget = WaitBudget::new(Duration::from_secs(600));
        let start = Instant::now();
        admission.wait_transfer_cooldown(&mut budget).await.unwrap();
        assert_eq!(
            start.elapsed(),
            MAX_TRANSFER_PAUSE,
            "capped at five minutes"
        );
    }

    #[test]
    fn retry_after_accepts_seconds_and_dates() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        assert_eq!(retry_after(Some(" 7 "), now), Some(Duration::from_secs(7)));
        let date = httpdate::fmt_http_date(now + Duration::from_secs(30));
        assert_eq!(retry_after(Some(&date), now), Some(Duration::from_secs(30)));
        assert_eq!(retry_after(Some("soon"), now), None);
        assert_eq!(retry_after(None, now), None);
        assert_eq!(
            retry_after(Some("99999999999999999999999"), now),
            Some(Duration::from_secs(u64::MAX))
        );
    }
}
