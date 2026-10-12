//! Per-class request budgets that slow down while the provider throttles.
//!
//! OpenList answers most listings from its directory cache, but every link
//! request calls the storage provider, and 115 throttles without a published
//! limit. Each class therefore has its own budget. A throttled class backs
//! off multiplicatively and recovers additively (AIMD).

use std::time::Duration;
use tokio::time::Instant;

/// The upstream budget a request draws on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestClass {
    /// `fs/list`, usually served from OpenList's cache. Limited by how many
    /// may be in flight.
    List,
    /// `fs/link` and `fs/get`, which call the provider on every request.
    /// Limited by requests per second.
    Link,
    /// `fs/remove`, which calls the provider on every request. Limited by
    /// requests per second, on a budget of its own so a cleanup does not
    /// slow down playback links.
    Mutate,
    /// Login. Never reaches the provider, so only the shared slots apply.
    Control,
}

impl RequestClass {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 4] = [Self::List, Self::Link, Self::Mutate, Self::Control];
}

/// Successes a throttled budget needs before it grows again.
pub(crate) const RECOVERY_STREAK: u32 = 20;
/// Slowest pace a throttled link budget falls to, in requests per second.
const MIN_RATE: f64 = 0.1;

/// Counts consecutive successes toward one recovery step.
#[derive(Debug, Default)]
struct Streak(u32);

impl Streak {
    /// Returns true once every `RECOVERY_STREAK` successes.
    fn advance(&mut self) -> bool {
        self.0 += 1;
        let complete = self.0 == RECOVERY_STREAK;
        if complete {
            self.0 = 0;
        }
        complete
    }

    fn reset(&mut self) {
        self.0 = 0;
    }
}

/// Spaces grants one interval apart with a burst of one.
#[derive(Debug)]
pub(crate) struct Pacer {
    configured: f64,
    rate: f64,
    /// Rate regained after each recovery streak.
    step: f64,
    next: Instant,
    streak: Streak,
}

impl Pacer {
    /// `rate` must be positive and finite, which the config check ensures.
    pub(crate) fn new(rate: f64) -> Self {
        Self {
            configured: rate,
            rate,
            // Ten steps climb back from the floor to the configured rate.
            step: (rate / 10.0).max(MIN_RATE.min(rate)),
            next: Instant::now(),
            streak: Streak::default(),
        }
    }

    pub(crate) fn rate(&self) -> f64 {
        self.rate
    }

    fn interval(&self) -> Duration {
        Duration::from_secs_f64(self.rate.recip())
    }

    /// Takes the next grant, or returns how long until one opens.
    pub(crate) fn reserve(&mut self, now: Instant) -> Result<(), Duration> {
        if now < self.next {
            return Err(self.next - now);
        }
        self.next = now + self.interval();
        Ok(())
    }

    pub(crate) fn on_success(&mut self) {
        if self.rate < self.configured && self.streak.advance() {
            self.rate = (self.rate + self.step).min(self.configured);
        }
    }

    /// Halves the rate and pushes the next grant out by the new interval.
    pub(crate) fn on_throttle(&mut self, now: Instant) {
        self.streak.reset();
        self.rate = (self.rate / 2.0).max(MIN_RATE.min(self.configured));
        self.next = self.next.max(now + self.interval());
    }
}

/// How many requests may be in flight, halved while the class is throttled.
#[derive(Debug)]
pub(crate) struct Window {
    limit: usize,
    size: usize,
    in_flight: usize,
    streak: Streak,
}

impl Window {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            size: limit,
            in_flight: 0,
            streak: Streak::default(),
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// Takes a place in the window if one is free.
    pub(crate) fn try_enter(&mut self) -> bool {
        let free = self.in_flight < self.size;
        if free {
            self.in_flight += 1;
        }
        free
    }

    pub(crate) fn leave(&mut self) {
        self.in_flight -= 1;
    }

    pub(crate) fn on_success(&mut self) {
        if self.size < self.limit && self.streak.advance() {
            self.size += 1;
        }
    }

    pub(crate) fn on_throttle(&mut self) {
        self.streak.reset();
        self.size = (self.size / 2).max(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn pacer_spaces_grants_by_the_configured_rate() {
        let mut pacer = Pacer::new(2.0);
        let start = Instant::now();
        assert_eq!(pacer.reserve(start), Ok(()));
        assert_eq!(pacer.reserve(start + 100 * MS), Err(400 * MS));
        assert_eq!(pacer.reserve(start + 500 * MS), Ok(()));
    }

    #[test]
    fn throttle_halves_the_rate_to_a_floor_and_delays_the_next_grant() {
        let mut pacer = Pacer::new(1.0);
        let start = Instant::now();
        pacer.reserve(start).unwrap();
        pacer.on_throttle(start);
        assert_eq!(pacer.interval(), 2000 * MS);
        assert_eq!(pacer.reserve(start + 1000 * MS), Err(1000 * MS));
        for _ in 0..10 {
            pacer.on_throttle(start);
        }
        assert_eq!(pacer.interval(), Duration::from_secs(10));
    }

    #[test]
    fn a_rate_below_the_floor_is_never_raised_by_throttling() {
        let mut pacer = Pacer::new(0.05);
        pacer.on_throttle(Instant::now());
        assert!((pacer.rate() - 0.05).abs() < 1e-9);
    }

    #[test]
    fn recovery_adds_one_step_per_streak_up_to_the_configured_rate() {
        let mut pacer = Pacer::new(1.0);
        pacer.on_throttle(Instant::now());
        for _ in 0..RECOVERY_STREAK - 1 {
            pacer.on_success();
        }
        assert_eq!(pacer.interval(), 2000 * MS, "one short of a streak");
        pacer.on_success();
        assert!((pacer.rate() - 0.6).abs() < 1e-9);
        for _ in 0..RECOVERY_STREAK * 20 {
            pacer.on_success();
        }
        assert_eq!(
            pacer.interval(),
            1000 * MS,
            "never above the configured rate"
        );
    }

    #[test]
    fn window_halves_on_throttle_and_regrows_one_per_streak() {
        let mut window = Window::new(4);
        window.on_throttle();
        assert_eq!(window.size(), 2);
        window.on_throttle();
        window.on_throttle();
        assert_eq!(window.size(), 1, "at least one request stays in flight");
        for _ in 0..RECOVERY_STREAK {
            window.on_success();
        }
        assert_eq!(window.size(), 2);
        for _ in 0..RECOVERY_STREAK * 10 {
            window.on_success();
        }
        assert_eq!(window.size(), 4, "never above the configured limit");
    }

    #[test]
    fn window_admits_up_to_its_size() {
        let mut window = Window::new(2);
        assert!(window.try_enter());
        assert!(window.try_enter());
        assert!(!window.try_enter());
        window.leave();
        assert!(window.try_enter());
    }
}
