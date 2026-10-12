mod common;

use std::{sync::atomic::Ordering, time::Duration};

use common::{Fault, USER, dir, file, start};
use openlist_client::{Auth, Client, Error, ErrorKind};

#[tokio::test]
async fn list_reads_every_page() {
    let server = start().await;
    let names: Vec<_> = (0..5).map(|i| file(&format!("{i:02}.mp3"), i)).collect();
    server.fake.put("/w", names);
    let mut config = server.config();
    config.per_page = 2;
    let client = Client::new(config).unwrap();

    let listing = client.list("/w").await.unwrap();
    let got: Vec<_> = listing.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(got, ["00.mp3", "01.mp3", "02.mp3", "03.mp3", "04.mp3"]);
    assert_eq!(server.fake.hits("list", "/w"), 3);
    assert!(listing.entries[0].modified.is_some());
}

#[tokio::test]
async fn a_failed_later_page_fails_the_whole_listing() {
    let server = start().await;
    server
        .fake
        .put("/w", (0..4).map(|i| file(&format!("{i}"), 1)).collect());
    *server.fake.fail_page.lock().unwrap() = Some(("/w".into(), 2));
    let mut config = server.config();
    config.per_page = 2;
    let client = Client::new(config).unwrap();

    let error = client.list("/w").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable, "{error:?}");
    // Page 1 once, then page 2 three times.
    assert_eq!(server.fake.hits("list", "/w"), 4);
}

#[tokio::test]
async fn a_directory_that_changes_between_pages_is_incomplete() {
    let server = start().await;
    server
        .fake
        .put("/w", (0..3).map(|i| file(&format!("{i}"), 1)).collect());
    *server.fake.grow_after_first_page.lock().unwrap() = Some("/w".into());
    let mut config = server.config();
    config.per_page = 2;
    let client = Client::new(config).unwrap();

    let error = client.list("/w").await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Incomplete {
                got: 2,
                total: 3,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test]
async fn missing_directory_is_not_found() {
    let server = start().await;
    let client = server.client();
    let error = client.list("/nope").await.unwrap_err();
    assert!(matches!(error, Error::NotFound { .. }), "{error:?}");
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert_eq!(
        server.fake.hits("list", "/nope"),
        1,
        "not found is not retried"
    );
}

#[tokio::test]
async fn business_errors_are_upstream_and_not_retried() {
    let server = start().await;
    server.fake.put("/w", vec![]);
    server.fake.fault(
        "list",
        "/w",
        Fault::Code(403, "password is incorrect or you have no permission"),
    );
    let client = server.client();
    let error = client.list("/w").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Upstream, "{error:?}");
    assert_eq!(server.fake.hits("list", "/w"), 1);
}

#[tokio::test]
async fn transient_errors_are_retried_up_to_three_times() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    server
        .fake
        .fault("list", "/w", Fault::Code(500, "upstream hiccup"));
    server.fake.fault("list", "/w", Fault::Status(502, None));
    let client = server.client();
    assert_eq!(client.list("/w").await.unwrap().entries.len(), 1);
    assert_eq!(server.fake.hits("list", "/w"), 3);

    for _ in 0..3 {
        server
            .fake
            .fault("list", "/x", Fault::Code(500, "upstream hiccup"));
    }
    server.fake.put("/x", vec![]);
    assert!(client.list("/x").await.is_err());
    assert_eq!(server.fake.hits("list", "/x"), 3);
}

#[tokio::test]
async fn an_expired_token_triggers_exactly_one_new_login() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    let client = server.client();
    client.list("/w").await.unwrap();
    assert_eq!(server.fake.logins(), 1);

    server.fake.expire_tokens();
    client.invalidate_listing("/w");
    client.list("/w").await.unwrap();
    assert_eq!(server.fake.logins(), 2);

    // A token rejected even after the new login is not chased forever.
    server
        .fake
        .fault("list", "/w", Fault::Code(401, "token is invalidated"));
    server
        .fake
        .fault("list", "/w", Fault::Code(401, "token is invalidated"));
    client.invalidate_listing("/w");
    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::Api { code: 401, .. }), "{error:?}");
    assert_eq!(server.fake.logins(), 3);
}

#[tokio::test]
async fn a_wrong_password_is_a_login_error() {
    let server = start().await;
    let mut config = server.config();
    config.auth = Auth::Password {
        username: USER.into(),
        password: "wrong".into(),
    };
    let client = Client::new(config).unwrap();
    let error = client.list("/").await.unwrap_err();
    assert!(matches!(error, Error::Login { code: 400, .. }), "{error:?}");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test]
async fn a_fixed_token_is_sent_without_logging_in() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    server.fake.accept_token("settings-token");
    let client = Client::new(server.token_config("settings-token")).unwrap();
    let listing = client.list("/w").await.unwrap();
    assert_eq!(listing.entries.len(), 1);
    assert_eq!(server.fake.logins(), 0);
}

#[tokio::test]
async fn a_rejected_fixed_token_is_a_login_error_without_a_login() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    let client = Client::new(server.token_config("revoked-token")).unwrap();
    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::Login { code: 401, .. }), "{error:?}");
    assert_eq!(server.fake.logins(), 0);
    // The rejection is not retried: one request, then the error.
    assert_eq!(server.fake.hits("list", "/w"), 1);
}

#[tokio::test]
async fn listings_are_served_from_memory_until_their_ttl() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    let mut config = server.config();
    config.listing_ttl = Duration::from_millis(300);
    let client = Client::new(config).unwrap();

    client.list("/w").await.unwrap();
    client.list("/w/").await.unwrap();
    assert_eq!(server.fake.hits("list", "/w"), 1);
    tokio::time::sleep(Duration::from_millis(350)).await;
    client.list("/w").await.unwrap();
    assert_eq!(server.fake.hits("list", "/w"), 2);
}

#[tokio::test]
async fn concurrent_listings_of_one_directory_share_a_request() {
    let server = start().await;
    server.fake.put("/", vec![dir("w")]);
    server.fake.put("/w", vec![file("a", 1)]);
    let client = server.client();
    // Log in first so the race is only between listings.
    client.list("/").await.unwrap();
    *server.fake.list_delay.lock().unwrap() = Duration::from_millis(100);
    let calls = (0..8).map(|_| client.list("/w"));
    for result in futures_util::future::join_all(calls).await {
        result.unwrap();
    }
    assert_eq!(server.fake.hits("list", "/w"), 1);
}

#[tokio::test]
async fn refresh_is_sent_once_on_the_first_page_and_updates_the_cache() {
    let server = start().await;
    server
        .fake
        .put("/w", (0..3).map(|i| file(&format!("{i}"), 1)).collect());
    let mut config = server.config();
    config.per_page = 2;
    let client = Client::new(config).unwrap();

    client.list("/w").await.unwrap();
    server
        .fake
        .put("/w", (0..4).map(|i| file(&format!("{i}"), 1)).collect());
    let refreshed = client.refresh("/w").await.unwrap();
    assert_eq!(refreshed.entries.len(), 4);
    assert_eq!(
        client.list("/w").await.unwrap().entries.len(),
        4,
        "cache updated"
    );

    let flags = server.fake.refresh_flags.lock().unwrap().clone();
    let refreshes: Vec<_> = flags.iter().filter(|(_, _, refresh)| *refresh).collect();
    assert_eq!(refreshes, [&("/w".to_owned(), 1, true)]);

    server
        .fake
        .fault("list", "/w", Fault::Code(500, "provider busy"));
    assert!(client.refresh("/w").await.is_err());
    let after = server.fake.refresh_flags.lock().unwrap().len();
    assert_eq!(after, flags.len(), "a failed refresh is not retried");
}

#[tokio::test]
async fn stat_uses_the_parent_listing_and_rechecks_a_miss_once() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 7)]);
    let mut config = server.config();
    config.stat_recheck_age = Duration::from_millis(100);
    let client = Client::new(config).unwrap();
    assert_eq!(client.stat("/w/a.mp3").await.unwrap().size, 7);
    assert_eq!(
        server.fake.hits("get", "/w/a.mp3"),
        0,
        "no fs/get, so no provider call"
    );

    server
        .fake
        .put("/w", vec![file("a.mp3", 7), file("b.mp3", 9)]);
    let error = client.stat("/w/b.mp3").await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::NotFound,
        "a fresh listing is trusted"
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        client.stat("/w/b.mp3").await.unwrap().size,
        9,
        "found after recheck"
    );
    assert_eq!(server.fake.hits("list", "/w"), 2);
    for _ in 0..20 {
        let error = client.stat("/w/c.mp3").await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }
    assert_eq!(
        server.fake.hits("list", "/w"),
        2,
        "misses on a fresh listing cost nothing"
    );
    assert!(client.stat("/").await.unwrap().is_dir);
}

#[tokio::test]
async fn walk_lists_the_tree_with_bounded_concurrency() {
    let server = start().await;
    let works: Vec<_> = (0..12).map(|i| dir(&format!("RJ{i:06}"))).collect();
    server.fake.put("/lib", works);
    for i in 0..12 {
        let work = format!("/lib/RJ{i:06}");
        server
            .fake
            .put(&work, vec![dir("mp3"), file("cover.jpg", 1)]);
        server
            .fake
            .put(&format!("{work}/mp3"), vec![file("01.mp3", 2)]);
    }
    *server.fake.list_delay.lock().unwrap() = Duration::from_millis(20);
    let mut config = server.config();
    config.list_concurrency = 3;
    let client = Client::new(config).unwrap();

    let found = client.walk("/lib").await.unwrap();
    assert_eq!(found.len(), 12 * 4);
    assert_eq!(found[0].path, "/lib/RJ000000");
    assert!(found.iter().any(|f| f.path == "/lib/RJ000011/mp3/01.mp3"));
    assert!(server.fake.list_peak.load(Ordering::SeqCst) <= 3);
    for i in 0..12 {
        assert_eq!(
            server
                .fake
                .hits("link", &format!("/lib/RJ{i:06}/mp3/01.mp3")),
            0
        );
    }
}

#[tokio::test]
async fn walk_fails_as_a_whole_when_one_directory_fails() {
    let server = start().await;
    server.fake.put("/lib", vec![dir("a"), dir("b")]);
    server.fake.put("/lib/a", vec![file("1", 1)]);
    server.fake.put("/lib/b", vec![file("1", 1)]);
    for _ in 0..3 {
        server
            .fake
            .fault("list", "/lib/b", Fault::Status(500, None));
    }
    let client = server.client();
    let error = client.walk("/lib").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test]
async fn digest_lets_a_second_scan_skip_an_unchanged_work() {
    let server = start().await;
    server
        .fake
        .put("/w", vec![dir("mp3"), file("cover.jpg", 1)]);
    server.fake.put("/w/mp3", vec![file("01.mp3", 2)]);
    let mut config = server.config();
    config.listing_ttl = Duration::ZERO;
    let client = Client::new(config).unwrap();

    let first = client.list("/w").await.unwrap();
    let stored = first.digest();
    client.walk_from(first).await.unwrap();
    assert_eq!(server.fake.hits("list", "/w/mp3"), 1);

    let second = client.list("/w").await.unwrap();
    assert_eq!(
        second.digest(),
        stored,
        "unchanged, so the caller skips the walk"
    );

    server
        .fake
        .put("/w", vec![dir("mp3"), file("cover.jpg", 2)]);
    assert_ne!(client.list("/w").await.unwrap().digest(), stored);
}

#[tokio::test]
async fn an_oversized_response_is_refused() {
    let server = start().await;
    server.fake.put(
        "/w",
        (0..50).map(|i| file(&format!("{i:03}.mp3"), 1)).collect(),
    );
    let mut config = server.config();
    config.max_response_bytes = 1024;
    let client = Client::new(config).unwrap();
    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::Protocol { .. }), "{error:?}");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test]
async fn a_directory_that_vanishes_after_page_one_is_incomplete_not_missing() {
    let server = start().await;
    server
        .fake
        .put("/w", (0..4).map(|i| file(&format!("{i}"), 1)).collect());
    *server.fake.vanish_after_first_page.lock().unwrap() = Some("/w".into());
    let mut config = server.config();
    config.per_page = 2;
    let client = Client::new(config).unwrap();
    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::Incomplete { .. }), "{error:?}");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test]
async fn an_unaddressable_name_fails_the_listing() {
    let server = start().await;
    server
        .fake
        .put("/w", vec![file("ok.mp3", 1), file("..\\escape", 1)]);
    let client = server.client();
    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::InvalidPath { .. }), "{error:?}");
    assert_eq!(error.kind(), ErrorKind::Upstream);
}

#[tokio::test]
async fn a_login_lockout_pauses_the_client_instead_of_retrying() {
    let server = start().await;
    server.fake.put("/w", vec![]);
    server.fake.fault(
        "login",
        "/",
        Fault::Code(429, "Too many unsuccessful sign-in attempts have been made using an incorrect username or password, Try again later."),
    );
    let mut config = server.config();
    config.cooldown_budget = Duration::from_secs(10);
    let client = Client::new(config).unwrap();

    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::RateLimited), "{error:?}");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(client.limits().cooldown > Duration::from_secs(50));

    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::RateLimited), "{error:?}");
    assert_eq!(server.fake.hits("login", "/"), 1, "no login while paused");
}

#[tokio::test]
async fn a_pause_beyond_the_budget_on_the_last_attempt_is_rate_limited() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    for fault in [
        Fault::Status(500, None),
        Fault::Status(500, None),
        Fault::Status(503, Some("301")),
    ] {
        server.fake.fault("list", "/w", fault);
    }
    let client = server.client();
    let error = client.list("/w").await.unwrap_err();
    assert!(matches!(error, Error::RateLimited), "{error:?}");
    assert_eq!(server.fake.hits("list", "/w"), 3);
}
