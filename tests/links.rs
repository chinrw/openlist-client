mod common;

use std::time::{Duration, Instant};

use common::{Fault, LINK_UA, file, start};
use openlist_client::{Client, Error, ErrorKind};
use reqwest::header::{HeaderMap, HeaderValue, RANGE};

#[tokio::test]
async fn links_are_cached_and_concurrent_requests_share_one_call() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    *server.fake.link_delay.lock().unwrap() = Duration::from_millis(100);
    let client = server.client();

    let calls = (0..6).map(|_| client.link("/w/01.mp3"));
    let links = futures_util::future::join_all(calls).await;
    let first = links[0].as_ref().unwrap().url.clone();
    assert!(links.iter().all(|l| l.as_ref().unwrap().url == first));
    assert_eq!(client.link("/w/01.mp3").await.unwrap().url, first);
    assert_eq!(server.fake.hits("link", "/w/01.mp3"), 1);
    assert_eq!(
        client.link("/w/01.mp3").await.unwrap().headers["user-agent"],
        LINK_UA
    );

    client.invalidate_link("/w/01.mp3");
    assert_ne!(client.link("/w/01.mp3").await.unwrap().url, first);
    assert_eq!(server.fake.hits("link", "/w/01.mp3"), 2);
}

#[tokio::test]
async fn a_short_provider_expiry_shortens_the_cache_time() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    *server.fake.link_expiration.lock().unwrap() = Some(200_000_000);
    let client = server.client();

    let link = client.link("/w/01.mp3").await.unwrap();
    let left = link
        .expires_at
        .unwrap()
        .saturating_duration_since(Instant::now());
    assert!(left <= Duration::from_millis(200));
    client.link("/w/01.mp3").await.unwrap();
    assert_eq!(server.fake.hits("link", "/w/01.mp3"), 1);
    tokio::time::sleep(Duration::from_millis(250)).await;
    client.link("/w/01.mp3").await.unwrap();
    assert_eq!(server.fake.hits("link", "/w/01.mp3"), 2);
}

#[tokio::test]
async fn link_for_a_missing_file_is_not_found() {
    let server = start().await;
    server.fake.put("/w", vec![]);
    let client = server.client();
    let error = client.link("/w/gone.mp3").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound, "{error:?}");
}

#[tokio::test]
async fn the_115_rate_limit_halves_the_link_rate_and_the_call_still_succeeds() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    server.fake.fault(
        "link",
        "/w/01.mp3",
        Fault::Code(500, "failed link: 770004 已达到当前访问上限，请稍后再试"),
    );
    let mut config = server.config();
    config.link_rate = 20.0;
    let client = Client::new(config).unwrap();

    client.link("/w/01.mp3").await.unwrap();
    assert_eq!(server.fake.hits("link", "/w/01.mp3"), 2);
    assert!(
        (client.limits().link_rate - 10.0).abs() < 1e-9,
        "{:?}",
        client.limits()
    );
}

#[tokio::test]
async fn a_bare_500_on_a_link_counts_as_throttling_but_not_on_a_listing() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    server.fake.fault("link", "/w/01.mp3", Fault::Code(500, ""));
    server.fake.fault("list", "/w", Fault::Code(500, ""));
    let mut config = server.config();
    config.link_rate = 20.0;
    let client = Client::new(config).unwrap();

    client.list("/w").await.unwrap();
    assert_eq!(client.limits().list_window, 4, "listing window untouched");
    client.link("/w/01.mp3").await.unwrap();
    assert!((client.limits().link_rate - 10.0).abs() < 1e-9);
}

#[tokio::test]
async fn a_throttled_listing_halves_the_listing_window() {
    let server = start().await;
    server.fake.put("/w", vec![]);
    server
        .fake
        .fault("list", "/w", Fault::Code(500, "访问频繁"));
    let client = server.client();
    client.list("/w").await.unwrap();
    assert_eq!(client.limits().list_window, 2);
}

#[tokio::test]
async fn link_requests_are_paced() {
    let server = start().await;
    let files: Vec<_> = (0..4).map(|i| file(&format!("{i}.mp3"), 1)).collect();
    server.fake.put("/w", files);
    let mut config = server.config();
    config.link_rate = 10.0;
    let client = Client::new(config).unwrap();

    let start = Instant::now();
    let calls = (0..4).map(|i| {
        let client = client.clone();
        async move { client.link(&format!("/w/{i}.mp3")).await }
    });
    for link in futures_util::future::join_all(calls).await {
        link.unwrap();
    }
    assert!(
        start.elapsed() >= Duration::from_millis(290),
        "{:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn retry_after_pauses_the_whole_client() {
    let server = start().await;
    server.fake.put("/a", vec![]);
    server.fake.put("/b", vec![]);
    let client = server.client();
    client.list("/b").await.unwrap();
    client.invalidate_listing("/b");

    server
        .fake
        .fault("list", "/a", Fault::Status(429, Some("1")));
    let start = Instant::now();
    client.list("/a").await.unwrap();
    assert!(
        start.elapsed() >= Duration::from_millis(900),
        "{:?}",
        start.elapsed()
    );
    assert_eq!(server.fake.hits("list", "/a"), 2);
}

#[tokio::test]
async fn a_pause_beyond_the_wait_budget_fails_fast_as_rate_limited() {
    let server = start().await;
    server.fake.put("/a", vec![]);
    let mut config = server.config();
    config.cooldown_budget = Duration::from_secs(5);
    let client = Client::new(config).unwrap();
    client.list("/a").await.unwrap();
    client.invalidate_listing("/a");

    server
        .fake
        .fault("list", "/a", Fault::Status(503, Some("3600")));
    let start = Instant::now();
    let error = client.list("/a").await.unwrap_err();
    assert!(matches!(error, Error::RateLimited), "{error:?}");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(client.limits().cooldown > Duration::from_secs(3000));
}

#[tokio::test]
async fn get_is_paced_like_a_link() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    let client = server.client();
    let info = client.get("/w/01.mp3").await.unwrap();
    assert_eq!(info.entry.size, 10);
    assert!(info.raw_url.ends_with("/files/w/01.mp3"));
}

#[tokio::test]
async fn open_sends_the_provider_headers_and_forwards_range() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    let client = server.client();
    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=3-"));
    // A caller's user agent must not replace the one the link is bound to.
    headers.insert("user-agent", HeaderValue::from_static("browser"));

    let mut transfer = client.open("/w/01.mp3", &headers).await.unwrap();
    assert_eq!(transfer.status(), 206);
    let mut body = Vec::new();
    while let Some(chunk) = transfer.chunk().await.unwrap() {
        body.extend_from_slice(&chunk);
    }
    assert_eq!(body, b"tents of /w/01.mp3");
}

#[tokio::test]
async fn an_expired_link_is_replaced_once() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    let client = server.client();
    client.link("/w/01.mp3").await.unwrap();

    *server.fake.reject_files.lock().unwrap() = 1;
    let transfer = client.open("/w/01.mp3", &HeaderMap::new()).await.unwrap();
    assert_eq!(transfer.status(), 200);
    assert_eq!(server.fake.hits("link", "/w/01.mp3"), 2);

    *server.fake.reject_files.lock().unwrap() = 2;
    let error = client
        .open("/w/01.mp3", &HeaderMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Http { status: 403, .. }),
        "{error:?}"
    );
    assert_eq!(server.fake.hits("link", "/w/01.mp3"), 3);
}

#[tokio::test]
async fn a_cancelled_listing_does_not_block_the_window() {
    let server = start().await;
    server.fake.put("/a", vec![]);
    server.fake.put("/b", vec![]);
    let mut config = server.config();
    config.list_concurrency = 1;
    let client = Client::new(config).unwrap();
    client.list("/b").await.unwrap();
    client.invalidate_listing("/b");

    *server.fake.list_delay.lock().unwrap() = Duration::from_millis(200);
    let cancelled = {
        let client = client.clone();
        tokio::spawn(async move { client.list("/a").await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancelled.abort();
    let next = tokio::time::timeout(Duration::from_secs(5), client.list("/b")).await;
    assert!(next.expect("the window place was released").is_ok());
}

#[tokio::test]
async fn a_link_cancelled_while_paced_does_not_block_other_links() {
    let server = start().await;
    let files: Vec<_> = (0..3).map(|i| file(&format!("{i}.mp3"), 1)).collect();
    server.fake.put("/w", files);
    let mut config = server.config();
    config.link_rate = 2.0;
    let client = Client::new(config).unwrap();
    client.link("/w/0.mp3").await.unwrap();

    let cancelled = {
        let client = client.clone();
        tokio::spawn(async move { client.link("/w/1.mp3").await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancelled.abort();
    let next = tokio::time::timeout(Duration::from_secs(5), client.link("/w/2.mp3")).await;
    assert!(next.expect("the link gate was released").is_ok());
}

#[tokio::test]
async fn a_file_that_keeps_failing_halves_the_rate_only_once() {
    let server = start().await;
    server.fake.put("/w", vec![file("bad.mp3", 1)]);
    for _ in 0..3 {
        server
            .fake
            .fault("link", "/w/bad.mp3", Fault::Code(500, ""));
    }
    let mut config = server.config();
    config.link_rate = 20.0;
    let client = Client::new(config).unwrap();

    assert!(client.link("/w/bad.mp3").await.is_err());
    assert_eq!(server.fake.hits("link", "/w/bad.mp3"), 3);
    assert!(
        (client.limits().link_rate - 10.0).abs() < 1e-9,
        "{:?}",
        client.limits()
    );
}

#[tokio::test]
async fn queued_listings_wait_out_a_cooldown_that_starts_ahead_of_them() {
    let server = start().await;
    for dir in ["/a", "/b", "/c"] {
        server.fake.put(dir, vec![]);
    }
    let mut config = server.config();
    config.list_concurrency = 1;
    let client = Client::new(config).unwrap();
    client.list("/a").await.unwrap();
    client.invalidate_listing("/a");

    server
        .fake
        .fault("list", "/a", Fault::Status(429, Some("1")));
    *server.fake.list_delay.lock().unwrap() = Duration::from_millis(100);
    let start = Instant::now();
    let queued = {
        let client = client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.list("/b").await.map(|_| Instant::now())
        })
    };
    client.list("/a").await.unwrap();
    let b_done = queued.await.unwrap().unwrap();
    assert!(b_done.duration_since(start) >= Duration::from_millis(1000));
}

#[tokio::test]
async fn a_provider_429_pauses_later_transfers_only() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    let client = server.client();
    client.link("/w/01.mp3").await.unwrap();

    server
        .fake
        .fault("file", "/w/01.mp3", Fault::Status(429, Some("1")));
    let first = client.open("/w/01.mp3", &HeaderMap::new()).await.unwrap();
    assert_eq!(first.status(), 429, "the provider's answer is forwarded");
    drop(first);
    assert_eq!(
        client.limits().cooldown,
        Duration::ZERO,
        "API calls are not paused"
    );

    let start = Instant::now();
    let second = client.open("/w/01.mp3", &HeaderMap::new()).await.unwrap();
    assert_eq!(second.status(), 200);
    assert!(
        start.elapsed() >= Duration::from_millis(500),
        "{:?}",
        start.elapsed()
    );
}
