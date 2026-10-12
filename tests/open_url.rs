mod common;

use std::time::Duration;

use common::{Fault, file, start};
use openlist_client::{Client, Error};
use reqwest::header::HeaderMap;
use url::Url;

async fn body(mut transfer: openlist_client::Transfer) -> String {
    let mut bytes = Vec::new();
    while let Some(chunk) = transfer.chunk().await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn a_d_link_carries_the_token_only_to_openlist() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    let client = server.client();
    // Log in first, as a scan would have.
    client.list("/w").await.unwrap();

    let url: Url = format!("{}/d/w/a.mp3", server.url).parse().unwrap();
    let transfer = client.open_url(&url, &HeaderMap::new()).await.unwrap();
    assert_eq!(transfer.status(), 200);
    assert_eq!(body(transfer).await, "contents of /w/a.mp3");
    let seen = server.fake.auth_seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        [
            ("/d/w/a.mp3".to_owned(), true),
            ("/raw/w/a.mp3".to_owned(), false)
        ]
    );
}

#[tokio::test]
async fn a_guest_opens_a_d_link_without_a_token() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    let client = Client::new(server.guest_config()).unwrap();

    let url: Url = format!("{}/d/w/a.mp3", server.url).parse().unwrap();
    let transfer = client.open_url(&url, &HeaderMap::new()).await.unwrap();
    assert_eq!(body(transfer).await, "contents of /w/a.mp3");
    assert_eq!(server.fake.logins(), 0);
    let seen = server.fake.auth_seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        [
            ("/d/w/a.mp3".to_owned(), false),
            ("/raw/w/a.mp3".to_owned(), false)
        ]
    );
}

#[tokio::test]
async fn a_provider_url_is_neither_paced_nor_authorized() {
    let server = start().await;
    let mut config = server.config();
    // One link per 1000 seconds: a paced request would time out below.
    config.link_rate = 0.001;
    let client = Client::new(config).unwrap();
    let url: Url = format!("{}/raw/w/a.mp3", server.fake.other_host())
        .parse()
        .unwrap();
    let headers = HeaderMap::new();
    for _ in 0..3 {
        let open = client.open_url(&url, &headers);
        let transfer = tokio::time::timeout(Duration::from_secs(5), open)
            .await
            .expect("provider hosts are not paced")
            .unwrap();
        assert_eq!(transfer.status(), 200);
    }
    assert_eq!(server.fake.logins(), 0, "no token is needed off OpenList");
    assert!(
        server
            .fake
            .auth_seen
            .lock()
            .unwrap()
            .iter()
            .all(|(_, seen)| !seen)
    );
}

#[tokio::test]
async fn a_slow_provider_does_not_hold_the_api_slot() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    let mut config = server.config();
    config.max_api_requests = 1;
    let client = Client::new(config).unwrap();
    client.list("/w").await.unwrap();
    *server.fake.raw_delay.lock().unwrap() = Duration::from_secs(3);

    let download = {
        let client = client.clone();
        let url: Url = format!("{}/d/w/a.mp3", server.url).parse().unwrap();
        tokio::spawn(async move { client.open_url(&url, &HeaderMap::new()).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.invalidate_listing("/w");
    let listing = tokio::time::timeout(Duration::from_secs(1), client.list("/w")).await;
    assert!(listing.expect("the API slot was released").is_ok());
    download.abort();
}

#[tokio::test]
async fn a_failing_d_link_slows_link_requests_down() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    server.fake.fault("d", "/w/a.mp3", Fault::Status(502, None));
    let client = server.client();
    let url: Url = format!("{}/d/w/a.mp3", server.url).parse().unwrap();

    let error = client.open_url(&url, &HeaderMap::new()).await.unwrap_err();
    assert!(
        matches!(error, Error::Http { status: 502, .. }),
        "{error:?}"
    );
    assert_eq!(client.limits().link_rate, 500.0);
    assert_eq!(server.fake.hits("d", "/w/a.mp3"), 1);
}

#[tokio::test]
async fn the_token_stays_below_the_base_path() {
    let server = start().await;
    let mut config = server.token_config("settings-token");
    // OpenList behind a reverse proxy at /openlist, next to another app.
    config.url = format!("{}/openlist", server.url).parse().unwrap();
    let client = Client::new(config).unwrap();
    let url: Url = format!("{}/raw/w/a.mp3", server.url).parse().unwrap();
    let transfer = client.open_url(&url, &HeaderMap::new()).await.unwrap();
    assert_eq!(transfer.status(), 200);
    let seen = server.fake.auth_seen.lock().unwrap().clone();
    assert_eq!(seen, [("/raw/w/a.mp3".to_owned(), false)]);
}

#[tokio::test]
async fn a_non_http_url_is_rejected() {
    let server = start().await;
    let client = server.client();
    let url: Url = "ftp://example.test/a".parse().unwrap();
    assert!(client.open_url(&url, &HeaderMap::new()).await.is_err());
}

#[tokio::test]
async fn caller_credentials_stop_where_the_request_leaves_openlist() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    let client = server.client();
    let url: Url = format!("{}/d/w/a.mp3", server.url).parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::COOKIE, "session=1".parse().unwrap());
    let transfer = client.open_url(&url, &headers).await.unwrap();
    assert_eq!(transfer.status(), 200);
    let seen = server.fake.cookie_seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        [
            ("/d/w/a.mp3".to_owned(), true),
            ("/raw/w/a.mp3".to_owned(), false)
        ]
    );
}

#[tokio::test]
async fn every_openlist_hop_is_admitted_and_observed() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    server.fake.fault("d", "/w/a.mp3", Fault::Status(502, None));
    let client = server.client();
    let url: Url = format!("{}/hop/w/a.mp3", server.url).parse().unwrap();

    let error = client.open_url(&url, &HeaderMap::new()).await.unwrap_err();
    assert!(
        matches!(error, Error::Http { status: 502, .. }),
        "{error:?}"
    );
    assert_eq!(
        client.limits().link_rate,
        500.0,
        "the second OpenList hop's 502 counts"
    );
    let seen = server.fake.auth_seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        [
            ("/hop/w/a.mp3".to_owned(), true),
            ("/d/w/a.mp3".to_owned(), true)
        ]
    );
}

#[tokio::test]
async fn a_return_to_openlist_is_admitted_but_unauthorized() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    server.fake.fault("d", "/w/a.mp3", Fault::Status(502, None));
    let client = server.client();
    let url: Url = format!("{}/back/w/a.mp3", server.url).parse().unwrap();

    let error = client.open_url(&url, &HeaderMap::new()).await.unwrap_err();
    assert!(
        matches!(error, Error::Http { status: 502, .. }),
        "{error:?}"
    );
    assert_eq!(client.limits().link_rate, 500.0);
    let seen = server.fake.auth_seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        [
            ("/back/w/a.mp3".to_owned(), true),
            ("/bounce/w/a.mp3".to_owned(), false),
            ("/d/w/a.mp3".to_owned(), false),
        ]
    );
}

#[tokio::test]
async fn a_304_is_returned_rather_than_followed() {
    let server = start().await;
    let client = server.client();
    let url: Url = format!("{}/not-modified/w/a.mp3", server.url)
        .parse()
        .unwrap();
    let transfer = client.open_url(&url, &HeaderMap::new()).await.unwrap();
    assert_eq!(transfer.status(), 304);
    assert!(server.fake.auth_seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_body_cut_short_is_a_retryable_transport_error() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
        }
        let reply = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort";
        socket.write_all(reply).await.unwrap();
    });
    let server = start().await;
    let client = server.client();
    let url: Url = format!("http://{addr}/file").parse().unwrap();
    let mut transfer = client.open_url(&url, &HeaderMap::new()).await.unwrap();
    let error = loop {
        match transfer.chunk().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the body must not look complete"),
            Err(error) => break error,
        }
    };
    assert!(
        matches!(
            error,
            openlist_client::Error::Transport {
                retryable: true,
                ..
            }
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_missing_or_blocked_file_does_not_slow_links_down() {
    let server = start().await;
    server
        .fake
        .put("/w", vec![file("a.nfo", 1), file("b.nfo", 1)]);
    let body = "failed link: failed get link: object not found";
    server
        .fake
        .fault("d", "/w/a.nfo", Fault::StatusBody(500, body));
    server
        .fake
        .fault("d", "/w/b.nfo", Fault::StatusBody(500, "违规内容"));
    let client = server.client();
    for name in ["a.nfo", "b.nfo"] {
        let url: Url = format!("{}/d/w/{name}", server.url).parse().unwrap();
        let error = client.open_url(&url, &HeaderMap::new()).await.unwrap_err();
        assert!(
            matches!(&error, Error::Http { status: 500, .. }),
            "{error:?}"
        );
    }
    assert_eq!(client.limits().link_rate, 1000.0);
}

#[tokio::test]
async fn a_pause_beyond_the_budget_fails_the_request_that_received_it() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.nfo", 1)]);
    server
        .fake
        .fault("d", "/w/a.nfo", Fault::Status(503, Some("301")));
    let mut config = server.config();
    config.cooldown_budget = Duration::from_secs(300);
    let client = Client::new(config).unwrap();
    let url: Url = format!("{}/d/w/a.nfo", server.url).parse().unwrap();
    let error = client.open_url(&url, &HeaderMap::new()).await.unwrap_err();
    assert!(matches!(error, Error::RateLimited), "{error:?}");
}

#[tokio::test]
async fn a_provider_pause_beyond_the_budget_is_rate_limited_before_capping() {
    let server = start().await;
    server
        .fake
        .fault("raw", "/w/a.nfo", Fault::Status(503, Some("3600")));
    let mut config = server.config();
    config.cooldown_budget = Duration::from_secs(300);
    let client = Client::new(config).unwrap();
    let url: Url = format!("{}/raw/w/a.nfo", server.fake.other_host())
        .parse()
        .unwrap();
    let error = client.open_url(&url, &HeaderMap::new()).await.unwrap_err();
    assert!(matches!(error, Error::RateLimited), "{error:?}");
}
