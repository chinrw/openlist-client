mod common;

use std::time::Duration;

use common::{Fault, file, start};
use openlist_client::Client;
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
async fn a_failing_d_link_slows_link_requests_down() {
    let server = start().await;
    server.fake.put("/w", vec![file("a.mp3", 1)]);
    server.fake.fault("d", "/w/a.mp3", Fault::Status(502, None));
    let client = server.client();
    let url: Url = format!("{}/d/w/a.mp3", server.url).parse().unwrap();

    let transfer = client.open_url(&url, &HeaderMap::new()).await.unwrap();
    assert_eq!(transfer.status(), 502, "the caller decides about retries");
    assert_eq!(client.limits().link_rate, 500.0);
    assert_eq!(server.fake.hits("d", "/w/a.mp3"), 1);
}

#[tokio::test]
async fn a_non_http_url_is_rejected() {
    let server = start().await;
    let client = server.client();
    let url: Url = "ftp://example.test/a".parse().unwrap();
    assert!(client.open_url(&url, &HeaderMap::new()).await.is_err());
}
