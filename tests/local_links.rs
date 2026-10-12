//! Links that point at this machine. They come from OpenList, which may be
//! hostile, so only OpenList's own host may be local.

mod common;

use common::{file, start};
use openlist_client::{Client, Error};
use reqwest::header::HeaderMap;

fn guarded(server: &common::Server) -> Client {
    let mut config = server.config();
    config.local_links = false;
    Client::new(config).unwrap()
}

#[tokio::test]
async fn a_link_to_openlist_itself_still_works() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    let transfer = guarded(&server)
        .open("/w/01.mp3", &HeaderMap::new())
        .await
        .unwrap();
    assert_eq!(transfer.status(), 200);
}

#[tokio::test]
async fn a_link_to_a_local_address_is_refused_unopened() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    *server.fake.link_base.lock().unwrap() = Some("http://169.254.169.254".into());
    let error = guarded(&server)
        .open("/w/01.mp3", &HeaderMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::Protocol { message, .. } if message.contains("local address")),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_name_for_this_machine_is_refused() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    // localhost is this same server, under a name that is not OpenList's.
    *server.fake.link_base.lock().unwrap() = Some(server.fake.other_host());
    let error = guarded(&server)
        .open("/w/01.mp3", &HeaderMap::new())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("no address outside this machine"),
        "{error}"
    );
    assert_eq!(server.fake.hits("file", "/w/01.mp3"), 0);
}

#[tokio::test]
async fn a_redirect_to_a_local_address_is_not_followed() {
    let server = start().await;
    server.fake.put("/w", vec![file("01.mp3", 10)]);
    // The link itself is on OpenList's host, and that host redirects.
    *server.fake.link_base.lock().unwrap() = Some(format!("{}/away", server.url));
    let client = guarded(&server);
    let result = client.open("/w/01.mp3", &HeaderMap::new()).await;
    assert!(result.is_err(), "the redirect was followed");

    let url = format!("{}/away/w/01.mp3", server.url).parse().unwrap();
    let error = client.open_url(&url, &HeaderMap::new()).await;
    assert!(
        matches!(&error, Err(Error::Protocol { message, .. }) if message.contains("local address")),
        "{:?}",
        error.map(|t| t.status())
    );
}
