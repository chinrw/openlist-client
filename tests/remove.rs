mod common;

use std::time::Duration;

use common::{Fault, dir, file, start};
use openlist_client::{Error, ErrorKind};

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[tokio::test]
async fn remove_deletes_and_drops_the_cached_listings() {
    let server = start().await;
    server
        .fake
        .put("/w", vec![file("a", 1), file("b", 2), dir("sub")]);
    server.fake.put("/w/sub", vec![file("c", 3)]);
    let client = server.client();
    assert_eq!(client.list("/w").await.unwrap().entries.len(), 3);
    client.list("/w/sub").await.unwrap();

    client.remove("/w", &names(&["a", "sub"])).await.unwrap();

    let listing = client.list("/w").await.unwrap();
    let left: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(
        left,
        ["b"],
        "the cached listing must not survive the removal"
    );
    assert!(matches!(
        client.list("/w/sub").await.unwrap_err(),
        Error::NotFound { .. }
    ));
    assert_eq!(server.fake.hits("list", "/w"), 2);
}

#[tokio::test]
async fn a_failed_removal_is_sent_once_and_still_drops_the_cache() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    server.fake.fault("remove", "/w", Fault::Status(502, None));
    let client = server.client();
    client.list("/w").await.unwrap();

    let error = client.remove("/w", &names(&["a"])).await.unwrap_err();
    assert!(
        matches!(error, Error::Http { status: 502, .. }),
        "{error:?}"
    );
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(
        server.fake.hits("remove", "/w"),
        1,
        "removals are never retried"
    );

    client.list("/w").await.unwrap();
    assert_eq!(server.fake.hits("list", "/w"), 2);
}

#[tokio::test]
async fn a_cancelled_removal_still_drops_the_cache() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    let client = server.client();
    client.list("/w").await.unwrap();
    *server.fake.remove_delay.lock().unwrap() = Duration::from_millis(300);

    let removal = {
        let client = client.clone();
        tokio::spawn(async move { client.remove("/w", &names(&["a"])).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    removal.abort();
    let _ = removal.await;

    client.list("/w").await.unwrap();
    assert_eq!(server.fake.hits("list", "/w"), 2);
}

#[tokio::test]
async fn throttled_removals_slow_down_without_touching_links() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    server.fake.fault(
        "remove",
        "/w",
        Fault::Code(500, "770004 已达到当前访问上限"),
    );
    let mut config = server.config();
    config.mutate_rate = 1000.0;
    let client = openlist_client::Client::new(config).unwrap();

    client.remove("/w", &names(&["a"])).await.unwrap_err();
    let limits = client.limits();
    assert_eq!(limits.mutate_rate, 500.0);
    assert_eq!(limits.link_rate, 1000.0);
}

#[tokio::test]
async fn unsafe_names_are_rejected_before_any_request() {
    let server = start().await;
    server.fake.put("/w", vec![file("a", 1)]);
    let client = server.client();
    for bad in ["..", "x/y", "a\\b", ""] {
        let error = client.remove("/w", &names(&["a", bad])).await.unwrap_err();
        assert!(
            matches!(error, Error::InvalidPath { .. }),
            "{bad:?}: {error:?}"
        );
    }
    assert_eq!(server.fake.hits("remove", "/w"), 0);
    client.remove("/w", &[]).await.unwrap();
    assert_eq!(server.fake.hits("remove", "/w"), 0);
}
