# openlist-client

A Rust client for the [OpenList](https://github.com/OpenListTeam/OpenList)
(formerly AList) HTTP API, built for storage providers that throttle, such as
115. It lists directories, looks up files and opens direct links while keeping
the number of provider calls low.

- Listings are limited by how many are in flight, and `fs/link` / `fs/get` by
  requests per second. When the provider throttles (115's 770004, or a bare
  500 on a link), the limit halves and recovers after 20 successes in a row.
- A 429 or 503 pauses every request. A call that would wait longer than its
  budget fails with `Error::RateLimited`.
- Listings and links are kept in memory, and concurrent requests for the same
  directory or file share one upstream request.
- `Client::stat` answers from the parent's listing instead of calling
  `fs/get`, and `Listing::digest` lets a scanner skip unchanged directories.

## Use

Add the crate as a git dependency pinned to a commit:

```toml
openlist-client = { git = "https://github.com/chinrw/openlist-client", rev = "<commit>" }
```

Create one `Client` per OpenList server and clone it freely. Clones share the
connection pool, rate limits and caches.

```rust,no_run
use openlist_client::{Client, Config};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let config = Config::new("http://openlist.lan:5244".parse()?, "reader", "secret");
let client = Client::new(config)?;
for found in client.walk("/media").await? {
    println!("{} {}", found.path, found.entry.size);
}
# Ok(())
# }
```

`Config::new` logs in with a username and password and logs in again when the
token expires.

`Client::link` and `Client::open` call `fs/link`, which needs an admin account
whose base path is `/`.

## Development

    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

The tests run against an in-process fake OpenList server and need no network.

## License

MIT
