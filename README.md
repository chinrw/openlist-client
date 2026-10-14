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
token expires. `Config::with_token` sends a token you already have, such as the
admin token on OpenList's settings page, and never logs in. If OpenList rejects
that token, the call fails with `Error::Login`. `Config::anonymous` sends no
token and acts as OpenList's guest user, which must be enabled and allowed to
read the paths you use. A refused guest also fails with `Error::Login`.

`Client::link` and `Client::open` call `fs/link`, which needs an admin account
whose base path is `/`. `Client::open_url` opens a `raw_url` from `Client::get`
or an OpenList `/d` link and works with any account. It sends the token only to
URLs below the configured base URL.

A `Transfer` carries the provider's status as it is, so a proxy can forward
it. A downloader calls `Transfer::error_for_status`, which turns a status
outside 2xx into `Error::Http` with the start of the error page.
`Error::is_retryable` then tells a passing fault from a missing object, and
`Error::is_blocked` reports content the provider refuses, such as 115's
违规内容.

`Client::walk` fails as a whole when any directory fails. `Client::walk_partial`
records a directory it cannot list in `PartialWalk::failed` and goes on, and
stops only when the wait budget runs out or OpenList refuses the login. A name
that repeats in its directory, which 115 allows, goes into
`PartialWalk::ambiguous` and is not walked, because OpenList resolves its path
to only one copy. Treat a path missing from an incomplete walk as unknown, not
as deleted.

File transfers never connect to a loopback or link-local address, such as
`127.0.0.1` or `169.254.169.254`, unless it is OpenList's own address and port.
Links come from OpenList, so this keeps a hostile OpenList from pointing the
client at services on its own machine. Private LAN addresses are allowed. Set
`Config::local_links` if a provider really runs on the same machine under
another name.

`Client::remove` deletes entries with `fs/remove`. It is paced on its own
budget (`mutate_rate`) and never retried, because a timeout leaves the outcome
unknown.

## Development

    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

The tests run against an in-process fake OpenList server and need no network.

## License

MIT
