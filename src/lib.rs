//! A client for the OpenList (formerly AList) HTTP API, built for storage
//! providers that throttle, such as 115.
//!
//! The expensive requests are the ones OpenList forwards to the provider:
//! `fs/link`, `fs/get` and forced refreshes. Listings are usually answered
//! from OpenList's own cache. The client therefore
//!
//! - limits listings by how many are in flight and links by requests per
//!   second, and halves either limit when the provider throttles (115's
//!   770004, or a bare 500 on a link), recovering after 20 successes in a
//!   row;
//! - pauses every request after a 429 or 503 and gives up with
//!   [`Error::RateLimited`] when the pause outlasts the wait budget;
//! - keeps listings and links in memory and merges concurrent requests for
//!   the same directory or file into one;
//! - never fetches links while listing or walking, and answers
//!   [`Client::stat`] from the parent's listing;
//! - offers [`Listing::digest`] so a scanner can skip directories that have
//!   not changed.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use openlist_client::{Client, Config};
//!
//! let config = Config::new("http://openlist.lan:5244".parse()?, "reader", "secret");
//! let client = Client::new(config)?;
//! let work = client.list("/media/works/RJ000001").await?;
//! let stored_digest = None;
//! if Some(work.digest()) != stored_digest {
//!     for found in client.walk_from(work).await? {
//!         println!("{} {}", found.path, found.entry.size);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

mod admission;
mod cache;
mod client;
mod config;
mod error;
mod limit;
pub mod path;
mod transfer;
mod types;

pub use admission::Limits;
pub use client::Client;
pub use config::{Config, ConfigError};
pub use error::{Error, ErrorKind};
pub use transfer::Transfer;
pub use types::{Entry, FileInfo, Link, Listing, ListingDigest, WalkEntry};
