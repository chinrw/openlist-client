//! An open file transfer that holds its transfer slot until it is dropped.

use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt};
use reqwest::{StatusCode, header::HeaderMap};
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    Error,
    error::{clean_body, error_prefix},
};

/// A response from the provider for [`crate::Client::open`].
///
/// The status and headers are the provider's. A proxy forwards them as they
/// are, including a 200 when the provider ignored `Range`.
pub struct Transfer {
    response: reqwest::Response,
    permit: OwnedSemaphorePermit,
    /// For errors. The URL's path without its query, which can carry a
    /// signature.
    path: String,
}

impl Transfer {
    pub(crate) fn new(
        response: reqwest::Response,
        permit: OwnedSemaphorePermit,
        path: impl Into<String>,
    ) -> Self {
        Self {
            response,
            permit,
            path: path.into(),
        }
    }

    pub fn status(&self) -> StatusCode {
        self.response.status()
    }

    pub fn headers(&self) -> &HeaderMap {
        self.response.headers()
    }

    /// Passes a 2xx response through and turns any other status into
    /// [`Error::Http`] carrying the start of the body, so a download failure
    /// classifies like an API one: [`Error::is_retryable`] and
    /// [`Error::is_blocked`] read the provider's markers in it.
    ///
    /// # Errors
    /// [`Error::Http`] for a status outside 2xx, including a 304 the caller
    /// asked for with a conditional header.
    pub async fn error_for_status(mut self) -> Result<Self, Error> {
        let status = self.status();
        if status.is_success() {
            return Ok(self);
        }
        let body = clean_body(&error_prefix(&mut self.response).await);
        Err(Error::Http {
            status: status.as_u16(),
            path: self.path,
            body,
        })
    }

    /// Reads the next chunk of the body. `None` means the body is complete.
    ///
    /// # Errors
    /// The connection failed mid-body.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, Error> {
        self.response
            .chunk()
            .await
            .map_err(|error| Error::transport("<transfer>", error))
    }

    /// Turns the body into a stream that keeps the transfer slot until the
    /// stream is dropped.
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes, Error>> + Send + 'static {
        let permit = self.permit;
        self.response.bytes_stream().map(move |chunk| {
            let _slot = &permit;
            chunk.map_err(|error| Error::transport("<transfer>", error))
        })
    }
}

impl std::fmt::Debug for Transfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The URL is a signed provider link, so it stays out of logs.
        f.debug_struct("Transfer")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}
