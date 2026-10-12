//! A fake OpenList server with a synthetic tree and scripted failures.

#![allow(dead_code)]

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use openlist_client::{Client, Config};
use serde_json::{Value, json};

pub const USER: &str = "reader";
pub const PASSWORD: &str = "synthetic-password";
pub const LINK_UA: &str = "fake-provider-ua";

/// A scripted answer that replaces the normal one for the next request to
/// an endpoint and path.
#[derive(Clone, Debug)]
pub enum Fault {
    /// HTTP 200 with this business code and message.
    Code(i64, &'static str),
    /// This HTTP status, with an optional `Retry-After`.
    Status(u16, Option<&'static str>),
    /// This HTTP status with this body.
    StatusBody(u16, &'static str),
}

#[derive(Clone)]
pub struct FakeFile {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: String,
}

pub fn file(name: &str, size: u64) -> FakeFile {
    FakeFile {
        name: name.into(),
        is_dir: false,
        size,
        modified: "2024-05-01T20:00:00+08:00".into(),
    }
}

pub fn dir(name: &str) -> FakeFile {
    FakeFile {
        name: name.into(),
        is_dir: true,
        size: 0,
        modified: "2024-05-01T20:00:00+08:00".into(),
    }
}

#[derive(Default)]
pub struct Fake {
    tree: Mutex<HashMap<String, Vec<FakeFile>>>,
    faults: Mutex<HashMap<(String, String), VecDeque<Fault>>>,
    tokens: Mutex<Vec<String>>,
    /// Whether a request without a token is served as the guest user.
    guest: AtomicBool,
    issued: AtomicUsize,
    /// Requests per endpoint and path, faults included.
    hits: Mutex<HashMap<(String, String), usize>>,
    /// `refresh` flags seen by fs/list, in order.
    pub refresh_flags: Mutex<Vec<(String, u32, bool)>>,
    list_in_flight: AtomicUsize,
    pub list_peak: AtomicUsize,
    /// Delay for every fs/list answer, to make overlap observable.
    pub list_delay: Mutex<Duration>,
    /// Delay for every fs/link answer.
    pub link_delay: Mutex<Duration>,
    /// Nanoseconds reported as the link's `Expiration`.
    pub link_expiration: Mutex<Option<i64>>,
    /// File requests to reject with 403 before serving, to mimic expired
    /// links.
    pub reject_files: Mutex<usize>,
    pub link_serial: AtomicUsize,
    /// Every request for this path and page answers HTTP 502.
    pub fail_page: Mutex<Option<(String, u32)>>,
    /// After page 1 of this path is served, the directory disappears.
    pub vanish_after_first_page: Mutex<Option<String>>,
    /// After page 1 of this path is served, one more entry appears.
    pub grow_after_first_page: Mutex<Option<String>>,
    /// Whether each `/hop`, `/d` and `/raw` request carried an
    /// Authorization header, in order.
    pub auth_seen: Mutex<Vec<(String, bool)>>,
    /// Whether each of those requests carried a Cookie header, in order.
    pub cookie_seen: Mutex<Vec<(String, bool)>>,
    /// Delay before fs/remove answers.
    pub remove_delay: Mutex<Duration>,
    /// Delay before a provider host (`/raw`) answers.
    pub raw_delay: Mutex<Duration>,
    /// The base of the URLs fs/link hands out, instead of this server's.
    pub link_base: Mutex<Option<String>>,
    addr: Mutex<Option<SocketAddr>>,
}

impl Fake {
    pub fn put(&self, path: &str, entries: Vec<FakeFile>) {
        self.tree.lock().unwrap().insert(path.into(), entries);
    }

    /// Makes a directory disappear, as an unmounted storage would.
    pub fn remove(&self, path: &str) {
        self.tree.lock().unwrap().remove(path);
    }

    pub fn fault(&self, endpoint: &str, path: &str, fault: Fault) {
        self.faults
            .lock()
            .unwrap()
            .entry((endpoint.into(), path.into()))
            .or_default()
            .push_back(fault);
    }

    pub fn hits(&self, endpoint: &str, path: &str) -> usize {
        let key = (endpoint.to_owned(), path.to_owned());
        self.hits.lock().unwrap().get(&key).copied().unwrap_or(0)
    }

    pub fn logins(&self) -> usize {
        self.issued.load(Ordering::SeqCst)
    }

    /// Accepts `token` without a login, like a token from the settings page.
    pub fn accept_token(&self, token: &str) {
        self.tokens.lock().unwrap().push(token.into());
    }

    /// Serves requests that carry no token, as OpenList does when its guest
    /// user is enabled.
    pub fn enable_guest(&self) {
        self.guest.store(true, Ordering::SeqCst);
    }

    /// Revokes every token, as a password change or expiry would.
    pub fn expire_tokens(&self) {
        self.tokens.lock().unwrap().clear();
    }

    fn take_fault(&self, endpoint: &str, path: &str) -> Option<Fault> {
        let key = (endpoint.to_owned(), path.to_owned());
        *self.hits.lock().unwrap().entry(key.clone()).or_default() += 1;
        self.faults.lock().unwrap().get_mut(&key)?.pop_front()
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(token) = headers.get(header::AUTHORIZATION) else {
            return self.guest.load(Ordering::SeqCst);
        };
        let Ok(token) = token.to_str() else {
            return false;
        };
        self.tokens.lock().unwrap().iter().any(|t| t == token)
    }

    fn base(&self) -> String {
        format!("http://{}", self.addr.lock().unwrap().unwrap())
    }

    /// The same server under another host name, as a provider host would be.
    pub fn other_host(&self) -> String {
        let port = self.addr.lock().unwrap().unwrap().port();
        format!("http://localhost:{port}")
    }

    fn note_auth(&self, path: String, headers: &HeaderMap) {
        let cookie = headers.contains_key(header::COOKIE);
        self.cookie_seen
            .lock()
            .unwrap()
            .push((path.clone(), cookie));
        let seen = headers.contains_key(header::AUTHORIZATION);
        self.auth_seen.lock().unwrap().push((path, seen));
    }
}

pub struct Server {
    pub fake: Arc<Fake>,
    pub url: String,
}

impl Server {
    pub fn config(&self) -> Config {
        let mut config = Config::new(self.url.parse().unwrap(), USER, PASSWORD);
        config.retry_delay = Duration::from_millis(1);
        config.link_rate = 1000.0;
        // Provider hosts are other names for this loopback server.
        config.local_links = true;
        config
    }

    /// Settings that send `token` instead of logging in.
    pub fn token_config(&self, token: &str) -> Config {
        let mut config = Config::with_token(self.url.parse().unwrap(), token);
        config.retry_delay = Duration::from_millis(1);
        config.link_rate = 1000.0;
        // Provider hosts are other names for this loopback server.
        config.local_links = true;
        config
    }

    /// Settings that send no token.
    pub fn guest_config(&self) -> Config {
        let mut config = Config::anonymous(self.url.parse().unwrap());
        config.retry_delay = Duration::from_millis(1);
        config.link_rate = 1000.0;
        // Provider hosts are other names for this loopback server.
        config.local_links = true;
        config
    }

    pub fn client(&self) -> Client {
        Client::new(self.config()).unwrap()
    }
}

pub async fn start() -> Server {
    let fake = Arc::new(Fake::default());
    let app = Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/fs/list", post(list))
        .route("/api/fs/get", post(fs_get))
        .route("/api/fs/link", post(link))
        .route("/api/fs/remove", post(remove))
        .route("/files/{*path}", get(serve_file))
        .route("/d/{*path}", get(download))
        .route("/hop/{*path}", get(hop))
        .route("/back/{*path}", get(back))
        .route("/bounce/{*path}", get(bounce))
        .route("/not-modified/{*path}", get(not_modified))
        .route("/raw/{*path}", get(raw))
        .route("/away/{*path}", get(away))
        .with_state(Arc::clone(&fake));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    *fake.addr.lock().unwrap() = Some(addr);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server {
        fake,
        url: format!("http://{addr}"),
    }
}

fn ok(data: Value) -> Response {
    Json(json!({"code": 200, "message": "success", "data": data})).into_response()
}

fn code(code: i64, message: &str) -> Response {
    Json(json!({"code": code, "message": message, "data": null})).into_response()
}

fn fault_response(fault: Fault) -> Response {
    match fault {
        Fault::Code(c, message) => code(c, message),
        Fault::StatusBody(status, body) => {
            (StatusCode::from_u16(status).unwrap(), body).into_response()
        }
        Fault::Status(status, retry_after) => {
            let mut response = (StatusCode::from_u16(status).unwrap(), "fault").into_response();
            if let Some(value) = retry_after {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, value.parse().unwrap());
            }
            response
        }
    }
}

fn entry_json(file: &FakeFile) -> Value {
    json!({
        "name": file.name, "size": file.size, "is_dir": file.is_dir,
        "modified": file.modified, "created": "0001-01-01T00:00:00Z",
        "sign": "", "thumb": "", "type": 0, "hashinfo": "null", "hash_info": null,
    })
}

async fn login(State(fake): State<Arc<Fake>>, Json(body): Json<Value>) -> Response {
    if let Some(fault) = fake.take_fault("login", "/") {
        return fault_response(fault);
    }
    if body["username"] != USER || body["password"] != PASSWORD {
        return code(400, "password is incorrect");
    }
    let n = fake.issued.fetch_add(1, Ordering::SeqCst) + 1;
    let token = format!("token-{n}");
    fake.tokens.lock().unwrap().push(token.clone());
    ok(json!({"token": token}))
}

async fn list(
    State(fake): State<Arc<Fake>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let path = body["path"].as_str().unwrap_or_default().to_owned();
    let now = fake.list_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    fake.list_peak.fetch_max(now, Ordering::SeqCst);
    let delay = *fake.list_delay.lock().unwrap();
    tokio::time::sleep(delay).await;
    fake.list_in_flight.fetch_sub(1, Ordering::SeqCst);

    if let Some(fault) = fake.take_fault("list", &path) {
        return fault_response(fault);
    }
    if !fake.authorized(&headers) {
        return code(401, "token is expired");
    }
    let page = body["page"].as_u64().unwrap_or(1) as u32;
    if fake.fail_page.lock().unwrap().as_ref() == Some(&(path.clone(), page)) {
        return fault_response(Fault::Status(502, None));
    }
    let per_page = body["per_page"].as_u64().unwrap_or(0) as usize;
    let refresh = body["refresh"].as_bool().unwrap_or(false);
    fake.refresh_flags
        .lock()
        .unwrap()
        .push((path.clone(), page, refresh));
    let Some(entries) = fake.tree.lock().unwrap().get(&path).cloned() else {
        return code(500, "failed get objs: failed get dir: object not found");
    };
    let total = entries.len();
    let per_page = if per_page == 0 { usize::MAX } else { per_page };
    let start = (page as usize - 1).saturating_mul(per_page).min(total);
    let end = start.saturating_add(per_page).min(total);
    let content: Vec<Value> = entries[start..end].iter().map(entry_json).collect();
    if page == 1 && fake.vanish_after_first_page.lock().unwrap().as_deref() == Some(path.as_str()) {
        fake.tree.lock().unwrap().remove(&path);
    }
    if page == 1 && fake.grow_after_first_page.lock().unwrap().as_deref() == Some(path.as_str()) {
        let mut tree = fake.tree.lock().unwrap();
        tree.get_mut(&path).unwrap().push(file("appeared-later", 1));
    }
    ok(
        json!({"content": content, "total": total, "readme": "", "header": "",
        "write": true, "provider": "Fake"}),
    )
}

async fn fs_get(
    State(fake): State<Arc<Fake>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let path = body["path"].as_str().unwrap_or_default().to_owned();
    if let Some(fault) = fake.take_fault("get", &path) {
        return fault_response(fault);
    }
    if !fake.authorized(&headers) {
        return code(401, "token is expired");
    }
    let Some(found) = find(&fake, &path) else {
        return code(500, "failed get obj: object not found");
    };
    let mut data = entry_json(&found);
    data["raw_url"] = json!(format!("{}/files{path}", fake.base()));
    ok(data)
}

async fn link(
    State(fake): State<Arc<Fake>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let path = body["path"].as_str().unwrap_or_default().to_owned();
    let delay = *fake.link_delay.lock().unwrap();
    tokio::time::sleep(delay).await;
    if let Some(fault) = fake.take_fault("link", &path) {
        return fault_response(fault);
    }
    if !fake.authorized(&headers) {
        return code(401, "token is expired");
    }
    if find(&fake, &path).is_none() {
        return code(500, "failed link: object not found");
    }
    let serial = fake.link_serial.fetch_add(1, Ordering::SeqCst) + 1;
    let expiration = *fake.link_expiration.lock().unwrap();
    let base = fake
        .link_base
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| fake.base());
    ok(json!({
        "url": format!("{base}/files{path}?serial={serial}"),
        "header": {"User-Agent": [LINK_UA]},
        "Expiration": expiration,
        "concurrency": 0, "part_size": 0, "content_length": 0,
    }))
}

async fn serve_file(
    State(fake): State<Arc<Fake>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(fault) = fake.take_fault("file", &format!("/{path}")) {
        return fault_response(fault);
    }
    if headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        != Some(LINK_UA)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let rejected = {
        let mut left = fake.reject_files.lock().unwrap();
        let rejected = *left > 0;
        *left = left.saturating_sub(1);
        rejected
    };
    if rejected {
        return StatusCode::FORBIDDEN.into_response();
    }
    let body = format!("contents of /{path}").into_bytes();
    match headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        Some(range) => {
            let start: usize = range
                .trim_start_matches("bytes=")
                .trim_end_matches('-')
                .parse()
                .unwrap();
            let slice = body[start..].to_vec();
            let content_range = format!("bytes {start}-{}/{}", body.len() - 1, body.len());
            (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_RANGE, content_range)],
                slice,
            )
                .into_response()
        }
        None => body.into_response(),
    }
}

async fn remove(
    State(fake): State<Arc<Fake>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let dir = body["dir"].as_str().unwrap_or_default().to_owned();
    let delay = *fake.remove_delay.lock().unwrap();
    tokio::time::sleep(delay).await;
    if let Some(fault) = fake.take_fault("remove", &dir) {
        return fault_response(fault);
    }
    if !fake.authorized(&headers) {
        return code(401, "token is expired");
    }
    let names: Vec<String> = serde_json::from_value(body["names"].clone()).unwrap();
    let mut tree = fake.tree.lock().unwrap();
    let Some(entries) = tree.get_mut(&dir) else {
        return code(500, "failed remove: object not found");
    };
    entries.retain(|entry| !names.contains(&entry.name));
    let base = dir.trim_end_matches('/').to_owned();
    for name in &names {
        let root = format!("{base}/{name}");
        tree.retain(|path, _| path != &root && !path.starts_with(&format!("{root}/")));
    }
    Json(json!({"code": 200, "message": "success", "data": null})).into_response()
}

/// OpenList's `/d`: asks the provider for a link and redirects to it.
async fn download(
    State(fake): State<Arc<Fake>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    fake.note_auth(format!("/d/{path}"), &headers);
    if let Some(fault) = fake.take_fault("d", &format!("/{path}")) {
        return fault_response(fault);
    }
    if find(&fake, &format!("/{path}")).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let target = format!("{}/raw/{path}", fake.other_host());
    (StatusCode::FOUND, [(header::LOCATION, target)]).into_response()
}

/// An OpenList URL that redirects to another OpenList URL, as a `/d`
/// behind a path rewrite would.
async fn hop(
    State(fake): State<Arc<Fake>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    fake.note_auth(format!("/hop/{path}"), &headers);
    let target = format!("{}/d/{path}", fake.base());
    (StatusCode::FOUND, [(header::LOCATION, target)]).into_response()
}

/// An OpenList URL that redirects to a provider which sends the client
/// back to OpenList's `/d`.
async fn back(
    State(fake): State<Arc<Fake>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    fake.note_auth(format!("/back/{path}"), &headers);
    let target = format!("{}/bounce/{path}", fake.other_host());
    (StatusCode::FOUND, [(header::LOCATION, target)]).into_response()
}

async fn bounce(
    State(fake): State<Arc<Fake>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    fake.note_auth(format!("/bounce/{path}"), &headers);
    let target = format!("{}/d/{path}", fake.base());
    (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, target)]).into_response()
}

/// Sends the client to this server under its IPv6 loopback address, a
/// local host that is not OpenList's.
async fn away(State(fake): State<Arc<Fake>>, Path(path): Path<String>) -> Response {
    let port = fake.addr.lock().unwrap().unwrap().port();
    let target = format!("http://[::1]:{port}/files/{path}");
    (StatusCode::FOUND, [(header::LOCATION, target)]).into_response()
}

/// A 304 that carries a Location, which must not be followed.
async fn not_modified(Path(path): Path<String>) -> Response {
    let target = format!("/raw/{path}");
    (StatusCode::NOT_MODIFIED, [(header::LOCATION, target)]).into_response()
}

/// A provider host that serves files to anyone.
async fn raw(
    State(fake): State<Arc<Fake>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    fake.note_auth(format!("/raw/{path}"), &headers);
    if let Some(fault) = fake.take_fault("raw", &format!("/{path}")) {
        return fault_response(fault);
    }
    let delay = *fake.raw_delay.lock().unwrap();
    tokio::time::sleep(delay).await;
    format!("contents of /{path}").into_response()
}

fn find(fake: &Fake, path: &str) -> Option<FakeFile> {
    let (parent, name) = path.rsplit_once('/')?;
    let parent = if parent.is_empty() { "/" } else { parent };
    let tree = fake.tree.lock().unwrap();
    tree.get(parent)?.iter().find(|f| f.name == name).cloned()
}
