//! A store of buckets in memory behind a local port, which answers S3 as a store does
//! ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! What the S3 endpoint's tests and `shoaladm bench s3`'s are run against: it reads HTTP/1.1
//! requests, checks every signature by making it again from what arrived - the `Host` header the
//! client sent included - and answers puts, gets, heads and deletes of objects and buckets with
//! S3's statuses and error codes. Never a benchmark target: it serves one request at a time a
//! connection, from a lock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use super::sigv4::{Request, Signer, UNSIGNED_PAYLOAD};

/// One request as the store read it
struct Seen {
    /// The method
    method: String,
    /// The path
    path: String,
    /// Every header, by lower case name
    headers: HashMap<String, String>,
    /// The body
    body: Vec<u8>,
}

/// A running store, and every request it was sent
pub struct LocalStore {
    /// Where it listens, as `http://127.0.0.1:port`
    pub address: String,
    /// Every request, as `METHOD path`, in the order they arrived
    seen: Arc<Mutex<Vec<String>>>,
}

/// What the store holds: its buckets, and each object by its whole path
#[derive(Default)]
struct Held {
    /// Every bucket made
    buckets: Vec<String>,
    /// Every object, by `/bucket/path`
    objects: HashMap<String, Vec<u8>>,
}

/// Read one HTTP/1.1 request, or nothing once the connection closed
///
/// # Arguments
///
/// * `reader` - The connection
async fn read_request(reader: &mut BufReader<tokio::net::TcpStream>) -> Option<Seen> {
    // the request line, then headers to a blank line, then a body of its length
    let mut line = String::new();
    if reader.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut headers = HashMap::new();
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).await.ok()?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':')?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let len: usize = headers.get("content-length").and_then(|len| len.parse().ok()).unwrap_or(0);
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await.ok()?;
    Some(Seen {
        method,
        path,
        headers,
        body,
    })
}

/// The status and body a request is answered with
///
/// # Arguments
///
/// * `signer` - The key every signature is checked against
/// * `request` - The request
/// * `held` - What the store holds
fn answer(signer: &Signer, request: Seen, held: &Mutex<Held>) -> (&'static str, Vec<u8>) {
    // the signature, made again from what arrived, Host header included
    let date = request.headers.get("x-amz-date").cloned().unwrap_or_default();
    let host = request.headers.get("host").cloned().unwrap_or_default();
    let expected = signer.authorization(&Request {
        method: &request.method,
        path: &request.path,
        query: "",
        headers: &[],
        host: &host,
        payload: UNSIGNED_PAYLOAD,
        amz_date: &date,
    });
    if request.headers.get("authorization") != Some(&expected) {
        return ("403 Forbidden", b"<Error><Code>SignatureDoesNotMatch</Code></Error>".to_vec());
    }
    let mut held = held.lock().expect("the store");
    let segments: Vec<&str> = request.path.trim_start_matches('/').splitn(2, '/').collect();
    // a bucket's own requests
    if segments.len() == 1 {
        let bucket = segments[0].to_string();
        return match request.method.as_str() {
            "PUT" if held.buckets.contains(&bucket) => {
                ("409 Conflict", b"<Error><Code>BucketAlreadyOwnedByYou</Code></Error>".to_vec())
            }
            "PUT" => {
                held.buckets.push(bucket);
                ("200 OK", Vec::new())
            }
            _ if held.buckets.contains(&bucket) => ("200 OK", Vec::new()),
            _ => ("404 Not Found", b"<Error><Code>NoSuchBucket</Code></Error>".to_vec()),
        };
    }
    // an object's, in a bucket that was made
    if !held.buckets.iter().any(|bucket| bucket == segments[0]) {
        return ("404 Not Found", b"<Error><Code>NoSuchBucket</Code></Error>".to_vec());
    }
    match request.method.as_str() {
        "PUT" => {
            held.objects.insert(request.path, request.body);
            ("200 OK", Vec::new())
        }
        "GET" => match held.objects.get(&request.path) {
            Some(bytes) => ("200 OK", bytes.clone()),
            None => ("404 Not Found", b"<Error><Code>NoSuchKey</Code></Error>".to_vec()),
        },
        "HEAD" if held.objects.contains_key(&request.path) => ("200 OK", Vec::new()),
        "HEAD" => ("404 Not Found", Vec::new()),
        "DELETE" => {
            held.objects.remove(&request.path);
            ("204 No Content", Vec::new())
        }
        _ => ("405 Method Not Allowed", Vec::new()),
    }
}

impl LocalStore {
    /// Start a store on a free local port that checks every signature against a key
    ///
    /// # Arguments
    ///
    /// * `signer` - The key every request must be signed with
    ///
    /// # Panics
    ///
    /// When no local port can be bound.
    pub async fn start(signer: Arc<Signer>) -> LocalStore {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("a local port");
        let address = format!("http://{}", listener.local_addr().expect("its address"));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let held = Arc::new(Mutex::new(Held::default()));
        let log = seen.clone();
        // each connection served on a task of its own, a request at a time
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (signer, held, log) = (signer.clone(), held.clone(), log.clone());
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    while let Some(request) = read_request(&mut reader).await {
                        log.lock().expect("the log").push(format!("{} {}", request.method, request.path));
                        let head_only = request.method == "HEAD";
                        let (status, body) = answer(&signer, request, &held);
                        // no content has no length, and a head says how long and sends none
                        let length = if status.starts_with("204") { 0 } else { body.len() };
                        let head = format!("HTTP/1.1 {status}\r\ncontent-length: {length}\r\n\r\n");
                        let stream = reader.get_mut();
                        if stream.write_all(head.as_bytes()).await.is_err() {
                            return;
                        }
                        if !head_only && length > 0 && stream.write_all(&body).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        LocalStore { address, seen }
    }

    /// Every request it was sent, as `METHOD path`
    ///
    /// # Panics
    ///
    /// When a serving task panicked holding the log.
    #[must_use]
    pub fn requests(&self) -> Vec<String> {
        self.seen.lock().expect("the log").clone()
    }
}
