//! Another store's S3 endpoint as an object endpoint ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! What the bench drives Ceph's RGW, MinIO, RustFS and SeaweedFS through: one HTTP/1.1 client a
//! endpoint, each with a pool of its own, as a Shoal cluster is driven through one client a member.
//! Every request is path style (`/{bucket}/{path}`), plaintext, and signed by SigV4 with an
//! unsigned payload ([`super::sigv4`]); a put is one `PutObject` of the whole object, whatever its
//! size, and a get one `GetObject` of all of it.
//!
//! # Invariants
//!
//! **No request is retried**, whatever it is answered with: the driver counts every failure where
//! it happened, as it does a Shoal client's. **The keys never leave the signer**: no field here,
//! and nothing a capture records, holds the secret.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use reqwest::{Method, StatusCode};

use super::endpoint::{EndpointError, ErrorKind, ObjectEndpoint};
use super::sigv4::{signed_host, uri_encode, Request, Signer, UNSIGNED_PAYLOAD};
use super::ObjectDataset;
use crate::spec::ReadLevel;

/// The longest an error's body is kept for its message
const ERROR_BODY: usize = 300;

/// How an S3 endpoint's client is built
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Settings {
    /// The longest one request may take, its whole answer read
    pub timeout: Duration,
    /// The longest a connection may take to open
    pub connect_timeout: Duration,
}

impl Default for S3Settings {
    /// Two minutes a request, the top of every window's histogram, and ten seconds a connection
    fn default() -> Self {
        S3Settings {
            timeout: Duration::from_secs(120),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// One S3 endpoint of a store, and the bucket each dataset's objects are put in
pub struct S3Endpoint {
    /// Where requests go, as `scheme://host:port`
    base: String,
    /// The host every request is signed for, as its client sends it
    host: String,
    /// The client, with a pool of connections of its own
    http: reqwest::Client,
    /// What signs every request
    signer: Arc<Signer>,
    /// The store's bucket for each dataset's, by the dataset's bucket
    buckets: BTreeMap<String, String>,
}

/// The code an S3 error body carries, `<Code>NoSuchKey</Code>`, if it carries one
///
/// # Arguments
///
/// * `body` - The body
fn error_code(body: &[u8]) -> Option<String> {
    // the first Code element, which an S3 error has exactly one of
    let text = std::str::from_utf8(body).ok()?;
    let start = text.find("<Code>")? + "<Code>".len();
    let end = text[start..].find("</Code>")? + start;
    Some(text[start..end].trim().to_string())
}

/// How the driver reads a status and the code its body carried
///
/// # Arguments
///
/// * `status` - The status
/// * `code` - The S3 code, if the body had one
#[must_use]
pub fn classify(status: StatusCode, code: Option<&str>) -> ErrorKind {
    // a key not found is a miss; a bucket not found is the store's misconfiguration
    match (status.as_u16(), code) {
        (404, Some("NoSuchBucket")) => ErrorKind::Other,
        (404, _) => ErrorKind::NotFound,
        (500 | 502 | 503 | 504, _) => ErrorKind::Retriable,
        (_, Some("SlowDown")) => ErrorKind::Retriable,
        _ => ErrorKind::Other,
    }
}

/// A request that never got an answer, by what stopped it
///
/// # Arguments
///
/// * `error` - What the client said
fn transport(error: &reqwest::Error) -> EndpointError {
    // a timeout and a refused connection are counted apart from the rest
    let code = if error.is_timeout() {
        "Timeout"
    } else if error.is_connect() {
        "Connect"
    } else {
        "Transport"
    };
    EndpointError::new(ErrorKind::Other, code, error.to_string())
}

impl S3Endpoint {
    /// An endpoint of a store, and the bucket each dataset's objects go in
    ///
    /// # Arguments
    ///
    /// * `endpoint` - Where it is, as `http://host:port`
    /// * `signer` - What signs its requests
    /// * `buckets` - The store's bucket for each dataset's, by the dataset's bucket
    /// * `settings` - How its client is built
    ///
    /// # Errors
    ///
    /// When the endpoint is not a URL with a host, or its client cannot be built.
    pub fn new(
        endpoint: &str,
        signer: Arc<Signer>,
        buckets: BTreeMap<String, String>,
        settings: S3Settings,
    ) -> Result<Self, String> {
        // the scheme, host and port, and the host as its client will send it
        let url = reqwest::Url::parse(endpoint).map_err(|error| format!("{endpoint} is not a URL: {error}"))?;
        let host = url.host_str().ok_or_else(|| format!("{endpoint} names no host"))?.to_string();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| format!("{endpoint} names no port its scheme has"))?;
        if !url.path().trim_matches('/').is_empty() {
            return Err(format!("{endpoint} has a path; an endpoint is a scheme, a host and a port"));
        }
        // one client, over HTTP/1.1 alone, that never goes through a proxy the environment names
        let http = reqwest::Client::builder()
            .http1_only()
            .no_proxy()
            .tcp_nodelay(true)
            .timeout(settings.timeout)
            .connect_timeout(settings.connect_timeout)
            .build()
            .map_err(|error| format!("a client of {endpoint} could not be built: {error}"))?;
        Ok(S3Endpoint {
            base: format!("{}://{host}:{port}", url.scheme()),
            host: signed_host(url.scheme(), &host, port),
            http,
            signer,
            buckets,
        })
    }

    /// Where requests go, as `scheme://host:port`
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The store's bucket a dataset's objects go in
    ///
    /// # Arguments
    ///
    /// * `dataset` - The dataset
    fn bucket(&self, dataset: &ObjectDataset) -> String {
        // a dataset with no bucket named for it puts its objects in its own name, lower case,
        // which is the only case an S3 bucket's name has
        self.buckets
            .get(&dataset.bucket)
            .cloned()
            .unwrap_or_else(|| dataset.bucket.to_ascii_lowercase())
    }

    /// Send one signed request
    ///
    /// # Arguments
    ///
    /// * `method` - The method
    /// * `path` - The path, encoded
    /// * `body` - The body, for a put
    async fn send(&self, method: Method, path: &str, body: Option<Bytes>) -> Result<reqwest::Response, EndpointError> {
        // signed for now, with the payload left unsigned
        let (amz_date, _) = crate::clock::amz_date(SystemTime::now());
        let authorization = self.signer.authorization(&Request {
            method: method.as_str(),
            path,
            query: "",
            headers: &[],
            host: &self.host,
            payload: UNSIGNED_PAYLOAD,
            amz_date: &amz_date,
        });
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header("x-amz-date", amz_date)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .header("authorization", authorization);
        // a body of known length, so every put says how long it is
        if let Some(body) = body {
            request = request.body(body);
        }
        request.send().await.map_err(|error| transport(&error))
    }

    /// The error a refused request is counted as, its body read for the code it carries
    ///
    /// # Arguments
    ///
    /// * `what` - What was asked, for the message
    /// * `response` - The answer
    async fn refused(what: &str, response: reqwest::Response) -> EndpointError {
        // the body names the S3 code, except a head's, which has none
        let status = response.status();
        let body = response.bytes().await.unwrap_or_default();
        let code = error_code(&body);
        let kind = classify(status, code.as_deref());
        let shown = String::from_utf8_lossy(&body[..body.len().min(ERROR_BODY)]).into_owned();
        EndpointError::new(
            kind,
            format!("{} {}", status.as_u16(), code.as_deref().unwrap_or("-")),
            format!("{what}: {status} {shown}"),
        )
    }

    /// The path of an object in its bucket, encoded once
    ///
    /// # Arguments
    ///
    /// * `dataset` - Its dataset
    /// * `path` - Its path in the bucket
    fn object(&self, dataset: &ObjectDataset, path: &str) -> String {
        uri_encode(&format!("/{}/{path}", self.bucket(dataset)))
    }

    /// Make a bucket, at a placement when one is named; a bucket that is there already is refused,
    /// since a run starts on an empty one
    ///
    /// # Arguments
    ///
    /// * `name` - The bucket
    /// * `location_constraint` - Where the store places it, such as Ceph's `default:ec21`
    ///
    /// # Errors
    ///
    /// When the store refuses it, or it exists.
    pub async fn create_bucket(&self, name: &str, location_constraint: Option<&str>) -> Result<(), EndpointError> {
        // a body only when there is a placement to name
        let body = match location_constraint {
            Some(location) => Bytes::from(format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <LocationConstraint>{location}</LocationConstraint></CreateBucketConfiguration>"
            )),
            None => Bytes::new(),
        };
        let response = self.send(Method::PUT, &uri_encode(&format!("/{name}")), Some(body)).await?;
        if response.status().is_success() {
            let _ = response.bytes().await;
            return Ok(());
        }
        Err(Self::refused(&format!("making bucket {name}"), response).await)
    }

    /// Whether a bucket answers through this endpoint, which also proves its signatures verify
    ///
    /// # Arguments
    ///
    /// * `name` - The bucket
    ///
    /// # Errors
    ///
    /// When the bucket is not there or the request is refused.
    pub async fn head_bucket(&self, name: &str) -> Result<(), EndpointError> {
        let response = self.send(Method::HEAD, &uri_encode(&format!("/{name}")), None).await?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(Self::refused(&format!("heading bucket {name}"), response).await)
    }
}

impl ObjectEndpoint for S3Endpoint {
    /// No S3 operation writes part of an object
    fn writes_in_place(&self) -> bool {
        false
    }

    /// `PutObject` of the whole object
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `bytes` - Its bytes
    async fn put(&self, dataset: &ObjectDataset, path: &str, bytes: Bytes) -> Result<(), EndpointError> {
        let response = self.send(Method::PUT, &self.object(dataset, path), Some(bytes)).await?;
        // the answer's body is read to its end, so its connection goes back to the pool
        if response.status().is_success() {
            let _ = response.bytes().await;
            return Ok(());
        }
        Err(Self::refused(&format!("putting {path}"), response).await)
    }

    /// `GetObject` of the whole object
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `level` - The level asked for, which S3 has no way to name: any but the default is
    ///   refused, never sent as something else
    async fn get(&self, dataset: &ObjectDataset, path: &str, level: ReadLevel) -> Result<Bytes, EndpointError> {
        // S3 names no read level, so one asked for is refused rather than measured as another
        if level != ReadLevel::Default {
            return Err(EndpointError::no_level(level));
        }
        let response = self.send(Method::GET, &self.object(dataset, path), None).await?;
        if response.status().is_success() {
            return response.bytes().await.map_err(|error| transport(&error));
        }
        Err(Self::refused(&format!("getting {path}"), response).await)
    }

    /// `HeadObject`
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `level` - The level asked for, refused unless it is the default, as for a get
    async fn stat(&self, dataset: &ObjectDataset, path: &str, level: ReadLevel) -> Result<(), EndpointError> {
        // as a get's: no level but the default reaches S3
        if level != ReadLevel::Default {
            return Err(EndpointError::no_level(level));
        }
        let response = self.send(Method::HEAD, &self.object(dataset, path), None).await?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(Self::refused(&format!("heading {path}"), response).await)
    }

    /// `DeleteObject`, which S3 answers alike whether or not anything was there
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    async fn delete(&self, dataset: &ObjectDataset, path: &str) -> Result<(), EndpointError> {
        let response = self.send(Method::DELETE, &self.object(dataset, path), None).await?;
        if response.status().is_success() {
            let _ = response.bytes().await;
            return Ok(());
        }
        Err(Self::refused(&format!("deleting {path}"), response).await)
    }
}

#[cfg(test)]
mod tests {
    use super::{classify, error_code, S3Endpoint, S3Settings};
    use crate::objects::endpoint::{ErrorKind, ObjectEndpoint};
    use crate::objects::sigv4::{Credentials, Signer};
    use crate::objects::{Description, ObjectDataset, SizeDistribution, GENERATOR};
    use crate::spec::ReadLevel;
    use bytes::Bytes;
    use reqwest::StatusCode;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// The key the test server checks every signature against
    fn signer() -> Arc<Signer> {
        Arc::new(Signer::new(
            Credentials {
                access_key: "bench".to_string(),
                secret_key: "a secret".to_string(),
            },
            "us-east-1",
        ))
    }

    /// A dataset of a bucket named `Posters`
    fn dataset() -> ObjectDataset {
        let description = Description {
            objects: 8,
            sizes: SizeDistribution::Fixed(4096),
            seed: 1,
            generator: GENERATOR.to_string(),
        };
        ObjectDataset {
            bucket: "Posters".to_string(),
            path: "Posters.yaml".into(),
            digest: description.digest(),
            description,
        }
    }

    /// An endpoint of the test server, putting `Posters` in `posters`
    ///
    /// # Arguments
    ///
    /// * `address` - The server
    fn endpoint(address: &str) -> S3Endpoint {
        let buckets = BTreeMap::from([("Posters".to_string(), "posters".to_string())]);
        S3Endpoint::new(address, signer(), buckets, S3Settings::default()).unwrap()
    }

    /// A bucket is made once and heads through the endpoint, and every operation round trips,
    /// each signed for the host it was sent to
    #[tokio::test]
    async fn a_round_trip_through_a_local_server() {
        let store = super::super::s3_local::LocalStore::start(signer()).await;
        let endpoint = endpoint(&store.address);
        let dataset = dataset();
        endpoint.create_bucket("posters", None).await.unwrap();
        endpoint.head_bucket("posters").await.unwrap();
        // a put, then its get, head and delete
        let bytes = Bytes::from(crate::objects::make(&dataset, None, 3));
        endpoint.put(&dataset, "sd/0a/0b/0003", bytes.clone()).await.unwrap();
        assert_eq!(endpoint.get(&dataset, "sd/0a/0b/0003", ReadLevel::Default).await.unwrap(), bytes);
        endpoint.stat(&dataset, "sd/0a/0b/0003", ReadLevel::Default).await.unwrap();
        endpoint.delete(&dataset, "sd/0a/0b/0003").await.unwrap();
        // and nothing there afterwards, a miss to a get and a head alike
        assert_eq!(endpoint.get(&dataset, "sd/0a/0b/0003", ReadLevel::Default).await.unwrap_err().kind, ErrorKind::NotFound);
        assert_eq!(endpoint.stat(&dataset, "sd/0a/0b/0003", ReadLevel::Default).await.unwrap_err().kind, ErrorKind::NotFound);
        // a read level S3 cannot name is refused by name, and nothing is sent for it (F95)
        let sent = store.requests().len();
        let refused = endpoint.get(&dataset, "sd/0a/0b/0003", ReadLevel::One).await.unwrap_err();
        assert_eq!(refused.code, "Unsupported", "{refused:?}");
        let refused = endpoint.stat(&dataset, "sd/0a/0b/0003", ReadLevel::Quorum).await.unwrap_err();
        assert_eq!(refused.code, "Unsupported", "{refused:?}");
        assert_eq!(store.requests().len(), sent);
        // every request went path style to the bucket the dataset is mapped to
        let seen = store.requests();
        assert!(seen.contains(&"PUT /posters/sd/0a/0b/0003".to_string()), "{seen:?}");
        assert!(!endpoint.writes_in_place());
    }

    /// A bucket that is there already is refused, since a run starts on an empty one
    #[tokio::test]
    async fn an_existing_bucket_is_refused() {
        let store = super::super::s3_local::LocalStore::start(signer()).await;
        let endpoint = endpoint(&store.address);
        endpoint.create_bucket("posters", None).await.unwrap();
        let error = endpoint.create_bucket("posters", None).await.unwrap_err();
        assert_eq!(error.code, "409 BucketAlreadyOwnedByYou");
        // a bucket nobody made does not head
        assert!(endpoint.head_bucket("missing").await.is_err());
    }

    /// A wrong key is refused by the store's code, a key not found is a miss and a bucket not
    /// found is not, a slow down is retriable, and a stopped store is a connection failure
    #[tokio::test]
    async fn error_codes_are_classified() {
        let store = super::super::s3_local::LocalStore::start(signer()).await;
        let wrong = S3Endpoint::new(
            &store.address,
            Arc::new(Signer::new(
                Credentials {
                    access_key: "bench".to_string(),
                    secret_key: "another".to_string(),
                },
                "us-east-1",
            )),
            BTreeMap::new(),
            S3Settings::default(),
        )
        .unwrap();
        let refused = wrong.put(&dataset(), "x", Bytes::from_static(b"x")).await.unwrap_err();
        assert_eq!((refused.kind, refused.code.as_str()), (ErrorKind::Other, "403 SignatureDoesNotMatch"));
        assert_eq!(error_code(b"<Error><Code>NoSuchKey</Code></Error>").as_deref(), Some("NoSuchKey"));
        assert_eq!(classify(StatusCode::NOT_FOUND, Some("NoSuchKey")), ErrorKind::NotFound);
        assert_eq!(classify(StatusCode::NOT_FOUND, None), ErrorKind::NotFound);
        assert_eq!(classify(StatusCode::NOT_FOUND, Some("NoSuchBucket")), ErrorKind::Other);
        assert_eq!(classify(StatusCode::SERVICE_UNAVAILABLE, Some("SlowDown")), ErrorKind::Retriable);
        assert_eq!(classify(StatusCode::FORBIDDEN, Some("AccessDenied")), ErrorKind::Other);
        // a port nobody listens on is a connection failure
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let error = endpoint(&closed).stat(&dataset(), "x", ReadLevel::Default).await.unwrap_err();
        assert_eq!(error.code, "Connect", "{error:?}");
        // and an endpoint with a path is refused, since a bucket's path is the endpoint's to make
        assert!(S3Endpoint::new("http://127.0.0.1:9000/bucket", signer(), BTreeMap::new(), S3Settings::default()).is_err());
    }
}
