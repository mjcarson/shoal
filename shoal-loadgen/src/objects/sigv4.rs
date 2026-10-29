//! AWS Signature Version 4 for an S3 request ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! What the bench's S3 endpoint signs every request with: the header form, path-style addressing,
//! and `UNSIGNED-PAYLOAD`, so no byte of an object is hashed for its signature and a put's time is
//! the store's and its transfer's, not a SHA-256 of its body. Pure, so it is tested against
//! vectors from AWS's own signer without a store.
//!
//! # Invariants
//!
//! **The path signed is the path sent**: [`uri_encode`] is applied once, by the caller, and the
//! encoded path is both the URL's and the canonical request's. S3 never encodes a path twice.
//! **The host signed is the host the HTTP client sends**: `host`, with `:port` unless the port is
//! the scheme's default ([`signed_host`]).

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// The payload hash every request carries, so its body is never hashed
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// The algorithm a SigV4 signature names
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The service every request is signed for
const SERVICE: &str = "s3";

/// An access key and its secret
#[derive(Clone)]
pub struct Credentials {
    /// The access key's id, which every request names
    pub access_key: String,
    /// Its secret, which only the signature is made with
    pub secret_key: String,
}

impl std::fmt::Debug for Credentials {
    /// The access key, never its secret
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

/// Signs requests for one region with one key
#[derive(Debug, Clone)]
pub struct Signer {
    /// The key
    credentials: Credentials,
    /// The region every request is signed for
    region: String,
}

/// One HMAC-SHA256
///
/// # Arguments
///
/// * `key` - The key
/// * `data` - What it is taken over
fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    // an HMAC takes a key of any length
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("an HMAC takes a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Bytes as lower case hex
///
/// # Arguments
///
/// * `bytes` - The bytes
fn hex(bytes: &[u8]) -> String {
    crate::read::hex(bytes)
}

/// A path's segments encoded as S3 signs them: every byte but `A-Z a-z 0-9 - _ . ~` as `%XX`,
/// and `/` kept between segments
///
/// # Arguments
///
/// * `path` - The path, unencoded
#[must_use]
pub fn uri_encode(path: &str) -> String {
    // each byte of the utf8, kept or escaped
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                encoded.push(char::from(byte));
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

/// The host a request is signed for: the HTTP client's `Host` header, which leaves out the
/// scheme's default port
///
/// # Arguments
///
/// * `scheme` - `http` or `https`
/// * `host` - The host
/// * `port` - The port
#[must_use]
pub fn signed_host(scheme: &str, host: &str, port: u16) -> String {
    // 80 and 443 are the defaults a client leaves out
    let default = match scheme {
        "https" => 443,
        _ => 80,
    };
    if port == default {
        host.to_string()
    } else {
        format!("{host}:{port}")
    }
}

/// One request as it is signed
#[derive(Debug, Clone)]
pub struct Request<'a> {
    /// `GET`, `PUT`, `HEAD` or `DELETE`
    pub method: &'a str,
    /// The path, encoded once by [`uri_encode`]
    pub path: &'a str,
    /// The query, already in canonical form; empty for every request the bench sends
    pub query: &'a str,
    /// Every header signed besides `host`, `x-amz-content-sha256` and `x-amz-date`, each lower
    /// case and trimmed; AWS's own examples sign `range`
    pub headers: &'a [(&'a str, &'a str)],
    /// The host, as [`signed_host`] gives it
    pub host: &'a str,
    /// The payload's hash, or [`UNSIGNED_PAYLOAD`]
    pub payload: &'a str,
    /// The time, as `x-amz-date` writes it
    pub amz_date: &'a str,
}

impl Signer {
    /// A signer for one region with one key
    ///
    /// # Arguments
    ///
    /// * `credentials` - The key
    /// * `region` - The region
    #[must_use]
    pub fn new(credentials: Credentials, region: impl Into<String>) -> Self {
        Signer {
            credentials,
            region: region.into(),
        }
    }

    /// The request's canonical form, every header it signs in name order, and their names
    ///
    /// # Arguments
    ///
    /// * `request` - The request
    #[must_use]
    pub fn canonical(&self, request: &Request<'_>) -> (String, String) {
        // the headers signed, by name, each once
        let mut headers: Vec<(&str, &str)> = vec![
            ("host", request.host),
            ("x-amz-content-sha256", request.payload),
            ("x-amz-date", request.amz_date),
        ];
        headers.extend(request.headers.iter().copied());
        headers.sort_by(|a, b| a.0.cmp(b.0));
        let lines: String = headers.iter().map(|(name, value)| format!("{name}:{value}\n")).collect();
        let names: Vec<&str> = headers.iter().map(|(name, _)| *name).collect();
        let signed = names.join(";");
        // method, path, query, headers, the names signed and the payload, a line each
        let canonical = format!(
            "{}\n{}\n{}\n{lines}\n{signed}\n{}",
            request.method, request.path, request.query, request.payload
        );
        (canonical, signed)
    }

    /// The `Authorization` header that signs a request
    ///
    /// # Arguments
    ///
    /// * `request` - The request
    #[must_use]
    pub fn authorization(&self, request: &Request<'_>) -> String {
        // the scope is the request's day, its region and the service
        let date = &request.amz_date[..8];
        let scope = format!("{date}/{}/{SERVICE}/aws4_request", self.region);
        let (canonical, signed) = self.canonical(request);
        let to_sign = format!(
            "{ALGORITHM}\n{}\n{scope}\n{}",
            request.amz_date,
            hex(&Sha256::digest(canonical.as_bytes()))
        );
        // the key, derived through the scope a part at a time
        let secret = format!("AWS4{}", self.credentials.secret_key);
        let key = hmac(secret.as_bytes(), date.as_bytes());
        let key = hmac(&key, self.region.as_bytes());
        let key = hmac(&key, SERVICE.as_bytes());
        let key = hmac(&key, b"aws4_request");
        let signature = hex(&hmac(&key, to_sign.as_bytes()));
        format!(
            "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
            self.credentials.access_key
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{signed_host, uri_encode, Credentials, Request, Signer, UNSIGNED_PAYLOAD};

    /// The key AWS's own examples sign with
    fn signer() -> Signer {
        Signer::new(
            Credentials {
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            },
            "us-east-1",
        )
    }

    /// One request the bench sends, path style, at AWS's examples' time
    ///
    /// # Arguments
    ///
    /// * `method` - The method
    /// * `path` - The path, encoded
    /// * `host` - The host signed
    fn bench(method: &'static str, path: &'static str, host: &'static str) -> Request<'static> {
        Request {
            method,
            path,
            query: "",
            headers: &[],
            host,
            payload: UNSIGNED_PAYLOAD,
            amz_date: "20130524T000000Z",
        }
    }

    /// AWS's documented GET Object example signs as AWS's page and botocore 1.40.72 both sign it
    ///
    /// The request, key and signature are the S3 API reference's "Example: GET Object" for header
    /// authentication; botocore's `S3SigV4Auth` gives the same signature at the same time
    /// (`shoal-spike/results/stores-sigv4-vectors.py`), which is how the value here was checked.
    #[test]
    fn the_aws_get_object_example_signs() {
        let request = Request {
            method: "GET",
            path: "/test.txt",
            query: "",
            headers: &[("range", "bytes=0-9")],
            host: "examplebucket.s3.amazonaws.com",
            payload: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            amz_date: "20130524T000000Z",
        };
        assert_eq!(
            signer().authorization(&request),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// Every request the bench makes signs as botocore 1.40.72's S3 signer signs it with an
    /// unsigned payload: a put, a get and a head of an object, and a bucket's creation
    ///
    /// Each signature was made by `shoal-spike/results/stores-sigv4-vectors.py` at AWS's examples'
    /// time, against the address and port the request names.
    #[test]
    fn bench_requests_sign_as_botocore_signs_them() {
        let signer = signer();
        let cases = [
            (
                bench("PUT", "/bench/s4k/3f/a0/00000000000004d2", "172.16.2.10:9000"),
                "bd04e950ddf2a4bae3ccddc13f3d01133c1ef1f165f1dea4555030ea96c0c5c7",
            ),
            (
                bench("GET", "/bench/s1m/00/ff/0000000000000001", "172.16.2.10:9000"),
                "628ed3a09e8387d70afa10bf6d0dc88f722335ef4fb35691d8aebce260bc87ec",
            ),
            (
                bench("HEAD", "/bench/a%24b", "127.0.0.1"),
                "1f395d5763a9b4608761cfc0dccd1b2a06215954c70962d10fbbef57429d0c88",
            ),
            (
                bench("PUT", "/bench", "titan:8080"),
                "088052a1b547b1d8d9beab54f24877742f6f950a2436d04bc04d040c8dcd1c5e",
            ),
        ];
        for (request, signature) in cases {
            let authorization = signer.authorization(&request);
            assert!(
                authorization.ends_with(&format!("Signature={signature}")),
                "{} {}: {authorization}",
                request.method,
                request.path
            );
            assert!(authorization.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date,"));
        }
    }

    /// A path is encoded once, its slashes kept, and the host signed is what a client sends
    #[test]
    fn the_signed_host_is_what_hyper_sends() {
        // an unreserved byte is kept, and anything else escaped in upper case hex
        assert_eq!(uri_encode("/bench/s4k/3f/a0/00000000000004d2"), "/bench/s4k/3f/a0/00000000000004d2");
        assert_eq!(uri_encode("/bench/a$b c~d"), "/bench/a%24b%20c~d");
        assert_eq!(uri_encode("/é"), "/%C3%A9");
        // a scheme's default port is left out, as hyper leaves it out of Host
        assert_eq!(signed_host("http", "172.16.2.10", 80), "172.16.2.10");
        assert_eq!(signed_host("http", "172.16.2.10", 9000), "172.16.2.10:9000");
        assert_eq!(signed_host("https", "europa", 443), "europa");
        assert_eq!(signed_host("https", "europa", 80), "europa:80");
    }

    /// A key's secret is never printed
    #[test]
    fn a_secret_is_never_printed() {
        let printed = format!("{:?}", signer());
        assert!(printed.contains("AKIAIOSFODNN7EXAMPLE") && !printed.contains("wJalr"), "{printed}");
    }
}
