# F94: sign requests with botocore's own S3 SigV4 signer at a fixed time, which gives the bench's
# signer (shoal-loadgen/src/objects/sigv4.rs) vectors from AWS's implementation rather than from memory.
# AWS's documented GET Object example comes out as its page prints it (f0e8bdb8...); the other four
# are requests shaped as the bench sends them. Run with europa's botocore (1.40.72): python3 -I <this>
import datetime
from unittest import mock
import botocore.auth
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

creds = Credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
fixed = datetime.datetime(2013, 5, 24, 0, 0, 0)

class Unsigned(S3SigV4Auth):
    def _should_sha256_sign_payload(self, request):
        return False

def sign(method, url, headers, body=b"", unsigned=True):
    req = AWSRequest(method=method, url=url, headers=headers, data=body)
    auth = (Unsigned if unsigned else S3SigV4Auth)(creds, "s3", "us-east-1")
    with mock.patch.object(botocore.auth, "get_current_datetime", return_value=fixed):
        auth.add_auth(req)
    print(method, url)
    for name in ("X-Amz-Date", "X-Amz-Content-SHA256", "Authorization"):
        print("  %s: %s" % (name, req.headers[name]))

# AWS's documented GET Object example: a Range header and the empty payload's hash
sign("GET", "https://examplebucket.s3.amazonaws.com/test.txt", {"Range": "bytes=0-9"}, unsigned=False)
# what the bench sends: path style on a lab address with a port, an unsigned payload
sign("PUT", "http://172.16.2.10:9000/bench/s4k/3f/a0/00000000000004d2", {}, b"x" * 16)
sign("GET", "http://172.16.2.10:9000/bench/s1m/00/ff/0000000000000001", {})
sign("HEAD", "http://127.0.0.1:80/bench/a%24b", {})
sign("PUT", "http://titan:8080/bench", {})
