"""X14's E5: an object PUT through RGW in one request and then overwritten, to see its old tail queued.

    python3 x14-s3.py <endpoint> <access> <secret> put|overwrite

The object is 6 MiB: past the 4 MiB head RGW inlines (rgw_max_chunk_size) and under boto3's 8 MiB
multipart threshold, and put with put_object, which never splits. boto3's own checksums are asked
for only when an operation requires one, so the requests are plain PUTs.
"""
import os
import sys
import time

import boto3
from botocore.config import Config


def main():
    """Make the bucket if needed, then put or overwrite one 6 MiB object, and say when."""
    endpoint, access, secret, what = sys.argv[1:5]
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access,
                      aws_secret_access_key=secret, region_name="default",
                      config=Config(request_checksum_calculation="when_required",
                                    response_checksum_validation="when_required"))
    if what == "put":
        s3.create_bucket(Bucket="x14")
    body = os.urandom(6 * 1024 * 1024)
    s3.put_object(Bucket="x14", Key="six-mib", Body=body)
    print(f"{what} x14/six-mib 6 MiB acknowledged at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")


if __name__ == "__main__":
    main()
