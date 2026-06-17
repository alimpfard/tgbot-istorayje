import hashlib
import hmac
import urllib.parse
from datetime import datetime, timezone
from os import environ

import requests

ENDPOINT = environ.get("S3_ENDPOINT", "https://s3.cxbyte.me").rstrip("/")
BUCKET = environ.get("S3_BUCKET", "bucket")
REGION = environ.get("S3_REGION", "us-east-1")  # server doesn't care
ACCESS_KEY_ID = environ.get("S3_ACCESS_KEY_ID", "")
SECRET_KEY = environ.get("S3_SECRET_KEY", "")
SERVICE = "s3"

_CONNECT_HOST = urllib.parse.urlparse(ENDPOINT).netloc
SIGN_HOST = environ.get("S3_SIGN_HOST", _CONNECT_HOST)


def enabled():
    return bool(ACCESS_KEY_ID and SECRET_KEY and _CONNECT_HOST)


def _now():
    return datetime.now(timezone.utc)


def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _signing_key(datestamp):
    k = _sign(("AWS4" + SECRET_KEY).encode(), datestamp)
    k = _sign(k, REGION)
    k = _sign(k, SERVICE)
    return _sign(k, "aws4_request")


def _canonical_uri(key):
    # each path segment percent-encoded, slashes preserved
    return "/" + BUCKET + "/" + urllib.parse.quote(key, safe="/~")


def put_object(key, data, content_type="application/octet-stream"):
    """Upload bytes to BUCKET/key.  Raises on failure."""
    now = _now()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(data).hexdigest()
    canonical_uri = _canonical_uri(key)

    headers = {
        "content-type": content_type,
        "host": SIGN_HOST,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))

    canonical_request = "\n".join([
        "PUT",
        canonical_uri,
        "",
        canonical_headers,
        signed_headers,
        payload_hash,
    ])
    credential_scope = f"{datestamp}/{REGION}/{SERVICE}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode()).hexdigest(),
    ])
    signature = hmac.new(
        _signing_key(datestamp), string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY_ID}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    headers["authorization"] = authorization

    resp = requests.put(
        ENDPOINT + canonical_uri, data=data, headers=headers, timeout=30
    )
    resp.raise_for_status()
    return key


def put_object_stream(key, body, content_type="application/octet-stream",
                      content_length=None):
    """Stream `body` (a file-like / iterable, e.g. requests' resp.raw) straight
    to BUCKET/key without buffering it in memory.  Uses UNSIGNED-PAYLOAD so we
    never have to hash the full body.  Raises on failure."""
    now = _now()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    payload_hash = "UNSIGNED-PAYLOAD"
    canonical_uri = _canonical_uri(key)

    headers = {
        "content-type": content_type,
        "host": SIGN_HOST,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))

    canonical_request = "\n".join([
        "PUT",
        canonical_uri,
        "",
        canonical_headers,
        signed_headers,
        payload_hash,
    ])
    credential_scope = f"{datestamp}/{REGION}/{SERVICE}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode()).hexdigest(),
    ])
    signature = hmac.new(
        _signing_key(datestamp), string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY_ID}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    # Content-Length isn't signed; forward it when known so we avoid chunked TE.
    if content_length is not None:
        headers["content-length"] = str(content_length)

    resp = requests.put(
        ENDPOINT + canonical_uri, data=body, headers=headers, timeout=60
    )
    resp.raise_for_status()
    return key


def presigned_get_url(key, expires=900):
    """Return a query-signed GET URL valid for `expires` seconds (default 15m)."""
    now = _now()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    credential_scope = f"{datestamp}/{REGION}/{SERVICE}/aws4_request"
    canonical_uri = _canonical_uri(key)

    query = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{ACCESS_KEY_ID}/{credential_scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(expires),
        "X-Amz-SignedHeaders": "host",
    }
    canonical_querystring = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(query.items())
    )
    canonical_request = "\n".join([
        "GET",
        canonical_uri,
        canonical_querystring,
        f"host:{SIGN_HOST}\n",
        "host",
        "UNSIGNED-PAYLOAD",
    ])
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode()).hexdigest(),
    ])
    signature = hmac.new(
        _signing_key(datestamp), string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    return (
        f"{ENDPOINT}{canonical_uri}?{canonical_querystring}"
        f"&X-Amz-Signature={signature}"
    )
