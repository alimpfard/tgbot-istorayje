import ast
import builtins as _builtins_module
from os import environ

import astor
import re
import requests
import json
from telegram import (
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineQueryResultPhoto,
    InlineQueryResultGif,
    InlineQueryResultVoice,
    InlineQueryResultAudio,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from uuid import uuid4, UUID
import urllib.parse
from PIL import Image, ImageDraw, ImageFont

from html.parser import HTMLParser
from lxml import html as xhtml
from flask import Flask, request, make_response
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
from type import checked_as

import s3store
import extern
import audiocodec
import xxhash
import base64
import io
import textwrap
import time as _time
import hmac
import hashlib
import secrets as _secrets
import gc as _gc
import ctypes as _ctypes
import ctypes.util as _ctypes_util

Image.MAX_IMAGE_PIXELS = 24_000_000

try:
    _libc = _ctypes.CDLL(_ctypes_util.find_library("c") or "libc.so.6")
    _libc.malloc_trim.argtypes = [_ctypes.c_size_t]
    _libc.malloc_trim.restype = _ctypes.c_int
except (OSError, AttributeError):
    _libc = None


def release_memory():
    _gc.collect()
    if _libc is not None:
        try:
            _libc.malloc_trim(0)
        except Exception:
            pass


PROXY_SECRET = environ.get("PROXY_SECRET") or _secrets.token_hex(32)
PROXY_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def proxy_sign(encoded_url):
    return hmac.new(
        PROXY_SECRET.encode(), encoded_url.encode(), hashlib.sha256
    ).hexdigest()[:32]


def proxy_verify(encoded_url, sig):
    if not sig or len(sig) != 32:
        return False
    return hmac.compare_digest(proxy_sign(encoded_url), sig)


VAR_STORE = {}

# --- Sandbox safety infrastructure ---

ALLOWED_IMPORT_MODULES = frozenset({
    # Modules actually used in existing API definitions
    'urllib', 're', 'random', 'pint', 'bs4', 'lxml',
    # Safe utility modules
    'json', 'math', 'string', 'collections', 'itertools', 'functools',
    'datetime', 'decimal', 'fractions', 'operator',
    'html', 'xml', 'base64', 'hashlib', 'hmac',
    'textwrap', 'unicodedata', 'difflib',
})

_real_import = _builtins_module.__import__


def _safe_import(name, *args, **kwargs):
    """Restricted __import__ that only allows whitelisted modules."""
    top_level = name.split('.')[0]
    if top_level not in ALLOWED_IMPORT_MODULES:
        raise ImportError(
            f"importing '{name}' is not allowed in API definitions; "
            f"allowed modules: {', '.join(sorted(ALLOWED_IMPORT_MODULES))}"
        )
    return _real_import(name, *args, **kwargs)


SAFE_BUILTINS = {
    '__import__': _safe_import,
    'abs': abs, 'all': all, 'any': any, 'bool': bool, 'chr': chr,
    'dict': dict, 'enumerate': enumerate, 'filter': filter, 'float': float,
    'format': format, 'frozenset': frozenset, 'hasattr': hasattr, 'hash': hash,
    'int': int, 'isinstance': isinstance, 'issubclass': issubclass,
    'iter': iter, 'len': len, 'list': list, 'map': map, 'max': max,
    'min': min, 'next': next, 'ord': ord, 'pow': pow, 'print': print,
    'range': range, 'repr': repr, 'reversed': reversed, 'round': round,
    'set': set, 'slice': slice, 'sorted': sorted, 'str': str, 'sum': sum,
    'tuple': tuple, 'zip': zip,
    'True': True, 'False': False, 'None': None,
}


class SafetyValidator(ast.NodeVisitor):
    """Validates that an AST does not contain dangerous constructs at define-time."""

    FORBIDDEN_NAMES = frozenset({
        '__builtins__', '__loader__', '__spec__',
        '__build_class__', '__name__', '__file__',
        'exec', 'eval', 'compile', 'open', 'breakpoint',
        'getattr', 'setattr', 'delattr',
        'globals', 'locals', 'vars',
        'exit', 'quit', 'input',
    })

    ALLOWED_DUNDERS = frozenset({
        '__import__',
    })

    def visit_Import(self, node):
        raise ValueError("import statements are not allowed in API definitions")

    def visit_ImportFrom(self, node):
        raise ValueError("import statements are not allowed in API definitions")

    def visit_Attribute(self, node):
        if node.attr.startswith('__') and node.attr.endswith('__'):
            raise ValueError(
                f"dunder attribute access '{node.attr}' is not allowed in API definitions"
            )
        self.generic_visit(node)

    def visit_Name(self, node):
        if node.id in self.FORBIDDEN_NAMES:
            raise ValueError(
                f"access to '{node.id}' is not allowed in API definitions"
            )
        if node.id.startswith('__') and node.id.endswith('__') and node.id not in self.ALLOWED_DUNDERS:
            raise ValueError(
                f"dunder name '{node.id}' is not allowed in API definitions"
            )
        self.generic_visit(node)


_safety_validator = SafetyValidator()


def validate_safety(body_source, iotype, name):
    """Validate that a body expression contains no dangerous constructs."""
    body_ast = ast.parse(body_source, f"{iotype}:{name}.safety", "eval")
    _safety_validator.visit(body_ast)


def subv(xbody, iotype, name):
    xast = ast.parse(xbody, f"{iotype}:{name}.body", "single").body[0]
    if isinstance(xast, ast.Assign):
        if (
            isinstance(xast.value, ast.Compare)
            and len(xast.value.ops) == 1
            and isinstance(xast.value.ops[0], ast.In)
        ):
            # x = y in z -> (lambda x: z)(y)
            rast = checked_as(
                ast.parse("x(y)", f"{iotype}:{name}.replacement_body", "eval").body,
                ast.Call,
            )
            targets = xast.targets[0]
            if isinstance(targets, ast.Tuple):
                targets = targets.elts
            else:
                targets = [targets]
            targets = [checked_as(t, ast.Name).id for t in targets]

            # a, b = (y0, y1) in z -> (lambda a, b: z)(*(y0, y1))
            rast.func = ast.Lambda(
                ast.arguments(
                    posonlyargs=[],
                    args=[ast.arg(x, None) for x in targets],
                    vararg=None,
                    kwonlyargs=[],
                    kw_defaults=[],
                    kwarg=None,
                    defaults=[],
                ),
                xast.value.comparators[0],
            )
            rast.args = [
                xast.value.left
                if len(targets) == 1
                else ast.Starred(xast.value.left, ast.Load())
            ]
            xast = rast
    return xast


def to_json(x):
    return json.dumps(x)


def suppress_exceptions(f):
    try:
        return f()
    except:
        return None


def get_source_query(x):
    return getattr(x, "__source_query__", None)

class NextStep:
    def __init__(self, obj, query):
        self.obj = obj
        self.query = query


def set_next_step(x, query):
    # query must be either a string, or a dict of "button text" -> query.
    if not isinstance(query, (str, dict)):
        raise Exception("nextStep argument must be either string or dict")
    if isinstance(query, dict):
        for k in query:
            if not isinstance(k, str) or not isinstance(query[k], str):
                raise Exception("nextStep dict must be string -> string")

    return NextStep(x, query)

class InternalPhoto:
    def __init__(self, url, thumb_url=None, caption=None):
        self.url = url
        self.caption = caption
        self.thumb_url = thumb_url if thumb_url is not None else url


class Blob(bytes):
    """Raw response bytes from a */bytes comm type, with the Content-Type attached."""

    content_type = "application/octet-stream"

    def __new__(cls, data, content_type=None):
        self = super().__new__(cls, data)
        if content_type:
            self.content_type = content_type.split(";")[0].strip()
        return self


class InternalAudio:
    def __init__(self, data=None, url=None, content_type=None, caption=None,
                 title=None, voice=True, headers=None):
        if data is None and url is None:
            raise Exception("@audio needs either audio bytes or a url")
        self.data = data
        self.url = url
        self.content_type = content_type
        self.caption = caption
        self.title = title
        self.voice = voice
        self.headers = headers


_AUDIO_MAGIC = (
    (b"OggS", "audio/ogg"),
    (b"RIFF", "audio/wav"),
    (b"ID3", "audio/mpeg"),
    (b"\xff\xfb", "audio/mpeg"),
    (b"\xff\xf3", "audio/mpeg"),
    (b"\xff\xf2", "audio/mpeg"),
    (b"fLaC", "audio/flac"),
)


def sniff_audio_type(data, hint=None):
    if hint and hint.startswith("audio/"):
        return hint
    for magic, ct in _AUDIO_MAGIC:
        if data[: len(magic)] == magic:
            return ct
    if data[4:8] == b"ftyp":
        return "audio/mp4"
    return hint or "application/octet-stream"


_AUDIO_MAX_BYTES = 20 * 1024 * 1024


def construct_audio(obj):
    """expr @audio -> InternalAudio.  Accepts raw bytes (wav/ogg/mp3/...),
    a data:audio/... URI, an http(s) url, or a dict of
    {url|data, caption?, title?, voice?, headers?}."""
    if isinstance(obj, (bytes, bytearray, memoryview)):
        data = bytes(obj)
        return InternalAudio(data=data, content_type=sniff_audio_type(
            data, getattr(obj, "content_type", None)))
    if isinstance(obj, str):
        if obj.startswith("data:"):
            header, data = obj.split(",", 1)
            raw = base64.b64decode(data) if ";base64" in header else urllib.parse.unquote_to_bytes(data)
            hint = header[5:].split(";")[0] or None
            return InternalAudio(data=raw, content_type=sniff_audio_type(raw, hint))
        if obj.startswith(("http://", "https://")):
            return InternalAudio(url=obj)
        raise Exception("@audio string must be a data: URI or an http(s) url")
    if isinstance(obj, dict):
        kwargs = dict(obj)
        src = kwargs.pop("data", None)
        if src is None:
            src = kwargs.pop("bytes", None)
        url = kwargs.pop("url", None)
        if src is None and url is None:
            raise Exception("@audio dict needs a 'url' or 'data' key")
        base = construct_audio(src if src is not None else url)
        base.caption = kwargs.get("caption", base.caption)
        base.title = kwargs.get("title", base.title)
        base.voice = bool(kwargs.get("voice", True))
        base.headers = kwargs.get("headers", base.headers)
        return base
    raise Exception("Invalid kind for @audio " + str(type(obj)))


def _fetch_audio(url, headers=None):
    req_headers = {"User-Agent": PROXY_BROWSER_UA}
    if headers:
        req_headers.update(headers)
    resp = requests.get(url, headers=req_headers, stream=True, timeout=30, allow_redirects=True)
    try:
        resp.raise_for_status()
        buf = io.BytesIO()
        for chunk in resp.iter_content(chunk_size=65536):
            buf.write(chunk)
            if buf.tell() > _AUDIO_MAX_BYTES:
                raise Exception("audio too large (>20MB)")
        data = buf.getvalue()
        return data, sniff_audio_type(data, (resp.headers.get("Content-Type") or "").split(";")[0].strip())
    finally:
        resp.close()


def telegram_audio_url(audio: InternalAudio):
    """Resolve an InternalAudio to (public url, content type) telegram can consume.
    Voice notes must be ogg/opus, audio must be mp3/m4a; anything else goes
    through the sidecar's ffmpeg."""
    if audio.data is not None:
        data, ct = audio.data, audio.content_type or sniff_audio_type(audio.data)
    else:
        data, ct = _fetch_audio(audio.url, audio.headers)

    if audio.voice:
        want = ("audio/ogg",)
        fmt = "ogg"
    else:
        want = ("audio/mpeg", "audio/mp4")
        fmt = "mp3"
    if ct not in want:
        if fmt == "ogg" and ct in ("audio/wav", "audio/x-wav", "audio/wave") and audiocodec.available():
            # wav -> ogg/opus in-process (libopus via ctypes, hand-rolled ogg)
            data = audiocodec.wav_to_ogg_opus(data, voice=True)
        else:
            # anything else (mp3/flac/m4a inputs, or mp3 output) needs ffmpeg,
            # which only the sidecar has
            data = extern.transcode_audio(data, fmt)
            if not data:
                raise Exception(f"could not transcode {ct} to {fmt}")
        ct = "audio/ogg" if fmt == "ogg" else "audio/mpeg"

    if not s3store.enabled():
        raise Exception("audio results need S3 to be configured")
    url = s3_key_url(s3_store_bytes(data, ct))
    if not url:
        raise Exception("audio results need APP_URL to be set")
    return url, ct


def render_text_card(text, bg, fg, size=512, padding=32):
    text = str(text) if text is not None else ''
    image = Image.new('RGB', (size, size), bg)
    draw = ImageDraw.Draw(image)
    max_width = size - 2 * padding
    max_height = size - 2 * padding

    def wrap(font):
        # estimate chars-per-line from a representative glyph width
        sample = draw.textlength('abcdefghijklmnopqrstuvwxyz', font=font) / 26 or 1
        cols = max(1, int(max_width / sample))
        lines = []
        for paragraph in text.split('\n'):
            if not paragraph:
                lines.append('')
                continue
            lines.extend(textwrap.wrap(
                paragraph, width=cols,
                break_long_words=True, break_on_hyphens=True,
                replace_whitespace=False, drop_whitespace=True,
            ) or [''])
        return lines

    def measure(font, lines):
        ascent, descent = font.getmetrics()
        line_height = ascent + descent
        widest = max((draw.textlength(line, font=font) for line in lines), default=0)
        return widest, line_height * len(lines), line_height

    lo, hi, best = 8, 200, None
    while lo <= hi:
        mid = (lo + hi) // 2
        font = ImageFont.load_default(size=mid)
        lines = wrap(font)
        width, height, line_height = measure(font, lines)
        if width <= max_width and height <= max_height:
            best = (font, lines, line_height)
            lo = mid + 1
        else:
            hi = mid - 1

    if best is None:
        font = ImageFont.load_default(size=8)
        lines = wrap(font)
        _, _, line_height = measure(font, lines)
    else:
        font, lines, line_height = best

    total_height = line_height * len(lines)
    y = (size - total_height) // 2
    for line in lines:
        line_width = draw.textlength(line, font=font)
        x = (size - line_width) // 2
        draw.text((x, y), line, font=font, fill=fg)
        y += line_height

    return image


def proxy_image_url(url, headers=None):
    if not isinstance(url, str):
        return url
    if not (url.startswith("http://") or url.startswith("https://")):
        return url
    app_url = environ.get("APP_URL")
    if not app_url:
        return url
    base = app_url.rstrip('/')
    if url.startswith(base + "/proxy-url/"):
        return url
    encoded = urllib.parse.quote(url, safe='')
    hdr = '/' + urllib.parse.quote(json.dumps(headers), safe='') if headers else ''
    return f"{base}/proxy-url/original/{proxy_sign(encoded)}/{encoded}{hdr}"


_S3_IMG_CACHE: dict[str, tuple[str, float]] = {}
_S3_REUPLOAD_AFTER = 90 * 60
_S3_CACHE_CAP = 1024
_S3_CT_EXT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "audio/ogg": ".ogg",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/wav": ".wav",
}


_S3_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="s3img")
_S3_CACHE_LOCK = Lock()


def _s3_remember(url, key, now):
    with _S3_CACHE_LOCK:
        if len(_S3_IMG_CACHE) >= _S3_CACHE_CAP:
            _S3_IMG_CACHE.pop(next(iter(_S3_IMG_CACHE)), None)
        _S3_IMG_CACHE[url] = (key, now)


def _s3_cached_key(url):
    now = _time.monotonic()
    with _S3_CACHE_LOCK:
        ent = _S3_IMG_CACHE.get(url)
        if ent and now - ent[1] < _S3_REUPLOAD_AFTER:
            return ent[0]
    return None


def s3_stream_store(url, headers=None):
    cached = _s3_cached_key(url)
    if cached:
        return cached
    req_headers = {"User-Agent": PROXY_BROWSER_UA}
    if headers:
        req_headers.update(headers)
    resp = requests.get(
        url, headers=req_headers, stream=True, timeout=30, allow_redirects=True
    )
    try:
        resp.raise_for_status()
        ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
        ct = ct or "application/octet-stream"
        key = f"images/{xxhash.xxh64(url.encode()).hexdigest()}{_S3_CT_EXT.get(ct, '')}"
        s3store.put_object_stream(key, resp.iter_content(chunk_size=65536), ct)
    finally:
        resp.close()
    _s3_remember(url, key, _time.monotonic())
    return key


def s3_presign_source(url, headers=None):
    return s3store.presigned_get_url(s3_stream_store(url, headers), expires=900)


def s3_store_bytes(data, content_type="image/jpeg"):
    key = f"images/gen-{xxhash.xxh64(data).hexdigest()}{_S3_CT_EXT.get(content_type, '')}"
    if _s3_cached_key(key):
        return key
    s3store.put_object_stream(
        key, io.BytesIO(data), content_type, content_length=len(data)
    )
    _s3_remember(key, key, _time.monotonic())
    return key


def s3_source_url(source, headers=None, eager=False):
    if not (isinstance(source, str) and source.startswith(("http://", "https://"))):
        return None
    if not s3store.enabled():
        return None
    app_url = environ.get("APP_URL")
    if not app_url:
        return None
    if eager:
        _S3_EXECUTOR.submit(s3_stream_store, source, headers)
    base = app_url.rstrip('/')
    encoded = urllib.parse.quote(source, safe='')
    hdr = '/' + urllib.parse.quote(json.dumps(headers), safe='') if headers else ''
    return f"{base}/s3img/u/{proxy_sign(encoded)}/{encoded}{hdr}"


def s3_key_url(key):
    app_url = environ.get("APP_URL")
    if not app_url:
        return None
    base = app_url.rstrip('/')
    encoded = urllib.parse.quote(key, safe='')
    return f"{base}/s3img/k/{proxy_sign(encoded)}/{encoded}"


def construct_image(obj):
    if isinstance(obj, str):
        if obj.startswith("data:image/"):
            # Actually not a URL, but a base64-encoded image
            # Parse it and present as Image
            # Extract the base64 data from the data URL
            _, data = obj.split(',', 1)
            # Decode the base64 data
            image_data = base64.b64decode(data)
            # Create a PIL Image from the decoded data
            image = Image.open(io.BytesIO(image_data))
            return image
        # A bare @image is its own thumbnail in the inline list, so eager-load it.
        return InternalPhoto(s3_source_url(obj, eager=True) or proxy_image_url(obj))
    if isinstance(obj, dict):
        if 'text' in obj:
            return render_text_card(obj['text'], obj.get('bg', '#000000'), obj.get('fg', '#ffffff'))
        kwargs = dict(obj)
        headers = None
        if 'headers' in kwargs:
            headers = kwargs['headers']
            del kwargs['headers']
        if 'url' in kwargs:
            kwargs['url'] = (
                s3_source_url(kwargs['url'], headers, eager=False)
                or proxy_image_url(kwargs['url'], headers)
            )
        if kwargs.get('thumb_url'):
            kwargs['thumb_url'] = (
                s3_source_url(kwargs['thumb_url'], headers, eager=True)
                or proxy_image_url(kwargs['thumb_url'], headers)
            )
        return InternalPhoto(**kwargs)
    raise Exception("Invalid kind for @image " + str(type(obj)))


def get_var_store(x):
    global VAR_STORE
    if not isinstance(x, str):
        raise Exception("varStore argument must be string")
    if x not in VAR_STORE:
        return None
    return VAR_STORE[x]

def set_var_store(value):
    global VAR_STORE
    x = uuid4().hex
    VAR_STORE[x] = value
    return x

def from_json(x):
    return DotDict({"x": json.loads(x)}).x


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.fed = []

    def handle_data(self, data):
        self.fed.append(data)

    def get_data(self):
        return "".join(self.fed)


def strip_tags(html):
    if not html:
        return "[nothing here]"
    s = MLStripper()
    s.feed(html)
    x = s.get_data()
    return x.strip()


DEFAULT_DEBOUNCE_SECONDS = 0.8


class TypeCastTransformationVisitor(ast.NodeTransformer):
    def __init__(self):
        self.start()

    def start(self):
        self.uses = {
            "json": False,
            "query": False,
            "varstore": False,
            "audio": False,
            "debounce": None,
        }
        return self

    def visit_BinOp(self, node: ast.BinOp):
        if isinstance(node.op, ast.MatMult):
            if isinstance(node.right, ast.Name):
                self.generic_visit(node)
                # x @ty -> transform
                if node.right.id.lower() == "json":
                    self.uses["json"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_to_json"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "image":
                    self.uses["query"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_construct_image"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "audio":
                    self.uses["audio"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_construct_audio"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "debounce":
                    # x @debounce -> x, but the adapter is marked as debounced
                    self.uses["debounce"] = DEFAULT_DEBOUNCE_SECONDS
                    return node.left
                elif node.right.id.lower() == "varstore":
                    # uuid @ varStore -> value
                    self.uses["varstore"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_get_var_store"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "freshvar":
                    # x @ freshvar -> (uuid)
                    self.uses["varstore"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_set_var_store"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "query":
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_get_source_query"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "catch":
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_suppress_exceptions"),
                            args=[
                                ast.copy_location(
                                    ast.Lambda(
                                        args=ast.arguments(
                                            args=[],
                                            posonlyargs=[],
                                            vararg=None,
                                            kwonlyargs=[],
                                            kw_defaults=[],
                                            kwarg=None,
                                            defaults=[],
                                        ),
                                        body=node.left,
                                    ),
                                    node,
                                )
                            ],
                            keywords=[],
                        ),
                        node,
                    )
            elif isinstance(node.right, ast.Call) and isinstance(
                node.right.func, ast.Name
            ):
                self.generic_visit(node)
                # x @ f(...) -> transform
                if node.right.func.id.lower() == "debounce":
                    # x @debounce(secs) -> x, adapter marked as debounced by `secs`
                    if len(node.right.args) != 1 or node.right.keywords:
                        raise Exception(
                            f"debounce expects exactly one argument, got {len(node.right.args)}"
                        )
                    secs = node.right.args[0]
                    if not isinstance(secs, ast.Constant) or isinstance(secs.value, bool) \
                            or not isinstance(secs.value, (int, float)):
                        raise Exception("debounce argument must be a numeric literal (seconds)")
                    if not 0 < secs.value <= 30:
                        raise Exception("debounce must be in (0, 30] seconds")
                    self.uses["debounce"] = float(secs.value)
                    return node.left
                if node.right.func.id.lower() == "nextstep":
                    # x#nextStep(query) -> set_next_step(x, query)
                    if len(node.right.args) != 1:
                        raise Exception(
                            f"nextStep expects exactly one argument, got {len(node.right.args)}"
                        )
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("set_next_step"),
                            args=[node.left, node.right.args[0]],
                            keywords=[],
                        ),
                        node,
                    )
            # not a type, visit it
            self.generic_visit(node)
        else:
            # not a name, visit it
            self.generic_visit(node)

        return node


class DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, dct):
        for key, value in dct.items():
            if hasattr(value, "keys"):
                value = DotDict(value)
            elif isinstance(value, list):
                value = [DotDict({"a": x}).a for x in value]
            self[key] = value


def _api_headers(api):
    name = f"{str(api).upper()}_HEADERS"
    raw = environ.get(name)
    if not raw:
        print(f"api {api!r}: no {name} in env, sending no custom headers")
        return None
    try:
        h = json.loads(raw)
    except Exception as e:
        print(f"WARNING: {name} is set but is not valid JSON ({e}); "
              f"sending no custom headers. value[:120]={raw[:120]!r}")
        return None
    if not isinstance(h, dict):
        print(f"WARNING: {name} must be a JSON object of header->value; "
              f"got {type(h).__name__}; sending no custom headers.")
        return None
    return h


def _bytes_or_raise(res, api, sent_headers):
    if not res.ok:
        ct = res.headers.get("Content-Type")
        raise Exception(
            f"API {api!r} returned {res.status_code} {res.reason}: url={res.url!r}, "
            f"sent headers={sorted(sent_headers or {})}, "
            f"content-type={ct!r}, body[:200]={res.text[:200]!r}"
        )
    return Blob(res.content, res.headers.get("Content-Type"))


def _json_or_raise(res, api):
    try:
        return DotDict({"x": res.json()}).x
    except ValueError as e:
        ct = res.headers.get("Content-Type")
        raise Exception(
            f"API {api!r} returned non-JSON: status={res.status_code}, "
            f"content-type={ct!r}, url={res.url!r}, body[:200]={res.text[:200]!r}"
        ) from e


class APIHandler(object):
    def __init__(self, bot):
        self.visitor = TypeCastTransformationVisitor()
        self.bot = bot
        self.input_adapters: dict = {}
        self.output_adapters: dict = {}
        self.apis = None
        self.load()
        self.res = {}
        self.ios = {
            "input": self.input_adapters,
            "output": self.output_adapters,
        }
        self.comms = (
            "graphql",
            "json/post",
            "html/xpath",
            "http/link",
            "http/json",
            "lit.http/json",
            "http/bytes",
            "json/post/bytes",
            "identity",
        )
        self.metavarre = re.compile(r"(?!\\)\$([\w:]+)")
        self.page_re = re.compile(
            r"(?!\\)\#page\[(\w+)\]\((.*)\)"
        )  # #page[var](...#var...)
        self.page_var_re = lambda var: re.compile(rf"(?!\\)\#{var}\b")  # #var

    def flush(self):
        self.bot.db.db.external_apis.update_one(
            {"kind": "api"}, {"$set": {"data": self.apis}}, upsert=True
        )
        self.bot.db.db.external_apis.update_one(
            {"kind": "input"}, {"$set": {"data": self.input_adapters}}, upsert=True
        )
        self.bot.db.db.external_apis.update_one(
            {"kind": "output"}, {"$set": {"data": self.output_adapters}}, upsert=True
        )

    def load(self):
        self.apis = (
            self.bot.db.db.external_apis.find_one({"kind": "api"}) or {"data": {}}
        )["data"]
        self.input_adapters = (
            self.bot.db.db.external_apis.find_one({"kind": "input"}) or {"data": {}}
        )["data"]
        self.output_adapters = (
            self.bot.db.db.external_apis.find_one({"kind": "output"}) or {"data": {}}
        )["data"]

    def gmetavarre(self, name):
        if name in self.res:
            return self.res[name]
        mre = re.compile(f"(?!\\\\)\\$({name})")
        self.res[name] = mre
        return mre

    def define(self, iotype, name, _type, vname, body):
        if name in self.ios[iotype]:
            raise Exception(f"duplicate {iotype} IO {name}")

        replacements = set()
        for metavar in self.metavarre.finditer(body):
            if metavar.group(1) != vname:
                if metavar.group(1) in self.ios[iotype]:
                    replacements.add(metavar.group(1))
                else:
                    raise Exception(
                        f"Unknown meta variable `{metavar.group(1)}` (at offset {metavar.pos})"
                    )
        xbody = self.gmetavarre(vname).sub(vname, body)

        for r in replacements:
            xbody = self.gmetavarre(r).sub(f"({self.ios[iotype][r][2]})", xbody)

        if not xbody:
            xbody = vname
        print("> ", xbody)
        xbody = astor.to_source(self.visitor.start().visit(subv(xbody, iotype, name)))
        print("> ", xbody)
        body = f"lambda {vname}: {xbody}"
        validate_safety(body, iotype, name)
        compile(body, f"{iotype}:{name}", "eval", dont_inherit=True)

        self.ios[iotype][name] = (vname, _type, body, self.visitor.uses)
        self.flush()

    def tgwrap(self, query, _type, stuff):
        def make_button(text: str, query: str):
            if query.startswith("http://") or query.startswith("https://"):
                return InlineKeyboardButton(text=text, url=query)
            return InlineKeyboardButton(text=text, switch_inline_query_current_chat=query)
        def convert_to_result(uuid: UUID, k, x, rest):
            reply_markup = None
            if isinstance(k, NextStep):
                buttons = { "Next Step": k.query } if isinstance(k.query, str) else k.query
                reply_markup = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [make_button(k, v) for k, v in buttons.items()]
                    ]
                )
                k = k.obj

            if isinstance(x, str):
                return InlineQueryResultArticle(
                    id=str(uuid),
                    title=f"result {k}",
                    input_message_content=InputTextMessageContent(
                        x,
                        parse_mode={"markdown": "Markdown", "html": "HTML"}.get(_type),
                    ),
                    thumbnail_url=None if len(rest) == 0 else rest[0],
                    reply_markup=reply_markup,
                )
            if isinstance(x, InternalPhoto):
                return InlineQueryResultPhoto(
                    id=str(uuid),
                    title=f"result {k}",
                    photo_url=x.url,
                    thumbnail_url=x.thumb_url,
                    caption=x.caption,
                    reply_markup=reply_markup,
                )
            if isinstance(x, InternalAudio):
                title = x.title or f"result {k}"
                try:
                    url, _ct = telegram_audio_url(x)
                except Exception as e:
                    print("audio result failed:", e)
                    return InlineQueryResultArticle(
                        id=str(uuid),
                        title=f"{title} - audio unavailable",
                        input_message_content=InputTextMessageContent(
                            f"{k}\n(audio unavailable: {e})"
                        ),
                        reply_markup=reply_markup,
                    )
                finally:
                    x.data = None
                    release_memory()
                if x.voice:
                    return InlineQueryResultVoice(
                        id=str(uuid),
                        title=title,
                        voice_url=url,
                        caption=x.caption,
                        reply_markup=reply_markup,
                    )
                return InlineQueryResultAudio(
                    id=str(uuid),
                    title=title,
                    audio_url=url,
                    caption=x.caption,
                    reply_markup=reply_markup,
                )
            if isinstance(x, Image.Image):
                buf = io.BytesIO()
                x.save(buf, format="JPEG")
                full_bytes = buf.getvalue()
                width, height = x.width, x.height
                x.close()
                buf = None

                try:
                    photo_url = s3_key_url(s3_store_bytes(full_bytes, "image/jpeg"))
                except Exception as e:
                    print("s3 store rendered image failed:", e)
                    photo_url = None
                full_bytes = None
                release_memory()
                if not photo_url:
                    return InlineQueryResultArticle(
                        id=str(uuid),
                        title=f"result {k}",
                        input_message_content=InputTextMessageContent(str(k)),
                        reply_markup=reply_markup,
                    )
                return InlineQueryResultPhoto(
                    id=str(uuid),
                    title=f"result {k}",
                    photo_url=photo_url,
                    thumbnail_url=photo_url,
                    photo_height=height,
                    photo_width=width,
                    caption=k,
                    reply_markup=reply_markup,
                )
            return InlineQueryResultArticle(
                id=str(uuid),
                title=f"{k} - Unknown result type",
                input_message_content=InputTextMessageContent(to_json(x)),
                reply_markup=reply_markup,
            )

        items = []
        for v in stuff:
            if isinstance(v, NextStep):
                items.append((NextStep(v.obj[0], v.query), *v.obj[1:]))
            else:
                items.append(v)

        x = [convert_to_result(uuid4(), k, x, rest) for k, x, *rest in items]
        return x

    def declare(self, name, comm_type, inp, out, path):
        if name in self.apis:
            raise Exception(f"duplicate API name {name}")

        for metavar in self.metavarre.finditer(path):
            if metavar.group(1) != "result":
                raise Exception(
                    f"Unknown meta variable `{metavar.group(1)}` (at offset {metavar.pos})"
                )

        self.apis[name] = (comm_type, inp, out, path)
        self.flush()

    def adapter(self, name, adapter, value, env=None):
        vname, _type, body, *uses = adapter
        print(vname, _type, body)
        if env is None:
            env = {}
        # Restrict builtins to prevent sandbox escapes (e.g. __import__('os').environ)
        env["__builtins__"] = SAFE_BUILTINS
        if "Image" not in env:
            env["Image"] = Image
        if "global_suppress_exceptions" not in env:
            env.update({"global_suppress_exceptions": suppress_exceptions})
        if "global_get_source_query" not in env:
            env.update({"global_get_source_query": get_source_query})
        if "set_next_step" not in env:
            env.update({"set_next_step": set_next_step})
        if len(uses) > 0:
            uses = uses[0]
            if uses:
                if "json" in uses and uses["json"]:
                    env.update({"global_to_json": to_json})
                if "query" in uses and uses["query"]:
                    env.update({"global_construct_image": construct_image})
                if "audio" in uses and uses["audio"]:
                    env.update({"global_construct_audio": construct_audio})
                if "varstore" in uses and uses["varstore"]:
                    env.update({"global_get_var_store": get_var_store, "global_set_var_store": set_var_store})

        env.update({"strip_tags": strip_tags})
        return (
            _type,
            eval(compile(body, name, "eval", dont_inherit=True), env, {})(value),
        )

    def invoke(self, api, query, extra):
        comm_type, inp, out, path = self.apis[api]
        if inp not in self.input_adapters:
            raise Exception(f"Undefined input adapter {inp}")

        if out not in self.output_adapters:
            raise Exception(f"Undefined ouput adapter {out}")

        inpv = self.input_adapters[inp]

        _, q = self.adapter(inp, inpv, query, env=extra)
        if isinstance(q, NextStep):
            # Input cannot have nextStep, remove it
            q = q.obj

        def res(path=path):
            if "page" in extra:
                path = self.page_re.sub(
                    (
                        lambda match: self.page_var_re(match.group(1)).sub(
                            str(extra["page"]), match.group(2)
                        )
                    ),
                    path,
                )
            else:
                path = self.page_re.sub("", path)

            if comm_type == "identity":
                return self.metavarre.sub(q, path)

            if comm_type == "http/link":
                path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
                return path

            if comm_type == "json/post":
                path = self.metavarre.sub(q.get("pvalue", ""), path)
                body = json.dumps(q.get("value", {}))
                res = requests.post(path, data=body, headers={"Content-Type": "application/json"})
                return _json_or_raise(res, api)

            if comm_type == "http/json":
                path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
                res = requests.get(path, headers=_api_headers(api))
                return _json_or_raise(res, api)

            if comm_type == "lit.http/json":
                path = self.metavarre.sub(q, path)
                res = requests.get(path, headers=_api_headers(api))
                return _json_or_raise(res, api)

            if comm_type == "http/bytes":
                path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
                headers = _api_headers(api)
                res = requests.get(path, headers=headers, timeout=60)
                return _bytes_or_raise(res, api, headers)

            if comm_type == "json/post/bytes":
                path = self.metavarre.sub(q.get("pvalue", ""), path)
                body = json.dumps(q.get("value", {}))
                headers = {"Content-Type": "application/json"}
                headers.update(_api_headers(api) or {})
                res = requests.post(path, data=body, headers=headers, timeout=60)
                return _bytes_or_raise(res, api, headers)

            if comm_type == "html/xpath":
                path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
                req = requests.get(path, headers=_api_headers(api))
                if req.status_code != 200:
                    raise Exception(f"{req.status_code}: {req.reason}")
                xml = xhtml.fromstring(req.content)
                return lambda x, xml=xml: xml.xpath(x)

            if comm_type == "graphql":
                req = requests.post(path, json={"query": q, "vars": {}}).json()
                return DotDict({"x": req}).x

            raise Exception(f"type {comm_type} not yet implemented")

        r = res()
        try:
            setattr(r, "__source_query__", q)
        except:
            pass
        return r

    def debounce_for(self, api):
        """Seconds the input adapter of `api` asked to debounce by, or None."""
        try:
            _, inp, _, _ = self.apis[api]
            uses = self.input_adapters[inp][3]
            return uses.get("debounce") or None
        except (KeyError, IndexError, AttributeError, ValueError):
            return None

    def render(self, api, value, extra):
        comm_type, inp, out, path = self.apis[api]

        outv = self.output_adapters[out]
        _type, q = self.adapter(out, outv, value, env=extra)
        return self.tgwrap(api, _type, q)
