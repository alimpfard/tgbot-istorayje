import os
import subprocess
import traceback

from flask import Flask, Response, jsonify, request

from tagify import tagify
from video import get_frame, process

app = Flask(__name__)


@app.get("/health")
def health():
    return "ok", 200


@app.post("/tagify")
def tagify_route():
    docs = request.get_json(force=True, silent=True) or []
    if not isinstance(docs, list):
        return jsonify([]), 400
    return jsonify(tagify(docs))


@app.post("/getframe")
def getframe_route():
    data = request.get_json(force=True, silent=True) or {}
    try:
        content = get_frame(data.get("url"), data.get("format"))
    except Exception:
        traceback.print_exc()
        content = b""
    return Response(content, mimetype="image/png")


@app.post("/gifop")
def gifop_route():
    data = request.get_json(force=True, silent=True) or {}
    try:
        content = process(
            data.get("url"),
            data.get("ops") or {},
            data.get("format"),
        )
    except Exception:
        traceback.print_exc()
        content = b""
    return Response(content, mimetype="video/mp4")


_TRANSCODE = {
    # format -> (ffmpeg args, mimetype)
    "ogg": (["-c:a", "libopus", "-b:a", "48k", "-vbr", "on", "-f", "ogg"], "audio/ogg"),
    "mp3": (["-c:a", "libmp3lame", "-b:a", "128k", "-f", "mp3"], "audio/mpeg"),
}


@app.post("/transcode")
def transcode_route():
    fmt = request.args.get("format", "ogg")
    if fmt not in _TRANSCODE:
        return jsonify({"error": f"unknown format {fmt!r}"}), 400
    data = request.get_data()
    if not data:
        return jsonify({"error": "empty body"}), 400
    args, mimetype = _TRANSCODE[fmt]
    try:
        proc = subprocess.run(
            ["ffmpeg", "-v", "error", "-i", "pipe:0", "-vn", "-map_metadata", "-1", *args, "pipe:1"],
            input=data,
            capture_output=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return jsonify({"error": "ffmpeg timed out"}), 504
    if proc.returncode != 0 or not proc.stdout:
        return jsonify({"error": proc.stderr.decode(errors="replace")[-500:]}), 422
    return Response(proc.stdout, mimetype=mimetype)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
