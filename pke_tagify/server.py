import os
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
