import json
import os
import subprocess
import sys
import tempfile

import requests


def _probe(path):
    out = subprocess.run(
        [
            "ffprobe", "-v", "error", "-print_format", "json",
            "-show_streams", "-show_format", path,
        ],
        capture_output=True, check=True,
    )
    data = json.loads(out.stdout)
    v = next(s for s in data["streams"] if s["codec_type"] == "video")
    duration = float(data["format"].get("duration") or v.get("duration") or 0)
    fps = 25.0
    rate = v.get("r_frame_rate") or v.get("avg_frame_rate")
    if rate and "/" in rate:
        num, den = rate.split("/")
        if float(den):
            fps = float(num) / float(den)
    return {
        "duration": duration,
        "fps": fps,
        "width": int(v.get("width") or 0),
        "height": int(v.get("height") or 0),
    }


def _to_seconds(spec, info):
    if not spec:
        return 0.0
    value = float(spec.get("value", 0))
    unit = spec.get("unit") or "ti"
    if unit == "ti":
        return value
    if unit == "fr":
        return value / info["fps"] if info["fps"] else 0.0
    if unit == "%":
        return value / 100.0 * info["duration"]
    return value


def _esc(s):
    # escape for use in drawtext text='...'
    return (
        str(s)
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\u2019")
        .replace(",", "\\,")
    )


def _animate_step(spec, info):
    """Build a single ffmpeg filter for one animate spec, or None if unsupported."""
    start = _to_seconds(spec["frame"]["start"], info)
    length = max(_to_seconds(spec["frame"]["length"], info), 1e-3)
    end = start + length
    eff = spec["effect"]
    name = eff.get("name")
    dx = float(eff.get("dx") or 0)
    dy = float(eff.get("dy") or 0)

    if name == "text":
        text = eff.get("text") or ""
        if not text:
            return None
        size = eff.get("font-size") or 32
        color = eff.get("color") or "white"
        outline = eff.get("outline")
        bg = eff.get("background")
        shadow = eff.get("shadow")
        parts = [
            f"text='{_esc(text)}'",
            f"x={dx}",
            f"y={dy}",
            f"fontcolor={color}",
            f"fontsize={size}",
            "font='DejaVu Sans'",
            f"enable='between(t,{start},{end})'",
        ]
        if outline:
            parts += [f"bordercolor={outline}", "borderw=2"]
        if bg:
            parts += ["box=1", f"boxcolor={bg}"]
        if shadow:
            parts += [f"shadowcolor={shadow}", "shadowx=2", "shadowy=2"]
        return "drawtext=" + ":".join(parts)

    if name == "rotate":
        turns = dx if dx else 1.0
        return (
            f"rotate=a='if(between(t\\,{start}\\,{end})"
            f"\\,(t-{start})/{length}*2*PI*{turns}\\,0)'"
            f":ow=iw:oh=ih:c=black@0"
        )

    if name == "scroll":
        # translate via pad+crop, x/y offset ramps from 0 to (dx, dy) over [start, end]
        progress = f"min(max((t-{start})/{length}\\,0)\\,1)"
        ax = max(int(abs(dx)) + 1, 1)
        ay = max(int(abs(dy)) + 1, 1)
        return (
            f"pad=iw+{2*ax}:ih+{2*ay}:{ax}:{ay}:color=black@0,"
            f"crop=iw-{2*ax}:ih-{2*ay}:"
            f"x='{ax}+{progress}*{dx}':y='{ay}+{progress}*{dy}'"
        )

    if name == "zoom":
        # uniform zoom; dx, if > 1, is the final zoom factor; default 1.5.
        factor = dx if dx > 1 else 1.5
        progress = f"min(max((t-{start})/{length}\\,0)\\,1)"
        scale = f"(1+{progress}*{factor - 1})"
        return (
            f"scale='iw*{scale}':'ih*{scale}',"
            f"crop=iw/{scale}:ih/{scale}"
        )

    # overlay-points / distort: not implemented in this impl
    print(f"animate effect not supported: {name}", file=sys.stderr)
    return None


def _build_filtergraph(ops, info):
    """Return (filter_complex string or None, output_label or '0:v')."""
    steps = []
    cur = "[0:v]"
    label_n = 0

    def push(filt, sources=None):
        nonlocal cur, label_n
        label_n += 1
        lbl = f"[v{label_n}]"
        src = sources if sources is not None else cur
        steps.append(f"{src}{filt}{lbl}")
        cur = lbl
        return lbl

    skip = ops.get("skip")
    early = ops.get("early")
    skip_t = _to_seconds(skip, info) if skip else 0.0
    early_t = _to_seconds(early, info) if early else 0.0
    if skip_t > 0 or early_t > 0:
        end_t = max(info["duration"] - early_t, skip_t + 0.001)
        push(f"trim=start={skip_t}:end={end_t},setpts=PTS-STARTPTS")

    if ops.get("reverse"):
        if ops.get("append"):
            label_n += 1
            a, b = f"[a{label_n}]", f"[b{label_n}]"
            steps.append(f"{cur}split{a}{b}")
            label_n += 1
            rev = f"[r{label_n}]"
            steps.append(f"{b}reverse,setpts=PTS-STARTPTS{rev}")
            label_n += 1
            cat = f"[c{label_n}]"
            steps.append(f"{a}{rev}concat=n=2:v=1:a=0{cat}")
            cur = cat
        else:
            push("reverse,setpts=PTS-STARTPTS")

    speed = ops.get("speed")
    if speed:
        push(f"setpts=PTS/{float(speed)}")

    for spec in ops.get("animate") or []:
        f = _animate_step(spec, info)
        if f:
            push(f)

    if not steps:
        return None, "0:v"

    # Tag the final stream as [out] with a no-op so we never have to mutate
    # earlier labels (some filter args may legitimately contain `[`).
    steps.append(f"{cur}null[out]")
    return ";".join(steps), "[out]"


def process(url, ops, fmt):
    if not url:
        return b""
    with tempfile.TemporaryDirectory() as tmp:
        ext = "gif" if (fmt or "").lower() == "gif" else "mp4"
        in_path = os.path.join(tmp, f"in.{ext}")
        out_path = os.path.join(tmp, "out.mp4")

        with requests.get(url, timeout=60, stream=True) as r:
            r.raise_for_status()
            with open(in_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 16):
                    f.write(chunk)

        info = _probe(in_path)
        graph, out_label = _build_filtergraph(ops or {}, info)

        cmd = ["ffmpeg", "-y", "-i", in_path]
        if graph:
            cmd += ["-filter_complex", graph, "-map", out_label]
        else:
            cmd += ["-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2"]
        cmd += [
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-movflags", "+faststart", "-an",
            out_path,
        ]
        try:
            subprocess.run(cmd, capture_output=True, check=True, timeout=180)
        except subprocess.CalledProcessError as e:
            print("ffmpeg failed:", e.stderr.decode("utf-8", "replace"), file=sys.stderr)
            return b""

        with open(out_path, "rb") as f:
            return f.read()


def get_frame(url, fmt):
    if not url:
        return b""
    with tempfile.TemporaryDirectory() as tmp:
        ext = "gif" if (fmt or "").lower() == "gif" else "mp4"
        in_path = os.path.join(tmp, f"in.{ext}")
        out_path = os.path.join(tmp, "out.png")

        with requests.get(url, timeout=60, stream=True) as r:
            r.raise_for_status()
            with open(in_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 16):
                    f.write(chunk)

        cmd = [
            "ffmpeg", "-y", "-i", in_path,
            "-frames:v", "1",
            "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            out_path,
        ]
        try:
            subprocess.run(cmd, capture_output=True, check=True, timeout=60)
        except subprocess.CalledProcessError as e:
            print("ffmpeg failed:", e.stderr.decode("utf-8", "replace"), file=sys.stderr)
            return b""

        with open(out_path, "rb") as f:
            return f.read()
