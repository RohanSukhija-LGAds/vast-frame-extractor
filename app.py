import io
import os
import re
import csv
import json
import random
import zipfile
import hashlib
import tempfile
import subprocess
from urllib.parse import urljoin

import requests
from lxml import etree
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

UA = {"User-Agent": "vast-frame-extractor/1.0"}
app = FastAPI()

INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>VAST Frame Extractor</title>
  <style>
    body{font-family:system-ui,Arial;margin:24px;max-width:900px}
    textarea{width:100%;height:220px}
    .row{margin:12px 0}
    label{display:block;margin-bottom:6px;font-weight:600}
    input,select{padding:8px}
    button{padding:10px 14px;font-weight:700;cursor:pointer}
    .small{color:#444;font-size:13px}
    .hint{background:#f6f7f8;padding:10px;border-radius:10px}
    code{background:#eee;padding:2px 6px;border-radius:6px}
  </style>
</head>
<body>
  <h2>VAST Frame Extractor</h2>

  <div class="hint">
    <div><b>Input supports:</b></div>
    <ul>
      <li>VAST <b>URLs</b> (one per line)</li>
      <li>Full VAST <b>XML</b> pasted directly (<code>&lt;VAST&gt;...&lt;/VAST&gt;</code>)</li>
      <li>CSV upload (first column = URL)</li>
    </ul>
    <div class="small">Output: ZIP of JPG screenshots + manifest.json (per-tag success/errors).</div>
  </div>

  <form action="/generate" method="post" enctype="multipart/form-data">
    <div class="row">
      <label>Paste VAST URLs or VAST XML</label>
      <textarea name="vast_text" placeholder="Paste VAST URLs (one per line) OR paste full VAST XML here..."></textarea>
    </div>

    <div class="row">
      <label>or Upload CSV</label>
      <input type="file" name="vast_csv" accept=".csv,text/csv"/>
    </div>

    <div class="row">
      <label>Frames per VAST</label>
      <input type="number" name="frames_per_vast" value="3" min="1" max="10"/>
    </div>

    <div class="row">
      <label>Mode</label>
      <select name="mode">
        <option value="random" selected>Random</option>
        <option value="fixed">Fixed (e.g., 5,15,30)</option>
      </select>
    </div>

    <div class="row">
      <label>Fixed timestamps (seconds, comma-separated)</label>
      <input type="text" name="fixed_timestamps" value="5,15,30"/>
      <div class="small">Used only when Mode = Fixed</div>
    </div>

    <button type="submit">Generate ZIP</button>
  </form>
</body>
</html>
"""

# -------------------------
# Utilities
# -------------------------

def slugify(s: str, maxlen: int = 40) -> str:
    s = s or ""
    return (re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_")[:maxlen]) or "vast"

def fetch(url: str, timeout: int = 20) -> str:
    r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text

def split_inputs(text: str):
    """
    Returns list of ("url", value) or ("xml", value).

    - If pasted content includes <VAST ...> ... </VAST>, treat that as XML doc(s)
    - Otherwise treat each line as a URL (or at least "URL-like")
    """
    text = (text or "").strip()
    if not text:
        return []

    if "<VAST" in text and "</VAST>" in text:
        # Could be multiple VAST docs pasted back-to-back; split by </VAST>
        parts = text.split("</VAST>")
        items = []
        for p in parts:
            p = p.strip()
            if not p:
                continue
            items.append(("xml", p + "</VAST>"))
        return items

    # No obvious full VAST doc; treat as lines (URLs)
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        items.append(("url", line))
    return items

def choose_best_mediafile(media_nodes):
    """
    Choose best MediaFile URL from a list of <MediaFile> elements.
    Prefer MP4 progressive > MP4 > HLS > anything else, then bitrate/resolution.
    """
    candidates = []
    for m in media_nodes:
        url = (m.text or "").strip()
        if not url:
            continue

        mime = (m.get("type") or "").lower()
        delivery = (m.get("delivery") or "").lower()
        bitrate = int(m.get("bitrate") or 0) if (m.get("bitrate") or "").isdigit() else 0
        w = int(m.get("width") or 0) if (m.get("width") or "").isdigit() else 0
        h2 = int(m.get("height") or 0) if (m.get("height") or "").isdigit() else 0

        score = 0
        if "video/mp4" in mime and delivery == "progressive":
            score += 1000
        elif "video/mp4" in mime:
            score += 800
        elif "mpegurl" in mime or "m3u8" in url.lower():
            score += 600
        else:
            score += 100

        score += min(bitrate, 8000)
        score += (w * h2) // 1000
        candidates.append((score, url, {"mime": mime, "delivery": delivery, "bitrate": bitrate, "w": w, "h": h2}))

    if not candidates:
        return None, None

    candidates.sort(key=lambda x: x[0], reverse=True)
    _, best_url, meta = candidates[0]
    return best_url, meta

def resolve_vast_url(vast_url: str, max_depth: int = 12):
    """
    Resolve a VAST URL through Wrapper chains to an Inline and return chosen media_url.
    """
    seen = set()
    cur = vast_url

    for _ in range(max_depth):
        h = hashlib.sha256(cur.encode()).hexdigest()[:16]
        if h in seen:
            raise RuntimeError(f"Wrapper loop detected: {cur}")
        seen.add(h)

        xml = fetch(cur)
        root = etree.fromstring(xml.encode("utf-8"))

        # Inline
        if root.xpath(".//InLine"):
            media_nodes = root.xpath(".//InLine//Linear//MediaFile")
            if not media_nodes:
                raise RuntimeError(f"No MediaFile found in Inline VAST: {cur}")

            media_url, meta = choose_best_mediafile(media_nodes)
            if not media_url:
                raise RuntimeError(f"No usable MediaFile URL in Inline VAST: {cur}")

            return {"final_vast_url": cur, "media_url": media_url, "media_meta": meta}

        # Wrapper
        if root.xpath(".//Wrapper"):
            nxt = root.xpath("string(.//Wrapper//VASTAdTagURI)").strip()
            if not nxt:
                raise RuntimeError(f"Wrapper without VASTAdTagURI: {cur}")
            cur = urljoin(cur, nxt)
            continue

        raise RuntimeError(f"Not Inline/Wrapper VAST XML at URL: {cur}")

    raise RuntimeError(f"Max wrapper depth exceeded ({max_depth}): {vast_url}")

def resolve_vast_xml(vast_xml: str):
    """
    Resolve Inline VAST from pasted XML (no HTTP wrapper chasing).
    If pasted XML is a Wrapper, ask user to paste URL instead.
    """
    root = etree.fromstring(vast_xml.encode("utf-8"))

    if root.xpath(".//Wrapper"):
        raise RuntimeError("Pasted VAST XML is a Wrapper. Please paste the VAST URL so wrappers can be followed.")

    if not root.xpath(".//InLine"):
        raise RuntimeError("Pasted XML is not an Inline VAST document.")

    media_nodes = root.xpath(".//InLine//Linear//MediaFile")
    if not media_nodes:
        raise RuntimeError("No MediaFile found in pasted Inline VAST XML.")

    media_url, meta = choose_best_mediafile(media_nodes)
    if not media_url:
        raise RuntimeError("No usable MediaFile URL found in pasted VAST XML.")

    return {"media_url": media_url, "media_meta": meta}

def ffprobe_duration(media_url: str):
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        media_url
    ]
    try:
        out = subprocess.check_output(cmd, text=True).strip()
        return float(out)
    except Exception:
        return None

def pick_timestamps(duration_s, frames_per_vast: int, mode: str, fixed_ts):
    if mode == "fixed":
        return fixed_ts[:frames_per_vast]

    # random
    if duration_s is None:
        return fixed_ts[:frames_per_vast]

    # avoid black/end frames
    lo = 1.5
    hi = max(lo + 1.0, duration_s - 1.5)

    # very short creative fallback
    if hi <= lo:
        return [min(5.0, max(0.2, duration_s / 2.0))]

    picks = []
    attempts = 0
    while len(picks) < frames_per_vast and attempts < 400:
        attempts += 1
        t = round(random.uniform(lo, hi), 1)
        # keep picks spaced out
        if all(abs(t - p) >= 3.0 for p in picks):
            picks.append(t)

    picks.sort()
    return picks

def extract_frame(media_url: str, t: float, out_path: str):
    # seek slightly after time to reduce keyframe/black issues
    seek = max(0.0, t + 0.2)
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(seek),
        "-i", media_url,
        "-frames:v", "1",
        "-q:v", "2",
        out_path
    ]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# -------------------------
# Routes
# -------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML

@app.post("/generate")
async def generate(
    vast_text: str = Form(""),
    vast_csv: UploadFile | None = File(None),
    frames_per_vast: int = Form(3),
    mode: str = Form("random"),
    fixed_timestamps: str = Form("5,15,30"),
):
    # Parse fixed timestamps
    try:
        fixed_ts = [float(x.strip()) for x in fixed_timestamps.split(",") if x.strip()]
        if not fixed_ts:
            fixed_ts = [5.0, 15.0, 30.0]
    except Exception:
        fixed_ts = [5.0, 15.0, 30.0]

    # Gather inputs
    inputs = []
    inputs.extend(split_inputs(vast_text))

    if vast_csv is not None and vast_csv.filename:
        content = (await vast_csv.read()).decode("utf-8", errors="ignore")
        for row in csv.reader(io.StringIO(content)):
            if row and row[0].strip():
                inputs.append(("url", row[0].strip()))

    # De-dup
    seen = set()
    deduped = []
    for kind, val in inputs:
        key = (kind, val)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((kind, val))
    inputs = deduped

    if not inputs:
        return JSONResponse({"error": "No VAST URLs or VAST XML provided"}, status_code=400)

    # Clamp settings
    frames_per_vast = max(1, min(int(frames_per_vast), 10))
    mode = "fixed" if (mode or "").strip().lower() == "fixed" else "random"

    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = os.path.join(tmpdir, "jpgs")
        os.makedirs(outdir, exist_ok=True)

        manifest = []
        for i, (kind, val) in enumerate(inputs, start=1):
            item = {"input_type": kind}

            try:
                # Resolve media URL
                if kind == "xml":
                    item["vast_xml"] = True
                    res = resolve_vast_xml(val)
                    media_url = res["media_url"]
                else:
                    item["vast_url"] = val
                    res = resolve_vast_url(val)
                    media_url = res["media_url"]
                    item["final_vast_url"] = res["final_vast_url"]

                item["media_url"] = media_url

                # Pick timestamps and extract
                dur = ffprobe_duration(media_url)
                ts = pick_timestamps(dur, frames_per_vast, mode, fixed_ts)
                item["duration_s"] = dur
                item["timestamps_s"] = ts

                base = f"{i:02d}_{slugify(val)}"
                jpgs = []
                for j, t in enumerate(ts, start=1):
                    path = os.path.join(outdir, f"{base}_{j:02d}_{t}s.jpg")
                    extract_frame(media_url, t, path)
                    jpgs.append(os.path.basename(path))
                item["jpgs"] = jpgs

            except Exception as e:
                item["error"] = str(e)

            manifest.append(item)

        # Build ZIP
        zip_bytes = io.BytesIO()
        with zipfile.ZipFile(zip_bytes, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for fn in sorted(os.listdir(outdir)):
                z.write(os.path.join(outdir, fn), arcname=fn)
            z.writestr("manifest.json", json.dumps(manifest, indent=2))

        zip_bytes.seek(0)
        return StreamingResponse(
            zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="vast_jpgs.zip"'},
        )
