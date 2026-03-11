import io, os, re, csv, json, random, zipfile, hashlib, tempfile, subprocess
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
  </style>
</head>
<body>
  <h2>VAST Frame Extractor</h2>
  <p class="small">Paste VAST URLs (one per line) or upload a CSV (first column). Output: ZIP of JPGs + manifest.json</p>

  <form action="/generate" method="post" enctype="multipart/form-data">
    <div class="row">
      <label>Paste VAST URLs</label>
      <textarea name="vast_text" placeholder="https://..."></textarea>
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
        <option value="fixed">Fixed (5,15,30)</option>
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

def fetch(url, timeout=20):
    r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text

def resolve_vast(vast_url, max_depth=12):
    seen = set()
    cur = vast_url
    for _ in range(max_depth):
        h = hashlib.sha256(cur.encode()).hexdigest()[:16]
        if h in seen:
            raise RuntimeError(f"Wrapper loop detected: {cur}")
        seen.add(h)

        xml = fetch(cur)
        root = etree.fromstring(xml.encode("utf-8"))

        if root.xpath(".//InLine"):
            media_nodes = root.xpath(".//InLine//Linear//MediaFile")
            if not media_nodes:
                raise RuntimeError(f"No MediaFile in Inline VAST: {cur}")

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

            candidates.sort(key=lambda x: x[0], reverse=True)
            best_url = candidates[0][1]
            return {"final_vast_url": cur, "media_url": best_url}

        if root.xpath(".//Wrapper"):
            nxt = root.xpath("string(.//Wrapper//VASTAdTagURI)").strip()
            if not nxt:
                raise RuntimeError(f"Wrapper without VASTAdTagURI: {cur}")
            cur = urljoin(cur, nxt)
            continue

        raise RuntimeError(f"Not Inline/Wrapper VAST: {cur}")

    raise RuntimeError(f"Max wrapper depth exceeded ({max_depth}): {vast_url}")

def ffprobe_duration(media_url):
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
           "-of", "default=noprint_wrappers=1:nokey=1", media_url]
    try:
        out = subprocess.check_output(cmd, text=True).strip()
        return float(out)
    except Exception:
        return None

def pick_timestamps(duration_s, frames_per_vast, mode, fixed_ts):
    if mode == "fixed":
        return fixed_ts[:frames_per_vast]

    if duration_s is None:
        return fixed_ts[:frames_per_vast]

    lo = 1.5
    hi = max(lo + 1.0, duration_s - 1.5)
    if hi <= lo:
        return [min(5.0, max(0.2, duration_s/2.0))]

    picks = []
    attempts = 0
    while len(picks) < frames_per_vast and attempts < 400:
        attempts += 1
        t = round(random.uniform(lo, hi), 1)
        if all(abs(t - p) >= 3.0 for p in picks):
            picks.append(t)
    picks.sort()
    return picks

def extract_frame(media_url, t, out_path):
    seek = max(0.0, t + 0.2)
    cmd = ["ffmpeg", "-y", "-ss", str(seek), "-i", media_url,
           "-frames:v", "1", "-q:v", "2", out_path]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def slugify(s, maxlen=40):
    return (re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_")[:maxlen]) or "vast"

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
    except:
        fixed_ts = [5.0, 15.0, 30.0]

    # Load VAST URLs
    vast_urls = []
    if vast_csv is not None and vast_csv.filename:
        content = (await vast_csv.read()).decode("utf-8", errors="ignore")
        for row in csv.reader(io.StringIO(content)):
            if row and row[0].strip():
                vast_urls.append(row[0].strip())
    if vast_text.strip():
        for line in vast_text.splitlines():
            u = line.strip()
            if u:
                vast_urls.append(u)

    vast_urls = list(dict.fromkeys(vast_urls))  # de-dup preserve order
    if not vast_urls:
        return JSONResponse({"error": "No VAST URLs provided"}, status_code=400)

    frames_per_vast = max(1, min(frames_per_vast, 10))
    mode = "fixed" if mode.strip().lower() == "fixed" else "random"

    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = os.path.join(tmpdir, "jpgs")
        os.makedirs(outdir, exist_ok=True)

        manifest = []
        for i, vast in enumerate(vast_urls, start=1):
            item = {"vast_url": vast}
            try:
                res = resolve_vast(vast)
                media = res["media_url"]
                dur = ffprobe_duration(media)
                ts = pick_timestamps(dur, frames_per_vast, mode, fixed_ts)

                item.update({"media_url": media, "duration_s": dur, "timestamps_s": ts})
                jpgs = []
                base = f"{i:02d}_{slugify(vast)}"
                for j, t in enumerate(ts, start=1):
                    path = os.path.join(outdir, f"{base}_{j:02d}_{t}s.jpg")
                    extract_frame(media, t, path)
                    jpgs.append(os.path.basename(path))
                item["jpgs"] = jpgs
            except Exception as e:
                item["error"] = str(e)
            manifest.append(item)

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
