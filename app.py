from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS
import yt_dlp
import logging
import os
import re
import tempfile
import threading
import queue
import json
import time
import hashlib
import glob
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

app = Flask(__name__)

# ─────────────────────────────────────────────
# CORS — open to all origins (public app, no auth)
# ─────────────────────────────────────────────
CORS(app)

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nova_dvr")

# ─────────────────────────────────────────────
# YouTube cookies — anti-bot bypass for cloud IPs
# Set YOUTUBE_COOKIES_B64 on Railway/Render to base64-encoded cookies.txt
# PowerShell encode: [Convert]::ToBase64String([IO.File]::ReadAllBytes("cookies.txt")) | Out-File cookies_b64.txt -NoNewline
# ─────────────────────────────────────────────

import base64 as _base64

_COOKIE_FILE: str | None = None

def _setup_cookies():
    global _COOKIE_FILE
    b64 = os.environ.get("YOUTUBE_COOKIES_B64", "").strip()
    if b64:
        try:
            cookie_content = _base64.b64decode(b64).decode("utf-8")
        except Exception as e:
            logger.warning(f"Failed to decode YOUTUBE_COOKIES_B64: {e}")
            cookie_content = ""
    else:
        cookie_content = os.environ.get("YOUTUBE_COOKIES", "").strip()

    if not cookie_content:
        logger.info("No YouTube cookies configured")
        return None
    try:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", prefix="yt_cookies_",
            delete=False, encoding="utf-8"
        )
        tmp.write(cookie_content)
        tmp.close()
        _COOKIE_FILE = tmp.name
        logger.info(f"YouTube cookies loaded → {_COOKIE_FILE}")
    except Exception as e:
        logger.warning(f"Failed to write cookie file: {e}")
    return _COOKIE_FILE

_setup_cookies()

def _cookie_opts() -> dict:
    """Return yt-dlp cookiefile option if cookies are available."""
    if _COOKIE_FILE and os.path.exists(_COOKIE_FILE):
        return {"cookiefile": _COOKIE_FILE}
    return {}


# ─────────────────────────────────────────────
# Directories
# ─────────────────────────────────────────────

# Primary: temp dir — file is streamed to browser → goes to system Downloads
TEMP_DOWNLOAD_DIR = os.path.join(tempfile.gettempdir(), "nova_dvr_temp")
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

# Secondary: user-configured persistent directory (optional)
DEFAULT_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "NovaDVR")

# ─────────────────────────────────────────────
# SQLite Persistent Job Queue
# ─────────────────────────────────────────────

DB_PATH = os.path.join(os.path.dirname(__file__), "jobs.db")
_db_lock = threading.Lock()

def _db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _init_db():
    with _db_lock:
        conn = _db_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT NOT NULL,
                title       TEXT,
                format_id   TEXT,
                resolution  TEXT,
                is_audio    INTEGER DEFAULT 0,
                status      TEXT DEFAULT 'queued',
                error_msg   TEXT,
                filepath    TEXT,
                created_at  TEXT,
                updated_at  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS error_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT,
                error_msg   TEXT,
                context     TEXT,
                created_at  TEXT
            )
        """)
        conn.commit()
        conn.close()

_init_db()

def _db_add_job(url, title="", format_id="", resolution="", is_audio=False):
    now = datetime.utcnow().isoformat()
    with _db_lock:
        conn = _db_conn()
        cur = conn.execute(
            "INSERT INTO jobs (url, title, format_id, resolution, is_audio, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (url, title, format_id, resolution, int(is_audio), "queued", now, now)
        )
        job_id = cur.lastrowid
        conn.commit()
        conn.close()
    return job_id

def _db_update_job(job_id, **kwargs):
    now = datetime.utcnow().isoformat()
    kwargs["updated_at"] = now
    fields = ", ".join(f"{k}=?" for k in kwargs)
    values = list(kwargs.values()) + [job_id]
    with _db_lock:
        conn = _db_conn()
        conn.execute(f"UPDATE jobs SET {fields} WHERE id=?", values)
        conn.commit()
        conn.close()

def _db_log_error(url, error_msg, context=""):
    now = datetime.utcnow().isoformat()
    with _db_lock:
        conn = _db_conn()
        conn.execute(
            "INSERT INTO error_log (url, error_msg, context, created_at) VALUES (?,?,?,?)",
            (url, error_msg, context, now)
        )
        # Keep only last 500 errors
        conn.execute("""
            DELETE FROM error_log WHERE id NOT IN (
                SELECT id FROM error_log ORDER BY id DESC LIMIT 500
            )
        """)
        conn.commit()
        conn.close()

def _db_get_jobs(limit=100, status=None):
    with _db_lock:
        conn = _db_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status=? ORDER BY id DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        result = [dict(r) for r in rows]
        conn.close()
    return result

def _db_get_errors(limit=100):
    with _db_lock:
        conn = _db_conn()
        rows = conn.execute(
            "SELECT * FROM error_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        result = [dict(r) for r in rows]
        conn.close()
    return result

# ─────────────────────────────────────────────
# In-memory metadata cache
# Avoids hammering yt-dlp for repeated inspect/format calls on the same URL
# ─────────────────────────────────────────────

_cache: dict = {}          # { cache_key: { "ts": float, "data": any } }
_cache_lock = threading.Lock()
CACHE_TTL = 300            # seconds (5 min)

def _cache_key(prefix: str, url: str) -> str:
    return f"{prefix}:{hashlib.md5(url.encode()).hexdigest()}"

def _cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry["ts"]) < CACHE_TTL:
            return entry["data"]
    return None

def _cache_set(key: str, data):
    with _cache_lock:
        _cache[key] = {"ts": time.time(), "data": data}

def _cache_evict():
    """Remove expired entries. Call periodically."""
    now = time.time()
    with _cache_lock:
        expired = [k for k, v in _cache.items() if now - v["ts"] >= CACHE_TTL]
        for k in expired:
            del _cache[k]

# ─────────────────────────────────────────────
# Concurrent download limiter
# ─────────────────────────────────────────────

MAX_CONCURRENT_DOWNLOADS = 5
_download_semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# ─────────────────────────────────────────────
# Temp file cleanup — remove files older than 30 min
# ─────────────────────────────────────────────

def _cleanup_old_temp_files():
    """Delete temp files older than 30 minutes. Safe to call at startup and periodically."""
    try:
        cutoff = time.time() - 1800   # 30 min
        for f in glob.glob(os.path.join(TEMP_DOWNLOAD_DIR, "*")):
            try:
                if os.path.isfile(f) and os.path.getmtime(f) < cutoff:
                    os.remove(f)
                    logger.info(f"Cleaned up old temp file: {f}")
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Temp cleanup error: {e}")

# Run cleanup at startup
_cleanup_old_temp_files()

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def get_download_dir(requested: str) -> str:
    """
    Returns the directory to save to.
    - If user provided a custom path → use that (secondary/persistent)
    - Otherwise → use temp dir (file will be streamed to browser)
    """
    d = (requested or "").strip()
    if d:
        os.makedirs(d, exist_ok=True)
        return d
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
    return TEMP_DOWNLOAD_DIR

def height_to_label(height):
    """Map pixel height to a clean resolution label covering 144p → 4K."""
    if not height:
        return None
    # Exact matches first
    for threshold, label in sorted(VIDEO_HEIGHT_LABELS.items(), reverse=True):
        if height >= threshold:
            return label
    return f"{height}p"  # fallback for anything below 144p

ALLOWED_VIDEO_EXTS   = {"mp4"}
ALLOWED_AUDIO_EXTS   = {"mp3", "m4a", "webm", "ogg", "opus"}

# All heights we want to show (240p → 4K)
VIDEO_HEIGHT_LABELS = {
    144:  "144p",
    240:  "240p",
    360:  "360p",
    480:  "480p",
    720:  "720p",
    1080: "1080p",
    1440: "1440p",
    2160: "4K",
}


# ─────────────────────────────────────────────
# /health  – liveness probe
# ─────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    _cache_evict()  # piggyback lightweight cleanup on health checks
    # Count jobs by status from SQLite
    with _db_lock:
        conn = _db_conn()
        total_jobs  = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        active_jobs = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='downloading'").fetchone()[0]
        error_count = conn.execute("SELECT COUNT(*) FROM error_log").fetchone()[0]
        conn.close()
    return jsonify({
        "status": "ok",
        "cache_entries": len(_cache),
        "active_downloads": MAX_CONCURRENT_DOWNLOADS - _download_semaphore._value,
        "total_jobs": total_jobs,
        "active_jobs": active_jobs,
        "error_log_count": error_count,
    })


# ─────────────────────────────────────────────
# /inspect  – validate URL + return metadata
# ─────────────────────────────────────────────
@app.route("/inspect", methods=["POST"])
def inspect():
    data = request.json or {}
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    # Return cached result if still fresh
    ck = _cache_key("inspect", url)
    cached = _cache_get(ck)
    if cached:
        logger.info(f"Cache hit: inspect {url}")
        return jsonify(cached)

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "socket_timeout": 20,
        **_cookie_opts(),
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except yt_dlp.utils.DownloadError as e:
        logger.warning(f"Inspect DownloadError for {url}: {e}")
        return jsonify({"error": str(e), "valid": False}), 400
    except Exception as e:
        logger.error(f"Inspect error for {url}: {e}")
        return jsonify({"error": f"Could not inspect URL: {str(e)}", "valid": False}), 400

    result = {
        "valid": True,
        "title":      info.get("title", "Unknown"),
        "uploader":   info.get("uploader") or info.get("channel", "Unknown"),
        "duration":   info.get("duration"),
        "thumbnail":  info.get("thumbnail"),
        "platform":   info.get("extractor_key", "Unknown"),
        "view_count": info.get("view_count"),
        "upload_date":info.get("upload_date"),
        "webpage_url":info.get("webpage_url", url),
    }
    _cache_set(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────
# /list-formats  – filtered smart format list
# ─────────────────────────────────────────────
@app.route("/list-formats", methods=["POST"])
def list_formats():
    data = request.json or {}
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    ck = _cache_key("formats", url)
    cached = _cache_get(ck)
    if cached:
        logger.info(f"Cache hit: list-formats {url}")
        return jsonify(cached)

    ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True, "socket_timeout": 20, **_cookie_opts()}

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    video_formats = {}   # key = resolution label → best mp4 video stream
    audio_formats = {}   # key = abr bucket → best audio stream

    for f in info.get("formats", []):
        acodec = f.get("acodec") or "none"
        vcodec = f.get("vcodec") or "none"
        ext    = (f.get("ext") or "").lower()
        height = f.get("height")
        abr    = f.get("abr")
        tbr    = f.get("tbr")

        has_video = vcodec not in ("none", "")
        has_audio = acodec not in ("none", "")

        # ── Video streams (video-only OR combined) ──
        # Accept mp4 or webm — prefer mp4. Height required.
        if has_video and ext in ("mp4", "webm", "m4v") and height:
            label = height_to_label(height)
            if not label:
                continue
            existing = video_formats.get(label)
            score = (tbr or 0)
            existing_score = (existing.get("tbr") or 0) if existing else -1
            # Prefer mp4 over anything else; within same ext prefer higher tbr
            if (
                existing is None
                or (ext == "mp4" and (existing.get("ext") != "mp4"))
                or (ext == existing.get("ext") and score > existing_score)
            ):
                video_formats[label] = {
                    "format_id": f.get("format_id"),
                    "ext":       "mp4",     # always output as mp4
                    "resolution": label,
                    "height":    height,
                    "fps":       f.get("fps"),
                    "tbr":       tbr,
                    "note":      f.get("format_note", ""),
                    "type":      "video+audio",   # will be merged with audio at download
                    "has_audio": has_audio,       # internal flag
                }

        # ── Audio-only streams ──
        if has_audio and not has_video:
            effective_abr = abr or tbr or 0
            abr_key = round(effective_abr / 32) * 32 if effective_abr else 0
            existing = audio_formats.get(abr_key)
            if (
                existing is None
                or effective_abr > (existing.get("_eff") or 0)
            ):
                display_abr = abr or tbr
                audio_formats[abr_key] = {
                    "format_id":  f.get("format_id"),
                    "ext":        "mp3",
                    "resolution": None,
                    "abr":        display_abr,
                    "note":       f"{int(display_abr)}kbps MP3" if display_abr else "MP3",
                    "type":       "audio-only",
                    "_eff":       effective_abr,
                }

    # ── Sort video by height desc ──
    height_order = {v: k for k, v in VIDEO_HEIGHT_LABELS.items()}
    sorted_video = sorted(
        video_formats.values(),
        key=lambda f: height_order.get(f["resolution"],
            int(f["resolution"].replace("p","")) if f["resolution"] and f["resolution"].endswith("p") else 0),
        reverse=True,
    )

    # Mark 4K and flag display_only; strip internal keys
    for f in sorted_video:
        if f["resolution"] == "4K":
            f["note"] = "4K reference · downloads as best available"
            f["display_only"] = True
        f.pop("has_audio", None)
        f.pop("tbr", None)

    # ── Sort audio by bitrate desc; strip internal keys ──
    sorted_audio = sorted(audio_formats.values(), key=lambda f: f.get("abr") or 0, reverse=True)
    for f in sorted_audio:
        f.pop("_eff", None)

    result = {
        "formats": sorted_video + sorted_audio,
        "title": info.get("title", ""),
    }
    _cache_set(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────
# SSE Downloader Helpers
# ─────────────────────────────────────────────

def run_download_thread(ydl_opts, url, q, job_id=None):
    """Runs yt-dlp in a daemon thread with automatic retry on transient errors."""
    PERMANENT_ERRORS = [
        "private", "login required", "age-restricted", "removed",
        "unavailable", "members only", "copyright", "geo", "region",
        "live", "is live", "unsupported url",
    ]
    try:
        def progress_hook(d):
            if d["status"] == "downloading":
                percent_str = re.sub(r'\x1b\[[0-9;]*m', '', d.get("_percent_str", "").strip().replace("%", ""))
                speed_str   = re.sub(r'\x1b\[[0-9;]*m', '', d.get("_speed_str", "").strip())
                eta_str     = re.sub(r'\x1b\[[0-9;]*m', '', d.get("_eta_str", "").strip())
                q.put({"status": "downloading", "percent": percent_str, "speed": speed_str, "eta": eta_str})
            elif d["status"] == "finished":
                q.put({"status": "processing", "message": "Finished downloading, post-processing..."})

        opts = dict(ydl_opts)
        opts["progress_hooks"] = opts.get("progress_hooks", []) + [progress_hook]
        opts["socket_timeout"] = 30
        opts.update(_cookie_opts())

        last_error = None
        MAX_RETRIES = 2

        if job_id:
            _db_update_job(job_id, status="downloading")

        for attempt in range(MAX_RETRIES + 1):
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    actual_filename = ydl.prepare_filename(info)
                    base = os.path.splitext(actual_filename)[0]
                    is_audio = any(
                        pp.get("key") == "FFmpegExtractAudio"
                        for pp in opts.get("postprocessors", [])
                    )
                    actual_filename = base + (".mp3" if is_audio else ".mp4")
                    save_dir = os.path.dirname(actual_filename)

                    if not os.path.exists(actual_filename):
                        try:
                            files = [
                                os.path.join(save_dir, f)
                                for f in os.listdir(save_dir)
                                if os.path.isfile(os.path.join(save_dir, f))
                            ]
                            if files:
                                actual_filename = max(files, key=os.path.getmtime)
                        except Exception:
                            pass

                    logger.info(f"Download complete: {actual_filename}")
                    if job_id:
                        _db_update_job(job_id, status="done", filepath=actual_filename)
                    q.put({
                        "status": "done",
                        "filepath": actual_filename,
                        "filename": os.path.basename(actual_filename),
                        "save_dir": save_dir,
                    })
                    return  # success — exit retry loop

            except yt_dlp.utils.DownloadError as e:
                last_error = str(e)
                err_lower = last_error.lower()
                if any(p in err_lower for p in PERMANENT_ERRORS):
                    logger.warning(f"Permanent download error (no retry): {last_error}")
                    break
                if attempt < MAX_RETRIES:
                    wait = 2 ** attempt  # 1s then 2s
                    logger.warning(f"Download attempt {attempt + 1} failed, retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    logger.error(f"Download failed after {MAX_RETRIES + 1} attempts: {last_error}")

        final_error = last_error or "Download failed after retries"
        if job_id:
            _db_update_job(job_id, status="error", error_msg=final_error)
        _db_log_error(url, final_error, context="run_download_thread")
        q.put({"status": "error", "error": final_error})

    except Exception as e:
        err_str = str(e)
        logger.error(f"Download thread exception for {url}: {e}")
        if job_id:
            _db_update_job(job_id, status="error", error_msg=err_str)
        _db_log_error(url, err_str, context="run_download_thread:exception")
        q.put({"status": "error", "error": err_str})


def make_download_stream(ydl_opts, url, is_temp, job_id=None):
    """
    Generator: acquire concurrency semaphore → start download thread
    → yield SSE events → release semaphore.
    """
    acquired = _download_semaphore.acquire(timeout=5)
    if not acquired:
        err = "Server is busy — too many concurrent downloads. Please try again in a moment."
        if job_id:
            _db_update_job(job_id, status="error", error_msg=err)
        yield (
            "data: "
            + json.dumps({"status": "error", "error": err})
            + "\n\n"
        )
        return

    q: queue.Queue = queue.Queue()
    t = threading.Thread(target=run_download_thread, args=(ydl_opts, url, q, job_id))
    t.daemon = True
    t.start()

    try:
        while True:
            try:
                event = q.get(timeout=0.3)
                if event["status"] == "done":
                    event["is_temp"] = is_temp
                yield f"data: {json.dumps(event)}\n\n"
                if event["status"] in ("done", "error"):
                    break
            except queue.Empty:
                if not t.is_alive():
                    err = "Download thread terminated unexpectedly."
                    if job_id:
                        _db_update_job(job_id, status="error", error_msg=err)
                    yield f"data: {json.dumps({'status': 'error', 'error': err})}\n\n"
                    break
    finally:
        _download_semaphore.release()

# ─────────────────────────────────────────────
# /download  – download to temp, streams progress via SSE, returns filepath
# ─────────────────────────────────────────────
@app.route("/download", methods=["POST"])
def download():
    data = request.json or {}
    url          = data.get("url", "").strip()
    format_id    = data.get("format_id", "").strip()
    is_audio     = data.get("is_audio", False)
    is_4k        = data.get("is_4k", False)
    custom_dir   = (data.get("download_dir") or "").strip()
    title        = data.get("title", "").strip()

    save_dir     = get_download_dir(custom_dir)
    is_temp      = (save_dir == TEMP_DOWNLOAD_DIR)

    if not url or not format_id:
        return jsonify({"error": "Missing URL or format_id"}), 400

    # Persist job to SQLite
    job_id = _db_add_job(url, title=title, format_id=format_id, is_audio=is_audio)

    if is_audio:
        ydl_opts = {
            "format": f"{format_id}/bestaudio",
            "outtmpl": os.path.join(save_dir, "%(title)s.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            **_cookie_opts(),
        }
    else:
        if is_4k:
            fmt = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best"
        else:
            fmt = f"{format_id}+bestaudio[ext=m4a]/bestaudio/{format_id}"
        ydl_opts = {
            "format": fmt,
            "outtmpl": os.path.join(save_dir, "%(title)s.%(ext)s"),
            "merge_output_format": "mp4",
            "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
            **_cookie_opts(),
        }

    return Response(make_download_stream(ydl_opts, url, is_temp, job_id=job_id), mimetype='text/event-stream')


# ─────────────────────────────────────────────
# /serve-file  – stream file to browser → system Downloads folder
# Optionally deletes temp file after sending
# ─────────────────────────────────────────────
@app.route("/serve-file", methods=["GET"])
def serve_file():
    filepath  = request.args.get("path", "").strip()
    is_temp   = request.args.get("temp", "0") == "1"

    if not filepath or not os.path.isfile(filepath):
        return jsonify({"error": "File not found"}), 404

    directory = os.path.dirname(os.path.abspath(filepath))
    filename  = os.path.basename(filepath)

    response = send_from_directory(
        directory,
        filename,
        as_attachment=True,   # browser shows Save As / Downloads panel
    )

    # Clean up temp file after response is sent
    if is_temp:
        @response.call_on_close
        def cleanup():
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception:
                pass

    return response


# ─────────────────────────────────────────────
# /batch-download  – sequential batch (legacy, kept for compatibility)
# ─────────────────────────────────────────────
@app.route("/batch-download", methods=["POST"])
def batch_download():
    data = request.json or {}
    jobs         = data.get("jobs", [])       # [{url, format_id, is_audio, is_4k}]
    download_dir = get_download_dir(data.get("download_dir", ""))

    if not jobs:
        return jsonify({"error": "No jobs provided"}), 400

    results = []
    for job in jobs:
        url       = job.get("url", "").strip()
        format_id = job.get("format_id", "").strip()
        is_audio  = job.get("is_audio", False)
        is_4k     = job.get("is_4k", False)
        title     = job.get("title", "")

        if not url or not format_id:
            results.append({"url": url, "status": "error", "message": "Missing URL or format_id"})
            continue

        job_id = _db_add_job(url, title=title, format_id=format_id, is_audio=is_audio)

        if is_audio:
            ydl_opts = {
                "format": format_id,
                "outtmpl": os.path.join(download_dir, "%(title)s.%(ext)s"),
                "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}],
                "quiet": True,
                **_cookie_opts(),
            }
        else:
            fmt = "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]" if is_4k else f"{format_id}+bestaudio/best"
            ydl_opts = {
                "format": fmt,
                "outtmpl": os.path.join(download_dir, "%(title)s.%(ext)s"),
                "merge_output_format": "mp4",
                "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
                "quiet": True,
                **_cookie_opts(),
            }

        try:
            _db_update_job(job_id, status="downloading")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            _db_update_job(job_id, status="done")
            results.append({"url": url, "status": "done", "job_id": job_id})
        except Exception as e:
            err_str = str(e)
            _db_update_job(job_id, status="error", error_msg=err_str)
            _db_log_error(url, err_str, context="batch-download")
            results.append({"url": url, "status": "error", "message": err_str})

    return jsonify({"results": results, "download_dir": download_dir})


# ─────────────────────────────────────────────
# /parallel-batch  – parallel downloads via SSE
# Streams per-item progress events with throttling (max 3 concurrent)
# ─────────────────────────────────────────────

MAX_PARALLEL_BATCH = 3  # throttle: at most 3 parallel downloads in a batch

def _run_single_parallel_job(job: dict, download_dir: str) -> dict:
    """
    Worker executed in a ThreadPoolExecutor thread.
    Returns a result dict with status, filepath, filename, job_id.
    Blocks the semaphore for its duration.
    """
    url       = job.get("url", "").strip()
    format_id = job.get("format_id", "").strip()
    is_audio  = job.get("is_audio", False)
    is_4k     = job.get("is_4k", False)
    title     = job.get("title", "")
    client_id = job.get("id")  # frontend item id for correlation

    job_id = _db_add_job(url, title=title, format_id=format_id, is_audio=is_audio)

    if not url or not format_id:
        return {"client_id": client_id, "url": url, "status": "error",
                "error": "Missing URL or format_id", "job_id": job_id}

    if is_audio:
        ydl_opts = {
            "format": f"{format_id}/bestaudio",
            "outtmpl": os.path.join(download_dir, "%(title)s.%(ext)s"),
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}],
            "quiet": True,
            "socket_timeout": 30,
            **_cookie_opts(),
        }
    else:
        fmt = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best" if is_4k else f"{format_id}+bestaudio[ext=m4a]/bestaudio/{format_id}"
        ydl_opts = {
            "format": fmt,
            "outtmpl": os.path.join(download_dir, "%(title)s.%(ext)s"),
            "merge_output_format": "mp4",
            "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
            "quiet": True,
            "socket_timeout": 30,
            **_cookie_opts(),
        }

    PERMANENT_ERRORS = [
        "private", "login required", "age-restricted", "removed",
        "unavailable", "members only", "copyright", "geo", "region",
        "live", "is live", "unsupported url",
    ]

    acquired = _download_semaphore.acquire(timeout=60)
    if not acquired:
        err = "Server busy — semaphore timeout"
        _db_update_job(job_id, status="error", error_msg=err)
        _db_log_error(url, err, context="parallel-batch")
        return {"client_id": client_id, "url": url, "status": "error", "error": err, "job_id": job_id}

    _db_update_job(job_id, status="downloading")
    last_error = None
    try:
        for attempt in range(3):
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    actual_filename = ydl.prepare_filename(info)
                    base = os.path.splitext(actual_filename)[0]
                    actual_filename = base + (".mp3" if is_audio else ".mp4")
                    save_dir = os.path.dirname(actual_filename)

                    if not os.path.exists(actual_filename):
                        try:
                            files = [os.path.join(save_dir, f) for f in os.listdir(save_dir)
                                     if os.path.isfile(os.path.join(save_dir, f))]
                            if files:
                                actual_filename = max(files, key=os.path.getmtime)
                        except Exception:
                            pass

                    _db_update_job(job_id, status="done", filepath=actual_filename)
                    return {
                        "client_id": client_id, "url": url, "status": "done",
                        "filepath": actual_filename,
                        "filename": os.path.basename(actual_filename),
                        "is_temp": (save_dir == TEMP_DOWNLOAD_DIR),
                        "job_id": job_id,
                    }
            except yt_dlp.utils.DownloadError as e:
                last_error = str(e)
                if any(p in last_error.lower() for p in PERMANENT_ERRORS):
                    break
                if attempt < 2:
                    time.sleep(2 ** attempt)

        err = last_error or "Download failed"
        _db_update_job(job_id, status="error", error_msg=err)
        _db_log_error(url, err, context="parallel-batch")
        return {"client_id": client_id, "url": url, "status": "error", "error": err, "job_id": job_id}

    except Exception as e:
        err = str(e)
        _db_update_job(job_id, status="error", error_msg=err)
        _db_log_error(url, err, context="parallel-batch:exception")
        return {"client_id": client_id, "url": url, "status": "error", "error": err, "job_id": job_id}
    finally:
        _download_semaphore.release()


@app.route("/parallel-batch", methods=["POST"])
def parallel_batch():
    """
    Parallel batch download with SSE streaming.
    Streams JSON events per item as they complete.

    Request:
      { "jobs": [{id, url, format_id, is_audio, is_4k, title}], "download_dir": "..." }

    SSE events:
      { "type": "start",    "client_id": N, "total": N }
      { "type": "done",     "client_id": N, "filepath": "...", "filename": "...", "is_temp": bool }
      { "type": "error",    "client_id": N, "error": "..." }
      { "type": "complete", "done": N, "errors": N, "total": N }
    """
    data         = request.json or {}
    jobs         = data.get("jobs", [])
    download_dir = get_download_dir(data.get("download_dir", ""))

    if not jobs:
        return jsonify({"error": "No jobs provided"}), 400

    def generate():
        total = len(jobs)
        yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

        done_count  = 0
        error_count = 0

        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_BATCH) as executor:
            future_map = {
                executor.submit(_run_single_parallel_job, job, download_dir): job
                for job in jobs
            }
            for future in as_completed(future_map):
                try:
                    result = future.result()
                    if result["status"] == "done":
                        done_count += 1
                        yield f"data: {json.dumps({'type': 'done', **result})}\n\n"
                    else:
                        error_count += 1
                        yield f"data: {json.dumps({'type': 'error', **result})}\n\n"
                except Exception as e:
                    error_count += 1
                    job = future_map[future]
                    yield f"data: {json.dumps({'type': 'error', 'client_id': job.get('id'), 'url': job.get('url'), 'error': str(e)})}\n\n"

        yield f"data: {json.dumps({'type': 'complete', 'done': done_count, 'errors': error_count, 'total': total})}\n\n"

    return Response(generate(), mimetype="text/event-stream")


# ─────────────────────────────────────────────
# /jobs  – list persistent job queue from SQLite
# ─────────────────────────────────────────────
@app.route("/jobs", methods=["GET"])
def get_jobs():
    """
    GET /jobs?limit=100&status=done
    Returns the persistent SQLite job history.
    """
    limit  = int(request.args.get("limit", 100))
    status = request.args.get("status", None)
    jobs   = _db_get_jobs(limit=limit, status=status)
    return jsonify({"jobs": jobs, "count": len(jobs)})


# ─────────────────────────────────────────────
# /logs  – error log viewer
# ─────────────────────────────────────────────
@app.route("/logs", methods=["GET"])
def get_logs():
    """
    GET /logs?limit=100
    Returns the last N error log entries from SQLite.
    """
    limit = int(request.args.get("limit", 100))
    errors = _db_get_errors(limit=limit)
    return jsonify({"errors": errors, "count": len(errors)})


@app.route("/logs", methods=["DELETE"])
def clear_logs():
    """DELETE /logs — wipe the error log table."""
    with _db_lock:
        conn = _db_conn()
        conn.execute("DELETE FROM error_log")
        conn.commit()
        conn.close()
    return jsonify({"status": "cleared"})


# ─────────────────────────────────────────────
# /search  – search via yt-dlp (YouTube default)
# ─────────────────────────────────────────────

def build_webpage_url(entry: dict, platform: str) -> str:
    """Reconstruct a proper watch URL from a flat search entry."""
    # Prefer explicit webpage_url first
    if entry.get("webpage_url"):
        return entry["webpage_url"]

    vid_id = entry.get("id", "")
    url_field = entry.get("url", "")

    if platform == "youtube":
        # flat search returns url = video_id only
        if vid_id and not url_field.startswith("http"):
            return f"https://www.youtube.com/watch?v={vid_id}"
        return url_field or f"https://www.youtube.com/watch?v={vid_id}"

    if platform == "soundcloud":
        return url_field if url_field.startswith("http") else f"https://soundcloud.com/{url_field}"

    if platform == "bilibili":
        if vid_id:
            return f"https://www.bilibili.com/video/{vid_id}"
        return url_field

    return url_field or entry.get("webpage_url", "")


def best_thumbnail(entry: dict) -> str | None:
    """Safely extract the best thumbnail URL from a flat entry."""
    # Direct thumbnail string
    t = entry.get("thumbnail")
    if t and isinstance(t, str):
        return t
    # List of thumbnail objects
    thumbs = entry.get("thumbnails")
    if thumbs and isinstance(thumbs, list):
        # pick the last (usually largest)
        for thumb in reversed(thumbs):
            if isinstance(thumb, dict) and thumb.get("url"):
                return thumb["url"]
    return None


@app.route("/search", methods=["POST"])
def search():
    data     = request.json or {}
    query    = data.get("query", "").strip()
    platform = data.get("platform", "youtube")
    limit    = min(int(data.get("limit", 20)), 50)

    if not query:
        return jsonify({"error": "No search query"}), 400

    # Cache search results (shorter TTL since trending changes)
    ck = _cache_key(f"search:{platform}:{limit}", query)
    cached = _cache_get(ck)
    if cached:
        logger.info(f"Cache hit: search '{query}' on {platform}")
        return jsonify(cached)

    extractors = {
        "youtube":     "ytsearch",
        "soundcloud":  "scsearch",
        "bilibili":    "bilisearch",
    }
    prefix = extractors.get(platform, "ytsearch")
    search_url = f"{prefix}{limit}:{query}"

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
        **_cookie_opts(),
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(search_url, download=False)
    except Exception as e:
        logger.error(f"Search error '{query}' on {platform}: {e}")
        return jsonify({"error": str(e)}), 500

    entries = info.get("entries") or []
    results = []
    for e in entries:
        if not e:
            continue
        webpage_url = build_webpage_url(e, platform)
        if not webpage_url:
            continue
        results.append({
            "id":         e.get("id"),
            "title":      e.get("title") or "Unknown",
            "url":        webpage_url,
            "thumbnail":  best_thumbnail(e),
            "duration":   e.get("duration"),
            "uploader":   e.get("uploader") or e.get("channel") or e.get("uploader_id") or "",
            "view_count": e.get("view_count"),
            "platform":   platform,
        })

    result = {
        "results":  results,
        "query":    query,
        "platform": platform,
        "returned": len(results),
        "note": "YouTube/SoundCloud/Bilibili limit search results to ~20-50 regardless of the requested amount. This is a platform restriction, not a Nova DVR limit."
    }
    _cache_set(ck, result)
    return jsonify(result)



# ─────────────────────────────────────────────
# /ai/summarize  – generate plain-language video summary from metadata
# ─────────────────────────────────────────────
@app.route("/ai/summarize", methods=["POST"])
def ai_summarize():
    data     = request.json or {}
    title    = data.get("title", "")
    uploader = data.get("uploader", "")
    duration = data.get("duration")       # seconds
    platform = data.get("platform", "")
    views    = data.get("view_count")
    date     = data.get("upload_date", "") # YYYYMMDD

    # Format duration
    dur_str = ""
    if duration:
        h = int(duration // 3600)
        m = int((duration % 3600) // 60)
        s = int(duration % 60)
        if h > 0:
            dur_str = f"{h} hour{'s' if h>1 else ''} {m} minute{'s' if m!=1 else ''}"
        elif m > 0:
            dur_str = f"{m} minute{'s' if m!=1 else ''} {s} second{'s' if s!=1 else ''}"
        else:
            dur_str = f"{s} second{'s' if s!=1 else ''}"

    # Format views
    views_str = ""
    if views:
        if views >= 1_000_000:
            views_str = f"{views/1_000_000:.1f} million views"
        elif views >= 1_000:
            views_str = f"{int(views/1_000)}K views"
        else:
            views_str = f"{views} views"

    # Format date
    date_str = ""
    if date and len(date) == 8:
        months = ["","January","February","March","April","May","June",
                  "July","August","September","October","November","December"]
        try:
            y, mo, d_num = date[:4], int(date[4:6]), int(date[6:8])
            date_str = f"{months[mo]} {d_num}, {y}"
        except Exception:
            pass

    # Detect content type from title keywords
    title_lower = title.lower()
    content_hint = ""
    if any(w in title_lower for w in ["tutorial", "how to", "guide", "learn", "course", "lesson"]):
        content_hint = "This appears to be a tutorial or educational video."
    elif any(w in title_lower for w in ["music", "song", "audio", "track", "album", "remix", "official audio", "lyric"]):
        content_hint = "This appears to be a music track or audio content."
    elif any(w in title_lower for w in ["mv", "official video", "music video"]):
        content_hint = "This appears to be an official music video."
    elif any(w in title_lower for w in ["podcast", "interview", "talk", "discussion", "episode"]):
        content_hint = "This appears to be a podcast or interview."
    elif any(w in title_lower for w in ["trailer", "teaser", "preview", "official trailer"]):
        content_hint = "This appears to be a trailer or preview."
    elif any(w in title_lower for w in ["gameplay", "gaming", "playthrough", "walkthrough", "lets play"]):
        content_hint = "This appears to be gaming or gameplay content."
    elif any(w in title_lower for w in ["vlog", "day in", "daily", "my life"]):
        content_hint = "This appears to be a personal vlog."
    elif any(w in title_lower for w in ["news", "breaking", "update", "report"]):
        content_hint = "This appears to be a news or current affairs video."
    elif any(w in title_lower for w in ["review", "unboxing", "hands on", "comparison"]):
        content_hint = "This appears to be a product review or unboxing."

    # Build summary sentence
    parts = []
    if uploader:
        parts.append(f'"{title}" is a {platform} video by {uploader}')
    else:
        parts.append(f'"{title}" is a {platform} video')
    if dur_str:
        parts.append(f"running {dur_str}")
    if date_str:
        parts.append(f"uploaded on {date_str}")
    if views_str:
        parts.append(f"with {views_str}")

    summary = ", ".join(parts) + "."
    if content_hint:
        summary += " " + content_hint

    return jsonify({"summary": summary})


# ─────────────────────────────────────────────
# /ai/recommend  – suggest best format based on download history
# ─────────────────────────────────────────────
@app.route("/ai/recommend", methods=["POST"])
def ai_recommend():
    data    = request.json or {}
    history = data.get("history", [])  # list of {resolution, format, type} from localStorage
    formats = data.get("formats", [])  # current available formats

    if not formats:
        return jsonify({"recommendation": None, "reason": "No formats available"})

    # Count what the user has downloaded most
    res_count: dict = {}
    type_count: dict = {"video+audio": 0, "audio-only": 0}

    for job in history:
        res = job.get("resolution", "")
        typ = "audio-only" if job.get("is_audio") else "video+audio"
        if res:
            res_count[res] = res_count.get(res, 0) + 1
        type_count[typ] = type_count.get(typ, 0) + 1

    prefer_audio = type_count["audio-only"] > type_count["video+audio"]

    # Find best matching format
    recommended = None
    reason = ""

    if prefer_audio:
        audio_formats = [f for f in formats if f.get("type") == "audio-only"]
        if audio_formats:
            # Pick highest bitrate
            best = max(audio_formats, key=lambda f: f.get("abr") or 0)
            recommended = best
            abr = best.get("abr")
            reason = f"You usually download audio. Suggesting {int(abr)}kbps MP3." if abr else "You usually download audio. Suggesting best available MP3."
    else:
        video_formats = [f for f in formats if f.get("type") == "video+audio"]
        if video_formats:
            # Check most used resolution
            height_order = {"4K": 2160, "1440p": 1440, "1080p": 1080, "720p": 720, "480p": 480, "360p": 360, "240p": 240, "144p": 144}

            if res_count:
                top_res = max(res_count, key=lambda r: res_count[r])
                # Find exact match or closest
                exact = next((f for f in video_formats if f.get("resolution") == top_res), None)
                if exact:
                    recommended = exact
                    reason = f"You usually download {top_res}. Suggesting the same."
                else:
                    # Pick best available
                    best = max(video_formats, key=lambda f: height_order.get(f.get("resolution",""), 0))
                    recommended = best
                    reason = f"Your preferred {top_res} isn't available. Suggesting {best.get('resolution')} instead."
            else:
                # No history — suggest 1080p or best available
                p1080 = next((f for f in video_formats if f.get("resolution") == "1080p"), None)
                if p1080:
                    recommended = p1080
                    reason = "Suggesting 1080p as a quality default."
                else:
                    best = max(video_formats, key=lambda f: height_order.get(f.get("resolution",""), 0))
                    recommended = best
                    reason = f"Suggesting best available: {best.get('resolution')}."

    return jsonify({
        "recommendation": recommended,
        "reason": reason,
    })


# ─────────────────────────────────────────────
# /ai/explain-error  – translate yt-dlp errors to plain English
# ─────────────────────────────────────────────
@app.route("/ai/explain-error", methods=["POST"])
def ai_explain_error():
    data  = request.json or {}
    error = data.get("error", "").lower()

    explanations = [
        (["private", "sign in", "login required", "age-restricted"],
         "This video is private or age-restricted. You need to be logged in to access it.",
         "Try using yt-dlp's --cookies option, or find a publicly accessible version."),

        (["unavailable", "not available", "removed", "deleted", "taken down"],
         "This video has been removed or is unavailable in your region.",
         "The content may have been deleted by the uploader or blocked in your country. Try a VPN or look for a re-upload."),

        (["geo", "region", "country", "blocked"],
         "This video is geo-restricted and not available in your region.",
         "Try using a VPN to access it from a supported country."),

        (["copyright", "content id"],
         "This video has been blocked due to a copyright claim.",
         "The uploader's content was claimed. Try finding an official version or a legal alternative."),

        (["rate limit", "too many requests", "429"],
         "You've been temporarily rate-limited by the platform.",
         "Wait a few minutes and try again. Avoid making too many requests in a short period."),

        (["connection", "network", "timeout", "timed out"],
         "There was a network connection issue.",
         "Check your internet connection and make sure the backend server is running. Try again."),

        (["unsupported url", "no suitable", "extractor"],
         "This URL isn't supported by yt-dlp.",
         "Nova DVR uses yt-dlp for downloads. Check if the platform is in the supported sites list."),

        (["format not available", "requested format not available"],
         "The selected format is no longer available for this video.",
         "Click 'Check Available Formats' again to refresh the list, then choose a different format."),

        (["live", "is live"],
         "This is a live stream and cannot be downloaded while it's active.",
         "Wait until the live stream ends and then download the recorded version."),

        (["members only", "membership"],
         "This content is restricted to channel members.",
         "You need an active channel membership to access this video."),
    ]

    for keywords, explanation, suggestion in explanations:
        if any(k in error for k in keywords):
            return jsonify({"explanation": explanation, "suggestion": suggestion})

    # Generic fallback
    return jsonify({
        "explanation": "Something went wrong while trying to download this content.",
        "suggestion": "Make sure the URL is correct, the backend is running, and FFmpeg is installed. If the problem persists, try a different format."
    })



# ─────────────────────────────────────────────
# /subtitles  – list available subtitle tracks for a URL
# ─────────────────────────────────────────────
@app.route("/subtitles", methods=["POST"])
def list_subtitles():
    data = request.json or {}
    url  = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True, **_cookie_opts()}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    subs = info.get("subtitles") or {}
    auto = info.get("automatic_captions") or {}

    tracks = []
    for lang, entries in subs.items():
        exts = [e.get("ext") for e in entries if e.get("ext")]
        tracks.append({"lang": lang, "name": lang, "auto": False, "formats": exts})
    for lang, entries in auto.items():
        if not any(t["lang"] == lang for t in tracks):
            exts = [e.get("ext") for e in entries if e.get("ext")]
            tracks.append({"lang": lang, "name": f"{lang} (auto)", "auto": True, "formats": exts})

    # ── Auto-detect recommended language ──
    # Priority: exact match → base language match → first non-auto track → first auto
    preferred = data.get("preferred_lang", "")   # e.g. "en", "en-US" sent by frontend
    recommended_lang = None

    def lang_score(lang: str, pref: str) -> int:
        if not pref:
            return 0
        if lang == pref:
            return 3
        if lang.split("-")[0] == pref.split("-")[0]:
            return 2
        if lang.startswith("en"):
            return 1
        return 0

    best_score = -1
    for t in tracks:
        score = lang_score(t["lang"], preferred)
        # Prefer non-auto over auto at equal score
        if not t["auto"]:
            score += 0.5
        if score > best_score:
            best_score = score
            recommended_lang = t["lang"]

    return jsonify({"subtitles": tracks, "recommended_lang": recommended_lang})


# ─────────────────────────────────────────────
# /download-with-options  – download with trim and/or subtitles
# ─────────────────────────────────────────────
@app.route("/download-with-options", methods=["POST"])
def download_with_options():
    data         = request.json or {}
    url          = data.get("url", "").strip()
    format_id    = data.get("format_id", "").strip()
    is_audio     = data.get("is_audio", False)
    is_4k        = data.get("is_4k", False)
    custom_dir   = (data.get("download_dir") or "").strip()
    start_time   = data.get("start_time", "")   # e.g. "00:02:00"
    end_time     = data.get("end_time", "")     # e.g. "00:05:00"
    sub_lang     = data.get("sub_lang", "")     # e.g. "en"
    sub_format   = data.get("sub_format", "srt")  # "srt" or "vtt"
    embed_subs   = data.get("embed_subs", False)
    title        = data.get("title", "").strip()

    if not url or not format_id:
        return jsonify({"error": "Missing URL or format_id"}), 400

    save_dir = get_download_dir(custom_dir)
    is_temp  = (save_dir == TEMP_DOWNLOAD_DIR)

    # Persist job to SQLite
    job_id = _db_add_job(url, title=title, format_id=format_id, is_audio=is_audio)

    postprocessors = []
    if is_audio:
        fmt = f"{format_id}/bestaudio"
        postprocessors.append({"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"})
    else:
        fmt = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best" if is_4k else f"{format_id}+bestaudio[ext=m4a]/bestaudio/{format_id}"
        postprocessors.append({"key": "FFmpegVideoConvertor", "preferedformat": "mp4"})

    # Trim via FFmpeg section download
    download_sections = None
    if start_time or end_time:
        s = start_time or "0"
        e = end_time or ""
        download_sections = [f"*{s}-{e}" if e else f"*{s}-inf"]
        postprocessors.append({"key": "FFmpegSplitChapters", "force_keyframes": True})

    # Subtitles
    if sub_lang:
        if embed_subs and not is_audio:
            postprocessors.append({"key": "FFmpegEmbedSubtitle", "already_have_subtitle": False})

    ydl_opts = {
        "format": fmt,
        "outtmpl": os.path.join(save_dir, "%(title)s.%(ext)s"),
        "merge_output_format": "mp4" if not is_audio else None,
        "postprocessors": postprocessors,
        **_cookie_opts(),
    }
    if not is_audio:
        ydl_opts["merge_output_format"] = "mp4"
    if download_sections:
        ydl_opts["download_ranges"] = yt_dlp.utils.download_range_func(None, [(
            float(sum(x * int(t) for x, t in zip([3600, 60, 1], (start_time or "0:0:0").split(":")))) if start_time else 0,
            float(sum(x * int(t) for x, t in zip([3600, 60, 1], (end_time or "0:0:0").split(":")))) if end_time else None,
        )])
        ydl_opts["force_keyframes_at_cuts"] = True
    if sub_lang:
        ydl_opts["writesubtitles"]   = not embed_subs
        ydl_opts["writeautomaticsub"] = True
        ydl_opts["subtitleslangs"]   = [sub_lang]
        ydl_opts["subtitlesformat"]  = sub_format

    return Response(make_download_stream(ydl_opts, url, is_temp, job_id=job_id), mimetype='text/event-stream')


# ─────────────────────────────────────────────
# /trending  – get trending/popular content for a platform
# ─────────────────────────────────────────────
@app.route("/trending", methods=["POST"])
def trending():
    data     = request.json or {}
    platform = data.get("platform", "youtube")
    limit    = min(int(data.get("limit", 20)), 50)  # allow up to 50

    if platform == "soundcloud":
        search_url = f"scsearch{limit}:trending popular music"
        ydl_opts = {
            "quiet": True, "no_warnings": True,
            "skip_download": True, "extract_flat": "in_playlist",
            **_cookie_opts(),
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(search_url, download=False)
            entries = info.get("entries") or []
            results = []
            for e in entries[:limit]:
                if not e:
                    continue
                webpage = e.get("webpage_url") or e.get("url") or ""
                if not webpage.startswith("http"):
                    sc_id = e.get("id", "")
                    webpage = f"https://soundcloud.com/{sc_id}" if sc_id else ""
                results.append({
                    "id":         e.get("id", ""),
                    "title":      e.get("title") or "Unknown",
                    "url":        webpage,
                    "thumbnail":  best_thumbnail(e),
                    "duration":   e.get("duration"),
                    "uploader":   e.get("uploader") or e.get("channel") or "",
                    "view_count": e.get("view_count"),
                    "platform":   platform,
                })
            return jsonify({"results": results, "platform": platform})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif platform == "youtube":
        # YouTube trending feed requires auth — use search as fallback
        search_url = f"ytsearch{limit}:trending music 2025"
        ydl_opts = {
            "quiet": True, "no_warnings": True,
            "skip_download": True, "extract_flat": "in_playlist",
            **_cookie_opts(),
        }
        results = []
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(search_url, download=False)
            entries = info.get("entries") or []
            for e in entries[:limit]:
                if not e:
                    continue
                vid_id = e.get("id", "")
                webpage = e.get("webpage_url") or e.get("url") or ""
                if vid_id and not webpage.startswith("http"):
                    webpage = f"https://www.youtube.com/watch?v={vid_id}"
                results.append({
                    "id":        vid_id,
                    "title":     e.get("title") or "Unknown",
                    "url":       webpage,
                    "thumbnail": best_thumbnail(e),
                    "duration":  e.get("duration"),
                    "uploader":  e.get("uploader") or e.get("channel") or "",
                    "view_count": e.get("view_count"),
                    "platform":  platform,
                })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        return jsonify({"results": results, "platform": platform,
                        "note": "YouTube trending uses popular search results (direct trending feed requires authentication)"})

    else:
        return jsonify({"error": f"Trending not supported for {platform}"}), 400


# ─────────────────────────────────────────────
# /playlist-explode  – expand playlist/channel to individual items
# ─────────────────────────────────────────────
@app.route("/playlist-explode", methods=["POST"])
def playlist_explode():
    data = request.json or {}
    url  = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True,
        **_cookie_opts(),
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    # Single video — not a playlist
    if info.get("_type") not in ("playlist", "multi_video") and not info.get("entries"):
        return jsonify({"is_playlist": False, "items": []})

    entries = info.get("entries") or []
    playlist_title = info.get("title") or info.get("uploader") or "Playlist"
    items = []
    for e in entries:
        if not e:
            continue
        vid_id = e.get("id", "")
        webpage = e.get("webpage_url") or e.get("url") or ""
        # Build proper URL if it's just an ID
        extractor = (e.get("ie_key") or info.get("extractor_key") or "").lower()
        if vid_id and not webpage.startswith("http"):
            if "youtube" in extractor or not extractor:
                webpage = f"https://www.youtube.com/watch?v={vid_id}"
            elif "soundcloud" in extractor:
                webpage = f"https://soundcloud.com/{vid_id}"
            elif "bilibili" in extractor:
                webpage = f"https://www.bilibili.com/video/{vid_id}"
        items.append({
            "id":        vid_id,
            "title":     e.get("title") or vid_id or "Unknown",
            "url":       webpage,
            "duration":  e.get("duration"),
            "thumbnail": e.get("thumbnail"),
            "uploader":  e.get("uploader") or e.get("channel") or "",
        })

    return jsonify({
        "is_playlist": True,
        "playlist_title": playlist_title,
        "count": len(items),
        "items": items,
    })




# ─────────────────────────────────────────────
# /ai/cluster  – classify a list of titles into content categories
# Uses multi-signal scoring: title keywords + uploader cues + duration
# ─────────────────────────────────────────────

CLUSTER_RULES = [
    {
        "label": "🎵 Music",
        "title_keywords": ["music","song","track","album","remix","official audio","lyric","mv",
                           "official video","audio","cover","beat","instrumental","feat","ft.","ost"],
        "uploader_keywords": ["music","records","vevo","official"],
        "short_ok": True,   # music can be any length
    },
    {
        "label": "🎓 Tutorial",
        "title_keywords": ["tutorial","how to","how-to","guide","learn","lesson","course",
                           "explained","step by step","beginners","introduction","for beginners"],
        "uploader_keywords": ["academy","learn","edu","school","tutorial"],
        "short_ok": False,
    },
    {
        "label": "🎮 Gaming",
        "title_keywords": ["gameplay","gaming","playthrough","walkthrough","lets play","let's play",
                           "speedrun","game","boss","raid","pvp","fps","rpg","mmorpg","esports"],
        "uploader_keywords": ["gaming","games","plays","gg"],
        "short_ok": True,
    },
    {
        "label": "📰 News",
        "title_keywords": ["news","breaking","update","report","press","latest","2024","2025",
                           "2026","today","exclusive","live stream"],
        "uploader_keywords": ["news","cnn","bbc","nbc","abc","fox","times","post","media"],
        "short_ok": True,
    },
    {
        "label": "🎙 Podcast",
        "title_keywords": ["podcast","interview","talk","discussion","episode","ep.","ep ",
                           "season","chat with","speaking with","conversation"],
        "uploader_keywords": ["podcast","radio","fm","show"],
        "short_ok": False,
    },
    {
        "label": "📦 Review",
        "title_keywords": ["review","unboxing","hands on","hands-on","comparison","vs","versus",
                           "test","benchmarks","first look","is it worth"],
        "uploader_keywords": ["tech","reviews","tech","gadget"],
        "short_ok": True,
    },
    {
        "label": "🎬 Trailer",
        "title_keywords": ["trailer","teaser","preview","official trailer","clip","promo",
                           "behind the scenes","bts","bloopers","featurette"],
        "uploader_keywords": ["films","movies","pictures","entertainment","studios"],
        "short_ok": True,
    },
    {
        "label": "🎭 Vlog",
        "title_keywords": ["vlog","day in","daily","my life","routine","with me","spend the day",
                           "travel","trip","adventure","challenge","reaction"],
        "uploader_keywords": ["vlogs","daily","life"],
        "short_ok": True,
    },
]

def classify_title(title: str, uploader: str = "", duration: int | None = None) -> str | None:
    """Score each cluster and return the highest-confidence label, or None."""
    title_lower  = (title or "").lower()
    upload_lower = (uploader or "").lower()
    best_label  = None
    best_score  = 0

    for rule in CLUSTER_RULES:
        score = 0
        # Title keyword matches (weighted by keyword length — longer = more specific)
        for kw in rule["title_keywords"]:
            if kw in title_lower:
                score += 1 + len(kw.split()) * 0.5
        # Uploader cues (lower weight)
        for kw in rule["uploader_keywords"]:
            if kw in upload_lower:
                score += 0.75
        # Duration bonus: podcasts/tutorials are usually >10 min; trailers/music <10 min
        if duration:
            if not rule["short_ok"] and duration >= 600:
                score += 0.5
            elif rule["short_ok"] and duration < 600:
                score += 0.25

        if score > best_score:
            best_score = score
            best_label = rule["label"]

    return best_label if best_score >= 1.0 else None


@app.route("/ai/cluster", methods=["POST"])
def ai_cluster():
    """
    Classify a batch of search results into content categories.

    Request:  { "items": [{ "title": "...", "uploader": "...", "duration": 213 }, ...] }
    Response: { "clusters": ["🎵 Music", null, "🎓 Tutorial", ...] }
    """
    data  = request.json or {}
    items = data.get("items", [])

    if not items or not isinstance(items, list):
        return jsonify({"error": "items array required"}), 400

    clusters = []
    for item in items:
        label = classify_title(
            title    = item.get("title", ""),
            uploader = item.get("uploader", ""),
            duration = item.get("duration"),
        )
        clusters.append(label)

    return jsonify({"clusters": clusters})


# ─────────────────────────────────────────────
# /chat  – unified NLP intent handler for the AI assistant
# Parses natural language and returns a structured action response
# ─────────────────────────────────────────────

SEARCH_VERBS  = {"search", "find", "look up", "show", "query", "discover", "browse"}
DOWNLOAD_VERBS = {"download", "get", "save", "extract", "fetch", "grab", "pull"}
HISTORY_VERBS  = {"history", "summary", "what did i download", "my downloads", "show downloads"}
TREND_VERBS    = {"trending", "popular", "what's hot", "top videos", "charts"}

PLATFORM_MAP = {
    "youtube": "youtube", "yt": "youtube",
    "soundcloud": "soundcloud", "sc": "soundcloud",
    "bilibili": "bilibili",
    "facebook": "facebook", "fb": "facebook",
    "instagram": "instagram", "ig": "instagram",
    "tiktok": "tiktok", "twitter": "twitter",
    "x": "twitter",
}

FORMAT_MAP = {
    "mp3": {"is_audio": True, "label": "MP3"},
    "audio": {"is_audio": True, "label": "MP3"},
    "music": {"is_audio": True, "label": "MP3"},
    "4k": {"resolution": "4K", "is_audio": False, "label": "4K"},
    "1080p": {"resolution": "1080p", "is_audio": False, "label": "1080p"},
    "720p": {"resolution": "720p", "is_audio": False, "label": "720p"},
    "480p": {"resolution": "480p", "is_audio": False, "label": "480p"},
    "360p": {"resolution": "360p", "is_audio": False, "label": "360p"},
    "best": {"resolution": "1080p", "is_audio": False, "label": "1080p"},
    "hd": {"resolution": "1080p", "is_audio": False, "label": "1080p"},
    "high quality": {"resolution": "1080p", "is_audio": False, "label": "1080p"},
}

@app.route("/chat", methods=["POST"])
def chat():
    """
    NLP intent parser for the Nova AI Assistant.

    Request:  { "message": "search for lofi hip hop on soundcloud", "history": [...] }
    Response: {
        "intent":    "search" | "download" | "trending" | "history" | "unknown",
        "platform":  "youtube" | "soundcloud" | "bilibili" | ...,
        "query":     "lofi hip hop",
        "url":       null | "https://...",
        "format":    null | { "is_audio": true, "resolution": null, "label": "MP3" },
        "reply":     "Searching for lofi hip hop on SoundCloud...",
    }
    """
    data    = request.json or {}
    message = data.get("message", "").strip()

    if not message:
        return jsonify({"error": "No message provided"}), 400

    text  = message.lower()
    words = set(re.split(r"[\s,]+", text))

    # ── Detect platform ──
    platform = "youtube"  # default
    for kw, pid in PLATFORM_MAP.items():
        if kw in text:
            platform = pid
            break

    # ── Strip platform mention from query ──
    clean = re.sub(r"\b(on|from|in|using|via)\s+(youtube|yt|soundcloud|sc|bilibili|tiktok|instagram|facebook|fb|ig|twitter|x)\b", "", message, flags=re.IGNORECASE).strip()

    # ── Detect URL ──
    url_match = re.search(r"(https?://[^\s]+)", message)
    url = url_match.group(1) if url_match else None

    # ── Detect intent ──
    intent = "unknown"
    query  = clean
    format_hint = None

    # History check first (specific phrases)
    if any(h in text for h in HISTORY_VERBS):
        intent = "history"
        query  = ""

    # Trending
    elif any(t in text for t in TREND_VERBS):
        intent = "trending"
        query  = ""

    # Explicit download URL
    elif url:
        intent = "download"
        query  = url

    # Download verbs
    elif any(text.startswith(v) or f" {v} " in text for v in DOWNLOAD_VERBS):
        intent = "download"
        # Strip the verb
        for v in sorted(DOWNLOAD_VERBS, key=len, reverse=True):
            pattern = re.compile(rf"^{re.escape(v)}\s+|(?<=\s){re.escape(v)}\s+", re.IGNORECASE)
            query = pattern.sub("", clean).strip()
            if query != clean:
                break

    # Search verbs
    elif any(text.startswith(v) or f" {v} " in text for v in SEARCH_VERBS):
        intent = "search"
        for v in sorted(SEARCH_VERBS, key=len, reverse=True):
            pattern = re.compile(rf"^{re.escape(v)}(ing|ed)?\s+(for\s+)?|(?<=\s){re.escape(v)}\s+", re.IGNORECASE)
            query = pattern.sub("", clean).strip()
            if query != clean:
                break

    # Fallback: if no verb found but message looks like a search query
    else:
        intent = "search"
        query  = clean

    # ── Detect format preference ──
    for kw, fhint in FORMAT_MAP.items():
        if kw in text:
            format_hint = fhint
            # Remove format keyword from query
            query = re.sub(rf"\b{re.escape(kw)}\b", "", query, flags=re.IGNORECASE).strip()
            break

    # ── Strip filler phrases from query ──
    fillers = [
        r"^the\s+latest\s+(track|song|video|music)\s+by\s+",
        r"^(the\s+)?(latest|newest|recent)\s+",
        r"^(a\s+)?(song|video|track|music)\s+(called|named|titled)\s+",
        r"^for\s+me\s+",
        r"^for\s+",
        r"\s+please\.?$",
    ]
    for f in fillers:
        query = re.sub(f, "", query, flags=re.IGNORECASE).strip()

    # ── Build human reply ──
    platform_label = platform.capitalize()
    if intent == "search":
        reply = f'🔍 Searching for "{query}" on {platform_label}…'
    elif intent == "download":
        if url:
            reply = f"👀 Inspecting and downloading from URL…"
        elif format_hint:
            reply = f'🚀 Finding "{query}" on {platform_label} and downloading as {format_hint["label"]}…'
        else:
            reply = f'🚀 Finding "{query}" on {platform_label} to download…'
    elif intent == "trending":
        reply = f"🔥 Loading trending content on {platform_label}…"
    elif intent == "history":
        reply = "📋 Here's your recent download history."
    else:
        reply = f'🤔 I\'m not sure what you mean by "{message}". Try "Search for lofi on YouTube" or "Download as MP3".'

    return jsonify({
        "intent":   intent,
        "platform": platform,
        "query":    query,
        "url":      url,
        "format":   format_hint,
        "reply":    reply,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV", "development") != "production"
    app.run(debug=debug, host="0.0.0.0", port=port)
