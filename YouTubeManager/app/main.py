import asyncio
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from typing import Optional, List

import aiofiles
import yt_dlp
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Google Auth / YouTube Upload imports (gracefully optional at import time)
# ---------------------------------------------------------------------------
try:
    from google_auth_oauthlib.flow import Flow
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants & shared state
# ---------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]
CREDENTIALS_DIR = "/app/credentials"
TOKEN_FILE = os.path.join(CREDENTIALS_DIR, "token.json")
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/app/downloads")
AUTH_STATE_TTL_SECONDS = 600
DAILY_UPLOAD_QUOTA_LIMIT = 10_000
UPLOAD_INSERT_COST = 1_600
QUOTA_USAGE_FILE = os.path.join(CREDENTIALS_DIR, "quota_usage.json")

executor = ThreadPoolExecutor(max_workers=4)
tasks: dict = {}  # task_id -> task dict
auth_sessions: dict[str, dict[str, str | float]] = {}

# ---------------------------------------------------------------------------
# FastAPI app setup
# ---------------------------------------------------------------------------
app = FastAPI(title="YouTube Manager", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class SearchRequest(BaseModel):
    query: str
    max_results: int = 10


class DownloadRequest(BaseModel):
    url: str
    format: str = "best"


class UploadLocalRequest(BaseModel):
    filename: str
    title: str
    description: str = ""
    tags: str = ""
    privacy_status: str = "private"


# ---------------------------------------------------------------------------
# Helper: new task
# ---------------------------------------------------------------------------
def new_task(task_type: str) -> str:
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "id": task_id,
        "type": task_type,
        "status": "pending",
        "progress": 0,
        "result": None,
        "error": None,
    }
    return task_id


def _today_utc() -> str:
    return datetime.utcnow().date().isoformat()


def load_quota_usage() -> dict:
    default = {
        "date": _today_utc(),
        "daily_limit": DAILY_UPLOAD_QUOTA_LIMIT,
        "estimated_units_used": 0,
        "estimated_upload_requests": 0,
        "last_video_id": None,
        "last_recorded_at": None,
    }
    if not os.path.exists(QUOTA_USAGE_FILE):
        return default
    try:
        with open(QUOTA_USAGE_FILE, "r") as f:
            data = json.load(f)
    except Exception:
        return default
    if data.get("date") != default["date"]:
        return default
    return {**default, **data}


def save_quota_usage(data: dict) -> None:
    os.makedirs(CREDENTIALS_DIR, exist_ok=True)
    with open(QUOTA_USAGE_FILE, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def record_quota_estimate(*, increment_units: bool = False, video_id: str | None = None) -> None:
    data = load_quota_usage()
    if increment_units:
        data["estimated_units_used"] = int(data.get("estimated_units_used", 0)) + UPLOAD_INSERT_COST
        data["estimated_upload_requests"] = int(data.get("estimated_upload_requests", 0)) + 1
    if video_id:
        data["last_video_id"] = video_id
    data["last_recorded_at"] = datetime.utcnow().isoformat() + "Z"
    save_quota_usage(data)


def get_quota_status() -> dict:
    data = load_quota_usage()
    used = int(data.get("estimated_units_used", 0))
    limit = int(data.get("daily_limit", DAILY_UPLOAD_QUOTA_LIMIT))
    return {
        "date": data["date"],
        "daily_limit": limit,
        "estimated_units_used": used,
        "estimated_units_remaining": max(0, limit - used),
        "estimated_upload_requests": int(data.get("estimated_upload_requests", 0)),
        "upload_cost_per_request": UPLOAD_INSERT_COST,
        "source": "local_estimate",
        "search_uses_official_quota": False,
        "last_video_id": data.get("last_video_id"),
        "last_recorded_at": data.get("last_recorded_at"),
        "note": (
            "YouTube does not expose a simple realtime remaining-quota endpoint here. "
            "This is a local estimate based on upload requests recorded by this system. "
            "yt-dlp search in this app does not consume official YouTube Data API quota."
        ),
    }


def build_return_url(base_url: str, status: str) -> str:
    parsed = urlparse(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["youtube_auth"] = status
    return urlunparse(parsed._replace(query=urlencode(query)))


def is_allowed_return_to(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.hostname not in {"localhost", "127.0.0.1"}:
        return False
    return True


# ---------------------------------------------------------------------------
# yt-dlp helpers
# ---------------------------------------------------------------------------
def search_youtube(query: str, max_results: int = 10) -> list:
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
        entries = result.get("entries", [])
        videos = []
        for entry in entries:
            thumbnails = entry.get("thumbnails") or []
            thumbnail = thumbnails[-1].get("url") if thumbnails else None
            videos.append(
                {
                    "id": entry.get("id"),
                    "title": entry.get("title"),
                    "url": f"https://www.youtube.com/watch?v={entry.get('id')}",
                    "thumbnail": thumbnail,
                    "duration": entry.get("duration"),
                    "channel": entry.get("channel") or entry.get("uploader"),
                    "view_count": entry.get("view_count"),
                    "upload_date": entry.get("upload_date"),
                }
            )
        return videos


def download_video(url: str, task_id: str, format_str: str = "best") -> None:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    def progress_hook(d):
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            downloaded = d.get("downloaded_bytes", 0)
            if total > 0:
                tasks[task_id]["progress"] = int(downloaded / total * 100)
            tasks[task_id]["status"] = "downloading"
        elif d["status"] == "finished":
            tasks[task_id]["status"] = "completed"
            tasks[task_id]["progress"] = 100
            tasks[task_id]["result"] = {"filename": os.path.basename(d["filename"])}

    ydl_opts = {
        "format": format_str,
        "outtmpl": os.path.join(DOWNLOAD_DIR, "%(title)s.%(ext)s"),
        "progress_hooks": [progress_hook],
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = str(e)


# ---------------------------------------------------------------------------
# Google OAuth helpers
# ---------------------------------------------------------------------------
def get_auth_flow() -> "Flow":
    if not GOOGLE_AVAILABLE:
        raise RuntimeError("google-auth-oauthlib is not installed")
    client_secrets = resolve_client_secrets_path()
    redirect_uri = os.environ.get(
        "OAUTH_REDIRECT_URI", "http://localhost:8899/api/auth/callback"
    )
    flow = Flow.from_client_secrets_file(
        client_secrets, scopes=SCOPES, redirect_uri=redirect_uri
    )
    return flow


def prune_auth_sessions() -> None:
    cutoff = time.time() - AUTH_STATE_TTL_SECONDS
    expired_states = [
        state for state, session in auth_sessions.items()
        if float(session.get("created_at", 0)) < cutoff
    ]
    for state in expired_states:
        auth_sessions.pop(state, None)


def resolve_client_secrets_path() -> str:
    client_secrets = os.environ.get("GOOGLE_CLIENT_SECRETS_FILE")
    if client_secrets and os.path.exists(client_secrets):
        return client_secrets

    legacy_default = os.path.join(CREDENTIALS_DIR, "client_secrets.json")
    if os.path.exists(legacy_default):
        return legacy_default

    matches = sorted(Path(CREDENTIALS_DIR).glob("client_secret*.json"))
    if not matches:
        raise RuntimeError(
            f"No OAuth client secrets found in {CREDENTIALS_DIR}. "
            "Add client_secret*.json or set GOOGLE_CLIENT_SECRETS_FILE."
        )
    return str(matches[0])


def get_credentials() -> Optional["Credentials"]:
    if not GOOGLE_AVAILABLE:
        return None
    if not os.path.exists(TOKEN_FILE):
        return None
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        os.makedirs(CREDENTIALS_DIR, exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds if (creds and creds.valid) else None


def upload_to_youtube(
    filepath: str,
    title: str,
    description: str,
    tags: List[str],
    privacy: str,
    task_id: str,
) -> None:
    creds = get_credentials()
    if not creds:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = "Not authenticated. Please authorize first."
        return

    try:
        youtube = build("youtube", "v3", credentials=creds)
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": tags,
            },
            "status": {
                "privacyStatus": privacy,
            },
        }
        media = MediaFileUpload(
            filepath, resumable=True, chunksize=10 * 1024 * 1024
        )
        request = youtube.videos().insert(
            part="snippet,status", body=body, media_body=media
        )
        record_quota_estimate(increment_units=True)

        tasks[task_id]["status"] = "uploading"
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                tasks[task_id]["progress"] = int(status.progress() * 100)

        record_quota_estimate(video_id=response["id"])
        tasks[task_id]["status"] = "completed"
        tasks[task_id]["progress"] = 100
        tasks[task_id]["result"] = {
            "video_id": response["id"],
            "url": f"https://www.youtube.com/watch?v={response['id']}",
        }
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = str(e)


# ---------------------------------------------------------------------------
# Routes: General
# ---------------------------------------------------------------------------
@app.get("/api/tasks")
async def list_tasks():
    return {"tasks": list(tasks.values())}


# ---------------------------------------------------------------------------
# Routes: Search
# ---------------------------------------------------------------------------
@app.post("/api/search")
async def search(request: SearchRequest):
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    loop = asyncio.get_event_loop()
    try:
        results = await loop.run_in_executor(
            executor, search_youtube, request.query, request.max_results
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
    return {"results": results}


# ---------------------------------------------------------------------------
# Routes: Download
# ---------------------------------------------------------------------------
@app.post("/api/download")
async def start_download(request: DownloadRequest):
    if not request.url.strip():
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    task_id = new_task("download")
    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor, download_video, request.url, task_id, request.format
    )
    return {"task_id": task_id, "status": "pending"}


@app.get("/api/status/{task_id}")
async def get_task_status(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks[task_id]


@app.get("/api/downloads")
async def list_downloads():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    files = []
    for entry in Path(DOWNLOAD_DIR).iterdir():
        if entry.is_file():
            stat = entry.stat()
            files.append(
                {
                    "filename": entry.name,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                }
            )
    files.sort(key=lambda x: x["modified"], reverse=True)
    return {"files": files}


@app.get("/api/download/{filename}")
async def serve_download(filename: str):
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    # Security: ensure the path is inside DOWNLOAD_DIR
    real_path = os.path.realpath(filepath)
    real_dir = os.path.realpath(DOWNLOAD_DIR)
    if not real_path.startswith(real_dir + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")
    return FileResponse(real_path, filename=filename)


@app.delete("/api/download/{filename}")
async def delete_download(filename: str):
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    real_path = os.path.realpath(filepath)
    real_dir = os.path.realpath(DOWNLOAD_DIR)
    if not real_path.startswith(real_dir + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")
    os.remove(real_path)
    return {"message": f"Deleted {filename}"}


# ---------------------------------------------------------------------------
# Routes: Auth
# ---------------------------------------------------------------------------
@app.get("/api/auth/url")
async def get_auth_url(return_to: str | None = None, mode: str | None = None):
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    try:
        resolve_client_secrets_path()
    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail=str(e),
        )
    try:
        prune_auth_sessions()
        flow = get_auth_flow()
        auth_kwargs = {
            "access_type": "offline",
            "include_granted_scopes": "true",
        }
        if not os.path.exists(TOKEN_FILE):
            auth_kwargs["prompt"] = "consent"
        auth_url, state = flow.authorization_url(**auth_kwargs)
        code_verifier = getattr(flow, "code_verifier", None)
        if not code_verifier:
            raise RuntimeError("Failed to create OAuth PKCE verifier")
        auth_sessions[state] = {
            "code_verifier": code_verifier,
            "created_at": time.time(),
            "return_to": return_to or "",
            "mode": mode or "",
        }
        return {"url": auth_url}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate auth URL: {str(e)}"
        )


@app.get("/api/auth/start")
async def start_auth(return_to: str | None = None, mode: str | None = None):
    payload = await get_auth_url(return_to=return_to, mode=mode)
    return RedirectResponse(url=payload["url"], status_code=307)


@app.get("/api/auth/callback")
async def auth_callback(code: str, state: str):
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    try:
        prune_auth_sessions()
        session = auth_sessions.pop(state, None)
        if not session:
            raise RuntimeError(
                "OAuth session expired or is invalid. Start login again from /api/auth/url."
            )
        flow = get_auth_flow()
        flow.fetch_token(code=code, code_verifier=str(session["code_verifier"]))
        creds = flow.credentials
        os.makedirs(CREDENTIALS_DIR, exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        return_to = str(session.get("return_to") or "")
        mode = str(session.get("mode") or "")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Authorization failed: {str(e)}"
        )
    if is_allowed_return_to(return_to) and mode != "popup":
        return RedirectResponse(url=build_return_url(return_to, "success"), status_code=303)
    if mode == "popup":
        safe_return_to = build_return_url(return_to, "success") if is_allowed_return_to(return_to) else ""
        target_origin = ""
        if safe_return_to:
            parsed_return_to = urlparse(safe_return_to)
            target_origin = f"{parsed_return_to.scheme}://{parsed_return_to.netloc}"
        return HTMLResponse(
            f"""
            <html>
              <head><title>YouTube Authorized</title></head>
              <body style="font-family:sans-serif;padding:32px;line-height:1.5">
                <h1>YouTube authorization complete</h1>
                <p>The token has been saved. This page should close automatically.</p>
                <button id="return-btn" type="button">Return to VideoProcess</button>
                <script>
                  const returnTo = {json.dumps(safe_return_to)};
                  const targetOrigin = {json.dumps(target_origin)};
                  const hasOpener = !!(window.opener && !window.opener.closed);

                  if (hasOpener) {{
                    try {{
                      window.opener.postMessage({{ type: "vp-youtube-auth", status: "success" }}, targetOrigin || window.location.origin);
                    }} catch (error) {{
                      console.error(error);
                    }}
                    window.setTimeout(() => window.close(), 300);
                  }} else if (returnTo) {{
                    window.location.replace(returnTo);
                  }}

                  document.getElementById("return-btn")?.addEventListener("click", () => {{
                    if (hasOpener) {{
                      window.close();
                      return;
                    }}
                    if (returnTo) {{
                      window.location.href = returnTo;
                    }}
                  }});
                </script>
              </body>
            </html>
            """
        )
    return HTMLResponse(
        """
        <html>
          <head><title>YouTube Authorized</title></head>
          <body style="font-family:sans-serif;padding:32px;line-height:1.5">
            <h1>YouTube authorization complete</h1>
            <p>The token has been saved. You can close this page and return to VideoProcess.</p>
            <button onclick="window.close()">Close</button>
          </body>
        </html>
        """
    )


@app.get("/api/auth/status")
async def auth_status():
    if not GOOGLE_AVAILABLE:
        return {"authenticated": False, "reason": "Google libraries not available"}
    try:
        resolve_client_secrets_path()
        has_secrets = True
    except RuntimeError:
        has_secrets = False
    creds = get_credentials()
    return {
        "authenticated": creds is not None,
        "has_client_secrets": has_secrets,
        "token_exists": os.path.exists(TOKEN_FILE),
        "quota_estimate": get_quota_status(),
    }


@app.post("/api/auth/logout")
async def auth_logout():
    if os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)
    return {"message": "Logged out successfully"}


# ---------------------------------------------------------------------------
# Routes: Upload
# ---------------------------------------------------------------------------
@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    title: str = Form(...),
    description: str = Form(""),
    tags: str = Form(""),
    privacy_status: str = Form("private"),
):
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    creds = get_credentials()
    if not creds:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated. Please authorize with Google first.",
        )

    # Save uploaded file to a temp location inside downloads dir
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    safe_name = os.path.basename(file.filename or "upload")
    temp_path = os.path.join(DOWNLOAD_DIR, f"_upload_{uuid.uuid4()}_{safe_name}")
    try:
        async with aiofiles.open(temp_path, "wb") as f_out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                await f_out.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    task_id = new_task("upload")

    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor,
        upload_to_youtube,
        temp_path,
        title,
        description,
        tag_list,
        privacy_status,
        task_id,
    )
    return {"task_id": task_id, "status": "pending"}


@app.post("/api/upload/local")
async def upload_local_file(request: UploadLocalRequest):
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    creds = get_credentials()
    if not creds:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated. Please authorize with Google first.",
        )

    filepath = os.path.join(DOWNLOAD_DIR, request.filename)
    real_path = os.path.realpath(filepath)
    real_dir = os.path.realpath(DOWNLOAD_DIR)
    if not real_path.startswith(real_dir + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")
    if not os.path.exists(real_path):
        raise HTTPException(
            status_code=404, detail=f"File '{request.filename}' not found in downloads"
        )

    tag_list = [t.strip() for t in request.tags.split(",") if t.strip()]
    task_id = new_task("upload")

    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor,
        upload_to_youtube,
        real_path,
        request.title,
        request.description,
        tag_list,
        request.privacy_status,
        task_id,
    )
    return {"task_id": task_id, "status": "pending"}
