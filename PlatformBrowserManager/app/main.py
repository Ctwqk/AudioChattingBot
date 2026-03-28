from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import signal
import sqlite3
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)

OPENCLI_VERSION = os.environ.get("OPENCLI_VERSION", "1.5.3")
DOWNLOAD_ROOT = Path(os.environ.get("DOWNLOAD_ROOT", "/app/downloads"))
PROFILE_ROOT = Path(os.environ.get("BROWSER_PROFILE_ROOT", "/app/browser-profiles"))
CHROME_BIN = os.environ.get("CHROME_BIN") or shutil.which("google-chrome") or shutil.which("chromium") or shutil.which("chromium-browser")
OPENCLI_BIN = os.environ.get("OPENCLI_BIN", "").strip()
OPENCLI_NPX = os.environ.get("OPENCLI_NPX", "npx")
AUTH_CACHE_TTL_SECONDS = int(os.environ.get("PLATFORM_AUTH_CACHE_TTL_SECONDS", "20"))
COMMAND_TIMEOUT_SECONDS = int(os.environ.get("PLATFORM_COMMAND_TIMEOUT_SECONDS", "180"))
HEADLESS_SEARCH_WAIT_SECONDS = float(os.environ.get("PLATFORM_HEADLESS_STARTUP_WAIT_SECONDS", "2"))


class AuthStartResponse(BaseModel):
    platform: str
    ok: bool = True
    browser_started: bool
    headed: bool
    message: str


class AuthStatusResponse(BaseModel):
    platform: str
    authenticated: bool
    browser_running: bool
    headed: bool
    cookie_present: bool
    reason: str | None = None
    detail: str | None = None
    last_checked_at: float


class SearchRequest(BaseModel):
    query: str
    max_results: int = Field(default=8, ge=1, le=50)


class DownloadRequest(BaseModel):
    url: str
    format: str = "best"


class SearchResultItem(BaseModel):
    id: str
    platform: str
    title: str
    url: str | None = None
    thumbnail: str | None = None
    duration: int | None = None
    channel: str | None = None


class SearchResponse(BaseModel):
    platform: str
    results: list[SearchResultItem]


class DownloadResponse(BaseModel):
    platform: str
    download_id: str
    filename: str
    size: int
    source_url: str


class PlatformSpec(BaseModel):
    key: str
    display_name: str
    domain: str
    login_url: str
    headless_search_target: str


@dataclass(frozen=True)
class PlatformConfig:
    key: str
    display_name: str
    domain: str
    login_url: str
    home_url: str
    command_site: str
    status_cookie_names: tuple[str, ...]
    port: int
    cdp_target: str

    @property
    def profile_dir(self) -> Path:
        return PROFILE_ROOT / self.key

    @property
    def downloads_dir(self) -> Path:
        return DOWNLOAD_ROOT / self.key

    @property
    def pid_file(self) -> Path:
        return self.profile_dir / "chrome.pid"

    @property
    def mode_file(self) -> Path:
        return self.profile_dir / "browser_mode.json"

    @property
    def cdp_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


PLATFORMS: dict[str, PlatformConfig] = {
    "xiaohongshu": PlatformConfig(
        key="xiaohongshu",
        display_name="Xiaohongshu",
        domain="xiaohongshu.com",
        login_url="https://www.xiaohongshu.com/explore",
        home_url="https://www.xiaohongshu.com/explore",
        command_site="xiaohongshu",
        status_cookie_names=("web_session",),
        port=19221,
        cdp_target="xiaohongshu",
    ),
    "bilibili": PlatformConfig(
        key="bilibili",
        display_name="Bilibili",
        domain="bilibili.com",
        login_url="https://www.bilibili.com",
        home_url="https://www.bilibili.com",
        command_site="bilibili",
        status_cookie_names=("SESSDATA", "bili_jct", "DedeUserID"),
        port=19222,
        cdp_target="bilibili",
    ),
}

auth_status_cache: dict[str, AuthStatusResponse] = {}
download_index: dict[str, Path] = {}

app = FastAPI(title="Platform Browser Manager", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _require_platform(platform: str) -> PlatformConfig:
    cfg = PLATFORMS.get(platform)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Unsupported platform '{platform}'")
    return cfg


def _cookie_db_candidates(profile_dir: Path) -> list[Path]:
    return [
        profile_dir / "Default" / "Network" / "Cookies",
        profile_dir / "Default" / "Cookies",
    ]


def _extract_cookie_names(profile_dir: Path, domain: str) -> set[str]:
    for candidate in _cookie_db_candidates(profile_dir):
        if not candidate.exists():
            continue

        temp_copy = candidate.with_suffix(".tmp")
        try:
            shutil.copy2(candidate, temp_copy)
            conn = sqlite3.connect(str(temp_copy))
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM cookies WHERE host_key LIKE ?",
                (f"%{domain}",),
            )
            names = {str(row[0]) for row in cur.fetchall() if row and row[0]}
            conn.close()
            return names
        except sqlite3.Error:
            continue
        finally:
            temp_copy.unlink(missing_ok=True)
    return set()


async def _http_get_json(url: str) -> Any | None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except Exception:
        return None


async def _browser_running(cfg: PlatformConfig) -> bool:
    payload = await _http_get_json(f"{cfg.cdp_url}/json/version")
    return isinstance(payload, dict) and bool(payload.get("Browser"))


def _record_browser_mode(cfg: PlatformConfig, *, headed: bool) -> None:
    cfg.profile_dir.mkdir(parents=True, exist_ok=True)
    cfg.mode_file.write_text(json.dumps({"headed": headed, "updated_at": time.time()}))


def _read_browser_mode(cfg: PlatformConfig) -> bool:
    if not cfg.mode_file.exists():
        return False
    try:
        payload = json.loads(cfg.mode_file.read_text())
    except json.JSONDecodeError:
        return False
    return bool(payload.get("headed"))


async def _open_tab(cfg: PlatformConfig, url: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.put(f"{cfg.cdp_url}/json/new?{quote_plus(url)}")
            return response.status_code < 400
    except Exception:
        return False


def _kill_pid(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        return


async def _stop_browser(cfg: PlatformConfig) -> None:
    if cfg.pid_file.exists():
        try:
            pid = int(cfg.pid_file.read_text().strip())
        except ValueError:
            pid = 0
        if pid > 0:
            _kill_pid(pid)
    await asyncio.sleep(1)
    if await _browser_running(cfg):
        subprocess.run(
            ["pkill", "-f", f"--user-data-dir={cfg.profile_dir}"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    cfg.pid_file.unlink(missing_ok=True)


async def _start_browser(cfg: PlatformConfig, *, headed: bool, url: str) -> bool:
    if not CHROME_BIN:
        raise HTTPException(status_code=503, detail="Chrome binary not found on server")
    if headed and not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        raise HTTPException(status_code=503, detail="Headed platform login requires DISPLAY/WAYLAND_DISPLAY on the host")

    if await _browser_running(cfg):
        current_headed = _read_browser_mode(cfg)
        if headed and not current_headed:
            await _stop_browser(cfg)
        else:
            await _open_tab(cfg, url)
            return False

    cfg.profile_dir.mkdir(parents=True, exist_ok=True)
    cfg.downloads_dir.mkdir(parents=True, exist_ok=True)

    args = [
        CHROME_BIN,
        f"--user-data-dir={cfg.profile_dir}",
        f"--remote-debugging-port={cfg.port}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-sync",
        "--password-store=basic",
        "--disable-popup-blocking",
    ]
    if not headed:
        args.extend(["--headless=new", "--disable-gpu", "--hide-scrollbars", "--mute-audio"])
    args.append(url)

    proc = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    cfg.pid_file.write_text(str(proc.pid))
    _record_browser_mode(cfg, headed=headed)

    deadline = time.time() + 15
    while time.time() < deadline:
        if await _browser_running(cfg):
            return True
        await asyncio.sleep(0.5)

    raise HTTPException(status_code=503, detail=f"Timed out starting browser for {cfg.display_name}")


async def _ensure_command_browser(cfg: PlatformConfig) -> None:
    if await _browser_running(cfg):
        return
    await _start_browser(cfg, headed=False, url=cfg.home_url)
    await asyncio.sleep(HEADLESS_SEARCH_WAIT_SECONDS)


def _opencli_command_base() -> list[str]:
    if OPENCLI_BIN:
        return [OPENCLI_BIN]
    return [OPENCLI_NPX, "-y", f"@jackwener/opencli@{OPENCLI_VERSION}"]


async def _run_opencli(cfg: PlatformConfig, args: list[str]) -> tuple[int, str, str]:
    env = {
        **os.environ,
        "OPENCLI_CDP_ENDPOINT": cfg.cdp_url,
        "OPENCLI_CDP_TARGET": cfg.cdp_target,
    }
    proc = await asyncio.create_subprocess_exec(
        *(_opencli_command_base() + args),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=COMMAND_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        proc.kill()
        raise HTTPException(status_code=504, detail=f"{cfg.display_name} command timed out")
    return proc.returncode or 0, stdout.decode("utf-8", errors="replace"), stderr.decode("utf-8", errors="replace")


def _map_opencli_error(detail: str, *, platform: PlatformConfig) -> HTTPException:
    lowered = detail.lower()
    if "auth_required" in lowered or "not logged in" in lowered or "login wall" in lowered or "请登录" in detail or "登录" in detail:
        return HTTPException(status_code=401, detail=f"{platform.display_name} login_required: {detail.strip()}")
    if "browser extension is not connected" in lowered or "cdp" in lowered or "chrome" in lowered:
        return HTTPException(status_code=503, detail=f"{platform.display_name} platform_unavailable: {detail.strip()}")
    return HTTPException(status_code=502, detail=f"{platform.display_name} platform_command_failed: {detail.strip()}")


def _parse_json_output(stdout: str, stderr: str) -> Any:
    text = (stdout or "").strip()
    if not text:
        raise HTTPException(status_code=502, detail=f"Platform command returned empty JSON output: {stderr.strip()}")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to parse platform JSON output: {text[:400]}") from exc


def _normalize_search_result(platform: str, item: dict[str, Any], index: int) -> SearchResultItem:
    title = str(item.get("title") or "").strip()
    url = str(item.get("url") or "").strip() or None
    channel = str(item.get("author") or item.get("channel") or "").strip() or None
    identifier = ""
    if platform == "xiaohongshu":
        identifier = _extract_xiaohongshu_note_id(url or "") or f"xhs-{index}"
    elif platform == "bilibili":
        identifier = _extract_bilibili_bvid(url or "") or f"bili-{index}"
    else:
        identifier = f"{platform}-{index}"
    return SearchResultItem(
        id=identifier,
        platform=platform,
        title=title or identifier,
        url=url,
        channel=channel,
    )


def _extract_bilibili_bvid(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path or url
    for part in path.split("/"):
        if part.startswith("BV"):
            return part
    return None


def _extract_xiaohongshu_note_id(url: str) -> str | None:
    import re

    match = re.search(r"([0-9a-f]{24})", url, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _map_quality(fmt: str) -> str:
    value = fmt.strip().lower()
    if value in {"best", "1080p", "720p", "480p"}:
        return value
    if value == "audio_only":
        raise HTTPException(status_code=400, detail="audio_only is not supported for Xiaohongshu/Bilibili downloads in phase 1")
    return "best"


def _find_downloaded_file(root: Path) -> Path | None:
    candidates = [path for path in root.rglob("*") if path.is_file()]
    if not candidates:
        return None
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0]


async def _resolve_auth_status(cfg: PlatformConfig) -> AuthStatusResponse:
    cached = auth_status_cache.get(cfg.key)
    now = time.time()
    if cached and (now - cached.last_checked_at) < AUTH_CACHE_TTL_SECONDS:
        return cached

    browser_running = await _browser_running(cfg)
    headed = _read_browser_mode(cfg)
    cookie_names = _extract_cookie_names(cfg.profile_dir, cfg.domain)
    cookie_present = any(name in cookie_names for name in cfg.status_cookie_names) if cfg.status_cookie_names else bool(cookie_names)

    if browser_running:
        reason = None if cookie_present else "login_required"
        authenticated = cookie_present
    elif cookie_present:
        reason = "browser_not_running"
        authenticated = True
    else:
        reason = "login_required"
        authenticated = False

    status = AuthStatusResponse(
        platform=cfg.key,
        authenticated=authenticated,
        browser_running=browser_running,
        headed=headed,
        cookie_present=cookie_present,
        reason=reason,
        detail=None if authenticated else f"{cfg.display_name} login is required",
        last_checked_at=now,
    )
    auth_status_cache[cfg.key] = status
    return status


@app.on_event("startup")
async def on_startup() -> None:
    DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    PROFILE_ROOT.mkdir(parents=True, exist_ok=True)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/platforms", response_model=list[PlatformSpec])
async def list_platforms() -> list[PlatformSpec]:
    return [
        PlatformSpec(
            key=cfg.key,
            display_name=cfg.display_name,
            domain=cfg.domain,
            login_url=cfg.login_url,
            headless_search_target=cfg.home_url,
        )
        for cfg in PLATFORMS.values()
    ]


@app.get("/api/platforms/{platform}/auth/status", response_model=AuthStatusResponse)
async def auth_status(platform: str) -> AuthStatusResponse:
    cfg = _require_platform(platform)
    return await _resolve_auth_status(cfg)


@app.get("/api/platforms/{platform}/auth/start")
async def auth_start_page(platform: str) -> HTMLResponse:
    cfg = _require_platform(platform)
    started = await _start_browser(cfg, headed=True, url=cfg.login_url)
    auth_status_cache.pop(cfg.key, None)
    response = AuthStartResponse(
        platform=cfg.key,
        browser_started=started,
        headed=True,
        message=f"{cfg.display_name} browser session is ready. Complete login in the host Chrome window, then return to VideoProcess.",
    )
    return HTMLResponse(
        f"""
        <html>
          <head><title>{cfg.display_name} Login</title></head>
          <body style="font-family:sans-serif;padding:32px;line-height:1.5">
            <h1>{cfg.display_name} login session started</h1>
            <p>{response.message}</p>
            <pre style="background:#111827;color:#e5e7eb;padding:12px;border-radius:8px">{json.dumps(response.model_dump(), indent=2, ensure_ascii=False)}</pre>
            <p>You can close this tab after login succeeds.</p>
          </body>
        </html>
        """
    )


@app.post("/api/platforms/{platform}/auth/logout")
async def auth_logout(platform: str) -> dict[str, str]:
    cfg = _require_platform(platform)
    await _stop_browser(cfg)
    shutil.rmtree(cfg.profile_dir, ignore_errors=True)
    auth_status_cache.pop(cfg.key, None)
    return {"message": f"{cfg.display_name} logged out"}


@app.post("/api/platforms/{platform}/search", response_model=SearchResponse)
async def search(platform: str, request: SearchRequest) -> SearchResponse:
    cfg = _require_platform(platform)
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    await _ensure_command_browser(cfg)
    if platform == "xiaohongshu":
        args = [cfg.command_site, "search", request.query.strip(), "--limit", str(request.max_results), "-f", "json"]
    else:
        args = [cfg.command_site, "search", request.query.strip(), "--type", "video", "--limit", str(request.max_results), "-f", "json"]

    code, stdout, stderr = await _run_opencli(cfg, args)
    if code != 0:
        raise _map_opencli_error(stderr or stdout, platform=cfg)

    payload = _parse_json_output(stdout, stderr)
    if not isinstance(payload, list):
        return SearchResponse(platform=platform, results=[])

    results = [
        _normalize_search_result(platform, item, index)
        for index, item in enumerate(payload, start=1)
        if isinstance(item, dict) and item.get("url")
    ]
    auth_status_cache.pop(cfg.key, None)
    return SearchResponse(platform=platform, results=results)


@app.post("/api/platforms/{platform}/download", response_model=DownloadResponse)
async def download(platform: str, request: DownloadRequest) -> DownloadResponse:
    cfg = _require_platform(platform)
    source_url = request.url.strip()
    if not source_url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")

    await _ensure_command_browser(cfg)
    download_id = str(uuid.uuid4())
    target_dir = cfg.downloads_dir / download_id
    target_dir.mkdir(parents=True, exist_ok=True)

    quality = _map_quality(request.format)
    if platform == "xiaohongshu":
        note_id = _extract_xiaohongshu_note_id(source_url)
        if not note_id:
            raise HTTPException(status_code=400, detail="Could not extract Xiaohongshu note id from URL")
        args = [cfg.command_site, "download", note_id, "--output", str(target_dir), "-f", "json"]
    else:
        bvid = _extract_bilibili_bvid(source_url)
        if not bvid:
            raise HTTPException(status_code=400, detail="Could not extract Bilibili BV id from URL")
        args = [cfg.command_site, "download", bvid, "--output", str(target_dir), "--quality", quality, "-f", "json"]

    code, stdout, stderr = await _run_opencli(cfg, args)
    if code != 0:
        raise _map_opencli_error(stderr or stdout, platform=cfg)

    payload = _parse_json_output(stdout, stderr)
    if isinstance(payload, list) and payload:
        first = payload[0]
        if isinstance(first, dict) and str(first.get("status") or "").lower() == "failed":
            detail = str(first.get("size") or first.get("message") or stdout or stderr)
            raise _map_opencli_error(detail, platform=cfg)

    file_path = _find_downloaded_file(target_dir)
    if not file_path:
        raise HTTPException(status_code=502, detail=f"{cfg.display_name} platform_download_failed: no media file was produced")

    download_index[download_id] = file_path
    auth_status_cache.pop(cfg.key, None)
    return DownloadResponse(
        platform=cfg.key,
        download_id=download_id,
        filename=file_path.name,
        size=file_path.stat().st_size,
        source_url=source_url,
    )


@app.get("/api/platforms/{platform}/downloads/{download_id}")
async def get_download(platform: str, download_id: str) -> FileResponse:
    cfg = _require_platform(platform)
    file_path = download_index.get(download_id)
    if not file_path:
        candidate = cfg.downloads_dir / download_id
        file_path = _find_downloaded_file(candidate)
    if not file_path or not file_path.exists():
        raise HTTPException(status_code=404, detail="Download not found")
    return FileResponse(str(file_path), filename=file_path.name)
