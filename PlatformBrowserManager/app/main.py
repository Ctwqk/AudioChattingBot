from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import mimetypes
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from pydantic import BaseModel, Field
from starlette.datastructures import UploadFile as StarletteUploadFile
import websockets


logger = logging.getLogger(__name__)

OPENCLI_VERSION = os.environ.get("OPENCLI_VERSION", "1.5.3")
DOWNLOAD_ROOT = Path(os.environ.get("DOWNLOAD_ROOT", "/app/downloads"))
PROFILE_ROOT = Path(os.environ.get("BROWSER_PROFILE_ROOT", "/app/browser-profiles"))
CHROME_BIN = os.environ.get("CHROME_BIN") or shutil.which("google-chrome") or shutil.which("chromium") or shutil.which("chromium-browser")
OPENCLI_BIN = os.environ.get("OPENCLI_BIN", "").strip()
OPENCLI_NPX = os.environ.get("OPENCLI_NPX", "npx")
XVFB_RUN_BIN = shutil.which("xvfb-run")
FFMPEG_BIN = shutil.which("ffmpeg")
FFPROBE_BIN = shutil.which("ffprobe")
AUTH_CACHE_TTL_SECONDS = int(os.environ.get("PLATFORM_AUTH_CACHE_TTL_SECONDS", "20"))
COMMAND_TIMEOUT_SECONDS = int(os.environ.get("PLATFORM_COMMAND_TIMEOUT_SECONDS", "180"))
XIAOHONGSHU_SEARCH_TIMEOUT_SECONDS = int(os.environ.get("PLATFORM_XIAOHONGSHU_SEARCH_TIMEOUT_SECONDS", "30"))
HEADLESS_SEARCH_WAIT_SECONDS = float(os.environ.get("PLATFORM_HEADLESS_STARTUP_WAIT_SECONDS", "2"))
AUTH_QR_CAPTURE_WAIT_SECONDS = float(os.environ.get("PLATFORM_AUTH_QR_CAPTURE_WAIT_SECONDS", "4"))
AUTH_QR_READY_TIMEOUT_SECONDS = float(os.environ.get("PLATFORM_AUTH_QR_READY_TIMEOUT_SECONDS", "12"))
AUTH_QR_POLL_INTERVAL_SECONDS = float(os.environ.get("PLATFORM_AUTH_QR_POLL_INTERVAL_SECONDS", "0.5"))
AUTH_QR_REFRESH_MS = int(os.environ.get("PLATFORM_AUTH_QR_REFRESH_MS", "15000"))
AUTH_STATUS_POLL_MS = int(os.environ.get("PLATFORM_AUTH_STATUS_POLL_MS", "3000"))
VIEWPORT_WIDTH = int(os.environ.get("PLATFORM_BROWSER_VIEWPORT_WIDTH", "1280"))
VIEWPORT_HEIGHT = int(os.environ.get("PLATFORM_BROWSER_VIEWPORT_HEIGHT", "900"))
UPLOAD_STAGING_ROOT = Path(os.environ.get("UPLOAD_STAGING_ROOT", "/tmp/platform-publish-uploads"))
X_VIDEO_SETTLE_SECONDS = float(os.environ.get("PLATFORM_X_VIDEO_SETTLE_SECONDS", "31"))
EXTERNAL_UPLOAD_HOST_ROOT = Path(os.environ["PLATFORM_EXTERNAL_UPLOAD_HOST_ROOT"]).resolve() if os.environ.get("PLATFORM_EXTERNAL_UPLOAD_HOST_ROOT") else None
X_MEDIA_HELPER_URL = os.environ.get("X_MEDIA_HELPER_URL", "http://127.0.0.1:7711/post/media/raw").strip()
X_EXTERNAL_TAB_CLEANUP_ENABLED = os.environ.get("PLATFORM_X_EXTERNAL_TAB_CLEANUP_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
X_EXTERNAL_TAB_KEEP_COUNT = max(1, int(os.environ.get("PLATFORM_X_EXTERNAL_TAB_KEEP_COUNT", "1")))
X_EXTERNAL_TAB_OPEN_WAIT_SECONDS = float(os.environ.get("PLATFORM_X_EXTERNAL_TAB_OPEN_WAIT_SECONDS", "1.5"))
X_EXTERNAL_PUBLISH_RETRY_COUNT = max(0, int(os.environ.get("PLATFORM_X_EXTERNAL_PUBLISH_RETRY_COUNT", "1")))
ENABLED_PLATFORM_KEYS = tuple(
    part.strip().lower()
    for part in os.environ.get("PLATFORM_ENABLED_PLATFORMS", "").split(",")
    if part.strip()
)
VIRTUAL_DISPLAY_PLATFORM_KEYS = tuple(
    part.strip().lower()
    for part in os.environ.get("PLATFORM_VIRTUAL_DISPLAY_PLATFORMS", "").split(",")
    if part.strip()
)


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


class PublishResponse(BaseModel):
    platform: str
    status: str
    detail: str | None = None
    url: str | None = None
    published_at: float = Field(default_factory=time.time)


class PlatformSpec(BaseModel):
    key: str
    display_name: str
    domain: str
    login_url: str
    headless_search_target: str
    capabilities: list[str]


class CdpTarget(BaseModel):
    id: str
    type: str = ""
    title: str = ""
    url: str = ""
    webSocketDebuggerUrl: str | None = None


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
    capabilities: tuple[str, ...]
    external_cdp_env: str | None = None
    external_cdp_target_env: str | None = None

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
    def local_cdp_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


ALL_PLATFORMS: dict[str, PlatformConfig] = {
    "xiaohongshu": PlatformConfig(
        key="xiaohongshu",
        display_name="Xiaohongshu",
        domain="xiaohongshu.com",
        login_url="https://www.xiaohongshu.com/login",
        home_url="https://www.xiaohongshu.com/explore",
        command_site="xiaohongshu",
        status_cookie_names=("web_session",),
        port=19221,
        cdp_target="xiaohongshu",
        capabilities=("auth", "search", "download"),
    ),
    "bilibili": PlatformConfig(
        key="bilibili",
        display_name="Bilibili",
        domain="bilibili.com",
        login_url="https://passport.bilibili.com/login",
        home_url="https://www.bilibili.com",
        command_site="bilibili",
        status_cookie_names=("SESSDATA", "bili_jct", "DedeUserID"),
        port=19222,
        cdp_target="bilibili",
        capabilities=("auth", "search", "download"),
    ),
    "x": PlatformConfig(
        key="x",
        display_name="X",
        domain="x.com",
        login_url="https://x.com/i/flow/login",
        home_url="https://x.com/home",
        command_site="twitter",
        status_cookie_names=("auth_token", "ct0"),
        port=19223,
        cdp_target="x.com",
        capabilities=("auth", "search", "download", "publish"),
        external_cdp_env="X_CDP_URL",
        external_cdp_target_env="X_CDP_TARGET",
    ),
}

if ENABLED_PLATFORM_KEYS:
    PLATFORMS = {
        key: cfg
        for key, cfg in ALL_PLATFORMS.items()
        if key in ENABLED_PLATFORM_KEYS
    }
else:
    PLATFORMS = dict(ALL_PLATFORMS)

auth_status_cache: dict[str, AuthStatusResponse] = {}
download_index: dict[str, Path] = {}
auth_bootstrap_tasks: dict[str, asyncio.Task[bool]] = {}

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
        try:
            exists = candidate.exists()
        except PermissionError:
            continue
        if not exists:
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


async def _extract_runtime_cookie_names(cfg: PlatformConfig) -> set[str]:
    target = _pick_auth_target(cfg, await _list_targets(cfg))
    if not target or not target.webSocketDebuggerUrl:
        return set()

    try:
        async with websockets.connect(
            target.webSocketDebuggerUrl,
            max_size=None,
            proxy=None,
            open_timeout=10,
            close_timeout=5,
            ping_interval=None,
        ) as socket:
            await _cdp_command(socket, "Network.enable")
            result = await _cdp_command(socket, "Network.getCookies", timeout=15.0)
    except Exception:
        return set()

    cookies = result.get("cookies")
    if not isinstance(cookies, list):
        return set()

    names: set[str] = set()
    for item in cookies:
        if not isinstance(item, dict):
            continue
        domain = str(item.get("domain") or "")
        name = str(item.get("name") or "")
        if cfg.domain in domain and name:
            names.add(name)
    return names


def _looks_like_authenticated_external_session(cfg: PlatformConfig, targets: list[CdpTarget]) -> bool:
    if cfg.key != "x" or not _uses_external_cdp(cfg):
        return False
    for target in targets:
        if target.type != "page":
            continue
        url = (target.url or "").lower()
        title = (target.title or "").lower()
        if "x.com" not in url and "twitter.com" not in url:
            continue
        if "/i/flow/login" in url or "/login" in url:
            continue
        if "login" in title and "home / x" not in title:
            continue
        return True
    return False


async def _http_get_json(url: str) -> Any | None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except Exception:
        return None


def _configured_cdp_url(cfg: PlatformConfig) -> str:
    if cfg.external_cdp_env:
        external = os.environ.get(cfg.external_cdp_env, "").strip()
        if external:
            return external.rstrip("/")
    return cfg.local_cdp_url


def _configured_cdp_target(cfg: PlatformConfig) -> str:
    if cfg.external_cdp_target_env:
        external = os.environ.get(cfg.external_cdp_target_env, "").strip()
        if external:
            return external
    return cfg.cdp_target


def _uses_external_cdp(cfg: PlatformConfig) -> bool:
    return _configured_cdp_url(cfg) != cfg.local_cdp_url


async def _browser_running(cfg: PlatformConfig) -> bool:
    payload = await _http_get_json(f"{_configured_cdp_url(cfg)}/json/version")
    return isinstance(payload, dict) and bool(payload.get("Browser"))


def _auth_qr_path(cfg: PlatformConfig) -> Path:
    return cfg.profile_dir / "auth-qr.png"


def _stale_profile_lock_paths(cfg: PlatformConfig) -> tuple[Path, ...]:
    return (
        cfg.profile_dir / "SingletonCookie",
        cfg.profile_dir / "SingletonLock",
        cfg.profile_dir / "SingletonSocket",
    )


def _clear_stale_profile_locks(cfg: PlatformConfig) -> None:
    for path in _stale_profile_lock_paths(cfg):
        try:
            path.unlink(missing_ok=True)
        except PermissionError:
            logger.warning("Could not remove stale profile lock for %s due to permissions: %s", cfg.key, path)


async def _list_targets(cfg: PlatformConfig) -> list[CdpTarget]:
    payload = await _http_get_json(f"{_configured_cdp_url(cfg)}/json/list")
    if not isinstance(payload, list):
        return []
    targets: list[CdpTarget] = []
    for item in payload:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        try:
            targets.append(CdpTarget.model_validate(item))
        except Exception:
            continue
    return targets


def _is_x_external_page_target(cfg: PlatformConfig, target: CdpTarget) -> bool:
    if cfg.key != "x" or target.type != "page":
        return False
    url = (target.url or "").lower()
    return ("x.com" in url) or ("twitter.com" in url)


def _is_x_login_target(target: CdpTarget) -> bool:
    url = (target.url or "").lower()
    title = (target.title or "").lower()
    return (
        "/i/flow/login" in url
        or url.endswith("/login")
        or "/login?" in url
        or ("login" in title and "home / x" not in title)
    )


def _is_x_dirty_target(target: CdpTarget) -> bool:
    url = (target.url or "").lower()
    title = (target.title or "").lower()
    return (
        "/compose/tweet" in url
        or "/intent/tweet" in url
        or "/intent/post" in url
        or "/share?" in url
        or ("compose" in title and "x" in title)
    )


def _is_x_stable_target(target: CdpTarget) -> bool:
    return (not _is_x_login_target(target)) and (not _is_x_dirty_target(target))


def _is_x_login_url(url: str) -> bool:
    lowered = (url or "").lower()
    return "/i/flow/login" in lowered or lowered.endswith("/login") or "/login?" in lowered


async def _close_target(cfg: PlatformConfig, target: CdpTarget) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{_configured_cdp_url(cfg)}/json/close/{target.id}")
            return response.status_code < 400
    except Exception:
        return False


async def _cleanup_x_external_tabs(cfg: PlatformConfig, *, keep_latest: int | None = None) -> dict[str, int]:
    if not X_EXTERNAL_TAB_CLEANUP_ENABLED or cfg.key != "x" or not _uses_external_cdp(cfg):
        return {"considered": 0, "closed": 0, "remaining": 0}

    targets = await _list_targets(cfg)
    x_pages = [target for target in targets if _is_x_external_page_target(cfg, target)]
    if not x_pages:
        return {"considered": 0, "closed": 0, "remaining": 0}

    keep_latest = X_EXTERNAL_TAB_KEEP_COUNT if keep_latest is None else max(0, keep_latest)
    stable = [target for target in x_pages if _is_x_stable_target(target)]
    close_candidates = [target for target in x_pages if not _is_x_stable_target(target)]

    if keep_latest == 0:
        close_candidates.extend(stable)
    elif len(stable) > keep_latest:
        close_candidates.extend(stable[:-keep_latest])

    closed = 0
    seen_ids: set[str] = set()
    for target in close_candidates:
        if target.id in seen_ids:
            continue
        seen_ids.add(target.id)
        if await _close_target(cfg, target):
            closed += 1

    if closed:
        await asyncio.sleep(0.25)

    remaining_targets = await _list_targets(cfg)
    remaining = len([target for target in remaining_targets if _is_x_external_page_target(cfg, target)])
    logger.info(
        "X external tab cleanup: considered=%s closed=%s remaining=%s",
        len(x_pages),
        closed,
        remaining,
    )
    return {"considered": len(x_pages), "closed": closed, "remaining": remaining}


async def _prepare_x_external_publish(cfg: PlatformConfig, *, retrying: bool) -> None:
    if cfg.key != "x" or not _uses_external_cdp(cfg):
        return

    await _cleanup_x_external_tabs(cfg, keep_latest=0 if retrying else X_EXTERNAL_TAB_KEEP_COUNT)
    targets = await _list_targets(cfg)
    stable = [target for target in targets if _is_x_external_page_target(cfg, target) and _is_x_stable_target(target)]
    if retrying or not stable:
        await _open_tab(cfg, cfg.home_url, cleanup_before=False)
        await asyncio.sleep(X_EXTERNAL_TAB_OPEN_WAIT_SECONDS)


def _is_retryable_x_opencli_error(detail: str) -> bool:
    lowered = detail.lower()
    return (
        "page.enable" in lowered
        or ("cdp command" in lowered and "timed out" in lowered)
        or "target closed" in lowered
        or "session closed" in lowered
        or "execution context was destroyed" in lowered
        or "cannot find context with specified id" in lowered
        or "websocket is not open" in lowered
    )


async def _run_opencli_x_publish_with_retry(cfg: PlatformConfig, args: list[str]) -> tuple[int, str, str]:
    if _uses_external_cdp(cfg):
        await _prepare_x_external_publish(cfg, retrying=False)

    code, stdout, stderr = await _run_opencli(cfg, args)
    detail = (stderr or stdout or "").strip()
    retries_left = X_EXTERNAL_PUBLISH_RETRY_COUNT

    while code != 0 and retries_left > 0 and _is_retryable_x_opencli_error(detail):
        logger.warning("Retrying X publish after cleaning stale external tabs: %s", detail[:300])
        await _prepare_x_external_publish(cfg, retrying=True)
        code, stdout, stderr = await _run_opencli(cfg, args)
        detail = (stderr or stdout or "").strip()
        retries_left -= 1

    return code, stdout, stderr


def _record_browser_mode(cfg: PlatformConfig, *, headed: bool) -> None:
    try:
        cfg.profile_dir.mkdir(parents=True, exist_ok=True)
        cfg.mode_file.write_text(json.dumps({"headed": headed, "updated_at": time.time()}))
    except PermissionError:
        logger.warning("Could not record browser mode for %s due to permissions", cfg.key)


def _read_browser_mode(cfg: PlatformConfig) -> bool:
    try:
        if not cfg.mode_file.exists():
            return False
    except PermissionError:
        return False
    try:
        payload = json.loads(cfg.mode_file.read_text())
    except (json.JSONDecodeError, PermissionError):
        return False
    return bool(payload.get("headed"))


def _use_virtual_display(cfg: PlatformConfig, *, headed: bool) -> bool:
    return (not headed) and (cfg.key in VIRTUAL_DISPLAY_PLATFORM_KEYS) and bool(XVFB_RUN_BIN)


async def _open_tab(cfg: PlatformConfig, url: str, *, cleanup_before: bool = True) -> bool:
    if cleanup_before and cfg.key == "x" and _uses_external_cdp(cfg):
        await _cleanup_x_external_tabs(cfg, keep_latest=0)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.put(f"{_configured_cdp_url(cfg)}/json/new?{quote_plus(url)}")
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
    if _uses_external_cdp(cfg):
        return
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
    _auth_qr_path(cfg).unlink(missing_ok=True)
    _clear_stale_profile_locks(cfg)


async def _start_browser(cfg: PlatformConfig, *, headed: bool, url: str) -> bool:
    if not CHROME_BIN:
        raise HTTPException(status_code=503, detail="Chrome binary not found on server")
    if _uses_external_cdp(cfg):
        if not await _browser_running(cfg):
            raise HTTPException(status_code=503, detail=f"{cfg.display_name} external browser is not reachable")
        await _open_tab(cfg, url)
        return False

    if await _browser_running(cfg):
        current_headed = _read_browser_mode(cfg)
        if headed and not current_headed:
            await _stop_browser(cfg)
        else:
            await _open_tab(cfg, url)
            return False

    cfg.profile_dir.mkdir(parents=True, exist_ok=True)
    cfg.downloads_dir.mkdir(parents=True, exist_ok=True)
    _clear_stale_profile_locks(cfg)

    chrome_args = [
        f"--user-data-dir={cfg.profile_dir}",
        f"--remote-debugging-port={cfg.port}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-sync",
        "--password-store=basic",
        "--disable-popup-blocking",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--lang=zh-CN",
        f"--window-size={VIEWPORT_WIDTH},{VIEWPORT_HEIGHT}",
    ]
    if _use_virtual_display(cfg, headed=headed):
        args = [
            XVFB_RUN_BIN,
            "-a",
            f"--server-args=-screen 0 {VIEWPORT_WIDTH}x{VIEWPORT_HEIGHT}x24",
            CHROME_BIN,
        ]
    else:
        args = [CHROME_BIN]
        if not headed:
            chrome_args.extend(["--headless=new", "--disable-gpu", "--hide-scrollbars", "--mute-audio"])
    args.extend(chrome_args)
    args.append(url)

    proc = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        cfg.pid_file.write_text(str(proc.pid))
    except PermissionError as exc:
        with contextlib.suppress(Exception):
            proc.kill()
        raise HTTPException(
            status_code=503,
            detail=f"{cfg.display_name} browser profile directory is not writable by this process",
        ) from exc
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
    if _uses_external_cdp(cfg):
        raise HTTPException(status_code=503, detail=f"{cfg.display_name} external browser is not reachable")
    await _start_browser(cfg, headed=False, url=cfg.home_url)
    await asyncio.sleep(HEADLESS_SEARCH_WAIT_SECONDS)


async def _ensure_auth_browser(cfg: PlatformConfig, *, wait_for_qr: bool = False) -> bool:
    started = await _start_browser(cfg, headed=False, url=cfg.login_url)
    if wait_for_qr:
        await asyncio.sleep(AUTH_QR_CAPTURE_WAIT_SECONDS)
    return started


def _schedule_auth_browser(cfg: PlatformConfig) -> asyncio.Task[bool]:
    existing = auth_bootstrap_tasks.get(cfg.key)
    if existing and not existing.done():
        return existing

    async def _runner() -> bool:
        try:
            return await _ensure_auth_browser(cfg, wait_for_qr=False)
        finally:
            auth_bootstrap_tasks.pop(cfg.key, None)

    task = asyncio.create_task(_runner(), name=f"{cfg.key}-auth-browser")
    auth_bootstrap_tasks[cfg.key] = task
    return task


def _cancel_auth_browser_bootstrap(cfg: PlatformConfig) -> None:
    task = auth_bootstrap_tasks.pop(cfg.key, None)
    if task and not task.done():
        task.cancel()


async def _wait_for_auth_browser(cfg: PlatformConfig, *, timeout_seconds: float = 15.0) -> bool:
    if await _browser_running(cfg):
        return False
    task = _schedule_auth_browser(cfg)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=503, detail=f"Timed out starting browser for {cfg.display_name}") from exc


def _opencli_command_base() -> list[str]:
    if OPENCLI_BIN:
        return [OPENCLI_BIN]
    return [OPENCLI_NPX, "-y", f"@jackwener/opencli@{OPENCLI_VERSION}"]


async def _run_opencli(cfg: PlatformConfig, args: list[str], *, timeout_seconds: float | None = None) -> tuple[int, str, str]:
    env = {
        **os.environ,
        "OPENCLI_CDP_ENDPOINT": _configured_cdp_url(cfg),
        "OPENCLI_CDP_TARGET": _configured_cdp_target(cfg),
    }
    proc = await asyncio.create_subprocess_exec(
        *(_opencli_command_base() + args),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout_seconds or COMMAND_TIMEOUT_SECONDS,
        )
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
    elif platform == "x":
        identifier = _extract_x_post_id(url or "") or f"x-{index}"
    else:
        identifier = f"{platform}-{index}"
    return SearchResultItem(
        id=identifier,
        platform=platform,
        title=title or identifier,
        url=url,
        thumbnail=str(item.get("thumbnail") or "").strip() or None,
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
    candidates = [
        path
        for path in root.rglob("*")
        if path.is_file() and not path.name.lower().endswith((".part", ".tmp", ".ytdl"))
    ]
    if not candidates:
        return None

    def sort_key(item: Path) -> tuple[int, float, int]:
        suffix = item.suffix.lower()
        if suffix in {".mp4", ".mkv", ".webm", ".mov", ".m4v"}:
            priority = 0
        elif suffix in {".m4a", ".mp3", ".wav", ".flac", ".aac", ".ogg"}:
            priority = 1
        elif suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            priority = 2
        else:
            priority = 3
        try:
            stat = item.stat()
            return (priority, -stat.st_mtime, -stat.st_size)
        except OSError:
            return (priority, float("inf"), 0)

    candidates.sort(key=sort_key)
    return candidates[0]


def _download_sort_key(item: Path) -> tuple[int, float, int]:
    suffix = item.suffix.lower()
    if suffix in {".mp4", ".mkv", ".webm", ".mov", ".m4v"}:
        priority = 0
    elif suffix in {".m4a", ".mp3", ".wav", ".flac", ".aac", ".ogg"}:
        priority = 1
    elif suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
        priority = 2
    else:
        priority = 3
    try:
        stat = item.stat()
        return (priority, -stat.st_mtime, -stat.st_size)
    except OSError:
        return (priority, float("inf"), 0)


def _normalized_download_basename(path: Path) -> str:
    return re.sub(r"\.f\d+$", "", path.stem, flags=re.IGNORECASE)


async def _probe_stream_types(path: Path) -> set[str]:
    if not FFPROBE_BIN or not path.exists():
        return set()
    proc = await asyncio.create_subprocess_exec(
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "csv=p=0",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    if (proc.returncode or 0) != 0:
        return set()
    return {
        line.strip().lower()
        for line in stdout.decode("utf-8", errors="replace").splitlines()
        if line.strip()
    }


async def _merge_av_streams(video_path: Path, audio_path: Path) -> Path | None:
    if not FFMPEG_BIN or not video_path.exists() or not audio_path.exists():
        return None

    base_name = _normalized_download_basename(video_path)
    target_path = video_path.with_name(f"{base_name}.mp4")
    if target_path == video_path:
        target_path = video_path.with_name(f"{base_name}.muxed.mp4")

    if target_path.exists():
        stream_types = await _probe_stream_types(target_path)
        if {"video", "audio"}.issubset(stream_types):
            return target_path

    proc = await asyncio.create_subprocess_exec(
        FFMPEG_BIN,
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(audio_path),
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        str(target_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await asyncio.wait_for(proc.communicate(), timeout=COMMAND_TIMEOUT_SECONDS)
    if (proc.returncode or 0) != 0 or not target_path.exists():
        logger.warning(
            "ffmpeg mux failed for %s + %s: %s",
            video_path,
            audio_path,
            stderr.decode("utf-8", errors="replace"),
        )
        return None
    return target_path


async def _resolve_downloaded_file(root: Path) -> Path | None:
    candidates = [
        path
        for path in root.rglob("*")
        if path.is_file() and not path.name.lower().endswith((".part", ".tmp", ".ytdl"))
    ]
    if not candidates:
        return None

    candidates.sort(key=_download_sort_key)
    videos = [path for path in candidates if path.suffix.lower() in {".mp4", ".mkv", ".webm", ".mov", ".m4v"}]
    audios = [path for path in candidates if path.suffix.lower() in {".m4a", ".mp3", ".wav", ".flac", ".aac", ".ogg"}]

    stream_types_by_path: dict[Path, set[str]] = {}
    for video_path in videos:
        stream_types = await _probe_stream_types(video_path)
        stream_types_by_path[video_path] = stream_types
        if {"video", "audio"}.issubset(stream_types):
            return video_path

    best_audio_by_base: dict[str, Path] = {}
    for audio_path in audios:
        base_name = _normalized_download_basename(audio_path)
        current = best_audio_by_base.get(base_name)
        if current is None or _download_sort_key(audio_path) < _download_sort_key(current):
            best_audio_by_base[base_name] = audio_path

    for video_path in videos:
        stream_types = stream_types_by_path.get(video_path, set())
        if "video" not in stream_types and stream_types:
            continue
        audio_path = best_audio_by_base.get(_normalized_download_basename(video_path))
        if not audio_path:
            continue
        merged_path = await _merge_av_streams(video_path, audio_path)
        if merged_path:
            return merged_path

    return candidates[0]


async def _download_http_media(url: str, output_dir: Path) -> Path | None:
    parsed = urlparse(url)
    name = Path(parsed.path).name or f"download-{uuid.uuid4().hex}"
    target = output_dir / name

    async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            if not target.suffix:
                content_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
                target = target.with_suffix(mimetypes.guess_extension(content_type) or ".bin")
            index = 1
            while target.exists():
                target = target.with_name(f"{target.stem}-{index}{target.suffix}")
                index += 1
            with target.open("wb") as handle:
                async for chunk in response.aiter_bytes():
                    handle.write(chunk)

    return target if target.exists() else None


async def _download_xiaohongshu_via_playwright(
    cfg: PlatformConfig,
    source_url: str,
    note_id: str,
    target_dir: Path,
) -> Path | None:
    target_url = source_url
    if "/search_result/" in source_url:
        parsed = urlparse(source_url)
        query = f"?{parsed.query}" if parsed.query else ""
        target_url = f"https://www.xiaohongshu.com/explore/{note_id}{query}"

    collected: list[str] = []

    def remember(candidate: str | None) -> None:
        if not candidate:
            return
        parsed = urlparse(candidate)
        if "xhscdn.com" not in parsed.netloc:
            return
        path = parsed.path.lower()
        if any(path.endswith(ext) for ext in (".mp4", ".m3u8", ".jpg", ".jpeg", ".png", ".webp", ".gif")):
            collected.append(candidate)

    playwright = None
    browser = None
    page = None
    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.connect_over_cdp(_configured_cdp_url(cfg))
        context = browser.contexts[0] if browser.contexts else await browser.new_context(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT}
        )
        page = await context.new_page()
        page.on("response", lambda response: remember(response.url))
        await page.goto(target_url, wait_until="networkidle", timeout=90_000)
        await page.wait_for_timeout(3_000)

        payload = await page.evaluate(
            """() => ({
                mediaUrls: Array.from(document.querySelectorAll('video, source, img'))
                    .map((element) => element.currentSrc || element.src || element.getAttribute('src') || '')
                    .filter(Boolean),
                resourceUrls: performance.getEntriesByType('resource')
                    .map((entry) => entry.name)
                    .filter((name) => /xhscdn|mp4|m3u8|image|stream/i.test(name)),
            })"""
        )
        if isinstance(payload, dict):
            for key in ("mediaUrls", "resourceUrls"):
                values = payload.get(key)
                if isinstance(values, list):
                    for value in values:
                        if isinstance(value, str):
                            remember(value)
    finally:
        if page is not None:
            with contextlib.suppress(Exception):
                await page.close()
        if playwright is not None:
            with contextlib.suppress(Exception):
                await playwright.stop()

    ordered: list[str] = []
    seen: set[str] = set()
    for candidate in collected:
        if candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    ordered.sort(key=lambda item: (0 if urlparse(item).path.lower().endswith(".mp4") else 1, item))
    for media_url in ordered:
        with contextlib.suppress(httpx.HTTPError, OSError):
            downloaded = await _download_http_media(media_url, target_dir)
            if downloaded:
                return downloaded
    return None


def _extract_x_post_id(url: str) -> str | None:
    match = re.search(r"/status/(\d+)", url)
    return match.group(1) if match else None


def _is_opencli_structure_error(detail: str) -> bool:
    lowered = detail.lower()
    return (
        "selector not found" in lowered
        or "page structure may have changed" in lowered
        or "resource was not found" in lowered
        or "evaluate error" in lowered
    )


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _collect_topics(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        values = raw
    else:
        values = str(raw).split(",")
    topics: list[str] = []
    for item in values:
        topic = str(item).strip().lstrip("#")
        if topic:
            topics.append(topic)
    return topics


def _guess_mime_type(path: Path) -> str:
    mime_type, _ = mimetypes.guess_type(path.name)
    return mime_type or "application/octet-stream"


async def _open_playwright_page(cfg: PlatformConfig, url: str) -> tuple[Any, Any, Any, Any]:
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(_configured_cdp_url(cfg))
        context = browser.contexts[0] if browser.contexts else await browser.new_context(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT}
        )
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        return playwright, browser, context, page
    except Exception:
        with contextlib.suppress(Exception):
            await playwright.stop()
        raise


async def _open_existing_playwright_page(
    cfg: PlatformConfig,
    *,
    url_hint: str | None = None,
) -> tuple[Any, Any, Any, Any, bool]:
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(_configured_cdp_url(cfg))
        context = browser.contexts[0] if browser.contexts else await browser.new_context(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT}
        )
        pages = list(context.pages)
        page = None
        if url_hint:
            for candidate in reversed(pages):
                if url_hint in (candidate.url or ""):
                    page = candidate
                    break
        if page is None and pages:
            page = pages[-1]
        if page is None:
            raise HTTPException(
                status_code=503,
                detail=f"{cfg.display_name} browser did not expose an existing page to reuse",
            )
        return playwright, browser, context, page, False
    except Exception:
        with contextlib.suppress(Exception):
            await playwright.stop()
        raise


async def _close_playwright_page(playwright: Any, browser: Any, page: Any) -> None:
    with contextlib.suppress(Exception):
        await page.close()
    with contextlib.suppress(Exception):
        await playwright.stop()


async def _search_x_via_playwright(cfg: PlatformConfig, query: str, limit: int) -> list[SearchResultItem]:
    playwright, browser, _context, page = await _open_playwright_page(
        cfg,
        f"https://x.com/search?q={quote_plus(query)}&src=typed_query&f=top",
    )
    try:
        await page.wait_for_selector('[data-testid="primaryColumn"], article[data-testid="tweet"]', timeout=25_000)
        for _ in range(3):
            cards = await page.locator('article[data-testid="tweet"]').count()
            if cards >= limit:
                break
            await page.mouse.wheel(0, 1800)
            await page.wait_for_timeout(1200)

        payload = await page.evaluate(
            """([wanted]) => {
                const items = [];
                const articles = Array.from(document.querySelectorAll('article[data-testid="tweet"]'));
                for (const article of articles) {
                  if (items.length >= wanted) break;
                  const link = article.querySelector('a[href*="/status/"]');
                  const url = link ? new URL(link.getAttribute('href'), location.origin).toString() : '';
                  if (!url) continue;
                  const text = article.querySelector('[data-testid="tweetText"]')?.innerText?.trim() || '';
                  const userName = article.querySelector('[data-testid="User-Name"]')?.innerText?.trim() || '';
                  const displayTitle = text || userName || url;
                  items.push({
                    url,
                    title: displayTitle,
                    author: userName,
                    thumbnail: article.querySelector('img[src*="pbs.twimg.com/media"]')?.src || null,
                  });
                }
                return items;
            }""",
            [limit],
        )
        if not isinstance(payload, list):
            return []
        return [
            SearchResultItem(
                id=_extract_x_post_id(str(item.get("url") or "")) or f"x-{index}",
                platform="x",
                title=str(item.get("title") or f"x-{index}"),
                url=str(item.get("url") or "") or None,
                channel=str(item.get("author") or "") or None,
                thumbnail=str(item.get("thumbnail") or "") or None,
            )
            for index, item in enumerate(payload, start=1)
            if isinstance(item, dict) and item.get("url")
        ]
    except PlaywrightTimeoutError as exc:
        raise HTTPException(
            status_code=401,
            detail=(
                "X login_required: the attached browser session did not expose a ready search page. "
                "Complete X login in the browser attached through X_CDP_URL, then try again."
            ),
        ) from exc
    finally:
        await _close_playwright_page(playwright, browser, page)


async def _search_xiaohongshu_via_playwright(cfg: PlatformConfig, query: str, limit: int) -> list[SearchResultItem]:
    playwright, browser, _context, page, close_page = await _open_existing_playwright_page(
        cfg,
        url_hint="xiaohongshu.com",
    )
    try:
        search_url = f"https://www.xiaohongshu.com/search_result?keyword={quote_plus(query)}"
        navigated = False
        with contextlib.suppress(Exception):
            await page.goto(search_url, wait_until="domcontentloaded", timeout=45_000)
            navigated = True
        if not navigated:
            with contextlib.suppress(Exception):
                await page.evaluate("(url) => { window.location.href = url; }", search_url)
                await page.wait_for_timeout(3500)

        await page.wait_for_timeout(2500)
        for _ in range(5):
            login_wall = await page.locator("text=登录后查看更多,text=请登录后查看更多").count()
            if login_wall:
                raise HTTPException(
                    status_code=401,
                    detail="Xiaohongshu login_required: the attached browser session hit a login wall on search",
                )
            note_links = await page.locator('a[href*="/search_result/"], a[href*="/explore/"]').count()
            if note_links >= limit:
                break
            await page.mouse.wheel(0, 1800)
            await page.wait_for_timeout(1200)

        dom_payload = await page.evaluate(
            """([wanted]) => {
                const anchors = Array.from(document.querySelectorAll('a[href*="/search_result/"], a[href*="/explore/"]'));
                const items = [];
                const seen = new Set();
                for (const anchor of anchors) {
                  const rawHref = anchor.getAttribute('href') || '';
                  const href = rawHref ? new URL(rawHref, location.origin).toString() : '';
                  if (!href || seen.has(href)) continue;
                  const match = href.match(/([0-9a-f]{24})/i);
                  if (!match) continue;
                  seen.add(href);
                  const card = anchor.closest('section, article, div') || anchor;
                  const titleNode =
                    anchor.querySelector('img[alt]') ||
                    card.querySelector('img[alt]') ||
                    anchor.querySelector('span') ||
                    card.querySelector('span[class*="title"], div[class*="title"], div[class*="desc"]');
                  const authorNode =
                    card.querySelector('a[href*="/user/profile/"] span') ||
                    card.querySelector('span[class*="author"], div[class*="author"], div[class*="user"]');
                  const thumbnailNode = anchor.querySelector('img[src]') || card.querySelector('img[src]');
                  const title =
                    titleNode?.getAttribute?.('alt')?.trim?.() ||
                    titleNode?.textContent?.trim?.() ||
                    anchor.textContent?.trim?.() ||
                    href;
                  const author = authorNode?.textContent?.trim?.() || '';
                  const thumbnail = thumbnailNode?.getAttribute?.('src') || '';
                  items.push({
                    id: match[1],
                    url: href,
                    title,
                    author,
                    thumbnail,
                  });
                  if (items.length >= wanted) break;
                }
                return items;
            }""",
            [limit],
        )
        if isinstance(dom_payload, list) and dom_payload:
            return [
                SearchResultItem(
                    id=str(item.get("id") or f"xhs-{index}"),
                    platform="xiaohongshu",
                    title=str(item.get("title") or f"xhs-{index}"),
                    url=str(item.get("url") or "") or None,
                    channel=str(item.get("author") or "") or None,
                    thumbnail=str(item.get("thumbnail") or "") or None,
                )
                for index, item in enumerate(dom_payload, start=1)
                if isinstance(item, dict) and item.get("url")
            ]

        payload = await page.evaluate(
            """async ([keyword, wanted]) => {
                const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
                const app = document.querySelector('#app')?.__vue_app__;
                const pinia = app?.config?.globalProperties?.$pinia;
                if (!pinia?._s) {
                  return { error: 'Page not ready', hint: 'Not logged in?' };
                }
                const searchStore = pinia._s.get('search');
                if (!searchStore) {
                  return { error: 'Search store not found', hint: 'Not logged in?' };
                }

                let captured = null;
                const origOpen = XMLHttpRequest.prototype.open;
                const origSend = XMLHttpRequest.prototype.send;
                XMLHttpRequest.prototype.open = function(method, url) {
                  this.__url = url;
                  return origOpen.apply(this, arguments);
                };
                XMLHttpRequest.prototype.send = function(body) {
                  if (this.__url?.includes('search/notes')) {
                    const request = this;
                    const originalHandler = request.onreadystatechange;
                    request.onreadystatechange = function() {
                      if (request.readyState === 4 && !captured) {
                        try {
                          captured = JSON.parse(request.responseText);
                        } catch (error) {
                        }
                      }
                      if (originalHandler) {
                        originalHandler.apply(this, arguments);
                      }
                    };
                  }
                  return origSend.apply(this, arguments);
                };

                try {
                  searchStore.mutateSearchValue(keyword);
                  await searchStore.loadMore();
                  await wait(800);
                } finally {
                  XMLHttpRequest.prototype.open = origOpen;
                  XMLHttpRequest.prototype.send = origSend;
                }

                if (!captured?.success) {
                  return {
                    error: captured?.msg || 'Search failed',
                    hint: captured?.msg || 'Not logged in?',
                  };
                }

                const notes = (captured.data?.items || []).slice(0, wanted).map((item, index) => ({
                  id: item.id || `xhs-${index + 1}`,
                  title: item.note_card?.display_title || item.note_card?.title || `xhs-${index + 1}`,
                  url: item.id ? `https://www.xiaohongshu.com/explore/${item.id}` : '',
                  author: item.note_card?.user?.nickname || '',
                  thumbnail:
                    item.note_card?.cover?.url ||
                    item.note_card?.cover?.info_list?.[0]?.url ||
                    item.note_card?.image_list?.[0]?.info_list?.[0]?.url ||
                    '',
                })).filter((item) => item.url);
                return { notes };
            }""",
            [query, limit],
        )
        if not isinstance(payload, dict):
            return []
        if payload.get("error"):
            detail = str(payload.get("hint") or payload.get("error") or "")
            lowered = detail.lower()
            if (
                "login" in lowered
                or "not logged in" in lowered
                or "page not ready" in lowered
                or "search store not found" in lowered
            ):
                raise HTTPException(
                    status_code=401,
                    detail=f"Xiaohongshu login_required: {detail}",
                )
            raise HTTPException(
                status_code=502,
                detail=f"Xiaohongshu platform_search_failed: {detail or 'search store returned an error'}",
            )
        notes = payload.get("notes")
        if not isinstance(notes, list):
            return []
        return [
            SearchResultItem(
                id=str(item.get("id") or f"xhs-{index}"),
                platform="xiaohongshu",
                title=str(item.get("title") or f"xhs-{index}"),
                url=str(item.get("url") or "") or None,
                channel=str(item.get("author") or "") or None,
                thumbnail=str(item.get("thumbnail") or "") or None,
            )
            for index, item in enumerate(notes, start=1)
            if isinstance(item, dict) and item.get("url")
        ]
    except PlaywrightTimeoutError as exc:
        raise HTTPException(
            status_code=502,
            detail="Xiaohongshu platform_search_failed: timed out waiting for search results",
        ) from exc
    finally:
        if close_page:
            await _close_playwright_page(playwright, browser, page)
        else:
            with contextlib.suppress(Exception):
                await playwright.stop()


async def _probe_xiaohongshu_session_ready(cfg: PlatformConfig) -> tuple[bool, str | None]:
    try:
        playwright, browser, _context, page, close_page = await _open_existing_playwright_page(
            cfg,
            url_hint="xiaohongshu.com",
        )
    except Exception:
        return False, "Xiaohongshu browser page is not ready; re-login may be required"

    try:
        payload = await page.evaluate(
            """() => {
                const app = document.querySelector('#app')?.__vue_app__;
                const pinia = app?.config?.globalProperties?.$pinia;
                return {
                    url: window.location.href || '',
                    title: document.title || '',
                    bodyText: (document.body?.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 1500),
                    hasPinia: !!pinia?._s,
                };
            }""",
        )
    except Exception:
        payload = {}
    finally:
        if close_page:
            await _close_playwright_page(playwright, browser, page)
        else:
            with contextlib.suppress(Exception):
                await playwright.stop()

    if not isinstance(payload, dict):
        return False, "Xiaohongshu browser page did not return a usable session state"

    text = str(payload.get("bodyText") or "")
    haystack = " ".join(
        part for part in (str(payload.get("title") or ""), str(payload.get("url") or ""), text) if part
    ).lower()
    if "登录后查看更多" in text or "请登录后查看更多" in text or "log in to view more" in haystack:
        return False, "Xiaohongshu login_required: the current browser page is still behind a login wall"
    if not payload.get("hasPinia"):
        return False, "Xiaohongshu login_required: the current browser page is not ready for logged-in search"
    return True, None


async def _download_with_ytdlp(url: str, output_dir: Path, fmt: str) -> Path | None:
    yt_dlp = shutil.which("yt-dlp")
    if not yt_dlp:
        return None

    template = str(output_dir / "%(title)s.%(ext)s")
    args = [
        yt_dlp,
        "--no-playlist",
        "--merge-output-format",
        "mp4",
        "-o",
        template,
    ]
    format_map = {
        "1080p": "bestvideo[height<=1080]+bestaudio/best[height<=1080]",
        "720p": "bestvideo[height<=720]+bestaudio/best[height<=720]",
        "480p": "bestvideo[height<=480]+bestaudio/best[height<=480]",
        "best": "bestvideo+bestaudio/best",
    }
    if fmt in format_map:
        args.extend(["-f", format_map[fmt]])
    args.append(url)

    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await asyncio.wait_for(proc.communicate(), timeout=COMMAND_TIMEOUT_SECONDS)
    if (proc.returncode or 0) != 0:
        logger.warning("yt-dlp fallback failed for %s: %s", url, stderr.decode("utf-8", errors="replace"))
        return None
    return await _resolve_downloaded_file(output_dir)


async def _read_upload_files(form_values: list[tuple[str, Any]], staging_dir: Path) -> list[Path]:
    media_paths: list[Path] = []
    for key, value in form_values:
        if not isinstance(value, (UploadFile, StarletteUploadFile)):
            continue
        original_name = Path(value.filename or f"{key}-{uuid.uuid4().hex}").name
        target_path = staging_dir / original_name
        with open(target_path, "wb") as handle:
            while True:
                chunk = await value.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
        await value.close()
        media_paths.append(target_path)
    return media_paths


async def _parse_publish_payload(request: Request) -> tuple[dict[str, Any], list[Path], Path | None]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Publish payload must be a JSON object")
        return payload, [], None

    form = await request.form()
    staging_dir = UPLOAD_STAGING_ROOT / uuid.uuid4().hex
    staging_dir.mkdir(parents=True, exist_ok=True)
    media_paths = await _read_upload_files(list(form.multi_items()), staging_dir)
    payload: dict[str, Any] = {}
    for key, value in form.multi_items():
        if isinstance(value, (UploadFile, StarletteUploadFile)):
            continue
        if key in payload:
            existing = payload[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                payload[key] = [existing, value]
        else:
            payload[key] = value
    return payload, media_paths, staging_dir


def _stage_external_upload_files(cfg: PlatformConfig, media_paths: list[Path]) -> tuple[list[Path], Path | None]:
    if not _uses_external_cdp(cfg):
        return media_paths, None
    shared_root = cfg.downloads_dir / "_publish-staging" / uuid.uuid4().hex
    shared_root.mkdir(parents=True, exist_ok=True)
    staged_paths: list[Path] = []
    for path in media_paths:
        target_path = shared_root / path.name
        shutil.copy2(path, target_path)
        effective_path = target_path
        if EXTERNAL_UPLOAD_HOST_ROOT:
            effective_path = EXTERNAL_UPLOAD_HOST_ROOT / target_path.relative_to(DOWNLOAD_ROOT)
        staged_paths.append(effective_path)
    return staged_paths, shared_root


async def _wait_for_x_media_ready(page: Page, media_paths: list[Path]) -> None:
    if not media_paths:
        return
    filenames = [path.name for path in media_paths]
    has_video = any(_guess_mime_type(path).startswith("video/") for path in media_paths)
    try:
        await page.wait_for_function(
            """(names) => {
                const bodyText = document.body?.innerText || "";
                const button = document.querySelector('[data-testid="tweetButton"], [data-testid="tweetButtonInline"]');
                const uploadComplete = names.some((name) => bodyText.includes(`${name}: Uploaded (100%)`));
                const uploadInFlight = /uploading|processing/i.test(bodyText);
                const hasMediaPreview = Boolean(
                  document.querySelector('video, [data-testid*="attachment"], [aria-label*="Image"], [aria-label*="Video"]')
                );
                const buttonReady = Boolean(
                  button &&
                  !button.hasAttribute('disabled') &&
                  button.getAttribute('aria-disabled') !== 'true'
                );
                return buttonReady && !uploadInFlight && (uploadComplete || hasMediaPreview);
            }""",
            filenames,
            timeout=120_000,
        )
    except PlaywrightTimeoutError as exc:
        debug_text = ""
        with contextlib.suppress(Exception):
            body_text = await page.locator("body").inner_text()
            debug_text = " | ".join(
                line.strip()
                for line in body_text.splitlines()
                if line.strip() and any(token in line.lower() for token in ("upload", "process", "media", "post"))
            )
        detail = f"X media upload did not reach ready state for {', '.join(filenames)}"
        if debug_text:
            detail = f"{detail}: {debug_text[:400]}"
        raise HTTPException(status_code=504, detail=detail) from exc
    if has_video and X_VIDEO_SETTLE_SECONDS > 0:
        await page.wait_for_timeout(int(X_VIDEO_SETTLE_SECONDS * 1000))


async def _publish_x_via_playwright(
    cfg: PlatformConfig,
    *,
    text: str,
    media_paths: list[Path],
    reply_to_url: str | None,
) -> PublishResponse:
    compose_url = reply_to_url.strip() if reply_to_url else "https://x.com/compose/tweet"
    playwright, browser, _context, page = await _open_playwright_page(cfg, compose_url)
    staged_dir: Path | None = None
    try:
        if reply_to_url:
            await page.wait_for_selector('[data-testid="reply"], article[data-testid="tweet"]', timeout=25_000)
            with contextlib.suppress(Exception):
                await page.locator('[data-testid="reply"]').first.click(timeout=5_000)
            await page.wait_for_timeout(1500)
        else:
            await page.wait_for_selector('[data-testid="tweetTextarea_0"]', timeout=25_000)

        composer = page.locator('[data-testid="tweetTextarea_0"]').first
        await composer.click()
        await composer.fill(text)
        await page.wait_for_timeout(1200)

        if media_paths:
            effective_media_paths, staged_dir = _stage_external_upload_files(cfg, media_paths)
            file_input = page.locator('input[data-testid="fileInput"], input[type="file"]').first
            await file_input.set_input_files([str(path) for path in effective_media_paths])
            await _wait_for_x_media_ready(page, effective_media_paths)
            await page.wait_for_timeout(1200)

        button = page.locator('[data-testid="tweetButton"], [data-testid="tweetButtonInline"]').first
        await button.wait_for(state="visible", timeout=20_000)
        await button.click()
        await page.wait_for_timeout(5000)
        return PublishResponse(
            platform="x",
            status="success",
            detail="X post submitted successfully",
            url=reply_to_url.strip() or "https://x.com/home",
        )
    except PlaywrightTimeoutError as exc:
        raise HTTPException(status_code=504, detail=f"X publish timed out: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"X publish failed: {exc}") from exc
    finally:
        if staged_dir:
            shutil.rmtree(staged_dir, ignore_errors=True)
        await _close_playwright_page(playwright, browser, page)


async def _publish_x_via_helper(
    *,
    text: str,
    media_paths: list[Path],
    reply_to_url: str | None,
) -> PublishResponse:
    payload = {
        "text": text,
        "media_paths": [str(path) for path in media_paths],
    }
    if reply_to_url:
        payload["reply_to_url"] = reply_to_url.strip()

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0, connect=10.0)) as client:
            response = await client.post(X_MEDIA_HELPER_URL, json=payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"X media helper unavailable: {exc}") from exc

    body = response.text
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    if response.status_code >= 400:
        detail = body
        if isinstance(parsed, dict):
            detail = str(parsed.get("error") or parsed.get("detail") or body)
        raise HTTPException(status_code=502, detail=f"X media helper failed: {detail}")

    if isinstance(parsed, dict) and parsed.get("ok"):
        return PublishResponse(
            platform="x",
            status="success",
            detail=str(parsed.get("detail") or "Tweet posted successfully."),
            url=str(parsed.get("url") or "https://x.com/home"),
        )

    raise HTTPException(status_code=502, detail=f"X media helper returned unexpected response: {body[:400]}")


async def _publish_xiaohongshu_video_via_playwright(
    cfg: PlatformConfig,
    *,
    video_path: Path,
    title: str,
    content: str,
    topics: list[str],
    draft: bool,
) -> PublishResponse:
    playwright, browser, _context, page = await _open_playwright_page(
        cfg,
        "https://creator.xiaohongshu.com/publish/publish?from=menu_left",
    )
    try:
        await page.wait_for_timeout(3000)
        for label in ("视频", "上传视频", "发布视频"):
            with contextlib.suppress(Exception):
                tab = page.get_by_text(label, exact=False).first
                if await tab.count() > 0:
                    await tab.click(timeout=3_000)
                    await page.wait_for_timeout(1000)
                    break

        file_input = page.locator(
            'input[type="file"][accept*="video"], input[type="file"][accept*="mp4"], input[type="file"]'
        ).first
        await file_input.set_input_files(str(video_path))
        await page.wait_for_timeout(4000)

        title_candidates = [
            'input[maxlength="20"]',
            'input[placeholder*="标题"]',
            'input[class*="title"]',
        ]
        body_candidates = [
            '[contenteditable="true"][placeholder*="正文"]',
            '[contenteditable="true"][placeholder*="内容"]',
            '[contenteditable="true"][placeholder*="描述"]',
            '[contenteditable="true"][class*="editor"]',
            'textarea[placeholder*="正文"]',
            'textarea[placeholder*="内容"]',
        ]

        if title.strip():
            for selector in title_candidates:
                locator = page.locator(selector).first
                if await locator.count() > 0:
                    with contextlib.suppress(Exception):
                        await locator.fill("")
                        await locator.fill(title[:20])
                        break

        body_text = content
        if topics:
            topic_suffix = " ".join(f"#{topic}" for topic in topics)
            body_text = f"{content}\n{topic_suffix}".strip()

        filled = False
        for selector in body_candidates:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            try:
                await locator.click(timeout=2_000)
                if "contenteditable" in selector:
                    await page.keyboard.press("Control+A")
                    await page.keyboard.type(body_text)
                else:
                    await locator.fill(body_text)
                filled = True
                break
            except Exception:
                continue
        if not filled:
            raise HTTPException(status_code=502, detail="Could not find Xiaohongshu video content editor")

        action_patterns = ["存草稿", "保存草稿"] if draft else ["发布", "发布笔记", "立即发布"]
        clicked = False
        for pattern in action_patterns:
            button = page.locator(f'text={pattern}').first
            if await button.count() == 0:
                continue
            with contextlib.suppress(Exception):
                await button.click(timeout=5_000)
                clicked = True
                break
        if not clicked:
            raise HTTPException(status_code=502, detail="Could not find Xiaohongshu publish button")

        await page.wait_for_timeout(5000)
        final_url = page.url
        return PublishResponse(
            platform="xiaohongshu",
            status="success",
            detail="Xiaohongshu video publish flow submitted",
            url=final_url if final_url else None,
        )
    except PlaywrightTimeoutError as exc:
        raise HTTPException(status_code=504, detail=f"Xiaohongshu video publish timed out: {exc}") from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Xiaohongshu video publish failed: {exc}") from exc
    finally:
        await _close_playwright_page(playwright, browser, page)


def _is_auth_target(cfg: PlatformConfig, target: CdpTarget) -> bool:
    if not target.webSocketDebuggerUrl or target.type != "page":
        return False
    if cfg.key == "x":
        return "x.com" in target.url or "twitter.com" in target.url
    if cfg.key == "bilibili":
        return "passport.bilibili.com" in target.url or "bilibili.com" in target.url
    if cfg.key == "xiaohongshu":
        return "/login" in target.url or "fe-login" in target.title.lower() or cfg.domain in target.url
    return cfg.domain in target.url


def _pick_auth_target(cfg: PlatformConfig, targets: list[CdpTarget]) -> CdpTarget | None:
    matching = [target for target in targets if _is_auth_target(cfg, target)]
    if matching:
        return matching[-1]
    with_socket = [target for target in targets if target.webSocketDebuggerUrl and target.type == "page"]
    return with_socket[-1] if with_socket else None


async def _wait_for_auth_target(cfg: PlatformConfig, *, timeout_seconds: float) -> CdpTarget | None:
    deadline = time.time() + timeout_seconds
    while True:
        target = _pick_auth_target(cfg, await _list_targets(cfg))
        if target and target.webSocketDebuggerUrl:
            return target
        if time.time() >= deadline:
            return None
        await asyncio.sleep(AUTH_QR_POLL_INTERVAL_SECONDS)


async def _cdp_command(socket, method: str, params: dict[str, Any] | None = None, *, timeout: float = 15.0) -> dict[str, Any]:
    message_id = _cdp_command.counter
    _cdp_command.counter += 1
    payload = {"id": message_id, "method": method}
    if params:
        payload["params"] = params
    await socket.send(json.dumps(payload))

    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise HTTPException(status_code=504, detail=f"Timed out waiting for CDP response to {method}")
        raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
        data = json.loads(raw)
        if data.get("id") != message_id:
            continue
        if "error" in data:
            raise HTTPException(status_code=502, detail=f"CDP command failed for {method}: {data['error']}")
        return data.get("result", {})


_cdp_command.counter = 1


async def _extract_qr_data_url(socket, cfg: PlatformConfig) -> str | None:
    if cfg.key == "bilibili":
        expression = """
(() => {
  const toCanvas = (node) => {
    if (!node) {
      return null;
    }
    if (node.tagName === 'CANVAS') {
      return node;
    }
    if (node.tagName !== 'IMG') {
      return null;
    }
    const width = node.naturalWidth || node.width || 0;
    const height = node.naturalHeight || node.height || 0;
    if (!width || !height) {
      return null;
    }
    const canvas = document.createElement('canvas');
    canvas.width = width;
    canvas.height = height;
    const context = canvas.getContext('2d');
    if (!context) {
      return null;
    }
    context.drawImage(node, 0, 0, width, height);
    return canvas;
  };

  const canvasLooksLikeQr = (canvas) => {
    const context = canvas.getContext('2d');
    if (!context) {
      return false;
    }
    const { data } = context.getImageData(0, 0, canvas.width, canvas.height);
    let opaque = 0;
    let dark = 0;
    let light = 0;
    for (let i = 0; i < data.length; i += 4) {
      const alpha = data[i + 3];
      if (alpha < 16) {
        continue;
      }
      opaque += 1;
      const brightness = data[i] + data[i + 1] + data[i + 2];
      if (brightness <= 96) {
        dark += 1;
      } else if (brightness >= 660) {
        light += 1;
      }
    }
    return opaque >= 5000 && dark >= 500 && light >= 500;
  };

  const renderNode = (node) => {
    const canvas = toCanvas(node);
    if (!canvas || !canvasLooksLikeQr(canvas)) {
      return null;
    }
    return canvas.toDataURL('image/png');
  };

  const visibleNodes = (selector, predicate = null) =>
    Array.from(document.querySelectorAll(selector))
      .filter((node) => {
        const rect = typeof node.getBoundingClientRect === 'function'
          ? node.getBoundingClientRect()
          : { width: 0, height: 0 };
        const width = Math.round(rect.width || node.width || 0);
        const height = Math.round(rect.height || node.height || 0);
        const style = window.getComputedStyle(node);
        const visible =
          width >= 120 &&
          height >= 120 &&
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          Number(style.opacity || '1') > 0;
        if (!visible) {
          return false;
        }
        return predicate ? predicate(node) : true;
      });

  const loginPageCandidate = visibleNodes('.login-scan__qrcode img, .login-scan__qrcode canvas')[0];
  if (loginPageCandidate) {
    const data = renderNode(loginPageCandidate);
    if (data) {
      return data;
    }
  }

  const loginBoxCandidate = visibleNodes('.login-scan-box img, .login-scan-box canvas')[0];
  if (loginBoxCandidate) {
    const data = renderNode(loginBoxCandidate);
    if (data) {
      return data;
    }
  }

  const scanWrapperCandidate = visibleNodes(
    '.login-scan-wp img, .login-scan-wp canvas',
    (node) => !node.closest('.login-client-qr-code')
  )[0];
  if (scanWrapperCandidate) {
    const data = renderNode(scanWrapperCandidate);
    if (data) {
      return data;
    }
  }

  const nodes = Array.from(document.querySelectorAll('img, canvas'));
  const candidates = nodes
    .map((node) => {
      const rect = typeof node.getBoundingClientRect === 'function'
        ? node.getBoundingClientRect()
        : { width: 0, height: 0 };
      const width = Math.round(rect.width || node.width || 0);
      const height = Math.round(rect.height || node.height || 0);
      const area = width * height;
      const style = window.getComputedStyle(node);
      const visible = width >= 120 && height >= 120 && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0;
      const src = node.tagName === 'IMG' ? (node.currentSrc || node.src || '') : '';
      const marker = `${node.tagName} ${node.id || ''} ${node.className || ''} ${src}`.toLowerCase();
      const containerMarker = (node.closest('.login-client-qr-code, .login-scan-box, .login-scan-wp, .login-scan__qrcode')?.className || '').toLowerCase();
      const score =
        (src.startsWith('data:image/') ? 50 : 0) +
        (/qr|qrcode|scan/.test(marker) ? 25 : 0) +
        (containerMarker.includes('login-scan__qrcode') ? 180 : 0) +
        (containerMarker.includes('login-scan-box') ? 160 : 0) +
        (containerMarker.includes('login-scan-wp') ? 80 : 0) +
        (containerMarker.includes('client-qr-code') ? -220 : 0) +
        (Math.abs(width - height) <= 10 ? 10 : 0) +
        Math.min(area / 1000, 50);
      return { node, src, visible, score };
    })
    .filter((item) => item.visible)
    .sort((left, right) => right.score - left.score);

  if (!candidates.length) {
    return null;
  }
  for (const candidate of candidates) {
    const data = renderNode(candidate.node);
    if (data) {
      return data;
    }
  }
  return null;
})()
"""
    else:
        expression = """
(() => {
  const nodes = Array.from(document.querySelectorAll('img, canvas'));
  const candidates = nodes
    .map((node) => {
      const rect = typeof node.getBoundingClientRect === 'function'
        ? node.getBoundingClientRect()
        : { width: 0, height: 0 };
      const width = Math.round(rect.width || node.width || 0);
      const height = Math.round(rect.height || node.height || 0);
      const area = width * height;
      const style = window.getComputedStyle(node);
      const visible = width >= 120 && height >= 120 && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0;
      const src = node.tagName === 'IMG' ? (node.currentSrc || node.src || '') : '';
      const marker = `${node.tagName} ${node.id || ''} ${node.className || ''} ${src}`.toLowerCase();
      const score =
        (src.startsWith('data:image/') ? 50 : 0) +
        (/qr|qrcode|scan/.test(marker) ? 25 : 0) +
        (Math.abs(width - height) <= 10 ? 10 : 0) +
        Math.min(area / 1000, 50);
      return { node, src, visible, score };
    })
    .filter((item) => item.visible)
    .sort((left, right) => right.score - left.score);

  if (!candidates.length) {
    return null;
  }

  const best = candidates[0];
  if (best.node.tagName === 'CANVAS') {
    return best.node.toDataURL('image/png');
  }
  if (best.src.startsWith('data:image/')) {
    return best.src;
  }
  if (best.node.tagName === 'IMG') {
    const canvas = document.createElement('canvas');
    const width = best.node.naturalWidth || best.node.width || 0;
    const height = best.node.naturalHeight || best.node.height || 0;
    if (!width || !height) {
      return null;
    }
    canvas.width = width;
    canvas.height = height;
    const context = canvas.getContext('2d');
    if (!context) {
      return null;
    }
    context.drawImage(best.node, 0, 0, width, height);
    return canvas.toDataURL('image/png');
  }
  return null;
})()
"""
    payload = await _cdp_command(
        socket,
        "Runtime.evaluate",
        {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
        },
        timeout=15.0,
    )
    result = payload.get("result") or {}
    value = result.get("value")
    return value if isinstance(value, str) and value.startswith("data:image/") else None


async def _capture_selector_screenshot_data(
    socket,
    selector: str,
    *,
    min_width: int = 120,
    min_height: int = 120,
) -> str | None:
    payload = await _cdp_command(
        socket,
        "Runtime.evaluate",
        {
            "expression": f"""
(() => {{
  const node = document.querySelector({json.dumps(selector)});
  if (!node || typeof node.getBoundingClientRect !== 'function') {{
    return null;
  }}
  const rect = node.getBoundingClientRect();
  const style = window.getComputedStyle(node);
  const width = Math.round(rect.width || 0);
  const height = Math.round(rect.height || 0);
  if (
    width < {min_width} ||
    height < {min_height} ||
    style.display === 'none' ||
    style.visibility === 'hidden' ||
    Number(style.opacity || '1') <= 0
  ) {{
    return null;
  }}
  return {{
    x: Math.max(0, rect.left + window.scrollX),
    y: Math.max(0, rect.top + window.scrollY),
    width,
    height,
  }};
}})()
""",
            "returnByValue": True,
            "awaitPromise": True,
        },
        timeout=15.0,
    )
    clip = payload.get("result", {}).get("value")
    if not isinstance(clip, dict):
        return None
    width = int(clip.get("width") or 0)
    height = int(clip.get("height") or 0)
    if width < min_width or height < min_height:
        return None
    screenshot = await _cdp_command(
        socket,
        "Page.captureScreenshot",
        {
            "format": "png",
            "fromSurface": True,
            "clip": {
                "x": float(clip.get("x") or 0),
                "y": float(clip.get("y") or 0),
                "width": float(width),
                "height": float(height),
                "scale": 1,
            },
        },
        timeout=20.0,
    )
    data = screenshot.get("data")
    return data if isinstance(data, str) and data else None


async def _capture_platform_qr_data(socket, cfg: PlatformConfig) -> str | None:
    if cfg.key == "bilibili":
        qr_data_url = await _extract_qr_data_url(socket, cfg)
        if qr_data_url:
            _, _, encoded = qr_data_url.partition(",")
            return encoded or None
        return None
    qr_data_url = await _extract_qr_data_url(socket, cfg)
    if not qr_data_url:
        return None
    _, _, encoded = qr_data_url.partition(",")
    return encoded or None


async def _inspect_auth_page(socket) -> dict[str, Any]:
    expression = """
(() => {
  const bodyText = (document.body?.innerText || '')
    .replace(/\\s+/g, ' ')
    .trim()
    .slice(0, 4000);
  const headingText = Array.from(document.querySelectorAll('h1, h2, h3, [role="heading"]'))
    .map((node) => (node.textContent || '').trim())
    .filter(Boolean)
    .join(' ')
    .slice(0, 1000);
  const qrHints = Array.from(document.querySelectorAll('img, canvas, [class*="qr"], [id*="qr"]'))
    .map((node) => {
      const text = `${node.tagName} ${node.id || ''} ${node.className || ''} ${(node.alt || '')}`.toLowerCase();
      return /qr|qrcode|scan/.test(text);
    })
    .some(Boolean);
  return {
    title: document.title || '',
    url: window.location.href || '',
    bodyText,
    headingText,
    qrHints,
  };
})()
"""
    payload = await _cdp_command(
        socket,
        "Runtime.evaluate",
        {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
        },
        timeout=15.0,
    )
    result = payload.get("result") or {}
    value = result.get("value")
    return value if isinstance(value, dict) else {}


def _auth_page_blocker_detail(cfg: PlatformConfig, page_state: dict[str, Any]) -> str | None:
    title = str(page_state.get("title") or "")
    url = str(page_state.get("url") or "")
    body_text = str(page_state.get("bodyText") or "")
    heading_text = str(page_state.get("headingText") or "")
    qr_hints = bool(page_state.get("qrHints"))
    haystack = " ".join((title, heading_text, body_text)).lower()

    if cfg.key == "xiaohongshu":
        if (
            "300012" in body_text
            or "ip at risk" in haystack
            or "switch to a secure network and retry" in haystack
            or "security verification" in haystack
            or "website-login/captcha" in url
        ):
            return (
                "Xiaohongshu blocked this login session with security verification "
                "(300012: IP at risk). Route Xiaohongshu traffic through a residential "
                "or mobile network, then click Refresh QR again."
            )
        if "qr code expired" in haystack:
            return "The Xiaohongshu QR code expired. Click Refresh QR to request a new one."

    if not qr_hints and ("security verification" in haystack or "captcha" in url):
        return f"{cfg.display_name} login is blocked by a security verification page right now."

    return None


async def _capture_auth_qr(cfg: PlatformConfig, *, force_refresh: bool = False) -> Path:
    if force_refresh:
        _cancel_auth_browser_bootstrap(cfg)
        await _stop_browser(cfg)
    await _wait_for_auth_browser(cfg)
    target = await _wait_for_auth_target(cfg, timeout_seconds=10.0)
    if not target or not target.webSocketDebuggerUrl:
        raise HTTPException(status_code=503, detail=f"{cfg.display_name} auth page is not ready yet")

    async with websockets.connect(
        target.webSocketDebuggerUrl,
        max_size=None,
        proxy=None,
        open_timeout=10,
        close_timeout=5,
        ping_interval=None,
    ) as socket:
        await _cdp_command(socket, "Page.enable")
        await _cdp_command(socket, "Runtime.enable")
        await _cdp_command(socket, "Emulation.setDeviceMetricsOverride", {
            "width": VIEWPORT_WIDTH,
            "height": VIEWPORT_HEIGHT,
            "deviceScaleFactor": 1,
            "mobile": False,
        })
        if force_refresh:
            await _cdp_command(socket, "Page.reload", {"ignoreCache": True}, timeout=15.0)

        deadline = time.time() + AUTH_QR_READY_TIMEOUT_SECONDS
        data = None
        while time.time() < deadline:
            blocker_detail = _auth_page_blocker_detail(cfg, await _inspect_auth_page(socket))
            if blocker_detail:
                raise HTTPException(status_code=409, detail=blocker_detail)
            data = await _capture_platform_qr_data(socket, cfg)
            if data:
                break
            await asyncio.sleep(AUTH_QR_POLL_INTERVAL_SECONDS)

        if not data:
            screenshot = await _cdp_command(socket, "Page.captureScreenshot", {
                "format": "png",
                "fromSurface": True,
            }, timeout=45.0)
            data = screenshot.get("data")

    if not data:
        raise HTTPException(status_code=502, detail=f"{cfg.display_name} did not produce an auth QR image")

    path = _auth_qr_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(base64.b64decode(data))
    return path


async def _resolve_auth_status(cfg: PlatformConfig) -> AuthStatusResponse:
    cached = auth_status_cache.get(cfg.key)
    now = time.time()
    if cached and (now - cached.last_checked_at) < AUTH_CACHE_TTL_SECONDS:
        return cached

    browser_running = await _browser_running(cfg)
    headed = True if (_uses_external_cdp(cfg) and browser_running) else _read_browser_mode(cfg)
    targets = await _list_targets(cfg) if (browser_running and _uses_external_cdp(cfg)) else []
    if browser_running:
        try:
            cookie_names = await asyncio.wait_for(_extract_runtime_cookie_names(cfg), timeout=5.0)
        except Exception:
            cookie_names = set()
    else:
        cookie_names = set()
    if not cookie_names and not _uses_external_cdp(cfg):
        cookie_names = _extract_cookie_names(cfg.profile_dir, cfg.domain)
    cookie_present = any(name in cookie_names for name in cfg.status_cookie_names) if cfg.status_cookie_names else bool(cookie_names)
    if not cookie_present and _looks_like_authenticated_external_session(cfg, targets):
        cookie_present = True

    detail_override = None
    if cfg.key == "xiaohongshu" and browser_running and cookie_present:
        session_ready, session_detail = await _probe_xiaohongshu_session_ready(cfg)
        if not session_ready:
            detail_override = session_detail
            cookie_present = True
            authenticated = False
            reason = "login_required"
        else:
            reason = None
            authenticated = True
    elif browser_running:
        reason = None if cookie_present else "login_required"
        authenticated = cookie_present
    elif cookie_present:
        if cfg.key == "xiaohongshu":
            detail_override = (
                "Xiaohongshu login_required: reopen Xiaohongshu login to validate the browser session before search/download"
            )
            reason = "login_required"
            authenticated = False
        else:
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
        detail=(
            None
            if authenticated
            else (
                detail_override
                or (
                    f"{cfg.display_name} login is required"
                    if not (_uses_external_cdp(cfg) and cfg.key == "x")
                    else "Complete login in the existing Chrome session attached through X_CDP_URL"
                )
            )
        ),
        last_checked_at=now,
    )
    auth_status_cache[cfg.key] = status
    return status


async def _require_authenticated_session(cfg: PlatformConfig) -> AuthStatusResponse:
    status = await _resolve_auth_status(cfg)
    if status.authenticated:
        return status
    detail = status.detail or f"{cfg.display_name} login is required"
    if not detail.lower().startswith(f"{cfg.display_name.lower()} login_required".lower()):
        detail = f"{cfg.display_name} login_required: {detail}"
    raise HTTPException(status_code=401, detail=detail)


@app.on_event("startup")
async def on_startup() -> None:
    if not PLATFORMS:
        raise RuntimeError("PLATFORM_ENABLED_PLATFORMS filtered out every platform")
    DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    PROFILE_ROOT.mkdir(parents=True, exist_ok=True)
    logger.info("Platform Browser Manager enabled platforms: %s", ", ".join(sorted(PLATFORMS)))


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
            capabilities=list(cfg.capabilities),
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
    if cfg.key == "x":
        if not await _browser_running(cfg):
            detail = (
                "X external browser is not reachable. Set X_CDP_URL to an existing Chrome DevTools endpoint "
                "and keep that browser logged into X."
            )
            raise HTTPException(status_code=503, detail=detail)
        await _open_tab(cfg, cfg.login_url)
        auth_status_cache.pop(cfg.key, None)
        return HTMLResponse(
            f"""
            <html>
              <head>
                <title>{cfg.display_name} Login</title>
                <meta name="viewport" content="width=device-width,initial-scale=1" />
              </head>
              <body style="font-family:system-ui,sans-serif;padding:24px;line-height:1.5;background:#0f172a;color:#e2e8f0;">
                <div style="max-width:640px;margin:0 auto;background:#111827;border-radius:16px;padding:24px;box-shadow:0 20px 45px rgba(15,23,42,0.35);">
                  <h1 style="margin:0 0 12px;font-size:24px;">{cfg.display_name} login</h1>
                  <p style="margin:0 0 16px;color:#cbd5e1;">
                    A login tab was opened in the existing Chrome session attached through CDP.
                    Complete login there, then keep this window open until the status flips to Connected.
                  </p>
                  <div id="status" style="margin-bottom:12px;font-size:14px;color:#fbbf24;">Waiting for login...</div>
                  <div id="detail" style="font-size:13px;color:#94a3b8;">Open the browser attached to X_CDP_URL and finish login.</div>
                  <div style="display:flex;gap:12px;margin-top:16px;">
                    <button id="reopen-btn" type="button" style="border:none;border-radius:10px;padding:10px 16px;background:#2563eb;color:#fff;cursor:pointer;">Open login tab again</button>
                    <button id="close-btn" type="button" style="border:1px solid rgba(148,163,184,0.3);border-radius:10px;padding:10px 16px;background:transparent;color:#cbd5e1;cursor:pointer;">Close</button>
                  </div>
                </div>
                <script>
                  const authBase = window.location.pathname.replace(/\\/auth\\/start\\/?$/, '');
                  const statusEl = document.getElementById('status');
                  const detailEl = document.getElementById('detail');
                  let notified = false;

                  function setStatus(message, color) {{
                    statusEl.textContent = message;
                    statusEl.style.color = color;
                  }}

                  async function reopenLogin() {{
                    window.location.reload();
                  }}

                  async function pollStatus() {{
                    try {{
                      const response = await fetch(`${{authBase}}/auth/status`, {{ cache: 'no-store' }});
                      const payload = await response.json();
                      if (!response.ok) {{
                        throw new Error(payload.detail || `status ${{response.status}}`);
                      }}
                      if (payload.authenticated) {{
                        setStatus('Connected. You can close this window.', '#86efac');
                        detailEl.textContent = 'X login detected in the existing browser session.';
                        if (!notified && window.opener) {{
                          window.opener.postMessage({{ type: 'vp-platform-auth', platform: payload.platform }}, window.location.origin);
                          notified = true;
                        }}
                        return;
                      }}
                      setStatus('Waiting for login...', '#fbbf24');
                      detailEl.textContent = payload.detail || 'Complete login in the attached browser tab.';
                    }} catch (error) {{
                      setStatus('Login polling unavailable', '#fca5a5');
                      detailEl.textContent = error instanceof Error ? error.message : String(error);
                    }}
                  }}

                  document.getElementById('reopen-btn').addEventListener('click', reopenLogin);
                  document.getElementById('close-btn').addEventListener('click', () => window.close());
                  pollStatus();
                  window.setInterval(pollStatus, {AUTH_STATUS_POLL_MS});
                </script>
              </body>
            </html>
            """
        )
    browser_running = await _browser_running(cfg)
    if browser_running:
        started = False
    else:
        _schedule_auth_browser(cfg)
        started = True
    auth_status_cache.pop(cfg.key, None)
    response = AuthStartResponse(
        platform=cfg.key,
        browser_started=started,
        headed=False,
        message=f"{cfg.display_name} QR login session is ready. Scan the QR code with your phone and this page will refresh automatically.",
    )
    return HTMLResponse(
        f"""
        <html>
          <head>
            <title>{cfg.display_name} Login</title>
            <meta name="viewport" content="width=device-width,initial-scale=1" />
          </head>
          <body style="font-family:system-ui,sans-serif;padding:24px;line-height:1.5;background:#0f172a;color:#e2e8f0;">
            <div style="max-width:640px;margin:0 auto;background:#111827;border-radius:16px;padding:24px;box-shadow:0 20px 45px rgba(15,23,42,0.35);">
              <h1 style="margin:0 0 12px;font-size:24px;">{cfg.display_name} login session started</h1>
              <p style="margin:0 0 16px;color:#cbd5e1;">{response.message}</p>
              <div id="status" style="margin-bottom:16px;font-size:14px;color:#fbbf24;">Preparing QR code...</div>
              <div style="background:#020617;border-radius:12px;padding:12px;display:flex;justify-content:center;align-items:center;min-height:360px;">
                <img id="qr-image" alt="{cfg.display_name} QR code" style="max-width:100%;max-height:540px;border-radius:12px;background:#fff;" />
              </div>
              <div id="detail" style="margin-top:12px;font-size:13px;color:#94a3b8;"></div>
              <div style="display:flex;gap:12px;margin-top:16px;">
                <button id="refresh-btn" type="button" style="border:none;border-radius:10px;padding:10px 16px;background:#2563eb;color:#fff;cursor:pointer;">Refresh QR</button>
                <button id="close-btn" type="button" style="border:1px solid rgba(148,163,184,0.3);border-radius:10px;padding:10px 16px;background:transparent;color:#cbd5e1;cursor:pointer;">Close</button>
              </div>
            </div>
            <script>
              const authBase = window.location.pathname.replace(/\\/auth\\/start\\/?$/, '');
              const qrImage = document.getElementById('qr-image');
              const statusEl = document.getElementById('status');
              const detailEl = document.getElementById('detail');
              const refreshButton = document.getElementById('refresh-btn');
              const closeButton = document.getElementById('close-btn');
              let notified = false;
              let qrRefreshTimer = null;
              let qrObjectUrl = null;
              let qrLoading = false;
              let qrErrorActive = false;

              function setStatus(message, color) {{
                statusEl.textContent = message;
                statusEl.style.color = color;
              }}

              function setRefreshLoading(loading) {{
                qrLoading = loading;
                refreshButton.disabled = loading;
                refreshButton.style.opacity = loading ? '0.65' : '1';
                refreshButton.style.cursor = loading ? 'wait' : 'pointer';
              }}

              function scheduleQrRefresh(delay = {AUTH_QR_REFRESH_MS}) {{
                if (qrRefreshTimer) {{
                  window.clearTimeout(qrRefreshTimer);
                }}
                qrRefreshTimer = window.setTimeout(() => {{
                  refreshQr(false);
                }}, delay);
              }}

              function parseErrorMessage(rawText, statusCode) {{
                if (!rawText) {{
                  return `QR request failed (${{statusCode}})`;
                }}
                try {{
                  const payload = JSON.parse(rawText);
                  if (payload && typeof payload.detail === 'string' && payload.detail) {{
                    return payload.detail;
                  }}
                }} catch (_error) {{
                }}
                return rawText.length > 240 ? `${{rawText.slice(0, 237)}}...` : rawText;
              }}

              function statusTitleForQrError(message) {{
                const haystack = (message || '').toLowerCase();
                if (haystack.includes('300012') || haystack.includes('ip at risk') || haystack.includes('security verification')) {{
                  return 'Xiaohongshu blocked QR login';
                }}
                if (haystack.includes('expired')) {{
                  return 'QR code expired';
                }}
                return 'QR code unavailable';
              }}

              async function refreshQr(forceNew = false) {{
                if (qrLoading) {{
                  return;
                }}
                qrErrorActive = false;
                setRefreshLoading(true);
                setStatus(forceNew ? 'Requesting a fresh QR code...' : 'Preparing QR code...', '#93c5fd');
                detailEl.textContent = forceNew
                  ? 'Asking Xiaohongshu for a new QR code...'
                  : 'Loading the current QR code...';
                try {{
                  const suffix = forceNew ? '&refresh=1' : '';
                  const response = await fetch(`${{authBase}}/auth/qr?ts=${{Date.now()}}${{suffix}}`, {{
                    cache: 'no-store',
                  }});
                  if (!response.ok) {{
                    throw new Error(parseErrorMessage(await response.text(), response.status));
                  }}
                  const blob = await response.blob();
                  qrErrorActive = false;
                  if (qrObjectUrl) {{
                    URL.revokeObjectURL(qrObjectUrl);
                  }}
                  qrObjectUrl = URL.createObjectURL(blob);
                  qrImage.src = qrObjectUrl;
                  setStatus('Scan the QR code with your phone to log in.', '#fbbf24');
                  detailEl.textContent = forceNew
                    ? 'Fresh QR code ready. Scan it right away.'
                    : 'QR code updated. If it expires, use Refresh QR.';
                }} catch (error) {{
                  qrErrorActive = true;
                  const message = error instanceof Error ? error.message : String(error);
                  setStatus(statusTitleForQrError(message), '#fca5a5');
                  detailEl.textContent = message;
                  qrImage.removeAttribute('src');
                }} finally {{
                  setRefreshLoading(false);
                  scheduleQrRefresh();
                }}
              }}

              async function pollStatus() {{
                try {{
                  const response = await fetch(`${{authBase}}/auth/status`, {{ cache: 'no-store' }});
                  const payload = await response.json();
                  if (!response.ok) {{
                    throw new Error(payload.detail || `status ${{response.status}}`);
                  }}
                  if (payload.authenticated) {{
                    qrErrorActive = false;
                    if (qrRefreshTimer) {{
                      window.clearTimeout(qrRefreshTimer);
                    }}
                    setStatus('Connected. You can close this window.', '#86efac');
                    detailEl.textContent = 'Login successful.';
                    if (!notified && window.opener) {{
                      window.opener.postMessage({{ type: 'vp-platform-auth', platform: payload.platform }}, window.location.origin);
                      notified = true;
                    }}
                    return;
                  }}
                  if (qrErrorActive) {{
                    return;
                  }}
                  setStatus('Scan the QR code with your phone to log in.', '#fbbf24');
                  detailEl.textContent = payload.detail || 'Waiting for login confirmation...';
                }} catch (error) {{
                  setStatus('Login polling unavailable', '#fca5a5');
                  detailEl.textContent = error instanceof Error ? error.message : String(error);
                }}
              }}

              qrImage.addEventListener('load', () => {{
                if (!qrLoading) {{
                  detailEl.textContent = 'QR code updated. If it expires, use Refresh QR.';
                }}
              }});
              qrImage.addEventListener('error', () => {{
                setStatus('QR code unavailable', '#fca5a5');
                detailEl.textContent = 'Failed to load QR image. Retrying soon.';
              }});
              refreshButton.addEventListener('click', () => refreshQr(true));
              closeButton.addEventListener('click', () => window.close());

              refreshQr(false);
              pollStatus();
              window.setInterval(pollStatus, {AUTH_STATUS_POLL_MS});
            </script>
          </body>
        </html>
        """
    )


@app.get("/api/platforms/{platform}/auth/qr")
async def auth_qr(platform: str, refresh: bool = False) -> FileResponse:
    cfg = _require_platform(platform)
    if cfg.key == "x":
        raise HTTPException(status_code=405, detail="X login does not use QR in the unified platform manager")
    path = await _capture_auth_qr(cfg, force_refresh=refresh)
    return FileResponse(str(path), media_type="image/png", filename=f"{cfg.key}-auth-qr.png")


@app.post("/api/platforms/{platform}/auth/logout")
async def auth_logout(platform: str) -> dict[str, str]:
    cfg = _require_platform(platform)
    if cfg.key == "x" and _uses_external_cdp(cfg):
        auth_status_cache.pop(cfg.key, None)
        return {"message": "X auth is managed by the external browser attached through X_CDP_URL"}
    await _stop_browser(cfg)
    shutil.rmtree(cfg.profile_dir, ignore_errors=True)
    auth_status_cache.pop(cfg.key, None)
    return {"message": f"{cfg.display_name} logged out"}


@app.post("/api/platforms/{platform}/search", response_model=SearchResponse)
async def search(platform: str, request: SearchRequest) -> SearchResponse:
    cfg = _require_platform(platform)
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    if platform == "x":
        await _require_authenticated_session(cfg)

    await _ensure_command_browser(cfg)
    if platform == "xiaohongshu":
        args = [cfg.command_site, "search", request.query.strip(), "--limit", str(request.max_results), "-f", "json"]
    elif platform == "x":
        args = [cfg.command_site, "search", request.query.strip(), "--limit", str(request.max_results), "-f", "json"]
    else:
        args = [cfg.command_site, "search", request.query.strip(), "--type", "video", "--limit", str(request.max_results), "-f", "json"]

    results: list[SearchResultItem]
    try:
        code, stdout, stderr = await _run_opencli(
            cfg,
            args,
            timeout_seconds=XIAOHONGSHU_SEARCH_TIMEOUT_SECONDS if platform == "xiaohongshu" else None,
        )
        if code == 0:
            payload = _parse_json_output(stdout, stderr)
            if not isinstance(payload, list):
                results = []
            else:
                results = [
                    _normalize_search_result(platform, item, index)
                    for index, item in enumerate(payload, start=1)
                    if isinstance(item, dict) and item.get("url")
                ]
        elif platform == "x" and _is_opencli_structure_error(stderr or stdout):
            logger.warning("Falling back to Playwright X search because opencli failed: %s", stderr or stdout)
            results = await _search_x_via_playwright(cfg, request.query.strip(), request.max_results)
        elif platform == "xiaohongshu":
            logger.warning("Falling back to Playwright Xiaohongshu search because opencli failed: %s", stderr or stdout)
            results = await _search_xiaohongshu_via_playwright(cfg, request.query.strip(), request.max_results)
        else:
            raise _map_opencli_error(stderr or stdout, platform=cfg)
    except HTTPException as exc:
        if platform == "xiaohongshu" and exc.status_code in {502, 504}:
            logger.warning("Falling back to Playwright Xiaohongshu search because opencli raised %s", exc.detail)
            results = await _search_xiaohongshu_via_playwright(cfg, request.query.strip(), request.max_results)
        else:
            raise

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
    used_opencli = True
    note_id: str | None = None
    if platform == "xiaohongshu":
        note_id = _extract_xiaohongshu_note_id(source_url)
        if not note_id:
            raise HTTPException(status_code=400, detail="Could not extract Xiaohongshu note id from URL")
        args = [cfg.command_site, "download", note_id, "--output", str(target_dir), "-f", "json"]
    elif platform == "x":
        if not _extract_x_post_id(source_url):
            raise HTTPException(status_code=400, detail="Could not extract X post id from URL")
        args = [cfg.command_site, "download", "--tweet-url", source_url, "--output", str(target_dir), "-f", "json"]
    else:
        bvid = _extract_bilibili_bvid(source_url)
        if not bvid:
            raise HTTPException(status_code=400, detail="Could not extract Bilibili BV id from URL")
        args = [cfg.command_site, "download", bvid, "--output", str(target_dir), "--quality", quality, "-f", "json"]

    code, stdout, stderr = await _run_opencli(cfg, args)
    if code != 0 and platform != "x":
        raise _map_opencli_error(stderr or stdout, platform=cfg)
    if code == 0:
        payload = _parse_json_output(stdout, stderr)
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict) and str(first.get("status") or "").lower() == "failed":
                detail = str(first.get("size") or first.get("message") or stdout or stderr)
                if platform == "xiaohongshu" and "no media found" in detail.lower():
                    used_opencli = False
                elif platform != "x":
                    raise _map_opencli_error(detail, platform=cfg)
                else:
                    used_opencli = False
            else:
                used_opencli = True
        else:
            used_opencli = True
    else:
        used_opencli = False

    file_path = await _resolve_downloaded_file(target_dir)
    if platform == "xiaohongshu" and not file_path and note_id:
        file_path = await _download_xiaohongshu_via_playwright(cfg, source_url, note_id, target_dir)
    if platform == "x" and (not used_opencli or not file_path):
        file_path = await _download_with_ytdlp(source_url, target_dir, quality)
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
        file_path = await _resolve_downloaded_file(candidate)
    if not file_path or not file_path.exists():
        raise HTTPException(status_code=404, detail="Download not found")
    return FileResponse(str(file_path), filename=file_path.name)


@app.post("/api/platforms/{platform}/publish", response_model=PublishResponse)
async def publish(platform: str, request: Request) -> PublishResponse:
    cfg = _require_platform(platform)
    if platform == "xiaohongshu":
        raise HTTPException(
            status_code=405,
            detail="Xiaohongshu publish is disabled by policy; only auth/search/download are enabled",
        )
    if "publish" not in cfg.capabilities:
        raise HTTPException(status_code=405, detail=f"{cfg.display_name} publish is not supported")

    payload, media_paths, staging_dir = await _parse_publish_payload(request)
    try:
        if platform == "x":
            text = str(payload.get("text") or payload.get("content") or "").strip()
            reply_to_url = str(payload.get("reply_to_url") or "").strip() or None
            if not text:
                raise HTTPException(status_code=400, detail="X publish requires non-empty text")

            await _require_authenticated_session(cfg)
            await _ensure_command_browser(cfg)
            if not media_paths:
                args = [cfg.command_site]
                if reply_to_url:
                    args.extend(["reply", reply_to_url, text, "-f", "json"])
                else:
                    args.extend(["post", "-f", "json", text])
                code, stdout, stderr = await _run_opencli_x_publish_with_retry(cfg, args)
                if code != 0:
                    raise _map_opencli_error(stderr or stdout, platform=cfg)
                result = _parse_json_output(stdout, stderr)
                detail = None
                if isinstance(result, list) and result and isinstance(result[0], dict):
                    detail = str(result[0].get("message") or "").strip() or None
                return PublishResponse(
                    platform="x",
                    status="success",
                    detail=detail or "X post submitted successfully",
                    url=reply_to_url or "https://x.com/home",
                )

            if _uses_external_cdp(cfg) and X_MEDIA_HELPER_URL:
                effective_media_paths, helper_staging_dir = _stage_external_upload_files(cfg, media_paths)
                if staging_dir is None:
                    staging_dir = helper_staging_dir
                return await _publish_x_via_helper(
                    text=text,
                    media_paths=effective_media_paths,
                    reply_to_url=reply_to_url,
                )

            return await _publish_x_via_playwright(
                cfg,
                text=text,
                media_paths=media_paths,
                reply_to_url=reply_to_url,
            )

        if platform == "xiaohongshu":
            content = str(payload.get("content") or payload.get("text") or "").strip()
            title = str(payload.get("title") or "").strip()
            topics = _collect_topics(payload.get("topics"))
            draft = _coerce_bool(payload.get("draft"))
            publish_mode = str(payload.get("publish_mode") or "").strip().lower()
            if not content:
                raise HTTPException(status_code=400, detail="Xiaohongshu publish requires content")
            if not publish_mode:
                publish_mode = "image_note" if media_paths and all(_guess_mime_type(path).startswith("image/") for path in media_paths) else "video_note"

            await _ensure_command_browser(cfg)
            if publish_mode == "image_note":
                if not media_paths:
                    raise HTTPException(status_code=400, detail="Xiaohongshu image_note publish requires at least one image")
                if not all(_guess_mime_type(path).startswith("image/") for path in media_paths):
                    raise HTTPException(status_code=400, detail="Xiaohongshu image_note publish accepts image files only")
                effective_title = title or content[:20] or media_paths[0].stem[:20] or "Untitled"
                args = [
                    cfg.command_site,
                    "publish",
                    content,
                    "--title",
                    effective_title[:20],
                    "--images",
                    ",".join(str(path) for path in media_paths),
                ]
                if topics:
                    args.extend(["--topics", ",".join(topics)])
                if draft:
                    args.extend(["--draft", "true"])
                args.extend(["-f", "json"])
                code, stdout, stderr = await _run_opencli(cfg, args)
                if code != 0:
                    raise _map_opencli_error(stderr or stdout, platform=cfg)
                result = _parse_json_output(stdout, stderr)
                detail = None
                if isinstance(result, list) and result and isinstance(result[0], dict):
                    detail = str(result[0].get("detail") or result[0].get("message") or "").strip() or None
                return PublishResponse(
                    platform="xiaohongshu",
                    status="success",
                    detail=detail or "Xiaohongshu image note submitted successfully",
                )

            if publish_mode == "video_note":
                video_path = next((path for path in media_paths if _guess_mime_type(path).startswith("video/")), None)
                if not video_path:
                    video_path = media_paths[0] if media_paths else None
                if not video_path:
                    raise HTTPException(status_code=400, detail="Xiaohongshu video_note publish requires a video file")
                return await _publish_xiaohongshu_video_via_playwright(
                    cfg,
                    video_path=video_path,
                    title=title,
                    content=content,
                    topics=topics,
                    draft=draft,
                )

            raise HTTPException(status_code=400, detail=f"Unsupported Xiaohongshu publish_mode '{publish_mode}'")

        raise HTTPException(status_code=405, detail=f"{cfg.display_name} publish is not supported")
    finally:
        if staging_dir:
            shutil.rmtree(staging_dir, ignore_errors=True)
