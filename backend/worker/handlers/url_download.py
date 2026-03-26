import asyncio
import hashlib
import logging
import os
import shutil
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from app.config import settings
from app.storage.manager import get_storage
from worker.handlers.base import BaseHandler, CancelledError

logger = logging.getLogger(__name__)


class UrlDownloadHandler(BaseHandler):
    async def execute(self, node_config: dict, input_paths: dict[str, str], output_path: str) -> dict:
        url = node_config.get("url", "")
        if not url:
            raise ValueError("No URL provided")

        fmt = node_config.get("format", "best")
        cache_path = self._cache_storage_path(url, fmt, output_path)
        if await self._restore_from_cache(cache_path, output_path):
            logger.info("URL download cache hit for %s (%s)", url, fmt)
            return {
                "_storage_path": cache_path,
                "_skip_upload": True,
                "cache_hit": True,
                "source_url": self._normalize_url(url),
            }

        logger.info("URL download cache miss for %s (%s)", url, fmt)

        yt_dlp = self.resolve_executable("yt-dlp")

        args = [
            yt_dlp,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "-o", output_path,
        ]

        format_map = {
            "1080p": "bestvideo[height<=1080]+bestaudio/best[height<=1080]",
            "720p": "bestvideo[height<=720]+bestaudio/best[height<=720]",
            "480p": "bestvideo[height<=480]+bestaudio/best[height<=480]",
            "audio_only": "bestaudio",
        }
        if fmt in format_map:
            args.extend(["-f", format_map[fmt]])

        args.append(url)

        if self._cancelled:
            raise CancelledError("Cancelled before download")

        logger.info(f"Running: {' '.join(args)}")
        self._proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await self._proc.communicate()

        if self._cancelled:
            raise CancelledError("Cancelled during download")
        if self._proc.returncode != 0:
            stderr_text = stderr.decode("utf-8", errors="replace")
            raise RuntimeError(f"yt-dlp failed (exit {self._proc.returncode}):\n{stderr_text[-2000:]}")

        await self._save_to_cache(cache_path, output_path)
        return {
            "_storage_path": cache_path,
            "_skip_upload": True,
            "cache_hit": False,
            "source_url": self._normalize_url(url),
        }

    @staticmethod
    def _cache_storage_path(url: str, fmt: str, output_path: str) -> str:
        normalized = UrlDownloadHandler._normalize_url(url)
        cache_key = hashlib.sha256(f"{normalized}\n{fmt}".encode("utf-8")).hexdigest()
        suffix = Path(output_path).suffix or ".mp4"
        return f"download-cache/{cache_key}{suffix}"

    @staticmethod
    def _normalize_url(url: str) -> str:
        parsed = urlparse(url.strip())
        host = parsed.netloc.lower()

        if "youtu.be" in host:
            video_id = parsed.path.strip("/")
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"

        if "youtube.com" in host:
            query = dict(parse_qsl(parsed.query, keep_blank_values=False))
            video_id = query.get("v", "").strip()
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"

        normalized_query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
        cleaned = parsed._replace(
            scheme=(parsed.scheme or "https").lower(),
            netloc=host,
            fragment="",
            query=normalized_query,
        )
        return urlunparse(cleaned)

    async def _restore_from_cache(self, cache_path: str, output_path: str) -> bool:
        storage = get_storage(settings.storage_backend)
        if not await storage.exists(cache_path):
            return False

        local_cached_path = storage.get_local_path(cache_path)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        if local_cached_path and os.path.exists(local_cached_path):
            shutil.copy2(local_cached_path, output_path)
            return True

        content = await storage.read(cache_path)
        with open(output_path, "wb") as f:
            f.write(content)
        return True

    async def _save_to_cache(self, cache_path: str, output_path: str) -> None:
        storage = get_storage(settings.storage_backend)
        if await storage.exists(cache_path):
            return
        with open(output_path, "rb") as f:
            await storage.save(cache_path, f)
