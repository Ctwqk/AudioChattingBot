import asyncio
import logging

from worker.handlers.base import BaseHandler, CancelledError

logger = logging.getLogger(__name__)


class UrlDownloadHandler(BaseHandler):
    async def execute(self, node_config: dict, input_paths: dict[str, str], output_path: str) -> None:
        url = node_config.get("url", "")
        if not url:
            raise ValueError("No URL provided")

        fmt = node_config.get("format", "best")

        args = [
            "yt-dlp",
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
