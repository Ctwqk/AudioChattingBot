from __future__ import annotations
import abc
import asyncio
import logging
import os
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


class CancelledError(Exception):
    """Raised when a handler detects its node has been cancelled."""


class BaseHandler(abc.ABC):
    """Base class for node execution handlers."""

    def __init__(self):
        self._cancelled = False
        self._proc: asyncio.subprocess.Process | None = None

    @abc.abstractmethod
    async def execute(
        self,
        node_config: dict,
        input_paths: dict[str, str],   # port_name -> local file path
        output_path: str,               # local file path for output
    ) -> None:
        """Execute the node operation. Raise on failure."""
        ...

    def cancel(self) -> None:
        """Signal cancellation. Kills any running subprocess."""
        self._cancelled = True
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.kill()
                logger.info("Killed ffmpeg subprocess due to cancellation")
            except ProcessLookupError:
                pass

    async def run_ffmpeg(self, args: list[str]) -> str:
        """Run an ffmpeg command and return stderr output."""
        if self._cancelled:
            raise CancelledError("Node cancelled before ffmpeg started")

        cmd = ["ffmpeg", "-y", "-hide_banner"] + args
        logger.info(f"Running: {' '.join(cmd)}")
        self._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await self._proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace")

        if self._cancelled:
            raise CancelledError("Node cancelled during ffmpeg execution")
        if self._proc.returncode != 0:
            raise RuntimeError(f"ffmpeg failed (exit {self._proc.returncode}):\n{stderr_text[-2000:]}")
        return stderr_text

    async def run_ffprobe(self, path: str) -> dict:
        """Run ffprobe and return JSON output."""
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format", "-show_streams",
            path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            return {}
        import json
        try:
            return json.loads(stdout.decode())
        except Exception:
            return {}
