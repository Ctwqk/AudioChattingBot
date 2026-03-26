from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import httpx

from worker.handlers.base import BaseHandler
from worker.handlers.subtitle_utils import parse_srt

logger = logging.getLogger(__name__)


class SubtitleToSpeechHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        subtitle_path = input_paths["subtitle_file"]
        reference_audio_path = input_paths.get("reference_audio")
        language = str(node_config.get("language", "en") or "en")
        tts_base_url = os.environ.get("VIDEO_TTS_BASE_URL", "http://127.0.0.1:8010").rstrip("/")

        try:
            with open(subtitle_path, "r", encoding="utf-8") as handle:
                cues = parse_srt(handle.read())
        except UnicodeDecodeError as exc:
            raise RuntimeError(
                "subtitle_file input is not a valid UTF-8 subtitle file. "
                "Connect a subtitle-producing node to 'subtitle_file' and put optional audio/video on 'reference_audio'."
            ) from exc
        if not cues:
            raise RuntimeError("Subtitle file contains no cues")

        temp_audio_files: list[str] = []
        temp_reference_audio: str | None = None
        local_speaker_id: str | None = None
        provider_used = "local"
        minimax_fallback_reason: str | None = None
        prefer_minimax = self._can_use_minimax(reference_audio_path)
        try:
            if reference_audio_path:
                temp_reference_audio = await self._ensure_wav_reference(reference_audio_path)

            async with httpx.AsyncClient(base_url=tts_base_url, timeout=240) as client:
                if temp_reference_audio and not prefer_minimax:
                    try:
                        local_speaker_id = await self._register_local_speaker(
                            client=client,
                            reference_audio_path=temp_reference_audio,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Local TTS speaker registration failed; falling back to per-request speaker upload: %s",
                            exc,
                        )
                        local_speaker_id = None
                audio_inputs: list[tuple[str, int]] = []
                total_cues = len(cues)
                for cue_index, cue in enumerate(cues, start=1):
                    temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
                    temp_output.close()
                    temp_audio_files.append(temp_output.name)
                    if prefer_minimax:
                        try:
                            await self._synthesize_cue_with_minimax(
                                text=cue.text,
                                language=language,
                                output_path=temp_output.name,
                            )
                            provider_used = "minimax"
                        except Exception as exc:
                            minimax_fallback_reason = str(exc)
                            logger.warning(
                                "MiniMax TTS failed for subtitle cue %s; falling back to local TTS: %s",
                                cue.index,
                                exc,
                            )
                            prefer_minimax = False

                    if not prefer_minimax:
                        local_provider = await self._synthesize_cue_with_local_service(
                            client=client,
                            text=cue.text,
                            language=language,
                            output_path=temp_output.name,
                            reference_audio_path=temp_reference_audio,
                            speaker_id=local_speaker_id,
                        )
                        if provider_used == "local":
                            provider_used = local_provider
                        elif provider_used != local_provider:
                            provider_used = "mixed"
                    audio_inputs.append((temp_output.name, int(round(cue.start_seconds * 1000))))
                    logger.info("subtitle_to_speech progress: %s/%s cues finished", cue_index, total_cues)

            await self._mix_audio_timeline(
                audio_inputs=audio_inputs,
                duration=max(cue.end_seconds for cue in cues),
                output_path=output_path,
            )
        finally:
            for path in temp_audio_files:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            if temp_reference_audio and temp_reference_audio != reference_audio_path:
                try:
                    os.unlink(temp_reference_audio)
                except OSError:
                    pass

        return {
            "subtitle_segments": len(cues),
            "tts_language": language,
            "output_duration": max(cue.end_seconds for cue in cues),
            "tts_provider": provider_used,
            "tts_fallback_reason": minimax_fallback_reason,
        }

    def _can_use_minimax(self, reference_audio_path: str | None) -> bool:
        if reference_audio_path:
            return False
        return bool(self._minimax_api_key())

    def _minimax_api_key(self) -> str:
        return str(os.environ.get("MINIMAX_API_KEY", "") or "").strip()

    def _minimax_base_url(self) -> str:
        return str(os.environ.get("VIDEO_MINIMAX_TTS_BASE_URL", "https://api.minimaxi.com/v1") or "").rstrip("/")

    def _minimax_model(self) -> str:
        return str(os.environ.get("VIDEO_MINIMAX_TTS_MODEL", "speech-2.8-hd") or "speech-2.8-hd").strip()

    def _minimax_voice_id(self, language: str) -> str:
        configured = str(os.environ.get("VIDEO_MINIMAX_TTS_VOICE_ID", "") or "").strip()
        if configured:
            return configured
        normalized = language.lower()
        if normalized.startswith("zh"):
            return "female-shaonv"
        return "female-shaonv"

    async def _synthesize_cue_with_minimax(
        self,
        *,
        text: str,
        language: str,
        output_path: str,
    ) -> None:
        api_key = self._minimax_api_key()
        if not api_key:
            raise RuntimeError("MINIMAX_API_KEY is not configured")

        payload = {
            "model": self._minimax_model(),
            "text": text,
            "voice_setting": {
                "voice_id": self._minimax_voice_id(language),
                "speed": float(os.environ.get("VIDEO_MINIMAX_TTS_SPEED", "1.0") or "1.0"),
                "vol": float(os.environ.get("VIDEO_MINIMAX_TTS_VOLUME", "1.0") or "1.0"),
            },
        }
        headers = {"Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=240) as client:
            response = await client.post(
                f"{self._minimax_base_url()}/t2a_v2",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            content_type = str(response.headers.get("content-type") or "").lower()
            if "json" in content_type:
                try:
                    payload = response.json()
                except ValueError:
                    payload = {"raw": response.text[:200]}
                base_resp = payload.get("base_resp") if isinstance(payload, dict) else None
                status_msg = ""
                if isinstance(base_resp, dict):
                    status_msg = str(base_resp.get("status_msg") or "").strip()
                audio_payload = None
                if isinstance(payload, dict):
                    data_payload = payload.get("data")
                    if isinstance(data_payload, dict):
                        audio_payload = data_payload.get("audio")
                if isinstance(audio_payload, str) and audio_payload.strip():
                    audio_bytes = self._decode_minimax_audio(audio_payload)
                else:
                    raise RuntimeError(
                        "MiniMax TTS returned JSON instead of audio"
                        + (f": {status_msg}" if status_msg else "")
                    )
            else:
                audio_bytes = response.content
            temp_source = tempfile.NamedTemporaryFile(
                delete=False,
                suffix=".mp3" if "mpeg" in content_type or "mp3" in content_type or "json" in content_type else ".wav",
            )
            temp_source.close()
            try:
                with open(temp_source.name, "wb") as handle:
                    handle.write(audio_bytes)
                await self.run_ffmpeg([
                    "-i", temp_source.name,
                    "-vn",
                    "-ac", "1",
                    "-ar", "24000",
                    output_path,
                ])
            finally:
                try:
                    os.unlink(temp_source.name)
                except OSError:
                    pass

    def _decode_minimax_audio(self, audio_payload: str) -> bytes:
        payload = audio_payload.strip()
        if not payload:
            raise RuntimeError("MiniMax TTS returned empty audio payload")
        try:
            return bytes.fromhex(payload)
        except ValueError:
            try:
                import base64
                return base64.b64decode(payload)
            except Exception as exc:  # pragma: no cover - defensive
                raise RuntimeError("MiniMax TTS returned an unknown audio encoding") from exc

    async def _ensure_wav_reference(self, input_path: str) -> str:
        if input_path.lower().endswith(".wav"):
            return input_path
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        tmp_file.close()
        await self.run_ffmpeg([
            "-i", input_path,
            "-vn",
            "-ac", "1",
            "-ar", "24000",
            tmp_file.name,
        ])
        return tmp_file.name

    async def _synthesize_cue_with_local_service(
        self,
        *,
        client: httpx.AsyncClient,
        text: str,
        language: str,
        output_path: str,
        reference_audio_path: str | None,
        speaker_id: Optional[str],
    ) -> str:
        files = {}
        data = {"text": text, "language": language}
        if speaker_id:
            data["speaker_id"] = speaker_id
        elif reference_audio_path:
            files["speaker_wav"] = (
                Path(reference_audio_path).name,
                open(reference_audio_path, "rb"),
                "audio/wav",
            )
        try:
            response = await client.post("/v1/tts/stream", data=data, files=files or None)
            if speaker_id and response.status_code >= 400 and reference_audio_path:
                logger.warning(
                    "Local TTS request with speaker_id %s failed (%s); retrying with speaker_wav upload",
                    speaker_id,
                    response.status_code,
                )
                retry_file = open(reference_audio_path, "rb")
                try:
                    response = await client.post(
                        "/v1/tts/stream",
                        data={"text": text, "language": language},
                        files={
                            "speaker_wav": (
                                Path(reference_audio_path).name,
                                retry_file,
                                "audio/wav",
                            )
                        },
                    )
                finally:
                    retry_file.close()
            response.raise_for_status()
            with open(output_path, "wb") as handle:
                handle.write(response.content)
            return str(response.headers.get("x-tts-provider") or "local").strip().lower() or "local"
        finally:
            upload = files.get("speaker_wav")
            if upload:
                upload[1].close()

    async def _register_local_speaker(
        self,
        *,
        client: httpx.AsyncClient,
        reference_audio_path: str,
    ) -> str:
        with open(reference_audio_path, "rb") as reference_audio:
            response = await client.post(
                "/v1/speakers/register",
                files={
                    "speaker_wav": (
                        Path(reference_audio_path).name,
                        reference_audio,
                        "audio/wav",
                    )
                },
            )
        response.raise_for_status()
        payload = response.json()
        speaker_id = str(payload.get("speaker_id") or "").strip()
        if not speaker_id:
            raise RuntimeError("Local TTS speaker registration did not return speaker_id")
        logger.info("Registered local TTS speaker %s", speaker_id)
        return speaker_id

    async def _mix_audio_timeline(
        self,
        *,
        audio_inputs: list[tuple[str, int]],
        duration: float,
        output_path: str,
    ) -> None:
        args = [
            "-f", "lavfi",
            "-t", f"{duration:.3f}",
            "-i", "anullsrc=r=24000:cl=mono",
        ]
        filter_parts: list[str] = []
        mix_inputs = ["[0:a]"]
        for index, (audio_path, delay_ms) in enumerate(audio_inputs, start=1):
            args.extend(["-i", audio_path])
            delayed_label = f"d{index}"
            filter_parts.append(f"[{index}:a]adelay={delay_ms}:all=1[{delayed_label}]")
            mix_inputs.append(f"[{delayed_label}]")

        filter_parts.append(
            "".join(mix_inputs) + f"amix=inputs={len(mix_inputs)}:duration=longest:dropout_transition=0[aout]"
        )
        args.extend([
            "-filter_complex", ";".join(filter_parts),
            "-map", "[aout]",
            "-t", f"{duration:.3f}",
            output_path,
        ])
        await self.run_ffmpeg(args)
