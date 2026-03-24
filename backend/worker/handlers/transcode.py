from worker.handlers.base import BaseHandler


class TranscodeHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["input"]
        video_codec = node_config.get("video_codec", "libx264")
        audio_codec = node_config.get("audio_codec", "aac")
        resolution = node_config.get("resolution", "")
        bitrate = node_config.get("bitrate", "")
        crf = node_config.get("crf", 23)
        preset = node_config.get("preset", "medium")

        args = ["-i", video]

        # Video codec
        args.extend(["-c:v", video_codec])

        if video_codec not in ("copy",):
            # CRF (only for x264/x265)
            if video_codec in ("libx264", "libx265"):
                args.extend(["-crf", str(int(crf))])
                args.extend(["-preset", preset])
            elif video_codec == "libvpx-vp9":
                args.extend(["-crf", str(int(crf)), "-b:v", "0"])

            # Resolution (convert WxH → W:H for ffmpeg scale filter)
            if resolution and resolution != "original":
                scale_val = resolution.replace("x", ":")
                args.extend(["-vf", f"scale={scale_val}"])

            # Bitrate
            if bitrate:
                args.extend(["-b:v", bitrate])

        # Audio codec
        args.extend(["-c:a", audio_codec])

        args.append(output_path)
        await self.run_ffmpeg(args)
