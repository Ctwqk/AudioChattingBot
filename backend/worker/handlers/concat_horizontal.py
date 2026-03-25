from worker.handlers.base import BaseHandler


class ConcatHorizontalHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        left = input_paths["video_left"]
        right = input_paths["video_right"]
        resize_mode = node_config.get("resize_mode", "match_height")
        left_probe = await self.run_ffprobe(left)
        right_probe = await self.run_ffprobe(right)
        left_has_audio = any(stream.get("codec_type") == "audio" for stream in left_probe.get("streams", []))
        right_has_audio = any(stream.get("codec_type") == "audio" for stream in right_probe.get("streams", []))

        if resize_mode == "match_height":
            # Scale both to the same height (480) maintaining aspect ratio
            filter_complex = (
                "[0:v]scale=-2:480[left];"
                "[1:v]scale=-2:480[right];"
                "[left][right]hstack=inputs=2[v]"
            )
        elif resize_mode == "match_width":
            # Scale both to same width, then hstack
            filter_complex = (
                "[0:v]scale=640:-2[left];"
                "[1:v]scale=640:-2[right];"
                "[left][right]hstack=inputs=2[v]"
            )
        else:
            # none: just hstack directly
            filter_complex = "[0:v][1:v]hstack=inputs=2[v]"

        if left_has_audio and right_has_audio:
            filter_complex += ";[0:a][1:a]amix=inputs=2:duration=longest:dropout_transition=2[a]"

        args = [
            "-i", left,
            "-i", right,
            "-filter_complex", filter_complex,
            "-map", "[v]",
        ]
        if left_has_audio and right_has_audio:
            args.extend(["-map", "[a]", "-c:a", "aac"])
        elif left_has_audio:
            args.extend(["-map", "0:a:0", "-c:a", "aac"])
        elif right_has_audio:
            args.extend(["-map", "1:a:0", "-c:a", "aac"])
        args.extend([
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            output_path,
        ])
        await self.run_ffmpeg(args)
