from worker.handlers.base import BaseHandler


class ConcatVerticalHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        top = input_paths["video_top"]
        bottom = input_paths["video_bottom"]
        resize_mode = node_config.get("resize_mode", "match_width")
        top_probe = await self.run_ffprobe(top)
        bottom_probe = await self.run_ffprobe(bottom)
        top_has_audio = any(stream.get("codec_type") == "audio" for stream in top_probe.get("streams", []))
        bottom_has_audio = any(stream.get("codec_type") == "audio" for stream in bottom_probe.get("streams", []))

        if resize_mode == "match_width":
            # Scale both to the same width (640) maintaining aspect ratio
            filter_complex = (
                "[0:v]scale=640:-2[top];"
                "[1:v]scale=640:-2[bottom];"
                "[top][bottom]vstack=inputs=2[v]"
            )
        elif resize_mode == "match_height":
            # Scale both to same height, then vstack
            filter_complex = (
                "[0:v]scale=-2:480[top];"
                "[1:v]scale=-2:480[bottom];"
                "[top][bottom]vstack=inputs=2[v]"
            )
        else:
            # none: just vstack directly
            filter_complex = "[0:v][1:v]vstack=inputs=2[v]"

        if top_has_audio and bottom_has_audio:
            filter_complex += ";[0:a][1:a]amix=inputs=2:duration=longest:dropout_transition=2[a]"

        args = [
            "-i", top,
            "-i", bottom,
            "-filter_complex", filter_complex,
            "-map", "[v]",
        ]
        if top_has_audio and bottom_has_audio:
            args.extend(["-map", "[a]", "-c:a", "aac"])
        elif top_has_audio:
            args.extend(["-map", "0:a:0", "-c:a", "aac"])
        elif bottom_has_audio:
            args.extend(["-map", "1:a:0", "-c:a", "aac"])
        args.extend([
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            output_path,
        ])
        await self.run_ffmpeg(args)
