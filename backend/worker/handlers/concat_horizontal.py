from worker.handlers.base import BaseHandler


class ConcatHorizontalHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        left = input_paths["video_left"]
        right = input_paths["video_right"]
        resize_mode = node_config.get("resize_mode", "match_height")

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

        args = [
            "-i", left,
            "-i", right,
            "-filter_complex", filter_complex,
            "-map", "[v]",
            "-c:v", "libx264", "-preset", "fast",
            output_path,
        ]
        await self.run_ffmpeg(args)
