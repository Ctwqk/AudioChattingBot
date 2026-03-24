from worker.handlers.base import BaseHandler


class ConcatVerticalHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        top = input_paths["video_top"]
        bottom = input_paths["video_bottom"]
        resize_mode = node_config.get("resize_mode", "match_width")

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

        args = [
            "-i", top,
            "-i", bottom,
            "-filter_complex", filter_complex,
            "-map", "[v]",
            "-c:v", "libx264", "-preset", "fast",
            output_path,
        ]
        await self.run_ffmpeg(args)
