from worker.handlers.base import BaseHandler


class TrimHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["input"]
        start = node_config.get("start_time", "00:00:00")
        end = node_config.get("end_time", "")
        duration = node_config.get("duration", "")

        args = ["-i", video]
        if start:
            args.extend(["-ss", start])
        if end:
            args.extend(["-to", end])
        elif duration:
            args.extend(["-t", duration])
        args.extend(["-c", "copy", output_path])

        await self.run_ffmpeg(args)
