from worker.handlers.base import BaseHandler
from worker.handlers.source import SourceHandler
from worker.handlers.trim import TrimHandler
from worker.handlers.concat_horizontal import ConcatHorizontalHandler
from worker.handlers.concat_vertical import ConcatVerticalHandler
from worker.handlers.concat_timeline import ConcatTimelineHandler
from worker.handlers.watermark import WatermarkHandler
from worker.handlers.subtitle import SubtitleHandler
from worker.handlers.bgm import BgmHandler
from worker.handlers.transcode import TranscodeHandler
from worker.handlers.url_download import UrlDownloadHandler
from worker.handlers.export import ExportHandler
from worker.handlers.youtube_upload import YouTubeUploadHandler

HANDLER_MAP: dict[str, type[BaseHandler]] = {
    "source": SourceHandler,
    "trim": TrimHandler,
    "concat_horizontal": ConcatHorizontalHandler,
    "concat_vertical": ConcatVerticalHandler,
    "concat_timeline": ConcatTimelineHandler,
    "watermark": WatermarkHandler,
    "subtitle": SubtitleHandler,
    "bgm": BgmHandler,
    "transcode": TranscodeHandler,
    "url_download": UrlDownloadHandler,
    "export": ExportHandler,
    "youtube_upload": YouTubeUploadHandler,
}
