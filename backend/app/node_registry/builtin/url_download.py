from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="url_download",
    display_name="URL Download",
    category="source",
    description="Download video from YouTube or URL via yt-dlp",
    icon="download",
    inputs=[],
    outputs=[
        PortDefinition(name="output", port_type=PortType.ANY_MEDIA, description="Downloaded video"),
    ],
    params=[
        ParamDefinition(name="url", param_type="string", required=True,
                       description="YouTube URL or direct video URL"),
        ParamDefinition(name="format", param_type="select", default="best",
                       options=["best", "1080p", "720p", "480p", "audio_only"],
                       description="Download quality"),
    ],
    worker_type="ffmpeg",
)
