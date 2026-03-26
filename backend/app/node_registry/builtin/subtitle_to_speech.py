from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="subtitle_to_speech",
    display_name="Subtitle To Speech",
    category="audio",
    description="Generate a timed speech track from subtitle cues using the configured TTS service",
    icon="audio-lines",
    inputs=[
        PortDefinition(name="subtitle_file", port_type=PortType.SUBTITLE, description="Input subtitle file"),
        PortDefinition(
            name="reference_audio",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional reference voice audio/video",
        ),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.AUDIO, description="Generated speech audio"),
    ],
    params=[
        ParamDefinition(
            name="language",
            param_type="string",
            default="en",
            description="TTS language code",
        ),
    ],
)
