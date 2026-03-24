from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="source",
    display_name="Source",
    category="source",
    description="Input source referencing an uploaded asset",
    icon="upload",
    inputs=[],
    outputs=[
        PortDefinition(name="output", port_type=PortType.ANY_MEDIA, description="Source media output"),
    ],
    params=[
        ParamDefinition(name="asset_id", param_type="string", required=True, description="ID of the uploaded asset"),
        ParamDefinition(name="media_type", param_type="select", default="video",
                       options=["video", "audio", "image", "subtitle"],
                       description="Type of the source media"),
    ],
    worker_type="none",
)
