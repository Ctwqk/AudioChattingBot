from __future__ import annotations
from collections import defaultdict, deque
from app.schemas.pipeline import (
    PipelineDefinition, ValidationError, ValidationWarning, ValidationResult,
)
from app.node_registry.registry import NodeTypeRegistry


def validate_pipeline(definition: PipelineDefinition) -> ValidationResult:
    """Validate a pipeline definition for correctness."""
    errors: list[ValidationError] = []
    warnings: list[ValidationWarning] = []
    registry = NodeTypeRegistry.get()

    nodes_by_id = {n.id: n for n in definition.nodes}

    # 1. Check all node types exist
    for node in definition.nodes:
        if registry.get_type(node.type) is None:
            errors.append(ValidationError(
                type="unknown_node_type",
                node_id=node.id,
                message=f"Unknown node type '{node.type}'",
            ))

    # 2. Build adjacency structures
    adjacency: dict[str, list[str]] = defaultdict(list)  # node -> downstream nodes
    in_degree: dict[str, int] = {n.id: 0 for n in definition.nodes}

    for edge in definition.edges:
        if edge.source not in nodes_by_id:
            errors.append(ValidationError(
                type="invalid_edge",
                edge_id=edge.id,
                message=f"Edge source '{edge.source}' does not exist",
            ))
            continue
        if edge.target not in nodes_by_id:
            errors.append(ValidationError(
                type="invalid_edge",
                edge_id=edge.id,
                message=f"Edge target '{edge.target}' does not exist",
            ))
            continue
        adjacency[edge.source].append(edge.target)
        in_degree[edge.target] = in_degree.get(edge.target, 0) + 1

    # 3. Cycle detection via Kahn's algorithm
    queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
    topo_order = []
    temp_in_degree = dict(in_degree)

    while queue:
        node_id = queue.popleft()
        topo_order.append(node_id)
        for downstream in adjacency.get(node_id, []):
            temp_in_degree[downstream] -= 1
            if temp_in_degree[downstream] == 0:
                queue.append(downstream)

    if len(topo_order) < len(nodes_by_id):
        cycle_nodes = [nid for nid in nodes_by_id if nid not in topo_order]
        labels = [nodes_by_id[nid].data.label or nid for nid in cycle_nodes]
        errors.append(ValidationError(
            type="cycle_detected",
            nodes=cycle_nodes,
            message=f"Cycle detected involving nodes: {', '.join(labels)}",
        ))

    # 4. Port type validation
    for edge in definition.edges:
        src_node = nodes_by_id.get(edge.source)
        tgt_node = nodes_by_id.get(edge.target)
        if not src_node or not tgt_node:
            continue

        if not registry.validate_edge(
            source_type=src_node.type,
            source_port=edge.sourceHandle,
            target_type=tgt_node.type,
            target_port=edge.targetHandle,
        ):
            errors.append(ValidationError(
                type="port_type_mismatch",
                edge_id=edge.id,
                source_port=edge.sourceHandle,
                target_port=edge.targetHandle,
                message=f"Cannot connect '{edge.sourceHandle}' to '{edge.targetHandle}' (type mismatch)",
            ))

    # 5. Duplicate input port check + required input check
    connected_inputs: dict[str, set[str]] = defaultdict(set)
    for edge in definition.edges:
        key = (edge.target, edge.targetHandle)
        if edge.targetHandle in connected_inputs.get(edge.target, set()):
            tgt_node = nodes_by_id.get(edge.target)
            tgt_label = (tgt_node.data.label or tgt_node.type) if tgt_node else edge.target
            errors.append(ValidationError(
                type="duplicate_input_port",
                node_id=edge.target,
                target_port=edge.targetHandle,
                message=f"Input port '{edge.targetHandle}' on '{tgt_label}' has multiple connections (only one allowed)",
            ))
        connected_inputs[edge.target].add(edge.targetHandle)

    for node in definition.nodes:
        node_def = registry.get_type(node.type)
        if not node_def:
            continue
        for port in node_def.inputs:
            if port.required and port.name not in connected_inputs.get(node.id, set()):
                errors.append(ValidationError(
                    type="missing_required_input",
                    node_id=node.id,
                    target_port=port.name,
                    message=f"Required input '{port.name}' on '{node.data.label or node.type}' is not connected",
                ))

    # 6. Disconnected node warning
    has_outgoing = set()
    for edge in definition.edges:
        has_outgoing.add(edge.source)

    terminal_types = {"transcode"}
    for node in definition.nodes:
        node_def = registry.get_type(node.type)
        if not node_def:
            continue
        if node_def.outputs and node.id not in has_outgoing and node.type not in terminal_types:
            warnings.append(ValidationWarning(
                type="disconnected_node",
                node_id=node.id,
                message=f"Node '{node.data.label or node.type}' has outputs but none are connected",
            ))

    # 7. Source node asset_id check
    for node in definition.nodes:
        if node.type == "source":
            asset_id = node.data.config.get("asset_id") or node.data.asset_id
            if not asset_id:
                errors.append(ValidationError(
                    type="missing_asset",
                    node_id=node.id,
                    message=f"Source node '{node.data.label or 'Source'}' has no asset_id configured",
                ))

    # 8. Node parameter validation
    for node in definition.nodes:
        node_def = registry.get_type(node.type)
        if not node_def:
            continue
        config = node.data.config
        for param in node_def.params:
            value = config.get(param.name)

            # Required param missing or empty string
            if param.required and (value is None or value == ""):
                errors.append(ValidationError(
                    type="invalid_param",
                    node_id=node.id,
                    param_name=param.name,
                    message=f"Required parameter '{param.name}' on '{node.data.label or node.type}' is missing or empty",
                ))
                continue

            # Number range checks
            if param.param_type == "number" and value is not None:
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be a number",
                    ))
                    continue
                if param.min_value is not None and numeric_value < param.min_value:
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be >= {param.min_value} (got {numeric_value})",
                    ))
                if param.max_value is not None and numeric_value > param.max_value:
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be <= {param.max_value} (got {numeric_value})",
                    ))

            # Select options check
            if param.param_type == "select" and param.options is not None and value is not None:
                if value not in param.options:
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be one of {param.options} (got '{value}')",
                    ))

    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def topological_sort(definition: PipelineDefinition) -> list[str]:
    """Return topologically sorted list of node IDs."""
    in_degree: dict[str, int] = {n.id: 0 for n in definition.nodes}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for edge in definition.edges:
        adjacency[edge.source].append(edge.target)
        in_degree[edge.target] = in_degree.get(edge.target, 0) + 1

    queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
    result = []

    while queue:
        node_id = queue.popleft()
        result.append(node_id)
        for downstream in adjacency.get(node_id, []):
            in_degree[downstream] -= 1
            if in_degree[downstream] == 0:
                queue.append(downstream)

    return result


def build_dependency_map(definition: PipelineDefinition) -> dict[str, list[str]]:
    """Build a map of node_id -> list of upstream node_ids it depends on."""
    deps: dict[str, list[str]] = {n.id: [] for n in definition.nodes}
    for edge in definition.edges:
        if edge.target in deps:
            deps[edge.target].append(edge.source)
    return deps
