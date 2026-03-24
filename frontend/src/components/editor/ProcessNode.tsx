import { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import useNodeTypes from '../../hooks/useNodeTypes';

interface ProcessNodeProps {
  data: {
    label: string;
    config: Record<string, unknown>;
    asset_id?: string;
    nodeType?: string;
  };
  selected: boolean;
}

const PORT_COLORS: Record<string, string> = {
  video: '#3b82f6',
  audio: '#22c55e',
  image: '#f59e0b',
  subtitle: '#a855f7',
  any_media: '#6b7280',
};

function ProcessNode({ data, selected }: ProcessNodeProps) {
  const { nodeTypes } = useNodeTypes();
  const typeName = data.nodeType || 'unknown';
  const typeDef = nodeTypes.find(t => t.type_name === typeName);

  const inputs = typeDef?.inputs || [];
  const outputs = typeDef?.outputs || [];
  const icon = typeDef?.icon || '⬡';

  return (
    <div
      style={{
        background: selected ? '#1e293b' : '#0f172a',
        border: `2px solid ${selected ? '#3b82f6' : '#334155'}`,
        borderRadius: 8,
        padding: '8px 12px',
        minWidth: 150,
        color: '#e2e8f0',
        fontSize: 12,
      }}
    >
      {/* Input handles */}
      {inputs.map((port, i) => (
        <Handle
          key={`in-${port.name}`}
          type="target"
          position={Position.Left}
          id={port.name}
          style={{
            top: `${((i + 1) / (inputs.length + 1)) * 100}%`,
            background: PORT_COLORS[port.port_type] || '#6b7280',
            width: 10,
            height: 10,
          }}
          title={`${port.name} (${port.port_type})`}
        />
      ))}

      {/* Node content */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
        <span style={{ fontSize: 16 }}>{icon}</span>
        <span style={{ fontWeight: 600 }}>{data.label || typeDef?.display_name || typeName}</span>
      </div>
      <div style={{ color: '#94a3b8', fontSize: 11 }}>
        {typeDef?.category || ''}
      </div>

      {/* Output handles */}
      {outputs.map((port, i) => (
        <Handle
          key={`out-${port.name}`}
          type="source"
          position={Position.Right}
          id={port.name}
          style={{
            top: `${((i + 1) / (outputs.length + 1)) * 100}%`,
            background: PORT_COLORS[port.port_type] || '#6b7280',
            width: 10,
            height: 10,
          }}
          title={`${port.name} (${port.port_type})`}
        />
      ))}
    </div>
  );
}

export default memo(ProcessNode);
