import { useState, useEffect } from 'react';
import useEditorStore from '../../store/editorStore';
import useNodeTypes from '../../hooks/useNodeTypes';
import apiClient from '../../api/client';
import type { Asset, ParamDefinition } from '../../api/types';

export default function ConfigPanel() {
  const { nodes, selectedNodeId, updateNodeConfig, updateNodeLabel, removeNode } = useEditorStore();
  const { nodeTypes } = useNodeTypes();
  const [assets, setAssets] = useState<Asset[]>([]);

  const node = nodes.find(n => n.id === selectedNodeId);
  const typeDef = node ? nodeTypes.find(t => t.type_name === (node.data.nodeType as string || node.type)) : null;

  useEffect(() => {
    // Load assets for source node picker
    apiClient.get('/assets').then(res => setAssets(res.data.items || [])).catch(() => {});
  }, []);

  if (!node || !typeDef) {
    return (
      <div style={{
        width: 280,
        backgroundColor: '#0f172a',
        borderLeft: '1px solid #1e293b',
        padding: 16,
        color: '#64748b',
        fontSize: 13,
      }}>
        Select a node to configure
      </div>
    );
  }

  const config = (node.data.config as Record<string, unknown>) || {};

  const handleChange = (name: string, value: unknown) => {
    updateNodeConfig(node.id, { [name]: value });
  };

  return (
    <div style={{
      width: 280,
      backgroundColor: '#0f172a',
      borderLeft: '1px solid #1e293b',
      overflowY: 'auto',
      padding: 16,
      color: '#e2e8f0',
      fontSize: 13,
    }}>
      <div style={{ marginBottom: 16 }}>
        <label style={{ display: 'block', fontSize: 11, color: '#64748b', marginBottom: 4 }}>Label</label>
        <input
          value={(node.data.label as string) || ''}
          onChange={e => updateNodeLabel(node.id, e.target.value)}
          style={{
            width: '100%',
            padding: '6px 8px',
            backgroundColor: '#1e293b',
            border: '1px solid #334155',
            borderRadius: 4,
            color: '#e2e8f0',
            fontSize: 13,
            outline: 'none',
          }}
        />
      </div>

      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>
        {typeDef.icon} {typeDef.display_name}
      </div>

      {/* Asset picker for source nodes */}
      {node.data.nodeType === 'source' && (
        <div style={{ marginBottom: 12 }}>
          <label style={{ display: 'block', fontSize: 11, color: '#64748b', marginBottom: 4 }}>Asset</label>
          <select
            value={(config.asset_id as string) || ''}
            onChange={e => handleChange('asset_id', e.target.value)}
            style={{
              width: '100%',
              padding: '6px 8px',
              backgroundColor: '#1e293b',
              border: '1px solid #334155',
              borderRadius: 4,
              color: '#e2e8f0',
              fontSize: 13,
            }}
          >
            <option value="">-- Select asset --</option>
            {assets.map(a => (
              <option key={a.id} value={a.id}>{a.original_name}</option>
            ))}
          </select>
        </div>
      )}

      {/* Params */}
      {typeDef.params
        .filter(p => p.name !== 'asset_id' && p.name !== 'media_type')
        .map(param => (
          <ParamField
            key={param.name}
            param={param}
            value={config[param.name]}
            onChange={val => handleChange(param.name, val)}
          />
        ))}

      <div style={{ borderTop: '1px solid #334155', marginTop: 16, paddingTop: 16 }}>
        <button
          onClick={() => removeNode(node.id)}
          style={{
            width: '100%',
            padding: '8px 12px',
            backgroundColor: '#7f1d1d',
            border: '1px solid #991b1b',
            borderRadius: 4,
            color: '#fca5a5',
            fontSize: 13,
            cursor: 'pointer',
          }}
        >
          Delete Node
        </button>
      </div>
    </div>
  );
}

function ParamField({
  param,
  value,
  onChange,
}: {
  param: ParamDefinition;
  value: unknown;
  onChange: (val: unknown) => void;
}) {
  const current = value ?? param.default;
  const inputStyle = {
    width: '100%',
    padding: '6px 8px',
    backgroundColor: '#1e293b',
    border: '1px solid #334155',
    borderRadius: 4,
    color: '#e2e8f0',
    fontSize: 13,
    outline: 'none',
  };

  return (
    <div style={{ marginBottom: 12 }}>
      <label style={{ display: 'block', fontSize: 11, color: '#64748b', marginBottom: 4 }}>
        {param.name.replace(/_/g, ' ')}
        {param.required && <span style={{ color: '#ef4444' }}> *</span>}
      </label>

      {param.param_type === 'select' && param.options ? (
        <select
          value={String(current || '')}
          onChange={e => onChange(e.target.value)}
          style={inputStyle}
        >
          {param.options.map(opt => (
            <option key={opt} value={opt}>{opt}</option>
          ))}
        </select>
      ) : param.param_type === 'boolean' ? (
        <label style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <input
            type="checkbox"
            checked={Boolean(current)}
            onChange={e => onChange(e.target.checked)}
          />
          <span>{current ? 'Yes' : 'No'}</span>
        </label>
      ) : param.param_type === 'number' ? (
        <input
          type="number"
          value={current !== undefined && current !== null ? Number(current) : ''}
          onChange={e => onChange(e.target.value === '' ? undefined : Number(e.target.value))}
          min={param.min_value ?? undefined}
          max={param.max_value ?? undefined}
          step={param.max_value && param.max_value <= 1 ? 0.01 : 1}
          style={inputStyle}
        />
      ) : (
        <input
          type="text"
          value={String(current || '')}
          onChange={e => onChange(e.target.value)}
          placeholder={param.description}
          style={inputStyle}
        />
      )}

      {param.description && (
        <div style={{ fontSize: 11, color: '#475569', marginTop: 2 }}>{param.description}</div>
      )}
    </div>
  );
}
