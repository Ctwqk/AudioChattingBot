import { useState, useEffect } from 'react';
import useEditorStore from '../../store/editorStore';
import useNodeTypes from '../../hooks/useNodeTypes';
import apiClient from '../../api/client';
import type { Asset, ParamDefinition } from '../../api/types';

type YouTubeSearchResult = {
  id: string;
  title: string;
  url: string;
  thumbnail?: string | null;
  duration?: number | null;
  channel?: string | null;
};

export default function ConfigPanel() {
  const { nodes, selectedNodeId, updateNodeConfig, updateNodeLabel, removeNode } = useEditorStore();
  const { nodeTypes } = useNodeTypes();
  const [assets, setAssets] = useState<Asset[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<YouTubeSearchResult[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [channelFilter, setChannelFilter] = useState('');
  const [durationFilter, setDurationFilter] = useState<'any' | 'short' | 'medium' | 'long'>('any');

  const node = nodes.find(n => n.id === selectedNodeId);
  const typeDef = node ? nodeTypes.find(t => t.type_name === (node.data.nodeType as string || node.type)) : null;

  useEffect(() => {
    // Load assets for source node picker
    apiClient.get('/assets').then(res => setAssets(res.data.items || [])).catch(() => {});
  }, []);

  useEffect(() => {
    setSearchResults([]);
    setSearchError(null);
    setSearchLoading(false);
    setChannelFilter('');
    setDurationFilter('any');
    if (node?.data.nodeType === 'url_download') {
      const nodeConfig = (node.data.config as Record<string, unknown> | undefined) || {};
      setSearchQuery(String(nodeConfig.query || ''));
      return;
    }
    setSearchQuery('');
  }, [node?.id, node?.data.nodeType, node?.data.config]);

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

  const handleSearch = async () => {
    const query = searchQuery.trim();
    if (!query) {
      setSearchError('Enter a search query first');
      setSearchResults([]);
      return;
    }

    try {
      setSearchLoading(true);
      setSearchError(null);
      const response = await fetch('/youtube/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, max_results: 8 }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        throw new Error(payload?.detail || `Search failed with status ${response.status}`);
      }
      const payload = await response.json() as { results?: YouTubeSearchResult[] };
      setSearchResults(payload.results || []);
      handleChange('query', query);
    } catch (error) {
      setSearchError(error instanceof Error ? error.message : 'Search failed');
      setSearchResults([]);
    } finally {
      setSearchLoading(false);
    }
  };

  const filteredSearchResults = searchResults.filter(result => {
    const channel = result.channel?.toLowerCase() || '';
    const channelNeedle = channelFilter.trim().toLowerCase();
    if (channelNeedle && !channel.includes(channelNeedle)) {
      return false;
    }

    const duration = result.duration || 0;
    if (durationFilter === 'short' && duration > 4 * 60) {
      return false;
    }
    if (durationFilter === 'medium' && (duration <= 4 * 60 || duration > 20 * 60)) {
      return false;
    }
    if (durationFilter === 'long' && duration <= 20 * 60) {
      return false;
    }
    return true;
  });

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

      {node.data.nodeType === 'url_download' && (
        <div style={{
          marginBottom: 16,
          padding: 12,
          borderRadius: 8,
          backgroundColor: '#111827',
          border: '1px solid #1f2937',
        }}>
          <div style={{ fontSize: 11, color: '#93c5fd', fontWeight: 700, marginBottom: 8 }}>
            YouTube Search
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
            Search uses yt-dlp, then fills the node URL. It does not consume official YouTube Data API quota.
          </div>
          <input
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            placeholder="Search YouTube videos"
            style={{
              width: '100%',
              padding: '6px 8px',
              backgroundColor: '#1e293b',
              border: '1px solid #334155',
              borderRadius: 4,
              color: '#e2e8f0',
              fontSize: 13,
              outline: 'none',
              marginBottom: 8,
            }}
          />
          <button
            type="button"
            onClick={() => void handleSearch()}
            disabled={searchLoading}
            style={{
              width: '100%',
              padding: '8px 10px',
              backgroundColor: '#1d4ed8',
              border: 'none',
              borderRadius: 6,
              color: '#eff6ff',
              fontSize: 12,
              cursor: searchLoading ? 'default' : 'pointer',
              opacity: searchLoading ? 0.7 : 1,
            }}
          >
            {searchLoading ? 'Searching...' : 'Search'}
          </button>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 110px', gap: 8, marginTop: 8 }}>
            <input
              value={channelFilter}
              onChange={e => setChannelFilter(e.target.value)}
              placeholder="Filter by channel"
              style={{
                width: '100%',
                padding: '6px 8px',
                backgroundColor: '#0f172a',
                border: '1px solid #334155',
                borderRadius: 4,
                color: '#e2e8f0',
                fontSize: 12,
                outline: 'none',
              }}
            />
            <select
              value={durationFilter}
              onChange={e => setDurationFilter(e.target.value as 'any' | 'short' | 'medium' | 'long')}
              style={{
                width: '100%',
                padding: '6px 8px',
                backgroundColor: '#0f172a',
                border: '1px solid #334155',
                borderRadius: 4,
                color: '#e2e8f0',
                fontSize: 12,
                outline: 'none',
              }}
            >
              <option value="any">Any length</option>
              <option value="short">Short</option>
              <option value="medium">Medium</option>
              <option value="long">Long</option>
            </select>
          </div>
          {searchError ? (
            <div style={{ fontSize: 11, color: '#fca5a5', marginTop: 8 }}>{searchError}</div>
          ) : null}
          {searchResults.length > 0 ? (
            <div style={{ marginTop: 10, display: 'grid', gap: 8 }}>
              <div style={{ fontSize: 11, color: '#64748b' }}>
                Showing {filteredSearchResults.length} of {searchResults.length} results
              </div>
              {filteredSearchResults.map(result => (
                <button
                  key={result.id}
                  type="button"
                  onClick={() => handleChange('url', result.url)}
                  style={{
                    textAlign: 'left',
                    padding: 10,
                    borderRadius: 6,
                    border: '1px solid #334155',
                    backgroundColor: '#0f172a',
                    color: '#e2e8f0',
                    cursor: 'pointer',
                  }}
                >
                  <div style={{ display: 'grid', gridTemplateColumns: '96px 1fr', gap: 10, alignItems: 'start' }}>
                    <div style={{
                      width: 96,
                      aspectRatio: '16 / 9',
                      borderRadius: 6,
                      overflow: 'hidden',
                      backgroundColor: '#1e293b',
                      border: '1px solid #334155',
                    }}>
                      {result.thumbnail ? (
                        <img
                          src={result.thumbnail}
                          alt={result.title}
                          style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
                        />
                      ) : null}
                    </div>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>
                        {result.title}
                      </div>
                      <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>
                        {[result.channel, formatDuration(result.duration)].filter(Boolean).join(' · ')}
                      </div>
                      <div style={{ fontSize: 10, color: '#60a5fa', wordBreak: 'break-all' }}>
                        {result.url}
                      </div>
                    </div>
                  </div>
                </button>
              ))}
              {filteredSearchResults.length === 0 ? (
                <div style={{ fontSize: 11, color: '#94a3b8' }}>
                  No results match the current filters.
                </div>
              ) : null}
            </div>
          ) : null}
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

function formatDuration(duration?: number | null) {
  if (!duration || duration <= 0) return null;
  const hours = Math.floor(duration / 3600);
  const minutes = Math.floor((duration % 3600) / 60);
  const seconds = duration % 60;
  const parts = [hours, minutes, seconds]
    .filter((value, index) => value > 0 || index > 0)
    .map(value => String(value).padStart(2, '0'));
  return parts.join(':');
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
