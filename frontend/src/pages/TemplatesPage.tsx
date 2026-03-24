import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import apiClient from '../api/client';
import type { Pipeline } from '../api/types';

export default function TemplatesPage() {
  const [templates, setTemplates] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    apiClient.get('/templates').then(res => {
      setTemplates(res.data.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const handleUseTemplate = async (templateId: string) => {
    try {
      const res = await apiClient.post(`/pipelines/${templateId}/duplicate`);
      navigate(`/editor/${res.data.id}`);
    } catch {
      alert('Failed to create from template');
    }
  };

  return (
    <div style={{ padding: 24, color: '#e2e8f0', overflowY: 'auto', height: '100%' }}>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Templates</h1>

      {loading ? (
        <div style={{ color: '#94a3b8' }}>Loading...</div>
      ) : templates.length === 0 ? (
        <div style={{ color: '#94a3b8' }}>
          No templates yet. Save a pipeline as a template from the editor.
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 16 }}>
          {templates.map(tpl => (
            <div
              key={tpl.id}
              style={{
                backgroundColor: '#1e293b',
                borderRadius: 8,
                padding: 16,
                border: '1px solid #334155',
              }}
            >
              <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 8 }}>{tpl.name}</div>
              <div style={{ fontSize: 12, color: '#94a3b8', marginBottom: 12 }}>
                {tpl.description || 'No description'}
              </div>
              <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
                {tpl.definition.nodes?.length || 0} nodes · v{tpl.version}
              </div>
              {tpl.template_tags?.length > 0 && (
                <div style={{ marginBottom: 12, display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                  {tpl.template_tags.map(tag => (
                    <span key={tag} style={{
                      fontSize: 11, padding: '2px 6px', backgroundColor: '#334155',
                      borderRadius: 4, color: '#94a3b8',
                    }}>
                      {tag}
                    </span>
                  ))}
                </div>
              )}
              <button
                onClick={() => handleUseTemplate(tpl.id)}
                style={{
                  padding: '6px 16px',
                  backgroundColor: '#2563eb',
                  color: '#fff',
                  border: 'none',
                  borderRadius: 6,
                  cursor: 'pointer',
                  fontSize: 13,
                  fontWeight: 500,
                }}
              >
                Use Template
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
