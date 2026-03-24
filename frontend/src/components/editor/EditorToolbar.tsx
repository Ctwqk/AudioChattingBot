import { useEffect, useMemo, useState } from 'react';
import useEditorStore from '../../store/editorStore';
import apiClient from '../../api/client';
import { useNavigate } from 'react-router-dom';
import type { Asset } from '../../api/types';

interface SourceNodeInfo {
  id: string;
  label: string;
  currentAssetId: string;
}

interface BatchRow {
  id: string;
  assignments: Record<string, string>;
}

export default function EditorToolbar() {
  const { nodes, edges, pipelineId, pipelineName, isDirty, setPipeline, setPipelineName } = useEditorStore();
  const [saving, setSaving] = useState(false);
  const [validating, setValidating] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submittingBatch, setSubmittingBatch] = useState(false);
  const [savingTemplate, setSavingTemplate] = useState(false);
  const [batchOpen, setBatchOpen] = useState(false);
  const [assets, setAssets] = useState<Asset[]>([]);
  const [batchRows, setBatchRows] = useState<BatchRow[]>([]);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const navigate = useNavigate();

  const sourceNodes = useMemo<SourceNodeInfo[]>(() => (
    nodes
      .filter(n => (n.data.nodeType as string || n.type) === 'source')
      .map(n => ({
        id: n.id,
        label: (n.data.label as string) || n.id,
        currentAssetId: ((n.data.config as Record<string, unknown>)?.asset_id as string) || '',
      }))
  ), [nodes]);

  useEffect(() => {
    if (!batchOpen) return;
    apiClient.get('/assets')
      .then(res => setAssets(res.data.items || []))
      .catch(() => setAssets([]));
  }, [batchOpen]);

  const getDefinition = () => ({
    nodes: nodes.map(n => ({
      id: n.id,
      type: n.data.nodeType as string || n.type,
      position: n.position,
      data: {
        label: (n.data.label as string) || '',
        config: (n.data.config as Record<string, unknown>) || {},
        asset_id: (n.data.config as Record<string, unknown>)?.asset_id as string || null,
      },
    })),
    edges: edges.map(e => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle || 'output',
      targetHandle: e.targetHandle || 'input',
    })),
    viewport: { x: 0, y: 0, zoom: 1 },
  });

  const createBatchRow = (): BatchRow => ({
    id: `row_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`,
    assignments: Object.fromEntries(sourceNodes.map(source => [source.id, source.currentAssetId])),
  });

  const ensureSaved = async (): Promise<string | null> => {
    setSaving(true);
    setMessage(null);
    try {
      const definition = getDefinition();
      if (pipelineId) {
        const res = await apiClient.put(`/pipelines/${pipelineId}`, { name: pipelineName, definition });
        setPipeline(res.data.id, res.data.name, nodes, edges);
        setMessage({ type: 'success', text: 'Saved' });
        return res.data.id as string;
      } else {
        const res = await apiClient.post('/pipelines', { name: pipelineName, definition });
        setPipeline(res.data.id, res.data.name, nodes, edges);
        navigate(`/editor/${res.data.id}`, { replace: true });
        setMessage({ type: 'success', text: 'Saved' });
        return res.data.id as string;
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Save failed';
      setMessage({ type: 'error', text: msg });
      return null;
    } finally {
      setSaving(false);
    }
  };

  const handleSave = async () => {
    await ensureSaved();
  };

  const handleValidate = async () => {
    setValidating(true);
    setMessage(null);
    try {
      const definition = getDefinition();
      const res = await apiClient.post('/pipelines/validate', definition);
      if (res.data.valid) {
        setMessage({ type: 'success', text: 'Pipeline is valid!' });
      } else {
        const errors = res.data.errors.map((e: { message: string }) => e.message).join('; ');
        setMessage({ type: 'error', text: errors });
      }
    } catch {
      setMessage({ type: 'error', text: 'Validation request failed' });
    } finally {
      setValidating(false);
    }
  };

  const handleSaveAsTemplate = async () => {
    setSavingTemplate(true);
    setMessage(null);
    try {
      const definition = getDefinition();
      if (isDirty || !pipelineId) {
        const savedPipelineId = await ensureSaved();
        if (!savedPipelineId) {
          return;
        }
      }

      const targetId = useEditorStore.getState().pipelineId;
      if (targetId) {
        await apiClient.put(`/pipelines/${targetId}`, {
          name: pipelineName,
          definition,
          is_template: true,
        });
        setMessage({ type: 'success', text: 'Saved as template!' });
      } else {
        const res = await apiClient.post('/pipelines', {
          name: pipelineName,
          definition,
          is_template: true,
        });
        setPipeline(res.data.id, res.data.name, nodes, edges);
        navigate(`/editor/${res.data.id}`, { replace: true });
        setMessage({ type: 'success', text: 'Saved as template!' });
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Save as template failed';
      setMessage({ type: 'error', text: msg });
    } finally {
      setSavingTemplate(false);
    }
  };

  const handleRun = async () => {
    setSubmitting(true);
    setMessage(null);
    try {
      const targetPipelineId = isDirty || !pipelineId
        ? await ensureSaved()
        : pipelineId;
      if (!targetPipelineId) {
        return;
      }

      const res = await apiClient.post('/jobs', { pipeline_id: targetPipelineId });
      setMessage({ type: 'success', text: 'Job submitted!' });
      setTimeout(() => navigate(`/jobs/${res.data.id}`), 1000);
    } catch (err: unknown) {
      const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setMessage({ type: 'error', text: detail || 'Submit failed' });
    } finally {
      setSubmitting(false);
    }
  };

  const openBatchDialog = async () => {
    if (sourceNodes.length === 0) {
      setMessage({ type: 'error', text: 'Batch run currently supports pipelines with source nodes' });
      return;
    }

    const targetPipelineId = isDirty || !pipelineId
      ? await ensureSaved()
      : pipelineId;
    if (!targetPipelineId) {
      return;
    }

    setBatchRows([createBatchRow()]);
    setBatchOpen(true);
  };

  const updateBatchAssignment = (rowId: string, nodeId: string, assetId: string) => {
    setBatchRows(rows => rows.map(row => (
      row.id === rowId
        ? { ...row, assignments: { ...row.assignments, [nodeId]: assetId } }
        : row
    )));
  };

  const addBatchRow = () => {
    setBatchRows(rows => [...rows, createBatchRow()]);
  };

  const removeBatchRow = (rowId: string) => {
    setBatchRows(rows => rows.filter(row => row.id !== rowId));
  };

  const handleBatchSubmit = async () => {
    setSubmittingBatch(true);
    setMessage(null);

    try {
      const targetPipelineId = useEditorStore.getState().pipelineId;
      if (!targetPipelineId) {
        setMessage({ type: 'error', text: 'Save the pipeline first' });
        return;
      }

      const invalidRow = batchRows.find(row =>
        sourceNodes.some(source => !row.assignments[source.id]),
      );
      if (invalidRow) {
        setMessage({ type: 'error', text: 'Select an asset for every source node in each batch row' });
        return;
      }

      const inputs = batchRows.map(row => (
        Object.fromEntries(sourceNodes.map(source => [
          source.id,
          { asset_id: row.assignments[source.id] },
        ]))
      ));

      const res = await apiClient.post('/jobs/batch', {
        pipeline_id: targetPipelineId,
        inputs,
      });

      const count = Array.isArray(res.data) ? res.data.length : batchRows.length;
      setBatchOpen(false);
      setMessage({ type: 'success', text: `Submitted ${count} batch jobs` });
      navigate('/jobs');
    } catch (err: unknown) {
      const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setMessage({ type: 'error', text: detail || 'Batch submit failed' });
    } finally {
      setSubmittingBatch(false);
    }
  };

  return (
    <>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        padding: '8px 16px',
        backgroundColor: '#0f172a',
        borderBottom: '1px solid #1e293b',
      }}>
        <input
          value={pipelineName}
          onChange={e => setPipelineName(e.target.value)}
          style={{
            backgroundColor: 'transparent',
            border: 'none',
            borderBottom: '1px solid #334155',
            color: '#e2e8f0',
            fontSize: 16,
            fontWeight: 600,
            padding: '4px 0',
            outline: 'none',
            width: 200,
          }}
        />
        {isDirty && <span style={{ color: '#f59e0b', fontSize: 12 }}>unsaved</span>}

        <div style={{ flex: 1 }} />

        {message && (
          <span style={{
            fontSize: 12,
            color: message.type === 'success' ? '#22c55e' : '#ef4444',
            maxWidth: 400,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}>
            {message.text}
          </span>
        )}

        <button onClick={handleValidate} disabled={validating}
          style={btnStyle('#334155')}>
          {validating ? '...' : 'Validate'}
        </button>
        <button onClick={handleSave} disabled={saving}
          style={btnStyle('#334155')}>
          {saving ? '...' : 'Save'}
        </button>
        <button onClick={handleSaveAsTemplate} disabled={savingTemplate}
          style={btnStyle('#7c3aed')}>
          {savingTemplate ? '...' : 'Save as Template'}
        </button>
        <button
          onClick={openBatchDialog}
          disabled={submittingBatch || saving || sourceNodes.length === 0}
          style={btnStyle('#0f766e')}
          title={sourceNodes.length === 0 ? 'Batch run currently supports pipelines with source nodes' : 'Submit multiple jobs with different source assets'}
        >
          {submittingBatch ? '...' : 'Batch Run'}
        </button>
        <button onClick={handleRun} disabled={submitting}
          style={btnStyle('#2563eb')}>
          {submitting ? '...' : '▶ Run'}
        </button>
      </div>

      {batchOpen && (
        <div style={overlayStyle}>
          <div style={modalStyle}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <div>
                <div style={{ fontSize: 18, fontWeight: 700, color: '#e2e8f0' }}>Batch Run</div>
                <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 4 }}>
                  One row = one job. Set source assets per row, then submit all at once.
                </div>
              </div>
              <button
                onClick={() => setBatchOpen(false)}
                style={btnStyle('#334155')}
              >
                Close
              </button>
            </div>

            <div style={{ display: 'flex', gap: 8, marginBottom: 12, alignItems: 'center' }}>
              <button onClick={addBatchRow} style={btnStyle('#1d4ed8')}>
                Add Row
              </button>
              <span style={{ fontSize: 12, color: '#94a3b8' }}>
                {sourceNodes.length} source node{sourceNodes.length === 1 ? '' : 's'} detected
              </span>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 12, maxHeight: '55vh', overflowY: 'auto' }}>
              {batchRows.map((row, index) => (
                <div
                  key={row.id}
                  style={{
                    border: '1px solid #334155',
                    borderRadius: 8,
                    backgroundColor: '#111827',
                    padding: 12,
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                    <span style={{ color: '#e2e8f0', fontSize: 13, fontWeight: 600 }}>Job #{index + 1}</span>
                    <button
                      onClick={() => removeBatchRow(row.id)}
                      disabled={batchRows.length === 1}
                      style={btnStyle(batchRows.length === 1 ? '#3f3f46' : '#7f1d1d')}
                    >
                      Remove
                    </button>
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12 }}>
                    {sourceNodes.map(source => (
                      <label key={source.id} style={{ display: 'block' }}>
                        <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>{source.label}</div>
                        <select
                          value={row.assignments[source.id] || ''}
                          onChange={e => updateBatchAssignment(row.id, source.id, e.target.value)}
                          style={selectStyle}
                        >
                          <option value="">-- Select asset --</option>
                          {assets.map(asset => (
                            <option key={asset.id} value={asset.id}>
                              {asset.original_name}
                            </option>
                          ))}
                        </select>
                      </label>
                    ))}
                  </div>
                </div>
              ))}
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16 }}>
              <span style={{ fontSize: 12, color: '#94a3b8' }}>
                {batchRows.length} job{batchRows.length === 1 ? '' : 's'} ready
              </span>
              <div style={{ display: 'flex', gap: 8 }}>
                <button onClick={() => setBatchOpen(false)} style={btnStyle('#334155')}>
                  Cancel
                </button>
                <button onClick={handleBatchSubmit} disabled={submittingBatch} style={btnStyle('#0f766e')}>
                  {submittingBatch ? 'Submitting...' : 'Submit Batch'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

const overlayStyle: React.CSSProperties = {
  position: 'fixed',
  inset: 0,
  backgroundColor: 'rgba(2, 6, 23, 0.78)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  zIndex: 50,
  padding: 24,
};

const modalStyle: React.CSSProperties = {
  width: 'min(980px, 100%)',
  backgroundColor: '#0f172a',
  border: '1px solid #1e293b',
  borderRadius: 12,
  padding: 20,
  boxShadow: '0 20px 50px rgba(0,0,0,0.35)',
};

const selectStyle: React.CSSProperties = {
  width: '100%',
  padding: '8px 10px',
  backgroundColor: '#1e293b',
  border: '1px solid #334155',
  borderRadius: 6,
  color: '#e2e8f0',
  fontSize: 13,
  outline: 'none',
};

function btnStyle(bg: string): React.CSSProperties {
  return {
    padding: '6px 16px',
    backgroundColor: bg,
    color: '#e2e8f0',
    border: 'none',
    borderRadius: 6,
    cursor: 'pointer',
    fontSize: 13,
    fontWeight: 500,
  };
}
