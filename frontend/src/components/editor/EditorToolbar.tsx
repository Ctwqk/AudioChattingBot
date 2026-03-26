import { useState } from 'react';
import useEditorStore from '../../store/editorStore';
import apiClient from '../../api/client';
import { useNavigate } from 'react-router-dom';
import type { PipelineDefinition } from '../../api/types';
import useNodeTypes from '../../hooks/useNodeTypes';
import { applyNodeDefaults } from '../../utils/nodeConfig';
import BatchExecuteModal, { buildBatchExample, parseBatchItems } from '../batch/BatchExecuteModal';
import { buildPlannerBatchItems, hasPlannerNodes } from '../../utils/plannerBatch';

export default function EditorToolbar() {
  const { nodes, edges, pipelineId, pipelineName, isDirty, setPipeline, setPipelineName } = useEditorStore();
  const { nodeTypes } = useNodeTypes();
  const [saving, setSaving] = useState(false);
  const [validating, setValidating] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submittingBatch, setSubmittingBatch] = useState(false);
  const [savingTemplate, setSavingTemplate] = useState(false);
  const [batchOpen, setBatchOpen] = useState(false);
  const [batchInputText, setBatchInputText] = useState('');
  const [batchInputError, setBatchInputError] = useState<string | null>(null);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const navigate = useNavigate();

  const getDefinition = (): PipelineDefinition => {
    return {
      nodes: nodes.map(n => {
        const nodeTypeName = ((n.data.nodeType as string | undefined) || n.type || '');
        const mergedConfig = applyNodeDefaults(
          nodeTypeName,
          nodeTypes,
          (n.data.config as Record<string, unknown>) || {},
        );
        return {
          id: n.id,
          type: nodeTypeName,
          position: n.position,
          data: {
            label: (n.data.label as string) || '',
            config: mergedConfig,
            asset_id: (mergedConfig.asset_id as string) || undefined,
          },
        };
      }),
      edges: edges.map(e => ({
        id: e.id,
        source: e.source,
        target: e.target,
        sourceHandle: e.sourceHandle || 'output',
        targetHandle: e.targetHandle || 'input',
      })),
      viewport: { x: 0, y: 0, zoom: 1 },
    };
  };

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
      const definition = getDefinition();
      const targetPipelineId = isDirty || !pipelineId
        ? await ensureSaved()
        : pipelineId;
      if (!targetPipelineId) {
        return;
      }

      const payload = hasPlannerNodes(definition)
        ? (() => {
            const items = buildPlannerBatchItems(definition);
            if (items.length !== 1) {
              throw new Error(`Planner flow resolved to ${items.length} records. Use Batch Run instead of Run.`);
            }
            return { pipeline_id: targetPipelineId, inputs: items[0] };
          })()
        : { pipeline_id: targetPipelineId };

      const res = await apiClient.post('/jobs', payload);
      setMessage({ type: 'success', text: 'Job submitted!' });
      setTimeout(() => navigate(`/jobs/${res.data.id}`), 1000);
    } catch (err: unknown) {
      const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      const message = err instanceof Error ? err.message : null;
      setMessage({ type: 'error', text: detail || message || 'Submit failed' });
    } finally {
      setSubmitting(false);
    }
  };

  const openBatchDialog = async () => {
    const targetPipelineId = isDirty || !pipelineId
      ? await ensureSaved()
      : pipelineId;
    if (!targetPipelineId) {
      return;
    }

    setBatchInputError(null);
    try {
      setBatchInputText(JSON.stringify(buildBatchExample(getDefinition()), null, 2));
      setBatchOpen(true);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Failed to prepare batch input';
      setMessage({ type: 'error', text: message });
    }
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

      const inputs = parseBatchItems(batchInputText);

      const res = await apiClient.post('/jobs/batch', {
        pipeline_id: targetPipelineId,
        inputs,
      });

      const count = Array.isArray(res.data) ? res.data.length : inputs.length;
      setBatchOpen(false);
      setBatchInputError(null);
      setMessage({ type: 'success', text: `Submitted ${count} batch jobs` });
      navigate('/jobs');
    } catch (err: unknown) {
      if (err instanceof Error && !('response' in (err as object))) {
        setBatchInputError(err.message);
        return;
      }
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
          disabled={submittingBatch || saving}
          style={btnStyle('#0f766e')}
          title="Submit multiple jobs with parameter dictionaries"
        >
          {submittingBatch ? '...' : 'Batch Run'}
        </button>
        <button onClick={handleRun} disabled={submitting}
          style={btnStyle('#2563eb')}>
          {submitting ? '...' : '▶ Run'}
        </button>
      </div>

      {batchOpen && (
        <BatchExecuteModal
          title="Batch Run"
          description={hasPlannerNodes(getDefinition())
            ? 'Planner nodes generated these batch items from selected search results. You can inspect or edit the JSON before submission.'
            : 'Submit a JSON array of parameter dictionaries to the pipeline batch API.'}
          value={batchInputText}
          submitting={submittingBatch}
          error={batchInputError}
          onChange={setBatchInputText}
          onClose={() => {
            if (submittingBatch) return;
            setBatchOpen(false);
            setBatchInputError(null);
          }}
          onSubmit={() => void handleBatchSubmit()}
        />
      )}
    </>
  );
}

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
