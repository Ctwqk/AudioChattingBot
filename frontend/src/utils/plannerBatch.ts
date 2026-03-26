import type { PipelineDefinition, PipelineNode } from '../api/types';

export type PlannerSearchResult = {
  id: string;
  title: string;
  url: string;
  thumbnail?: string | null;
  duration?: number | null;
  channel?: string | null;
};

const SEARCH_NODE_TYPE = 'youtube_search';
const ZIP_NODE_TYPE = 'zip_records';
const URL_DOWNLOAD_NODE_TYPE = 'url_download';

function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function asSearchResults(value: unknown): PlannerSearchResult[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asObject(item))
    .filter(item => typeof item.id === 'string' && typeof item.url === 'string')
    .map(item => ({
      id: String(item.id),
      title: String(item.title || item.url),
      url: String(item.url),
      thumbnail: typeof item.thumbnail === 'string' ? item.thumbnail : null,
      duration: typeof item.duration === 'number' ? item.duration : null,
      channel: typeof item.channel === 'string' ? item.channel : null,
    }));
}

function getConfig(node: PipelineNode): Record<string, unknown> {
  return asObject(node.data?.config);
}

function getChannelCount(node: PipelineNode): number {
  const raw = getConfig(node).channel_count;
  const value = typeof raw === 'number' ? raw : Number(raw || 2);
  if (!Number.isFinite(value)) return 2;
  return Math.max(1, Math.trunc(value));
}

function getRecordLimit(node: PipelineNode): number {
  const raw = getConfig(node).record_limit;
  const value = typeof raw === 'number' ? raw : Number(raw || 0);
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.trunc(value));
}

function getSelectedSearchResults(node: PipelineNode): PlannerSearchResult[] {
  const config = getConfig(node);
  const results = asSearchResults(config.search_results);
  const selectedIds = Array.isArray(config.selected_video_ids)
    ? config.selected_video_ids.map(String)
    : [];

  if (selectedIds.length === 0) {
    return results;
  }

  const resultById = new Map(results.map(result => [result.id, result]));
  return selectedIds
    .map(id => resultById.get(id))
    .filter((item): item is PlannerSearchResult => Boolean(item));
}

function mergeItemSets(itemSets: Array<Array<Record<string, unknown>>>): Array<Record<string, unknown>> {
  if (itemSets.length === 0) {
    return [];
  }
  if (itemSets.length === 1) {
    return itemSets[0];
  }

  const count = Math.min(...itemSets.map(set => set.length));
  const merged: Array<Record<string, unknown>> = [];
  for (let index = 0; index < count; index += 1) {
    const item: Record<string, unknown> = {};
    for (const set of itemSets) {
      for (const [key, value] of Object.entries(set[index] || {})) {
        if (key in item) {
          throw new Error(`Planner outputs collided on '${key}'. Use distinct URL Download targets.`);
        }
        item[key] = value;
      }
    }
    merged.push(item);
  }
  return merged;
}

export function hasPlannerNodes(definition: PipelineDefinition): boolean {
  return definition.nodes.some(node => node.type === SEARCH_NODE_TYPE || node.type === ZIP_NODE_TYPE);
}

export function buildPlannerBatchItems(definition: PipelineDefinition): Array<Record<string, unknown>> {
  const nodesById = new Map(definition.nodes.map(node => [node.id, node]));
  const zipNodes = definition.nodes.filter(node => node.type === ZIP_NODE_TYPE);

  if (zipNodes.length === 0) {
    throw new Error('Planner flow requires at least one Zip Records node.');
  }

  const itemSets = zipNodes.map(zipNode => {
    const channelCount = getChannelCount(zipNode);
    const recordLimit = getRecordLimit(zipNode);
    const channels: Array<PlannerSearchResult[]> = [];
    const downloadTargets: string[] = [];

    for (let index = 1; index <= channelCount; index += 1) {
      const inputHandle = `input_${index}`;
      const outputHandle = `output_${index}`;

      const inputEdge = definition.edges.find(edge => edge.target === zipNode.id && edge.targetHandle === inputHandle);
      if (!inputEdge) {
        throw new Error(`${zipNode.data.label || 'Zip Records'} is missing ${inputHandle}.`);
      }
      const sourceNode = nodesById.get(inputEdge.source);
      if (!sourceNode || sourceNode.type !== SEARCH_NODE_TYPE) {
        throw new Error(`${zipNode.data.label || 'Zip Records'} ${inputHandle} must come from a YouTube Search node.`);
      }

      const selected = getSelectedSearchResults(sourceNode);
      if (selected.length === 0) {
        throw new Error(`${sourceNode.data.label || 'YouTube Search'} has no selected videos.`);
      }
      channels.push(selected);

      const outputEdge = definition.edges.find(edge => edge.source === zipNode.id && edge.sourceHandle === outputHandle);
      if (!outputEdge) {
        throw new Error(`${zipNode.data.label || 'Zip Records'} is missing ${outputHandle}.`);
      }
      const targetNode = nodesById.get(outputEdge.target);
      if (!targetNode || targetNode.type !== URL_DOWNLOAD_NODE_TYPE) {
        throw new Error(`${zipNode.data.label || 'Zip Records'} ${outputHandle} must connect to a URL Download node.`);
      }
      downloadTargets.push(targetNode.id);
    }

    let count = Math.min(...channels.map(channel => channel.length));
    if (recordLimit > 0) {
      count = Math.min(count, recordLimit);
    }
    if (count <= 0) {
      throw new Error(`${zipNode.data.label || 'Zip Records'} produced no records.`);
    }

    return Array.from({ length: count }, (_, rowIndex) => {
      const item: Record<string, unknown> = {};
      channels.forEach((channel, channelIndex) => {
        item[`${downloadTargets[channelIndex]}.url`] = channel[rowIndex].url;
      });
      return item;
    });
  });

  return mergeItemSets(itemSets);
}

export function buildBatchItems(definition: PipelineDefinition): Array<Record<string, unknown>> {
  if (hasPlannerNodes(definition)) {
    return buildPlannerBatchItems(definition);
  }

  const example: Record<string, unknown> = {};

  for (const node of definition.nodes || []) {
    const config = node.data?.config || {};
    if (node.type === 'source') {
      example[`${node.id}.asset_id`] = (config.asset_id as string) || '';
      continue;
    }

    for (const [key, value] of Object.entries(config)) {
      if (value === '' || value === null || value === undefined) {
        continue;
      }
      example[`${node.id}.${key}`] = value;
    }
  }

  return [example];
}

export function getZipConnectionSummary(definition: PipelineDefinition, zipNodeId: string): Array<{
  channel: number;
  searchLabel: string | null;
  selectedCount: number;
  downloadLabel: string | null;
}> {
  const nodesById = new Map(definition.nodes.map(node => [node.id, node]));
  const zipNode = nodesById.get(zipNodeId);
  if (!zipNode || zipNode.type !== ZIP_NODE_TYPE) {
    return [];
  }

  const channelCount = getChannelCount(zipNode);
  const summary = [];
  for (let index = 1; index <= channelCount; index += 1) {
    const inputEdge = definition.edges.find(edge => edge.target === zipNode.id && edge.targetHandle === `input_${index}`);
    const outputEdge = definition.edges.find(edge => edge.source === zipNode.id && edge.sourceHandle === `output_${index}`);
    const searchNode = inputEdge ? nodesById.get(inputEdge.source) : undefined;
    const downloadNode = outputEdge ? nodesById.get(outputEdge.target) : undefined;
    summary.push({
      channel: index,
      searchLabel: searchNode?.data.label || null,
      selectedCount: searchNode ? getSelectedSearchResults(searchNode).length : 0,
      downloadLabel: downloadNode?.data.label || null,
    });
  }
  return summary;
}
