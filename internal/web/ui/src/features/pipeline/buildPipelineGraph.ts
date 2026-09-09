import {
  deduplicateSignals,
  hasInferredSignals,
  inferPipelineStage,
  inferSignalsFromComponentName,
  type PipelineEdge,
  type PipelineGraphData,
  type PipelineNode,
  type PipelineNodeHealth,
  PipelineStage,
  SignalKind,
} from '@grafana/alloy-pipeline-graph';

import { componentDocsUrl } from '../../utils/docs';
import type { ComponentHealthState, ComponentInfo } from '../component/types';

const ALERTMANAGER_COMPONENT_PREFIX = 'prometheus.alertmanager.';

function isAlertmanagerComponent(name: string | undefined): boolean {
  return name?.startsWith(ALERTMANAGER_COMPONENT_PREFIX) ?? false;
}

// The graph library infers metrics from the prometheus namespace. Alertmanager
// pipeline components exchange typed alerts, for which Other is appropriate.
function componentSignals(name: string): SignalKind[] {
  if (isAlertmanagerComponent(name)) {
    return [SignalKind.Other];
  }
  return inferSignalsFromComponentName(name);
}

function componentStage(name: string): PipelineStage {
  switch (name) {
    case 'prometheus.alertmanager.receive':
    case 'prometheus.alertmanager.http_receive':
      return PipelineStage.Collect;
    case 'prometheus.alertmanager.transform':
    case 'prometheus.alertmanager.decode':
      return PipelineStage.Process;
    case 'prometheus.alertmanager.http':
    case 'prometheus.alertmanager.write':
      return PipelineStage.Send;
    default:
      return inferPipelineStage(name);
  }
}

const INNER_NODE_ID_SEPARATOR = '/';

function innerNodeId(containerId: string, innerLocalId: string): string {
  return `${containerId}${INNER_NODE_ID_SEPARATOR}${innerLocalId}`;
}

function mapHealth(state: ComponentHealthState): PipelineNodeHealth {
  return state as PipelineNodeHealth;
}

function buildComponentNode(component: ComponentInfo, overrides: Partial<PipelineNode> = {}): PipelineNode {
  return {
    id: component.localID,
    componentName: component.name,
    label: component.label ?? null,
    stage: componentStage(component.name),
    signals: deduplicateSignals(componentSignals(component.name)),
    docsUrl: componentDocsUrl(component.name),
    health: mapHealth(component.health.state),
    meta: {
      moduleID: component.moduleID,
      localID: component.localID,
    },
    ...overrides,
  };
}

function inferDataFlowSignals(sourceName: string, targetName: string | undefined): SignalKind[] {
  if (isAlertmanagerComponent(sourceName) || isAlertmanagerComponent(targetName)) {
    return [SignalKind.Other];
  }
  const sourceSignals = componentSignals(sourceName);
  if (hasInferredSignals(sourceSignals)) {
    return deduplicateSignals(sourceSignals);
  }
  if (targetName) {
    return deduplicateSignals(componentSignals(targetName));
  }
  return [SignalKind.Other];
}

/**
 * Projects dependency edges for the Alertmanager schema boundary into the
 * order in which alert data is processed.
 *
 * Transformer and decoder values are configuration dependencies. The runtime
 * therefore reports transformer -> http and decoder -> http_receive edges,
 * even though the processing order is receive -> transform -> http and
 * http_receive -> decode -> downstream receiver.
 */
function projectAlertmanagerDataFlow(components: ComponentInfo[]): Map<string, Set<string>> {
  const byID = new Map(components.map((component) => [component.localID, component]));
  const targets = new Map(components.map((component) => [component.localID, new Set(component.dataFlowEdgesTo)]));

  for (const sender of components.filter((component) => component.name === 'prometheus.alertmanager.http')) {
    const transformerID = sender.referencesTo.find((id) => byID.get(id)?.name === 'prometheus.alertmanager.transform');
    if (!transformerID) continue;

    for (const upstream of components) {
      if (upstream.localID === transformerID) continue;
      const upstreamTargets = targets.get(upstream.localID);
      if (upstreamTargets?.delete(sender.localID)) upstreamTargets.add(transformerID);
    }
    targets.get(transformerID)?.add(sender.localID);
  }

  for (const receiver of components.filter((component) => component.name === 'prometheus.alertmanager.http_receive')) {
    const decoderID = receiver.referencesTo.find((id) => byID.get(id)?.name === 'prometheus.alertmanager.decode');
    if (!decoderID) continue;

    const receiverTargets = targets.get(receiver.localID) ?? new Set<string>();
    const downstreamIDs = [...receiverTargets].filter((id) => id !== decoderID);
    receiverTargets.clear();
    receiverTargets.add(decoderID);
    targets.set(receiver.localID, receiverTargets);

    const decoderTargets = targets.get(decoderID) ?? new Set<string>();
    decoderTargets.delete(receiver.localID);
    for (const downstreamID of downstreamIDs) decoderTargets.add(downstreamID);
    targets.set(decoderID, decoderTargets);
  }

  return targets;
}

function addEdges(
  edges: PipelineEdge[],
  edgeIdSet: Set<string>,
  components: ComponentInfo[],
  idForComponent: (localID: string) => string,
  nameByLocalID: Map<string, string>
): void {
  const targetsByLocalID = projectAlertmanagerDataFlow(components);
  for (const component of components) {
    const sourceID = idForComponent(component.localID);
    for (const targetLocalID of targetsByLocalID.get(component.localID) ?? []) {
      const targetID = idForComponent(targetLocalID);
      const signals = inferDataFlowSignals(component.name, nameByLocalID.get(targetLocalID));

      let edgeId = `${sourceID}->${targetID}`;
      let counter = 0;
      while (edgeIdSet.has(edgeId)) {
        counter += 1;
        edgeId = `${sourceID}->${targetID}#${counter}`;
      }
      edgeIdSet.add(edgeId);

      edges.push({ id: edgeId, source: sourceID, target: targetID, signals });
    }
  }
}

/**
 * Builds the shared `PipelineGraphData` from Alloy's runtime component API.
 *
 * Custom components (module loaders) become expandable containers when
 * `moduleInternals` includes their `createdModuleIDs` entries.
 */
export function buildPipelineGraph(
  components: ComponentInfo[],
  moduleInternals: Map<string, ComponentInfo[]> = new Map()
): PipelineGraphData {
  const nameByLocalID = new Map<string, string>();
  for (const component of components) {
    nameByLocalID.set(component.localID, component.name);
  }

  const nodes: PipelineNode[] = [];
  const edges: PipelineEdge[] = [];
  const edgeIdSet = new Set<string>();

  for (const component of components) {
    const isContainer = (component.createdModuleIDs?.length ?? 0) > 0;

    nodes.push(
      buildComponentNode(component, {
        kind: isContainer ? 'customComponentContainer' : 'component',
      })
    );

    if (!isContainer) {
      continue;
    }

    const innerComponents: ComponentInfo[] = [];
    for (const moduleId of component.createdModuleIDs ?? []) {
      const moduleComponents = moduleInternals.get(moduleId) ?? [];
      innerComponents.push(...moduleComponents);
      for (const inner of moduleComponents) {
        nameByLocalID.set(inner.localID, inner.name);
      }
    }

    for (const inner of innerComponents) {
      nodes.push(
        buildComponentNode(inner, {
          id: innerNodeId(component.localID, inner.localID),
          parentId: component.localID,
          kind: 'component',
          meta: {
            moduleID: inner.moduleID,
            localID: inner.localID,
            containerID: component.localID,
          },
        })
      );
    }

    addEdges(edges, edgeIdSet, innerComponents, (localID) => innerNodeId(component.localID, localID), nameByLocalID);
  }

  addEdges(edges, edgeIdSet, components, (localID) => localID, nameByLocalID);

  return { nodes, edges };
}
