import { PipelineStage, SignalKind } from '@grafana/alloy-pipeline-graph';
import { describe, expect, it } from 'vitest';

import { ComponentHealthState, type ComponentInfo } from '../component/types';
import { buildPipelineGraph } from './buildPipelineGraph';

function component(name: string, label: string, targets: string[] = []): ComponentInfo {
  return {
    name,
    label,
    localID: `${name}.${label}`,
    moduleID: '',
    health: { state: ComponentHealthState.HEALTHY },
    referencedBy: [],
    referencesTo: [],
    dataFlowEdgesTo: targets,
    liveDebuggingEnabled: false,
  };
}

describe('Alertmanager graph', () => {
  it('preserves receive to write fanout and classifies alerts as Other', () => {
    const a = component('prometheus.alertmanager.write', 'a');
    const b = component('prometheus.alertmanager.write', 'b');
    const source = component('prometheus.alertmanager.receive', 'input', [a.localID, b.localID]);
    const graph = buildPipelineGraph([source, a, b]);
    expect(graph.edges).toHaveLength(2);
    expect(graph.edges.map((edge) => edge.target)).toEqual([a.localID, b.localID]);
    for (const edge of graph.edges) {
      expect(edge.source).toBe(source.localID);
      expect(edge.signals).toEqual([SignalKind.Other]);
    }
    for (const node of graph.nodes) expect(node.signals).toEqual([SignalKind.Other]);
  });

  it('shows the sender boundary in processing order and classifies every node as Other', () => {
    const transformer = component('prometheus.alertmanager.transform', 'boundary', ['prometheus.alertmanager.http.remote']);
    const sender = component('prometheus.alertmanager.http', 'remote');
    sender.referencesTo = [transformer.localID];
    const receive = component('prometheus.alertmanager.receive', 'input', [sender.localID]);
    receive.referencesTo = [sender.localID];

    const graph = buildPipelineGraph([transformer, sender, receive]);
    expect(graph.edges.map(({ source, target }) => ({ source, target }))).toEqual([
      { source: transformer.localID, target: sender.localID },
      { source: receive.localID, target: transformer.localID },
    ]);
    for (const edge of graph.edges) expect(edge.signals).toEqual([SignalKind.Other]);
    for (const node of graph.nodes) expect(node.signals).toEqual([SignalKind.Other]);
    expect(graph.nodes.map(({ id, stage }) => ({ id, stage }))).toEqual([
      { id: transformer.localID, stage: PipelineStage.Process },
      { id: sender.localID, stage: PipelineStage.Send },
      { id: receive.localID, stage: PipelineStage.Collect },
    ]);
  });

  it('shows the receiver boundary in processing order', () => {
    const write = component('prometheus.alertmanager.write', 'destination');
    const decoder = component('prometheus.alertmanager.decode', 'boundary', [
      'prometheus.alertmanager.http_receive.incoming',
    ]);
    const receive = component('prometheus.alertmanager.http_receive', 'incoming', [write.localID]);
    receive.referencesTo = [decoder.localID, write.localID];

    const graph = buildPipelineGraph([write, decoder, receive]);
    expect(graph.edges.map(({ source, target }) => ({ source, target }))).toEqual([
      { source: decoder.localID, target: write.localID },
      { source: receive.localID, target: decoder.localID },
    ]);
    for (const edge of graph.edges) expect(edge.signals).toEqual([SignalKind.Other]);
    for (const node of graph.nodes) expect(node.signals).toEqual([SignalKind.Other]);
    expect(graph.nodes.map(({ id, stage }) => ({ id, stage }))).toEqual([
      { id: write.localID, stage: PipelineStage.Send },
      { id: decoder.localID, stage: PipelineStage.Process },
      { id: receive.localID, stage: PipelineStage.Collect },
    ]);
  });
});
