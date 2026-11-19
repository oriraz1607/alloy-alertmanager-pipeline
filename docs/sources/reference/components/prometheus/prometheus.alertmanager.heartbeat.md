---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.heartbeat/
description: Observe the newest decoded heartbeat from each expected source network
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.heartbeat
---

# `prometheus.alertmanager.heartbeat`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.heartbeat` records the newest source evaluation timestamp that central Alloy receives in a firing `PipelineHeartbeat` alert for each configured network.
Connect it after [HTTP decoding](../prometheus.alertmanager.http_receive/) to measure the path through the network boundary.

## Usage

```alloy
prometheus.alertmanager.heartbeat "networks" {
  expected_networks = ["network-a", "network-b"]
}
```

## Arguments

| Name | Type | Description | Required |
| --- | --- | --- | --- |
| `expected_networks` | `list(string)` | Explicit, nonempty list of unique network names. Each name creates one freshness series. | yes |

The observer checks these fields of each decoded alert independently.

| Field | Required value |
| --- | --- |
| State | `firing` |
| Label `alertname` | `PipelineHeartbeat` |
| Label `source_network` | Exact configured network name |
| Annotation `source_timestamp_seconds` | Finite, positive Unix timestamp in seconds from the **source rule evaluation**. Decimal fractions and scientific notation are accepted. |

Keep the clocks on source Prometheus and central Alloy synchronized. The observer ignores timestamps more than 30 seconds ahead of its local clock.
It also ignores ordinary alerts, resolved probes, unexpected networks, and missing or malformed timestamps.
Ignored probes succeed and have no effect on metrics or on other alerts in the collection.
Each accepted timestamp can only advance its own network. A queued notification, duplicate, or writer refresh carrying old evidence cannot extend freshness beyond the time recorded at the source.

## Blocks

This component has no configurable blocks.

## Exported fields

| Name | Type | Description |
| --- | --- | --- |
| `receiver` | `AlertReceiver` | Terminal receiver that observes typed alerts. |

To continue delivering production alerts, connect the same listener to a second branch through [`prometheus.alertmanager.relabel`](../prometheus.alertmanager.relabel/).
Put a `drop` rule matching `alertname="PipelineHeartbeat"` on that branch before **every** NOC or ServiceNow receiver.
Filtering by name also excludes resolved probes and probes with invalid annotations or unknown network names.

## Component health

A configuration reload retains the latest source timestamp for networks still configured, removes departed networks, and initializes new ones to zero.
An invalid configuration leaves the last valid one active.
State lives in memory. A restarted replica exports zero for every expected network until it receives a valid probe.
Component health and scrape success alone give no evidence of traffic from a particular network.

## Debug information

Use the decoder's live debugging output to inspect the state, labels, and timestamp annotation delivered to the observer.

## Debug metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `prometheus_alertmanager_heartbeat_last_source_timestamp_seconds` | Gauge | `source_network` | Newest valid source timestamp received by this replica, or zero if it has never seen one for the network. |

Scrape each central replica. When any replica can receive the heartbeat, aggregate the newest timestamp with `max by (source_network)` over the replicas of the same observer.
For example, this query returns the age in seconds of the newest evidence for each network that has reported a heartbeat.
A network with a zero timestamp appears in the example's never-seen alert rule.

```promql
time() - max by (source_network) (
  prometheus_alertmanager_heartbeat_last_source_timestamp_seconds{
    job="central-alloy", component_id="prometheus.alertmanager.heartbeat.networks"
  } > 0
)
```

Scope the selector to your deployment's scrape job and component ID. Alert separately when its metric disappears from scraping.

## Example

Connect the central listener to the observer and the filtered incident branch.

```alloy
prometheus.alertmanager.http_receive "central" {
  decoder = prometheus.alertmanager.decode.boundary.decoder
  forward_to = [
    prometheus.alertmanager.heartbeat.networks.receiver,
    prometheus.alertmanager.relabel.incidents.receiver,
  ]
}

prometheus.alertmanager.relabel "incidents" {
  forward_to = [prometheus.alertmanager.write.central.receiver]

  rule {
    source_labels = ["alertname"]
    regex         = "PipelineHeartbeat"
    action        = "drop"
  }
}
```

The [complete heartbeat example](https://github.com/oriraz1607/alloy-alertmanager-pipeline/tree/main/examples/alertmanager-heartbeat) includes both Alloy configurations, the source rule, notification timings, and central alerting rules.
Arrival at this observer proves that a heartbeat with that source timestamp reached central Alloy and decoded successfully.
Verifying subsequent delivery or incident creation requires an observation at Alertmanager, Splunk, ServiceNow, or the chosen destination.
