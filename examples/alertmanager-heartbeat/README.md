# Monitor the alert path from each source network

A `PipelineHeartbeat` notification travels through the same edge listener, sender, network boundary and central decoder as a production alert. Central Alloy exposes the newest source evaluation time it has received for each expected network.

## Source network

1. Install [the source PrometheusRule](source-prometheus-rule.yaml) in a namespace selected by the source Prometheus. Add any metadata labels required by its rule selector. The rule evaluates `time()` every 30 seconds and writes the result, in Unix seconds, to the `source_timestamp_seconds` annotation. Its labels stay constant across evaluations.
2. Configure the source Prometheus to send alerts to the source Alertmanager. Merge [the source Alertmanager route](source-alertmanager.yaml) into that Alertmanager. Put the heartbeat child route before broader matchers. Keep existing production routes and receivers. The standalone example sends production alerts and heartbeats to the same edge webhook, with a shorter notification interval for heartbeats.
3. Run [edge.alloy](edge.alloy) in each source network. Set the `source_network` replacement to `network-a` on edge A and `network-b` on edge B. Each edge stamps all its outgoing alerts, normalizes the legacy cluster label, and forwards one JSON object per alert. Replace `edge-alloy:9095` in the Alertmanager receiver and `central-alloy:5002` in the edge HTTP sender with your reachable addresses.

Run this command on each edge with the custom Alloy binary.

```sh
alloy run --feature.community-components.enabled edge.alloy
```

The heartbeat path assumes that each edge accepts alerts from its own network and that central Alloy trusts the sender of the network label. Reserve the alert name `PipelineHeartbeat` for this probe. Synchronize the source and central clocks. A source clock lag counts against the freshness budget.

## Central Alloy and Prometheus

1. Run [central.alloy](central.alloy) on every central replica. Keep the same expected networks on each. Replace `http://alertmanager:9093` with your existing incident destination. You can also put existing typed HTTP senders in `relabel.incidents.forward_to`.
2. Point the edges at the shared central listener on `POST /alerts`. Its decoder reconstructs the full labels and annotations, source fingerprint, explicit alert state, timestamps and generator URL. The listener delivers to the observer first. A second branch drops *every* alert named `PipelineHeartbeat`, including resolved and malformed probes. All ordinary alerts go through that branch to the configured incident receivers. Connect every NOC or ServiceNow receiver behind the filter.
3. Scrape `/metrics` on **each central replica's** Alloy HTTP server under the job name `central-alloy` (or change both job selectors in the central rule). Scraping a load-balanced service as one target can miss the replica holding the latest sample. The command below exposes the HTTP server on port 12345 for your scraper. Restrict access to the management endpoint as you would for your existing Alloy deployment.
4. Install [the central PrometheusRule](central-prometheus-rule.yaml) in a namespace selected by the central Prometheus. The rule selects the observer component ID used in `central.alloy`. Scope the selectors to one central deployment if your Prometheus scrapes multiple deployments under the same job.

```sh
alloy run --feature.community-components.enabled --server.http.listen-addr=0.0.0.0:12345 central.alloy
```

Query `pipeline_heartbeat:last_source_timestamp_seconds` for the last source timestamp by network. The recording rule takes the maximum across replicas, since any replica can receive a probe. Zero means no currently scraped replica has seen a valid firing probe for that network. Each observer keeps its evidence in memory. Restarting all replicas clears it until a probe arrives. An old probe can repopulate the timestamp and still be stale.

| Setting | Example value | Effect |
| --- | --- | --- |
| Source rule evaluation | 30s | Updates the annotation on the continuously firing alert. Its original `StartsAt` stays fixed. |
| Source Alertmanager notification | 5s group wait, 30s group interval, 1m repeat | Sends the current alert periodically even when the group membership stays the same. Keep the Prometheus alert resend delay within this freshness budget (default 1m). |
| Central scrape and rule evaluation | Scrape every 30s or faster, evaluate every 30s | Makes accepted evidence visible on the next scrape and evaluation. |
| Per-network stale threshold | Source age over 5m | Queuing, retries, writer refreshes and repeated old notifications cannot advance the source evaluation timestamp. |
| Startup grace and sustained staleness | 3m pending | A continuously zero timestamp fires after 3m of observation. A previously healthy network fires after its evidence is over 5m old for 3m, rounded to rule evaluations. |
| Observer disappearance | 2m pending | Fires when all matching observer metrics disappear, including when a pod keeps exposing other metrics or all scrape targets vanish. |

`PipelineHeartbeatStale` carries the affected `source_network`. `PipelineHeartbeatObserverMissing` reports that central Prometheus has lost visibility into every selected observer. A surviving observed replica continues to provide evidence for the deployment. A successful heartbeat proves receipt and decoding at central Alloy. To confirm ingestion by Splunk, ServiceNow, or the downstream Alertmanager, observe that destination separately.

## Verify the configuration and alert rules

Run these commands in this directory. The script extracts the rule groups from the same Kubernetes manifests you deploy.

```sh
alloy validate --feature.community-components.enabled edge.alloy
alloy validate --feature.community-components.enabled central.alloy
amtool check-config source-alertmanager.yaml
./test-rules.sh
```
