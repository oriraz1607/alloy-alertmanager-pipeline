---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.write/
description: Receive typed Alloy alerts and write them to the Alertmanager API
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.write
---

# `prometheus.alertmanager.write`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.write` receives typed alerts from other Alloy components and sends them to an Alertmanager using `POST /api/v2/alerts`.

You can specify multiple `prometheus.alertmanager.write` components by giving them different labels.

## Usage

```alloy
prometheus.alertmanager.write "<LABEL>" {
  endpoint {
    url = "<ALERTMANAGER_URL>"
  }
}
```

## Arguments

You can use the following arguments with `prometheus.alertmanager.write`:

| Name                    | Type       | Description                                                               | Default | Required |
| ----------------------- | ---------- | ------------------------------------------------------------------------- | ------- | -------- |
| `firing_alert_duration` | `duration` | Future lifetime assigned to each firing alert sent to Alertmanager.       | `"5m"`  | no       |
| `firing_alert_timeout`  | `duration` | Time since the last firing input after which refresh state expires.       | `"5h"`  | no       |
| `refresh_interval`      | `duration` | How often retained firing and resolved alerts are resent.                 | `"1m"`  | no       |
| `resolved_retention`    | `duration` | How long an explicitly resolved alert is retained for repeated delivery.  | `"5m"`  | no       |

`firing_alert_duration` must be greater than `refresh_interval`.
This ensures that a successfully refreshed firing alert doesn't expire before the next scheduled refresh.

`firing_alert_timeout` must be greater than `refresh_interval`.
Set it to a value greater than the source Alertmanager's `repeat_interval`.

## Blocks

You can use the following blocks with `prometheus.alertmanager.write`:

{{< docs/alloy-config >}}

| Block                                              | Description                                             | Required |
| -------------------------------------------------- | ------------------------------------------------------- | -------- |
| [`endpoint`][endpoint]                             | Configure the destination Alertmanager.                 | yes      |
| `endpoint` > [`authorization`][authorization]      | Configure generic authorization to the endpoint.        | no       |
| `endpoint` > [`basic_auth`][basic_auth]            | Configure basic authentication to the endpoint.         | no       |
| `endpoint` > [`oauth2`][oauth2]                    | Configure OAuth 2.0 authentication to the endpoint.     | no       |
| `endpoint` > `oauth2` > [`tls_config`][tls_config] | Configure TLS for the OAuth 2.0 token endpoint.          | no       |
| `endpoint` > [`tls_config`][tls_config]            | Configure TLS for the destination Alertmanager.         | no       |
| [`queue_config`][queue_config]                     | Configure the bounded alert queue.            | no       |

[authorization]: #authorization
[basic_auth]: #basic_auth
[endpoint]: #endpoint
[oauth2]: #oauth2
[queue_config]: #queue_config
[tls_config]: #tls_config

{{< /docs/alloy-config >}}

### `endpoint`

The required `endpoint` block configures the destination Alertmanager, batching, retries, and the HTTP client.

You can use the following arguments with `endpoint`:

| Name                     | Type                | Description                                                                                      | Default   | Required |
| ------------------------ | ------------------- | ------------------------------------------------------------------------------------------------ | --------- | -------- |
| `url`                    | `string`            | Destination Alertmanager URL.                                                                    |           | yes      |
| `batch_size`             | `int`               | Maximum number of alerts in one API request.                                                     | `1`       | no       |
| `batch_wait`             | `duration`          | Maximum time to wait for a partially filled batch.                                               | `"1s"`    | no       |
| `bearer_token`           | `secret`            | Bearer token to authenticate with.                                                               |           | no       |
| `bearer_token_file`      | `string`            | File containing a bearer token to authenticate with.                                             |           | no       |
| `enable_http2`           | `bool`              | Whether HTTP/2 is supported for requests.                                                        | `true`    | no       |
| `follow_redirects`       | `bool`              | Whether to follow redirects returned by the destination.                                         | `true`    | no       |
| `http_headers`           | `map(list(secret))` | Custom HTTP headers to send with each request.                                                    |           | no       |
| `max_backoff_period`     | `duration`          | Maximum backoff time between retries.                                                            | `"5m"`    | no       |
| `max_retries`            | `int`               | Maximum retries after the first attempt. A value of `0` retries until cancellation.              | `10`      | no       |
| `min_backoff_period`     | `duration`          | Initial backoff time between retries.                                                            | `"500ms"` | no       |
| `no_proxy`               | `string`            | Comma-separated addresses and domain names to exclude from proxying.                             |           | no       |
| `proxy_connect_header`   | `map(list(secret))` | Headers to send to proxies during `CONNECT` requests.                                             |           | no       |
| `proxy_from_environment` | `bool`              | Whether to use the proxy URL indicated by environment variables.                                 | `false`   | no       |
| `proxy_url`              | `string`            | HTTP proxy through which to send requests.                                                       |           | no       |
| `retry_on_http_429`      | `bool`              | Whether an HTTP `429` response is retryable.                                                     | `true`    | no       |
| `timeout`                | `duration`          | Maximum duration of one request to the destination Alertmanager.                                 | `"10s"`   | no       |

If `url` has no path, the component appends `/api/v2/alerts`.
If the URL has a path, the component uses that path without changing it.

Setting `batch_size` to `1` guarantees that every request body contains exactly one alert.
For larger values, the component sends a batch when it reaches `batch_size` or `batch_wait` elapses.

Connection failures, timeouts, HTTP `5xx` responses, and optionally HTTP `429` responses are retried with exponential backoff.
Other HTTP `4xx` responses aren't retried.
With the default in-memory queue, exhausting `max_retries` abandons the batch, but retained alert state remains eligible for scheduled refresh.
With `queue_config.persistent = true`, pending batches retry retryable failures until delivery or shutdown, regardless of `max_retries`.
Periodic refresh requests still use `max_retries` so they don't indefinitely block newly queued alerts.

At most one of the following authentication options can be provided:

* An [`authorization`](#authorization) block.
* A [`basic_auth`](#basic_auth) block.
* The `bearer_token_file` argument.
* The `bearer_token` argument.
* An [`oauth2`](#oauth2) block.

{{< docs/shared lookup="reference/components/http-client-proxy-config-description.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `authorization`

{{< docs/shared lookup="reference/components/authorization-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `basic_auth`

{{< docs/shared lookup="reference/components/basic-auth-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `oauth2`

{{< docs/shared lookup="reference/components/oauth2-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `queue_config`

The optional `queue_config` block configures the bounded alert queue.

| Name                | Type       | Description                                                            | Default | Required |
| ------------------- | ---------- | ---------------------------------------------------------------------- | ------- | -------- |
| `block_on_overflow` | `bool`     | Whether producers wait for capacity when the queue is full.            | `true`  | no       |
| `capacity`          | `int`      | Maximum queued alerts; persistent mode also counts in-flight alerts.                               | `1000`  | no       |
| `drain_timeout`     | `duration` | Maximum time to deliver queued alerts during component shutdown.       | `"15s"` | no       |
| `persistent` | `bool` | Persist pending alerts and refresh state before acknowledging acceptance. | `false` | no |
| `directory` | `string` | Queue directory; defaults to `queue` under the component data path. | `""` | no |
| `max_disk_bytes` | `int` | Combined snapshot and temporary-file byte limit. | `67108864` | no |

The complete collection from one upstream send is admitted atomically.
If the collection is larger than `capacity`, it is rejected.
When `block_on_overflow` is `true`, the producer waits for enough capacity or for its context to expire.
For an HTTP producer such as `prometheus.alertmanager.receive`, this applies backpressure to the webhook request.
When `block_on_overflow` is `false`, a full queue rejects the collection immediately.

By default, the queue is in memory and alerts left after `drain_timeout` are dropped.
Set `persistent = true` to preserve pending alerts across shutdown and restart.
`directory` requires persistence and must be exclusive to one component.
Changing persistence mode, directory, or disk limit requires a component restart.
Changing the endpoint on reload sends pending alerts to the new endpoint.
Reducing `capacity` below the restored queue size preserves existing alerts and backpressures new input until space becomes available.

### Durable delivery

Persistent mode commits a checksummed snapshot before reporting successful acceptance.
The snapshot includes pending and in-flight alerts, plus retained firing and resolved state with their original timestamps.
The component restores pending alerts in order and applies the configured batching policy.
Pending alerts don't expire merely because the destination is unavailable.
A shutdown drains for at most `drain_timeout` and leaves undelivered alerts on disk.

Delivery is at least once for accepted alerts while failures remain retryable and the storage survives.
An HTTP success followed by a crash before local acknowledgement can resend an alert.
A lost webhook response or a partial fan-out failure can also cause upstream retries and duplicates.
Fan-out admission isn't transactional across writers.
Non-retryable responses, including other HTTP `4xx` responses and `429` when retries are disabled, remain terminal: the component logs and counts the failed batch, then removes it from the queue.
TLS verification failures remain terminal as well.
Retained refresh state follows its existing policy, including after a terminal delivery failure.

Each snapshot is limited to half of `max_disk_bytes`, reserving the other half for atomic replacement.
The limit includes encoded pending alerts and refresh state; the empty lock file and filesystem metadata are additional overhead.
`max_disk_bytes` must be at least `4096`.
Disk-limit and storage failures reject new collections immediately, including when `block_on_overflow` is enabled.
Alert-count exhaustion follows `block_on_overflow`.
Successful delivery removes records through snapshot replacement, so the queue doesn't accumulate WAL segments.
Snapshot writes are proportional to retained state; this design targets bounded alert queues rather than bulk telemetry ingestion.

The component writes and syncs a temporary snapshot, renames it, then syncs the directory.
An interrupted temporary write leaves the previous committed snapshot intact.
A corrupt, truncated, unsupported, or oversized committed snapshot prevents startup instead of silently discarding accepted alerts.
Preserve the files and restore a valid backup or the previous disk limit before restarting.
A directory-sync failure rejects further persistence operations until restart because the commit outcome is uncertain.

Keep the component label and data path stable across restarts.
Use persistent storage for Alloy's `--storage.path` or an explicit queue directory to survive pod replacement.
Storage must support file locking, atomic rename, and file and directory synchronization.
Deleting the storage or losing its underlying volume loses the delivery guarantee.

### `tls_config`

The `tls_config` block configures TLS for requests to the destination Alertmanager.
TLS certificate verification remains enabled unless you explicitly set `insecure_skip_verify` to `true`.

{{< docs/shared lookup="reference/components/tls-config-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

## Exported fields

The following fields are exported and can be referenced by other components:

| Name       | Type            | Description                                             |
| ---------- | --------------- | ------------------------------------------------------- |
| `receiver` | `AlertReceiver` | A value other components use to send typed alert data.  |

## Component health

`prometheus.alertmanager.write` reports as healthy while it can accept alerts and its most recent terminal delivery attempt succeeded.
It reports as unhealthy when the queue rejects input or a destination request exhausts its retry policy.
A later successful delivery restores healthy status.

## Debug information

`prometheus.alertmanager.write` supports [live debugging](../../../../troubleshoot/debug/) in the standard Alloy UI.
Enable the [`livedebugging` block](../../../config-blocks/livedebugging/) with `enabled = true`, open the component page, and start live debugging.

The live debugging stream shows each typed alert accepted by the queue as `[IN]`.
`[OUT] ALERTMANAGER REQUEST` shows the final JSON array before delivery to `/api/v2/alerts`, including the writer's existing timestamp adjustments.
Retries and refresh deliveries can produce repeated output entries.

Values are formatted only when a live debugging consumer requests their text.
Alloy controls subscriptions, sampling, and stream delivery.

## Debug metrics

`prometheus.alertmanager.write` exposes the following metrics:

* `prometheus_alertmanager_write_dropped_alerts_total` (counter): Alerts rejected or abandoned, partitioned by `reason`.
* `prometheus_alertmanager_write_expired_firing_alerts_total` (counter): Firing alerts whose refresh state expired without a resolved update.
* `prometheus_alertmanager_write_http_request_duration_seconds` (histogram): Duration of destination requests.
* `prometheus_alertmanager_write_http_request_failures_total` (counter): Failed destination requests, partitioned by `reason`.
* `prometheus_alertmanager_write_http_requests_total` (counter): Destination HTTP requests.
* `prometheus_alertmanager_write_queue_length` (gauge): Alerts currently in the in-memory queue.
* `prometheus_alertmanager_write_received_alerts_total` (counter): Alerts accepted from upstream components.
* `prometheus_alertmanager_write_retries_total` (counter): Destination request retries.
* `prometheus_alertmanager_write_sent_alerts_total` (counter): Alerts accepted by the destination Alertmanager.
* `prometheus_alertmanager_write_tracked_firing_alerts` (gauge): Firing alerts retained for refresh.
* `prometheus_alertmanager_write_tracked_resolved_alerts` (gauge): Resolved alerts retained for repeated delivery.

The `reason` label for request failures can be `connection`, `tls`, `timeout`, `status_4xx`, `status_5xx`, or `status_other`.
The `reason` label for dropped alerts can be `invalid`, `queue_full`, `persistence`, `stopped`, `delivery_failed`, or `shutdown`.

## Examples

### Send one alert per request

The following example creates the complete receive and write pipeline and emits exactly one alert in each Alertmanager API request:

```alloy
prometheus.alertmanager.receive "edge" {
  http {
    listen_address = "0.0.0.0"
    listen_port    = 9095
  }

  forward_to = [
    prometheus.alertmanager.write.central.receiver,
  ]
}

prometheus.alertmanager.write "central" {
  endpoint {
    url        = "https://central-alertmanager.example.com"
    batch_size = 1

    basic_auth {
      username = sys.env("ALERTMANAGER_USERNAME")
      password = sys.env("ALERTMANAGER_PASSWORD")
    }

    tls_config {
      ca_file = "/etc/alloy/alertmanager-ca.pem"
    }
  }

  queue_config {
    capacity         = 1000
    block_on_overflow = true
    persistent       = true
    max_disk_bytes   = 67108864
  }
}
```

### Batch alerts

The following endpoint sends up to 100 alerts per request and waits at most one second for a partial batch:

```alloy
prometheus.alertmanager.write "central" {
  endpoint {
    url        = "http://alertmanager:9093"
    batch_size = 100
    batch_wait = "1s"
  }
}
```

## Alert refresh semantics

Alertmanager's alerts API is state-oriented rather than a notification-only endpoint.
Clients are expected to resend firing alerts until they resolve and to resend resolved alerts for up to five minutes.

For every firing input, `prometheus.alertmanager.write`:

1. Stores the latest alert by its label-derived fingerprint.
2. Replaces its outbound `endsAt` value with the current time plus `firing_alert_duration`.
3. Resends it every `refresh_interval`.
4. Stops retaining it when an explicit resolved value arrives or no firing update has arrived for `firing_alert_timeout`.

When stale firing state expires, the component stops refreshing it.
The destination resolves the alert after the last generated `endsAt` passes, which takes at most `firing_alert_duration` after the last successful refresh.

For every resolved input, the component removes matching firing state immediately.
It ensures the outbound `endsAt` isn't in the future and resends the resolved alert every `refresh_interval` for `resolved_retention`.

{{< admonition type="caution" >}}
Alertmanager webhook notifications don't provide a continuous source-of-truth stream.
The component can't distinguish a still-firing alert whose repeat notification is late from one whose resolved webhook was disabled or permanently lost.
Configure `send_resolved: true` on the source webhook and set `firing_alert_timeout` above the source route's `repeat_interval`.
{{< /admonition >}}

By default, restarting Alloy loses queue and refresh state.
Persistent mode restores both and preserves their original expiration timestamps; downtime doesn't extend `firing_alert_timeout` or `resolved_retention`.
Expired refresh state isn't resent, but previously accepted pending deliveries remain queued until delivery or a terminal failure.
The destination resolves previously refreshed alerts when their last `endsAt` passes if no further refresh arrives.

## Migration from `prometheus.alertmanager.relay`

Replace one relay block with one receive block and one write block.
Move listener, webhook path, body limit, and server TLS settings to `prometheus.alertmanager.receive`.
Move destination URL, client authentication, client TLS, proxy, and timeout settings to `prometheus.alertmanager.write`.
Connect them with `forward_to = [prometheus.alertmanager.write.<LABEL>.receiver]`.

Unlike the earlier monolithic relay, a successful webhook response now means every writer accepted the collection into its bounded queue.
It doesn't mean that the destination Alertmanager has already returned `2xx`.
Destination failures are retried asynchronously and eventually cause queue backpressure when the destination remains unavailable.
