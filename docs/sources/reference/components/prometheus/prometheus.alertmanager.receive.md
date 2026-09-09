---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.receive/
description: Receive Alertmanager webhooks and forward typed alerts to other Alloy components
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.receive
---

# `prometheus.alertmanager.receive`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.receive` accepts Alertmanager generic webhook notifications and forwards typed alert values to other Alloy components.

You can specify multiple `prometheus.alertmanager.receive` components by giving them different labels.

## Usage

```alloy
prometheus.alertmanager.receive "<LABEL>" {
  forward_to = <RECEIVER_LIST>
}
```

The component exposes `POST /webhook` by default.

## Arguments

You can use the following arguments with `prometheus.alertmanager.receive`:

| Name                    | Type                  | Description                                                    | Default      | Required |
| ----------------------- | --------------------- | -------------------------------------------------------------- | ------------ | -------- |
| `forward_to`            | `list(AlertReceiver)` | Receivers to which alerts are forwarded.                       |              | yes      |
| `forward_timeout`       | `duration`            | Maximum time to wait for all receivers to accept a collection. | `"10s"`    | no       |
| `max_request_body_size` | `string`              | Maximum size of one webhook request body.                      | `"1MiB"`   | no       |
| `webhook_path`          | `string`              | HTTP path on which to accept webhook requests.                 | `"/webhook"` | no       |

The webhook path only accepts `POST` requests.
The request body must contain exactly one Alertmanager webhook JSON object and at least one alert.
Each alert must have a non-empty valid label set and a `status` of either `firing` or `resolved`.
Unknown webhook fields are ignored so that Alertmanager can add fields without breaking the receiver.

The component responds with:

* `200` after every configured downstream receiver accepts the complete alert collection.
* `400` for malformed or invalid webhook data.
* `405` for methods other than `POST`.
* `413` when the request exceeds `max_request_body_size`.
* `503` when a downstream receiver rejects the collection.
* `504` when downstream acceptance exceeds `forward_timeout`.

An upstream Alertmanager retries a webhook notification when it receives an error response.
If one receiver in a fan-out accepts a collection and a later receiver rejects it, the component returns an error.
The upstream retry can deliver the collection to the earlier receiver again.
Alertmanager deduplicates alerts by their labels.

## Blocks

You can use the following blocks with `prometheus.alertmanager.receive`:

{{< docs/alloy-config >}}

| Block                  | Description                                        | Required |
| ---------------------- | -------------------------------------------------- | -------- |
| [`http`][http]         | Configure the HTTP server that receives webhooks.  | no       |
| `http` > [`tls`][tls]  | Configure TLS for the HTTP server.                 | no       |

[http]: #http
[tls]: #tls

{{< /docs/alloy-config >}}

### `http`

{{< docs/shared lookup="reference/components/server-http.md" source="alloy" version="<ALLOY_VERSION>" >}}

The default listen address is `127.0.0.1` and the default listen port is `5001`.

### `tls`

The `tls` block configures TLS for the HTTP server.
You can require and verify client certificates by configuring `client_auth_type` and a client CA.

{{< docs/shared lookup="reference/components/server-tls-config-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

## Exported fields

`prometheus.alertmanager.receive` doesn't export any fields.

## Component health

`prometheus.alertmanager.receive` reports as healthy while its HTTP listener is running.
Invalid server configuration or a listener bind failure prevents the component from starting.
Downstream backpressure is reported in debug metrics and HTTP responses.

## Debug information

`prometheus.alertmanager.receive` supports [live debugging](../../../../troubleshoot/debug/) in the standard Alloy UI.
Enable the [`livedebugging` block](../../../config-blocks/livedebugging/) with `enabled = true`, open the component page, and start live debugging.

The live debugging stream shows the decoded webhook as `[IN] WEBHOOK` and each validated typed alert as `[OUT]` before forwarding.
Typed alerts include labels, annotations, timestamps, generator URL, and explicit firing or resolved status.
The webhook entry represents the decoded application value, rather than the original HTTP bytes.

Values are formatted only when a live debugging consumer requests their text.
Alloy controls subscriptions, sampling, and stream delivery.

## Debug metrics

`prometheus.alertmanager.receive` exposes the following metrics:

* `prometheus_alertmanager_receive_active_webhook_requests` (gauge): Current number of active webhook requests.
* `prometheus_alertmanager_receive_forwarded_alerts_total` (counter): Total number of alerts accepted by every configured receiver.
* `prometheus_alertmanager_receive_forwarding_failures_total` (counter): Total number of webhook deliveries rejected by a downstream receiver.
* `prometheus_alertmanager_receive_invalid_requests_total` (counter): Total number of invalid webhook requests.
* `prometheus_alertmanager_receive_received_alerts_total` (counter): Total number of alerts received in valid webhook envelopes.
* `prometheus_alertmanager_receive_webhook_request_duration_seconds` (histogram): Duration of webhook requests.
* `prometheus_alertmanager_receive_webhook_requests_total` (counter): Total number of webhook requests.

The shared HTTP server also exposes connection and request metrics with the `prometheus_alertmanager_receive` prefix.

## Examples

### Receive and write alerts

The following example listens on all network interfaces and forwards alerts to an Alertmanager writer:

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
  }
}
```

Start Alloy with the `--feature.community-components.enabled` flag to use these community components.

Configure the source Alertmanager to call the receiver:

```yaml
receivers:
  - name: alloy-alert-pipeline
    webhook_configs:
      - url: http://alloy:9095/webhook
        send_resolved: true
```

Reference `alloy-alert-pipeline` from the appropriate Alertmanager route.

### Fan out alerts

The following example independently queues each collection in two writers:

```alloy
prometheus.alertmanager.receive "edge" {
  forward_to = [
    prometheus.alertmanager.write.central.receiver,
    prometheus.alertmanager.write.backup.receiver,
  ]
}
```

## Technical details

The internal alert value embeds the Prometheus ecosystem's generic alert representation.
It preserves labels, annotations, start and end timestamps, and the generator URL.
It also carries the webhook's explicit firing or resolved state and source fingerprint.
The status is required because a webhook can contain firing and resolved alerts in the same notification.

The receiver and downstream components exchange typed Go values directly.
They don't communicate through a localhost HTTP endpoint.
The typed receiver boundary permits future alert processing components to receive, transform, and forward the same values.

The HTTP server applies configured read, write, idle, connection, and TLS limits.
Keep the default loopback listen address unless another host must reach the webhook.
When exposing the endpoint outside a trusted network, use network access controls or mutual TLS.
The shared server configuration doesn't provide application-layer basic authentication.
The component doesn't log complete webhook payloads at normal log levels.
