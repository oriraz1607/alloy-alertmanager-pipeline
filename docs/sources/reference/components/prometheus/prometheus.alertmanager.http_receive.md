---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.http_receive/
description: Receive custom JSON on a configurable HTTP path and forward typed Alertmanager alerts
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.http_receive
---

# `prometheus.alertmanager.http_receive`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.http_receive` accepts one JSON object on a configurable HTTP path, decodes it into one typed alert, and forwards that alert to downstream Alloy components.

You can specify multiple `prometheus.alertmanager.http_receive` components by giving them different labels.

## Usage

```alloy
prometheus.alertmanager.http_receive "<LABEL>" {
  path       = "<HTTP_PATH>"
  decoder    = <ALERT_DECODER>
  forward_to = <RECEIVER_LIST>
}
```

## Arguments

You can use the following arguments with `prometheus.alertmanager.http_receive`:

| Name                    | Type                    | Description                                                    | Default     | Required |
| ----------------------- | ----------------------- | -------------------------------------------------------------- | ----------- | -------- |
| `decoder`               | `AlertDecoder`          | Decoder that reconstructs one typed alert from the request.    |             | yes      |
| `forward_to`            | `list(AlertReceiver)`   | Non-empty list of receivers to which the alert is forwarded.   |             | yes      |
| `forward_timeout`       | `duration`              | Maximum time to wait for downstream queue acceptance.          | `"10s"`     | no       |
| `max_request_body_size` | `string`                | Maximum request body size.                                     | `"1MiB"`    | no       |
| `path`                   | `string`                | Exact HTTP path on which to receive custom JSON.                | `"/alerts"` | no       |

`path` must start with `/`, must be a clean path, and must not contain a query string or fragment.
Only exact-path `POST` requests are decoded.
Other paths return `404`, other methods return `405`, oversized bodies return `413`, and decoder errors return `400`.
A downstream rejection returns `503`, and a downstream timeout returns `504`.
The component returns `200` only after every downstream receiver accepts the decoded alert.

Changing `decoder` changes subsequent request decoding without restarting the listener.
Changing `path` stops the old listener and starts a listener with only the new route, so the old path isn't left active.
Changing the server configuration also restarts the listener.

Each component owns a dedicated HTTP server.
Multiple instances can use different paths on different ports.
Two instances can't share one address and port; the second listener fails to bind rather than silently replacing the first route.

## Blocks

You can use the following blocks with `prometheus.alertmanager.http_receive`:

{{< docs/alloy-config >}}

| Block                 | Description                                   | Required |
| --------------------- | --------------------------------------------- | -------- |
| [`http`][http]        | Configure the HTTP server.                    | no       |
| `http` > [`tls`][tls] | Configure TLS for the HTTP server.            | no       |

[http]: #http
[tls]: #tls

{{< /docs/alloy-config >}}

### `http`

{{< docs/shared lookup="reference/components/server-http.md" source="alloy" version="<ALLOY_VERSION>" >}}

The HTTP server listens on `127.0.0.1:5002` by default.
Set `listen_address` and `listen_port` inside the `http` block.

### `tls`

{{< docs/shared lookup="reference/components/server-tls-config.md" source="alloy" version="<ALLOY_VERSION>" >}}

## Exported fields

`prometheus.alertmanager.http_receive` doesn't export any fields.

## Component health

`prometheus.alertmanager.http_receive` reports healthy while its configured HTTP listener is running.

## Debug information

`prometheus.alertmanager.http_receive` supports [live debugging](../../../../troubleshoot/debug/) in the standard Alloy UI.
Enable the [`livedebugging` block](../../../config-blocks/livedebugging/) with `enabled = true`, open the component page, and start live debugging.

The live debugging stream shows `[IN] HTTP RECEIVED` after the body passes the request size limit, before decoding.
The entry contains the method, path, sanitized headers, content type, body size, and original application body.
Compare this body with `prometheus.alertmanager.http` on the sending instance.
`[OUT]` shows the validated typed alert before forwarding.

Header values are redacted except for `Content-Type`, `Content-Length`, `Accept`, and `Retry-After`.
URL user information and fragments are omitted, and query values are redacted.
Application bodies remain unchanged; live debugging displays the alert data you send.

Values are formatted only when a live debugging consumer requests their text.
Alloy controls subscriptions, sampling, and stream delivery.

## Debug metrics

`prometheus.alertmanager.http_receive` exposes the following metrics:

* `prometheus_alertmanager_http_receive_active_decode_requests` (gauge): Active custom decode requests.
* `prometheus_alertmanager_http_receive_decode_request_duration_seconds` (histogram): Custom decode request duration.
* `prometheus_alertmanager_http_receive_decode_requests_total` (counter): Requests handled by the configured decoder.
* `prometheus_alertmanager_http_receive_forwarded_alerts_total` (counter): Alerts accepted by all downstream receivers.
* `prometheus_alertmanager_http_receive_forwarding_failures_total` (counter): Downstream rejections.
* `prometheus_alertmanager_http_receive_invalid_requests_total` (counter): Rejected requests by reason.
* `prometheus_alertmanager_http_receive_received_alerts_total` (counter): Successfully decoded alerts.

The shared HTTP server also exposes connection and request metrics with the `prometheus_alertmanager_http_receive` prefix.

## Example

The following Site B configuration accepts the Site A schema on `POST /alerts` and sends reconstructed alerts to Alertmanager:

```alloy
prometheus.alertmanager.decode "boundary" {
  labels_from      = ".metadata"
  annotations_from = ".annotations"

  labels = {
    alertname = ".name",
  }

  starts_at    = ".started"
  ends_at      = ".ended"
  generator_url = ".generator"
  status        = ".status"
}

prometheus.alertmanager.http_receive "incoming" {
  path       = "/alerts"
  decoder    = prometheus.alertmanager.decode.boundary.decoder
  forward_to = [prometheus.alertmanager.write.local.receiver]

  http {
    listen_address = "0.0.0.0"
    listen_port    = 9095
  }
}

prometheus.alertmanager.write "local" {
  endpoint {
    url = "http://alertmanager:9093"
  }
}
```

To use `/webhook` instead, change only the route:

```alloy
prometheus.alertmanager.http_receive "incoming" {
  path       = "/webhook"
  decoder    = prometheus.alertmanager.decode.boundary.decoder
  forward_to = [prometheus.alertmanager.write.local.receiver]

  http {
    listen_address = "0.0.0.0"
    listen_port    = 9095
  }
}
```
