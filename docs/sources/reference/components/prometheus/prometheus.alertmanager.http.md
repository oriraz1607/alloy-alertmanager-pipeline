---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.http/
description: Reliably send transformed Alertmanager alerts as custom JSON over HTTP
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.http
---

# `prometheus.alertmanager.http`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.http` accepts typed alerts, transforms each alert with a configured `AlertTransformer`, and sends each resulting JSON document to an HTTP endpoint.

You can specify multiple `prometheus.alertmanager.http` components by giving them different labels.

## Usage

```alloy
prometheus.alertmanager.http "<LABEL>" {
  transformer = <ALERT_TRANSFORMER>

  endpoint {
    url = "<DESTINATION_URL>"
  }
}
```

## Arguments

You can use the following argument with `prometheus.alertmanager.http`:

| Name          | Type                        | Description                                         | Default | Required |
| ------------- | --------------------------- | --------------------------------------------------- | ------- | -------- |
| `transformer` | `AlertTransformer` | Transformer that creates one JSON body per alert.   |         | yes      |

The component validates and transforms a complete upstream collection before it admits any body to the queue.
If one transformation fails or returns invalid JSON, the complete collection is rejected and no request is queued.
Three accepted alerts always produce three separate `POST` requests; this component doesn't create JSON arrays or batches.

## Blocks

You can use the following blocks with `prometheus.alertmanager.http`:

{{< docs/alloy-config >}}

| Block                                              | Description                                  | Required |
| -------------------------------------------------- | -------------------------------------------- | -------- |
| [`endpoint`][endpoint]                             | Configure the JSON HTTP destination.         | yes      |
| `endpoint` > [`authorization`][authorization]      | Configure generic authorization.             | no       |
| `endpoint` > [`basic_auth`][basic_auth]            | Configure basic authentication.              | no       |
| `endpoint` > [`oauth2`][oauth2]                    | Configure OAuth 2.0 authentication.           | no       |
| `endpoint` > `oauth2` > [`tls_config`][tls_config] | Configure TLS for the OAuth 2.0 endpoint.     | no       |
| `endpoint` > [`tls_config`][tls_config]            | Configure TLS for the destination.            | no       |
| [`queue_config`][queue_config]                     | Configure the bounded transformed-body queue. | no       |

[authorization]: #authorization
[basic_auth]: #basic_auth
[endpoint]: #endpoint
[oauth2]: #oauth2
[queue_config]: #queue_config
[tls_config]: #tls_config

{{< /docs/alloy-config >}}

### `endpoint`

The required `endpoint` block configures the exact destination URL, retry policy, and HTTP client.

| Name                     | Type                | Description                                                                         | Default   | Required |
| ------------------------ | ------------------- | ----------------------------------------------------------------------------------- | --------- | -------- |
| `url`                    | `string`            | Exact destination URL, including its user-configured path.                          |           | yes      |
| `bearer_token`           | `secret`            | Bearer token to authenticate with.                                                  |           | no       |
| `bearer_token_file`      | `string`            | File containing a bearer token.                                                     |           | no       |
| `enable_http2`           | `bool`              | Whether requests support HTTP/2.                                                    | `true`    | no       |
| `follow_redirects`       | `bool`              | Whether requests follow redirects.                                                  | `true`    | no       |
| `http_headers`           | `map(list(secret))` | Custom headers to send with each request.                                           |           | no       |
| `max_backoff_period`     | `duration`          | Maximum retry backoff.                                                              | `"5m"`    | no       |
| `max_retries`            | `int`               | Retries after the first attempt; `0` retries until cancellation.                    | `10`      | no       |
| `min_backoff_period`     | `duration`          | Initial retry backoff.                                                              | `"500ms"` | no       |
| `no_proxy`               | `string`            | Comma-separated addresses to exclude from proxying.                                 |           | no       |
| `proxy_from_environment` | `bool`              | Whether to use proxy environment variables.                                        | `false`   | no       |
| `proxy_url`              | `string`            | HTTP proxy URL.                                                                     |           | no       |
| `retry_on_http_429`      | `bool`              | Whether HTTP `429` is retryable.                                                    | `true`    | no       |
| `timeout`                | `duration`          | Maximum duration of one request.                                                    | `"10s"`   | no       |

The component uses the URL path exactly as configured.
It doesn't append `/alerts`, `/webhook`, or `/api/v2/alerts`.
Every request uses `POST` and `Content-Type: application/json`.

Connection failures, timeouts, HTTP `5xx`, and optionally HTTP `429` responses are retried with capped exponential backoff.
An alert waiting for its next retry doesn't block other ready alerts in the queue.
The sender continues processing ready alerts and retries the failed alert after its backoff expires.
Requests remain sequential, so a slow active request can delay a retry beyond its scheduled time.
Other HTTP `4xx` responses are terminal.
After a terminal response or retry exhaustion, the component records the drop, reports unhealthy, and logs the delivery error.

At most one authentication method can be configured.

{{< docs/shared lookup="reference/components/http-client-proxy-config-description.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `authorization`

{{< docs/shared lookup="reference/components/authorization-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `basic_auth`

{{< docs/shared lookup="reference/components/basic-auth-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `oauth2`

{{< docs/shared lookup="reference/components/oauth2-block.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `tls_config`

{{< docs/shared lookup="reference/components/http-client-tls-config-description.md" source="alloy" version="<ALLOY_VERSION>" >}}

### `queue_config`

The optional `queue_config` block controls backpressure and shutdown draining.

| Name                | Type       | Description                                                  | Default | Required |
| ------------------- | ---------- | ------------------------------------------------------------ | ------- | -------- |
| `block_on_overflow` | `bool`     | Whether producers wait when the queue is full.               | `true`  | no       |
| `capacity`          | `int`      | Maximum transformed JSON bodies held in memory.              | `1000`  | no       |
| `drain_timeout`     | `duration` | Maximum time to deliver queued bodies during shutdown.       | `"15s"` | no       |

Queue admission is all-or-nothing for each upstream collection.
The queue stores already transformed JSON, so a template reload affects alerts accepted after the reload; bodies already queued keep the schema that was validated when accepted.
The queue is memory-only and doesn't survive process restart.

## Exported fields

The following field is exported and can be referenced by other components:

| Name       | Type                     | Description                                        |
| ---------- | ------------------------ | -------------------------------------------------- |
| `receiver` | `AlertReceiver` | Receiver for typed alerts that must cross HTTP.    |

## Component health

`prometheus.alertmanager.http` reports healthy while it can accept alerts and its most recent terminal delivery attempt succeeded.

## Debug information

`prometheus.alertmanager.http` supports [live debugging](../../../../troubleshoot/debug/) in the standard Alloy UI.
Enable the [`livedebugging` block](../../../config-blocks/livedebugging/) with `enabled = true`, open the component page, and start live debugging.

The live debugging stream includes the following entries:

- **HTTP REQUEST:** Shows the method, destination URL, path, sanitized application headers, body size, zero-based retry attempt, and the actual JSON body passed to the HTTP client.
- **HTTP RESPONSE:** Shows the status code, sanitized response headers, request duration, and up to 4096 response body bytes.
- **HTTP TRANSPORT ERROR:** Shows the destination and a safe failure diagnosis when no response is available, including unknown certificate authority failures.
- **DELIVERED:** Shows the application body after successful delivery.

The request body preserves whitespace and isn't reconstructed from the alert.
Request headers describe the logical request before HTTP client wrappers run.
Configured authentication and custom header names are shown with `***` values; transport-added headers and redirects aren't captured separately.
Other header values are redacted except for `Content-Type`, `Content-Length`, `Accept`, and `Retry-After`.
URL user information and fragments are omitted, and query values are redacted.

Response entries mark potentially truncated or incomplete bodies.
Response bodies are omitted for endpoints with authentication, proxy configuration, configured custom headers, URL user information, or query parameters, because those responses can reflect credentials.
Transport errors use safe diagnoses instead of arbitrary error strings that might contain credentials.
Application request bodies remain unchanged; live debugging displays the alert data you send.

Values are formatted only when a live debugging consumer requests their text.
Alloy controls subscriptions, sampling, and stream delivery.

## Debug metrics

`prometheus.alertmanager.http` exposes the following metrics:

* `prometheus_alertmanager_http_dropped_alerts_total` (counter): Alerts rejected or abandoned by reason.
* `prometheus_alertmanager_http_request_failures_total` (counter): Failed requests by reason.
* `prometheus_alertmanager_http_request_duration_seconds` (histogram): Request duration.
* `prometheus_alertmanager_http_requests_total` (counter): Destination requests.
* `prometheus_alertmanager_http_queue_length` (gauge): Queued JSON bodies.
* `prometheus_alertmanager_http_received_alerts_total` (counter): Alerts accepted into the queue.
* `prometheus_alertmanager_http_retries_total` (counter): Retried requests.
* `prometheus_alertmanager_http_sent_alerts_total` (counter): Alerts accepted by the destination.
* `prometheus_alertmanager_http_transformation_errors_total` (counter): Transformation failures.

## Example

The following Site A configuration receives Alertmanager generic webhooks, transforms every typed alert, and sends custom JSON to Site B:

```alloy
prometheus.alertmanager.receive "source" {
  forward_to = [prometheus.alertmanager.http.site_b.receiver]

  http {
    listen_address = "0.0.0.0"
    listen_port    = 9095
  }
}

prometheus.alertmanager.transform "boundary" {
  template = `
  {
    "name": {{ to_json .Labels.alertname }},
    "metadata": {{ to_json .Labels }},
    "annotations": {{ to_json .Annotations }},
    "started": {{ to_json .StartsAt }},
    "ended": {{ to_json .EndsAt }},
    "generator": {{ to_json .GeneratorURL }},
    "status": {{ to_json .Status }}
  }
  `
}

prometheus.alertmanager.http "site_b" {
  transformer = prometheus.alertmanager.transform.boundary.transformer

  endpoint {
    url = "https://site-b.example.com/alerts"
  }
}
```
