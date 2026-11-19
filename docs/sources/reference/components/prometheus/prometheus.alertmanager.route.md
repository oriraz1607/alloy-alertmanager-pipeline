---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.route/
description: Route individual Alertmanager alerts to downstream receivers by label
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.route
---

# `prometheus.alertmanager.route`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.route` selects downstream receivers for each alert using its labels.
It runs inside Alloy, so one incoming webhook can serve multiple destinations.
Use it after [`prometheus.alertmanager.receive`](../prometheus.alertmanager.receive/) or [`prometheus.alertmanager.http_receive`](../prometheus.alertmanager.http_receive/).

## Usage

```alloy
prometheus.alertmanager.route "<LABEL>" {
  default_forward_to = <RECEIVER_LIST>
}
```

## Arguments

The router requires a default destination even when you configure routes.

| Name | Type | Description | Default | Required |
| --- | --- | --- | --- | --- |
| `default_forward_to` | `list(AlertReceiver)` | Nonempty list of receivers for alerts that match zero route blocks. | | yes |

## Blocks

Add a `route` block for each rule. If you omit all rules, every alert goes to the defaults.

### `route`

The router evaluates blocks in declaration order for each alert in the incoming collection.

| Name | Type | Description | Default | Required |
| --- | --- | --- | --- | --- |
| `forward_to` | `list(AlertReceiver)` | Nonempty list of receivers to select for a matching alert. | | yes |
| `matchers` | `list(string)` | Alertmanager label matchers. Every matcher must match. An empty list matches every alert. | | yes |
| `continue` | `bool` | Whether to evaluate subsequent blocks after this block matches. | `false` | no |

Each string specifies one matcher. Use `=` and `!=` for string equality and inequality, or `=~` and `!~` for a regular expression and its negation.
Regular expressions match the entire label value. For example, `namespace=~"pay.*"` matches `payments`, while `namespace=~"pay"` matches only `pay`.
A missing label compares as the empty string. For an alert with no `team` label, `team=""` and `team!="payments"` match, as does `team!~".+"`.
To require a nonempty value, use `team!=""`.

The first matching block selects all its receivers and normally ends evaluation for that alert.
With `continue = true`, later matches can add receivers. Once any block matches, the alert skips `default_forward_to`.
Only the individual alert's labels participate. Webhook group labels and common labels have no effect.
Firing and resolved alerts follow the same rules. The router preserves labels, annotations, status, source fingerprint, timestamps, and generator URL.

## Exported fields

Reference the router's `.receiver` in the upstream component's `forward_to` list.

| Name | Type | Description |
| --- | --- | --- |
| `receiver` | `AlertReceiver` | Accepts a collection of typed alerts for routing. |

Each selected receiver gets one collection per incoming call, with its alerts in their original order.
Repeated references to the same configured receiver select it once per alert. Two separately configured senders both receive the alert even when their endpoint URLs are identical.
You can use the `.receiver` export from either [`prometheus.alertmanager.http`](../prometheus.alertmanager.http/) or [`prometheus.alertmanager.write`](../prometheus.alertmanager.write/) in any destination list.

## Delivery and errors

The router calls selected destinations in the order of their first appearance in the route blocks, reading each `forward_to` list from left to right.
It then calls any selected receivers that appear only in `default_forward_to`, in list order.
A full downstream queue can block admission until the caller's deadline.
The first downstream error stops forwarding and returns to the caller. Earlier destinations keep their accepted alerts, and later destinations get no call.
If the source retries the webhook, an earlier destination can receive the same alert again. Deduplication of overlapping route selections applies only within one incoming call.

Both HTTP input components respond with an empty `200` once all selected destinations accept their alerts.
A downstream rejection yields `503`, or `504` when the forwarding deadline expires.
The chosen senders handle HTTP delivery, retries, and any configured persistence after admission.

## Component health

Alloy validates the matchers and destinations before activating a routing table.
An invalid reload leaves the last valid table active. Each incoming collection uses one table throughout forwarding.

## Debug information

Inspect the upstream receiver and downstream sender components in Alloy's Live Debugging UI to trace an alert's labels and delivery.

## Debug metrics

Use the upstream receiver's forwarding counters and each sender's queue and delivery metrics to monitor a routed pipeline.

## Examples

Start the custom Alloy build with `--feature.community-components.enabled` to use these components.

### Route an Alertmanager webhook

Keep your source Prometheus and Alertmanager configuration. On the receiving Alloy instance, this configuration accepts the existing webhook at `POST /webhook` on port `9095`.
Replace the three example endpoint URLs with your destinations. If your installation uses a different listener address, path, or port, retain those settings in the `receive` block.

```alloy
prometheus.alertmanager.receive "incoming" {
  http {
    listen_address = "0.0.0.0"
    listen_port    = 9095
  }

  forward_to = [prometheus.alertmanager.route.dispatch.receiver]
}

prometheus.alertmanager.route "dispatch" {
  route {
    matchers   = [`severity="critical"`]
    forward_to = [prometheus.alertmanager.http.noc.receiver]
    continue   = true
  }

  route {
    matchers   = [`namespace="payments"`, `environment="prod"`]
    forward_to = [prometheus.alertmanager.http.payments.receiver]
  }

  default_forward_to = [prometheus.alertmanager.http.general.receiver]
}

prometheus.alertmanager.transform "payload" {
  template = "{{ to_json . }}"
}

prometheus.alertmanager.http "noc" {
  transformer = prometheus.alertmanager.transform.payload.transformer

  endpoint {
    url = "https://noc.example.com/alerts"
  }
}

prometheus.alertmanager.http "payments" {
  transformer = prometheus.alertmanager.transform.payload.transformer

  endpoint {
    url = "https://payments.example.com/alerts"
  }
}

prometheus.alertmanager.http "general" {
  transformer = prometheus.alertmanager.transform.payload.transformer

  endpoint {
    url = "https://general.example.com/alerts"
  }
}
```

For one webhook containing the following alerts in order, NOC gets A and C, payments gets A and B, and general gets D. This holds for both firing and resolved notifications.

| Alert | `severity` | `namespace` | `environment` | Selected receivers |
| --- | --- | --- | --- | --- |
| A | critical | payments | prod | noc, payments |
| B | warning | payments | prod | payments |
| C | critical | infra | prod | noc |
| D | warning | infra | prod | general |

C matches the first block, so the default receives only D. Each HTTP sender sends one JSON object per selected alert.

### Route decoded JSON

If your existing source sends a custom JSON object per request, replace only the `receive "incoming"` block above with the following two blocks.
This example serves `POST /alerts` on the same port and decodes the field names emitted by the `payload` template above.
`http_receive` passes the decoded alert directly to the same router. Adjust the field paths to match your wire format.

```alloy
prometheus.alertmanager.decode "boundary" {
  labels_from        = ".Labels"
  annotations_from   = ".Annotations"
  status             = ".Status"
  starts_at          = ".StartsAt"
  ends_at            = ".EndsAt"
  generator_url      = ".GeneratorURL"
  source_fingerprint = ".SourceFingerprint"
}

prometheus.alertmanager.http_receive "incoming" {
  http {
    listen_address = "0.0.0.0"
    listen_port    = 9095
  }

  path       = "/alerts"
  decoder    = prometheus.alertmanager.decode.boundary.decoder
  forward_to = [prometheus.alertmanager.route.dispatch.receiver]
}
```
