---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.decode/
description: Decode config-defined JSON schemas into typed Alertmanager alerts
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.decode
---

# `prometheus.alertmanager.decode`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.decode` maps one JSON object into one typed Alertmanager alert.
All field paths and the incoming wire schema are defined in Alloy configuration.

You can specify multiple `prometheus.alertmanager.decode` components by giving them different labels.

## Usage

```alloy
prometheus.alertmanager.decode "<LABEL>" {
  labels_from = ".labels"
  starts_at   = ".startsAt"
}
```

## Arguments

You can use the following arguments with `prometheus.alertmanager.decode`:

| Name                 | Type          | Description                                                    | Default | Required |
| -------------------- | ------------- | -------------------------------------------------------------- | ------- | -------- |
| `starts_at`          | `string`      | Path to the required RFC3339 alert start time.                  |         | yes      |
| `annotations`        | `map(string)` | Annotation names mapped to required string field paths.        |         | no       |
| `annotations_from`   | `string`      | Optional path to an object whose entries become annotations.   |         | no       |
| `ends_at`            | `string`      | Path to an RFC3339 alert end time.                              |         | no       |
| `generator_url`      | `string`      | Path to a string generator URL.                                |         | no       |
| `labels`             | `map(string)` | Label names mapped to required string field paths.             |         | no       |
| `labels_from`        | `string`      | Optional path to an object whose entries become labels.        |         | no       |
| `source_fingerprint` | `string`      | Path to a string source fingerprint.                           |         | no       |
| `status`             | `string`      | Path to an explicit `firing` or `resolved` value.               |         | no       |

Configure at least one of `labels_from` or `labels`.
Paths support nested object lookup in the form `.foo`, `.foo.bar`, or `.foo.bar.baz`.
Arrays, filters, expressions, query strings, and type coercion aren't supported.

When `labels_from` or `annotations_from` exists, it must select a JSON object containing only string values.
If an optional whole-map path is absent, the decoder imports an empty map.
New entries in a selected object are imported without a configuration change.
An explicit mapping path is required when configured and must select a string.

The decoder applies maps in this order:

1. It imports entries from `labels_from` or `annotations_from`.
2. It applies explicit `labels` or `annotations` mappings.

Explicit mappings therefore override imported entries with the same name.
The decoder doesn't treat `alertname` as a protocol field; it is an ordinary label.

If `status` is configured, the selected value must be `firing` or `resolved`.
Without `status`, an `ends_at` value at or before decode time means `resolved`; a missing, zero, or future end time means `firing`.
Map `status` explicitly when a schema must preserve source lifecycle state without time-based inference.

After mapping, the decoder runs the shared typed-alert validation.
Invalid label names, empty labels, missing start time, invalid URLs, malformed timestamps, and unsupported status values fail decoding.

## Blocks

`prometheus.alertmanager.decode` doesn't support any blocks.

## Exported fields

The following field is exported and can be referenced by other components:

| Name      | Type                    | Description                                  |
| --------- | ----------------------- | -------------------------------------------- |
| `decoder` | `AlertDecoder` | Decodes one JSON object into one typed alert. |

## Component health

`prometheus.alertmanager.decode` is only reported as unhealthy if its configuration is invalid.

## Debug information

`prometheus.alertmanager.decode` supports [live debugging](../../../../troubleshoot/debug/) in the standard Alloy UI.
Enable the [`livedebugging` block](../../../config-blocks/livedebugging/) with `enabled = true`, open the component page, and start live debugging.

The live debugging stream shows the original JSON as `[IN]` and the validated typed alert as `[OUT]`.
Use the output labels to verify field mappings and `labels_from` overrides.
A failed decode has no output entry; `prometheus.alertmanager.http_receive` reports the failure through its existing logs and HTTP response.

Values are formatted only when a live debugging consumer requests their text.
Alloy controls subscriptions, sampling, and stream delivery.

## Debug metrics

`prometheus.alertmanager.decode` doesn't expose any component-specific metrics.

## Example

The following example imports complete maps and overrides `alertname` with an explicit nested mapping:

```alloy
prometheus.alertmanager.decode "boundary" {
  labels_from      = ".metadata"
  annotations_from = ".annotations"

  labels = {
    alertname = ".event.name",
  }

  annotations = {
    summary = ".message",
  }

  starts_at    = ".timestamps.started"
  ends_at      = ".timestamps.ended"
  generator_url = ".generator"
  status        = ".status"
}
```

Custom schemas can omit data, but omitted data can't be reconstructed.
Dropping labels can change alert identity, deduplication, and grouping behavior in downstream Alertmanager instances.
