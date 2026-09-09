---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.transform/
description: Transform typed Alertmanager alerts into config-defined JSON documents
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.transform
---

# `prometheus.alertmanager.transform`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.transform` uses a Go text template to turn one typed Alertmanager alert into one JSON document.
The template defines the wire schema in Alloy configuration.

You can specify multiple `prometheus.alertmanager.transform` components by giving them different labels.

## Usage

```alloy
prometheus.alertmanager.transform "<LABEL>" {
  template = "<JSON_TEMPLATE>"
  compact  = <BOOLEAN>
}
```

## Arguments

You can use the following arguments with `prometheus.alertmanager.transform`:

| Name       | Type     | Description                                              | Default | Required |
| ---------- | -------- | -------------------------------------------------------- | ------- | -------- |
| `compact`  | `bool`   | Remove insignificant whitespace from the rendered JSON.  | `false` | no       |
| `template` | `string` | Go text template that must render one valid JSON value.   |         | yes      |

The template receives the following fields:

| Field                | Description                                      |
| -------------------- | ------------------------------------------------ |
| `.Labels`            | Complete `map(string)` of alert labels.          |
| `.Annotations`       | Complete `map(string)` of alert annotations.     |
| `.StartsAt`          | Alert start time.                                 |
| `.EndsAt`            | Alert end time.                                   |
| `.GeneratorURL`      | Source generator URL.                             |
| `.Status`            | Explicit `firing` or `resolved` pipeline status. |
| `.SourceFingerprint` | Fingerprint supplied by the original source.      |

You can access a complete map with `.Labels` or one entry with `.Labels.severity`.
The same behavior applies to annotations.
New labels and annotations automatically appear when a template serializes the complete map.

The template provides these small helper functions:

| Function   | Description                                                               |
| ---------- | ------------------------------------------------------------------------- |
| `to_json`  | Serializes a value with Go `encoding/json`, including safe string escaping. |
| `default`  | Returns a fallback when the supplied value is empty.                      |
| `required` | Stops rendering with the supplied message when the value is empty.        |

A missing map entry evaluates to an empty string.
Use `default` when an empty value is acceptable or `required` when the field must be present.
Template syntax errors fail component evaluation.
Rendering errors and invalid rendered JSON reject the alert before HTTP queue admission.
When `compact` is `true`, the component removes insignificant JSON whitespace after rendering.
Whitespace inside string values is preserved.
Compact and non-compact output are accepted identically by `prometheus.alertmanager.http_receive` and `prometheus.alertmanager.decode`.

## Blocks

`prometheus.alertmanager.transform` doesn't support any blocks.

## Exported fields

The following field is exported and can be referenced by other components:

| Name          | Type                        | Description                                  |
| ------------- | --------------------------- | -------------------------------------------- |
| `transformer` | `AlertTransformer` | Transforms one typed alert into one JSON body. |

## Component health

`prometheus.alertmanager.transform` is only reported as unhealthy if its configuration is invalid.

## Debug information

`prometheus.alertmanager.transform` supports [live debugging](../../../../troubleshoot/debug/) in the standard Alloy UI.
Enable the [`livedebugging` block](../../../config-blocks/livedebugging/) with `enabled = true`, open the component page, and start live debugging.

The live debugging stream shows the typed alert as `[IN]` and the actual generated JSON as `[OUT]`.
Use these entries to verify template results, including whitespace when `compact` is `false`.
A failed transformation has no output entry and produces a warning in the component log.
The error also propagates to the component that calls the transformer.

Values are formatted only when a live debugging consumer requests their text.
Alloy controls subscriptions, sampling, and stream delivery.

## Debug metrics

`prometheus.alertmanager.transform` doesn't expose any component-specific metrics.

## Example

The following example defines a nested custom schema:

```alloy
prometheus.alertmanager.transform "boundary" {
  compact = true

  template = `
  {
    "schema_version": 1,
    "event": {
      "type": {{ to_json .Labels.alertname }},
      "priority": {{ to_json (default "unknown" .Labels.severity) }},
      "status": {{ to_json .Status }}
    },
    "metadata": {{ to_json .Labels }},
    "annotations": {{ to_json .Annotations }},
    "started": {{ to_json .StartsAt }},
    "ended": {{ to_json .EndsAt }},
    "generator": {{ to_json .GeneratorURL }}
  }
  `
}
```

Dropping labels can change alert identity, deduplication, and grouping behavior in downstream Alertmanager instances.
Custom transformations aren't inherently lossless; preserve every field required by your downstream semantics.
