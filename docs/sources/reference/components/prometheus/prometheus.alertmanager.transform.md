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
| `remove_special_characters` | `bool` | Remove punctuation, symbols, and controls from label names and values before template evaluation. | `false` | no |
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
| `to_string` | Serializes a string map as sorted, percent-escaped `{name:value,...}` text. |
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

## String labels for restricted schemas

Use `to_string` when your boundary requires labels in a JSON string:

```alloy
prometheus.alertmanager.transform "string_labels" {
  compact = true
  template = `{
    "labels": "{{ to_string .Labels }}",
    "started": {{ to_json .StartsAt }}
  }`
}
```

`{{ .Labels | to_string }}` is equivalent to `{{ to_string .Labels }}`.
The helper accepts a string map, including `.Labels` or `.Annotations`.
It returns text without surrounding JSON quotes. Put it inside quotes as shown,
or use `{{ .Labels | to_string | to_json }}` without surrounding quotes.
`to_json` continues to support nested JSON objects.

The format is `{name:value,name:value}` with names sorted by Go string order.
An empty map becomes `{}`. Empty values are preserved, and spaces aren't trimmed.
The encoder escapes these bytes in both names and values using `%HH` with uppercase hexadecimal digits:

| Byte | Escape |
| ---- | ------ |
| `%` | `%25` |
| `,` | `%2C` |
| `:` | `%3A` |
| `{` and `}` | `%7B` and `%7D` |
| Backslash | `%5C` |
| Double quote | `%22` |
| Control bytes `0x00`–`0x1F` and `0x7F` | Corresponding `%HH` |

Spaces and valid Unicode remain unchanged. Invalid UTF-8 is rejected.
The decoder accepts either case for hexadecimal digits, decodes escapes once,
and rejects incomplete escapes, invalid hex, unescaped reserved bytes, duplicate names,
and missing delimiters. It never returns a partial map on failure.

For example, `alertname=test` and `severity=critical` become:

```json
{"labels":"{alertname:test,severity:critical}","started":"2026-09-07T14:00:00Z"}
```

The label `message=disk: almost, full` becomes `{message:disk%3A almost%2C full}`.
Decoding restores `disk: almost, full` exactly.
Live debugging shows the actual JSON string in the transform output.

Configure the receiving decoder to reverse the string representation:

```alloy
prometheus.alertmanager.decode "string_labels" {
  labels_from   = ".labels"
  labels_format = "to_string"
  starts_at     = ".started"
}
```

Use `prometheus.alertmanager.transform.string_labels.transformer` as the sender's
`transformer` and `prometheus.alertmanager.decode.string_labels.decoder` as the
HTTP receiver's `decoder`. Both components require the community-components flag.
The decoder output contains the original label map. Its live debugging output shows
labels as an object, using the existing typed-alert display.
These examples preserve labels and the required start time. Map other alert fields
when you need to preserve annotations, end time, URL, or explicit status as well.

### Remove special characters

Set `remove_special_characters = true` alongside `compact` to clean `.Labels`
before the template runs. The default is `false`, which preserves label data.
The option retains Unicode letters, numbers, and combining marks.
It deletes whitespace (including spaces), punctuation (including underscores), symbols (including emoji), and
control characters without replacements. It doesn't change annotations or the
source alert. All template uses of `.Labels`, including `to_json`, see cleaned data;
use cleaned names when accessing an individual label in the template.

For example, this template keeps the serialization delimiters:

```alloy
prometheus.alertmanager.transform "clean_labels" {
  compact                   = true
  remove_special_characters = true
  template = `{"labels":"{{ to_string .Labels }}","started":{{ to_json .StartsAt }}}`
}
```

The labels `severity=critical` and `Alert_Name=Node_Down!` produce
`{"labels":"{AlertName:NodeDown,severity:critical}","started":"2026-09-07T14:00:00Z"}`
for an alert starting at that time. The existing `labels_format = "to_string"`
decoder reconstructs the cleaned map. Removed characters can't be recovered.
Empty cleaned values are allowed; empty cleaned names or duplicate cleaned names
reject the transformation instead of silently dropping labels.
