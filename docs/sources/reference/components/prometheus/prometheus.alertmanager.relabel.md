---
canonical: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.alertmanager.relabel/
description: Apply Prometheus relabel rules to typed Alertmanager alerts
labels:
  products:
    - oss
  tags:
    - text: Community
      tooltip: This component is developed, maintained, and supported by the Alloy user community.
title: prometheus.alertmanager.relabel
---

# `prometheus.alertmanager.relabel`

{{< docs/shared lookup="stability/community.md" source="alloy" version="<ALLOY_VERSION>" >}}

`prometheus.alertmanager.relabel` applies ordered Prometheus relabel rules to each typed alert.
Place its `.receiver` in the `forward_to` list of `prometheus.alertmanager.receive` or `prometheus.alertmanager.http_receive`.
Downstream receivers see the resulting labels. Other branches of the upstream fan-out keep the original labels.

## Usage

```alloy
prometheus.alertmanager.relabel "<LABEL>" {
  forward_to = <RECEIVER_LIST>
}
```

## Arguments

| Name | Type | Description | Required |
| --- | --- | --- | --- |
| `forward_to` | `list(AlertReceiver)` | Nonempty list of receivers for surviving alerts. Each entry must be a valid receiver. | yes |

## Blocks

Add one `rule` block for each operation. With no rules, the component passes alerts through.
Each rule reads the labels left by the preceding rule.
When rules run, Prometheus treats an empty label value as an absent label.

### `rule`

The fields use the same configuration conventions as [`prometheus.relabel`](../prometheus.relabel/#rule).

| Name | Type | Default | Purpose |
| --- | --- | --- | --- |
| `action` | `string` | `"replace"` | Operation to apply. |
| `source_labels` | `list(string)` | `[]` | Label names whose values form the input string. A missing label contributes an empty string. |
| `separator` | `string` | `";"` | Separator between source values. |
| `regex` | `string` | `"(.*)"` | Regular expression anchored at both ends. |
| `target_label` | `string` | `""` | Destination label for value operations, or comparison label for equality operations. |
| `replacement` | `string` | `"$1"` | Replacement text. Use capture groups such as `$1` or `${1}`. |
| `modulus` | `uint` | `0` | Positive divisor required by `hashmod`. |

These are all supported actions.

| Action | Effect |
| --- | --- |
| `replace` | On a regex match, write the expanded replacement to the target label. An empty result deletes that label. |
| `labeldrop`, `labelkeep` | Remove labels with matching names, or retain only labels with matching names. |
| `labelmap` | Copy each matching label's value to a name derived from the replacement. Follow with `labeldrop` to remove the original names. |
| `keep`, `drop` | Keep or discard the **entire alert** when the joined source values match the regex. A nonmatch has the opposite effect. |
| `keepequal`, `dropequal` | Keep or discard the entire alert when the joined source values equal the target label value. |
| `lowercase`, `uppercase` | Write the lower- or uppercase joined source values to the target label. |
| `hashmod` | Hash the joined source values, take the result modulo `modulus`, and write it to the target label. |

A keep or drop filter can discard every alert in a collection. In that case, the receiver returns success and skips downstream delivery.
A surviving alert still requires a valid, nonempty label set. A rule that removes every label from a surviving alert causes the collection to fail validation.
Otherwise the component forwards surviving alerts in their original order and propagates downstream errors and backpressure.

### Alert identity

Rules affect only labels. They preserve annotations, firing or resolved state, timestamps, generator URL, and the source fingerprint.
The source fingerprint records the identity reported by the upstream Alertmanager. The writer and the destination Alertmanager identify an alert from its **resulting label names and values**.
Keep normalization consistent between firing and resolved updates. Removing distinguishing labels can merge separate alerts. Changing an identity label during an incident can leave its earlier identity active until it expires.

## Exported fields

| Name | Type | Description |
| --- | --- | --- |
| `receiver` | `AlertReceiver` | Accepts a typed collection for relabeling. |

## Component health

An invalid rule prevents the configuration from loading. A failed reload leaves the last valid rules and destinations active.

## Debug information

Inspect the output labels at the next HTTP transformer or Alertmanager writer using Alloy's live debugging UI.

## Debug metrics

This component doesn't export component-specific metrics.

## Example

The first rule fills an empty or absent `cluster` from `clustername`. The second sets a stable edge identity.
The third removes the legacy name after normalization.

```alloy
prometheus.alertmanager.relabel "normalize" {
  forward_to = [prometheus.alertmanager.write.central.receiver]

  rule {
    source_labels = ["cluster", "clustername"]
    separator     = ";"
    regex         = ";(.+)"
    target_label  = "cluster"
    replacement   = "$1"
    action        = "replace"
  }

  rule {
    target_label = "source_network"
    replacement  = "network-a"
    action       = "replace"
  }

  rule {
    regex  = "clustername"
    action = "labeldrop"
  }
}

prometheus.alertmanager.write "central" {
  endpoint {
    url = "http://alertmanager:9093"
  }
}
```
