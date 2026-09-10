<p align="center">
    <img src="docs/sources/assets/logo_alloy_light.svg#gh-dark-mode-only" alt="Grafana Alloy logo" height="100px">
    <img src="docs/sources/assets/logo_alloy_dark.svg#gh-light-mode-only" alt="Grafana Alloy logo" height="100px">
</p>

# Alloy Alertmanager pipeline

This repository is a custom Grafana Alloy build with six community components
for forwarding Alertmanager alerts, sending configuration-defined JSON across
HTTP boundaries, and reconstructing alerts at the destination.
It retains Alloy's support for metrics, logs, traces, and profiles.
This is a community build, not an official Grafana release.

[Update from upstream Alloy](https://github.com/oriraz1607/alloy-alertmanager-pipeline/actions/workflows/update-alloy.yml)
· [Build release files](https://github.com/oriraz1607/alloy-alertmanager-pipeline/actions/workflows/build-custom-release.yml)

## Supported pipelines

Use a direct pipeline when the destination accepts the Alertmanager API:

```text
Alertmanager A → receive → typed alerts → write → Alertmanager B
```

Use two Alloy instances when an HTTP boundary requires a custom JSON schema:

```text
Alertmanager A → Alloy A: receive → http (using transform)
                                      ↓ custom JSON over HTTP
Alertmanager B ← Alloy B: write ← http_receive (using decode)
```

Components inside one Alloy instance exchange typed alerts directly.
`transform` and `decode` export helpers used by the HTTP components; they don't
run their own listeners or delivery queues.

| Component | Purpose |
| --- | --- |
| [`prometheus.alertmanager.receive`](docs/sources/reference/components/prometheus/prometheus.alertmanager.receive.md) | Accept Alertmanager webhook notifications on `POST /webhook` and forward typed alerts. |
| [`prometheus.alertmanager.transform`](docs/sources/reference/components/prometheus/prometheus.alertmanager.transform.md) | Render each typed alert as JSON using a Go template, with optional compact output. |
| [`prometheus.alertmanager.http`](docs/sources/reference/components/prometheus/prometheus.alertmanager.http.md) | Queue transformed JSON and send one HTTP POST per alert to the exact configured URL. |
| [`prometheus.alertmanager.http_receive`](docs/sources/reference/components/prometheus/prometheus.alertmanager.http_receive.md) | Accept one custom JSON object on a configurable HTTP path and forward the decoded alert. |
| [`prometheus.alertmanager.decode`](docs/sources/reference/components/prometheus/prometheus.alertmanager.decode.md) | Map JSON fields and complete label or annotation maps back to a typed alert. |
| [`prometheus.alertmanager.write`](docs/sources/reference/components/prometheus/prometheus.alertmanager.write.md) | Batch alerts for the Alertmanager API v2, retry delivery, and refresh alert lifecycle state. |

## Direct forwarding quick start

Use a binary built from this repository and save this as `config.alloy`:

```alloy
prometheus.alertmanager.receive "source" {
  http {
    listen_address = "0.0.0.0"
    listen_port    = 9095
  }

  forward_to = [prometheus.alertmanager.write.destination.receiver]
}

prometheus.alertmanager.write "destination" {
  endpoint {
    url        = "http://alertmanager:9093"
    batch_size = 1
  }
}
```

Replace `alertmanager` with your destination hostname. Start Alloy with the
community components flag:

```sh
./alloy run --feature.community-components.enabled --storage.path=./data config.alloy
```

Configure the source Alertmanager receiver with your Alloy hostname:

```yaml
receivers:
  - name: alloy
    webhook_configs:
      - url: http://<ALLOY_HOST>:9095/webhook
        send_resolved: true
```

Route the relevant alerts to this receiver in your Alertmanager configuration.
The writer sends a JSON array to `POST /api/v2/alerts` when its destination URL
has no path. You can fan out by adding more writer receivers to `forward_to`.

## Custom JSON between Alloy instances

On Alloy A, connect `receive.forward_to` to an `http.receiver`, then configure
`http.transformer` with a `transform.transformer`. The template controls the
JSON field names and structure. Use `to_json` for safe escaping and
`compact = true` to remove whitespace outside string values.

On Alloy B, configure `decode` with matching field paths, assign its `decoder`
to `http_receive`, and forward the decoded alerts to `write.receiver`.
Set the sender's full endpoint URL to the receiver's configured path, such as
`http://alloy-b:5002/alerts`. The HTTP sender doesn't append a path.

The component references include complete [sender configuration](docs/sources/reference/components/prometheus/prometheus.alertmanager.http.md#example)
and [receiver configuration](docs/sources/reference/components/prometheus/prometheus.alertmanager.http_receive.md#example).
Preserve complete labels and annotations when they must survive the boundary,
and map status explicitly to preserve firing and resolved state. Omitted fields
can't be reconstructed; dropping labels can change alert identity and grouping.

### Labels as a single JSON string

For boundaries that only accept simple fields, use `to_string` in the sender's
transform template and opt into string decoding on the receiver:

```alloy
// Alloy A: assign this transformer to prometheus.alertmanager.http.
prometheus.alertmanager.transform "boundary" {
  compact = true
  template = `{
    "labels": "{{ to_string .Labels }}",
    "started": {{ to_json .StartsAt }}
  }`
}

// Alloy B: assign this decoder to prometheus.alertmanager.http_receive.
prometheus.alertmanager.decode "boundary" {
  labels_from   = ".labels"
  labels_format = "to_string"
  starts_at     = ".started"
}
```

`{{ to_string .Labels }}` and `{{ .Labels | to_string }}` are equivalent.
For `alertname=test` and `severity=critical`, the labels field is a JSON string:

```json
{"labels":"{alertname:test,severity:critical}","started":"2026-09-07T14:00:00Z"}
```

Names are sorted for deterministic output. Reserved bytes use `%HH` escapes:
commas, colons, braces, percent signs, quotes, backslashes, and control bytes.
For example, `disk: almost, full` becomes `disk%3A almost%2C full` and decodes
back unchanged. Spaces, Unicode, and empty values are preserved. Malformed
strings and duplicate names fail decoding; existing label-name and typed-alert
validation still apply. Explicit `labels` mappings override imported entries.

Object decoding remains the default, and `to_json`, `default`, and `required`
keep their existing behavior. These examples preserve labels and the required
start time; include matching fields for other alert data you need to retain.
Live Debugging shows the actual JSON string on the sender and the reconstructed
labels on the decoder. Refer to the [format and escaping rules](docs/sources/reference/components/prometheus/prometheus.alertmanager.transform.md#string-labels-for-restricted-schemas)
for details.

## Native Live Debugging

All six Alertmanager components support Alloy's existing Live Debugging UI.
Add this top-level block to each Alloy configuration you want to inspect:

```alloy
livedebugging {
  enabled = true
}
```

Run Alloy with `--feature.community-components.enabled`, open its web UI, select
a component, and start **Live debugging** before sending a test alert. Use a
100% sampling rate when comparing a particular alert across the pipeline.

| Component | Values visible in Live Debugging |
| --- | --- |
| `receive` | Decoded webhook and each typed alert forwarded downstream. |
| `transform` | Input typed alert and the actual generated JSON. |
| `http` | Outgoing HTTP request, response or transport failure, and successfully delivered body. |
| `http_receive` | Incoming method, path, sanitized headers, body size, actual body, and decoded alert. |
| `decode` | Input JSON and the resulting typed alert, including mapped labels. |
| `write` | Typed alert admitted to the queue and the final Alertmanager API v2 request body. |

For a `TestAlert`, follow its labels through `receive`, inspect the custom JSON
in `transform`, compare that body in `http` and `http_receive`, then check that
`decode` and `write` show the expected `labels.alertname` and severity. Typed
entries include labels, annotations, status, timestamps, and generator URL.
JSON bodies preserve their original whitespace. Entries use the native text
stream, with readable JSON for alert values.

HTTP request entries include the method, destination URL, path, sanitized
application headers, body size, and zero-based retry attempt. Responses include
status, duration, and up to 4096 body bytes, with potentially truncated or
incomplete bodies marked. Transport failures show safe diagnoses, including
unknown certificate authority errors. Transformation failures use normal
component logging and error propagation.

Authentication and custom header values are redacted. Only `Content-Type`,
`Content-Length`, `Accept`, and `Retry-After` header values are displayed.
URL user information and fragments are omitted, and query values are redacted.
Response bodies are omitted when endpoint authentication, proxy settings,
custom headers, or URL credentials/query parameters could be reflected back.
Request entries describe the logical request before HTTP client wrappers run;
transport-added headers and redirects aren't captured separately. Application
bodies remain unchanged, so the stream contains the alert data you send.
See the [HTTP debugging reference](docs/sources/reference/components/prometheus/prometheus.alertmanager.http.md#debug-information)
for details.

Alloy's native service handles subscriptions, sampling, and stream delivery.
Values are formatted lazily, and inactive debugging avoids payload copies made
solely for inspection. Debugging preserves the existing queues, retries,
batching, receiver interfaces, and component graph behavior.

## Delivery and persistence

An incoming HTTP success means downstream receivers accepted the alerts; it
doesn't confirm delivery to the final Alertmanager. Queues apply backpressure,
and senders retry retryable delivery failures. Partial fan-out and retries can
produce duplicate deliveries.

Both senders use memory queues by default. The custom JSON `http` queue remains
memory-only. The `write` component supports optional persistence for pending
alerts and firing/resolved refresh state. To enable it, add this block inside
`prometheus.alertmanager.write` and keep Alloy's storage directory across restarts:

```alloy
queue_config {
  persistent = true
}
```

Persistent delivery is at least once while failures remain retryable and storage
survives. Terminal failures can still discard a batch. Refer to the
[durable delivery reference](docs/sources/reference/components/prometheus/prometheus.alertmanager.write.md#durable-delivery)
for disk limits, retry behavior, and restart requirements.

The writer periodically refreshes firing alerts, expires stale firing state,
and retains resolved alerts for a configured period. Refer to
[alert refresh semantics](docs/sources/reference/components/prometheus/prometheus.alertmanager.write.md#alert-refresh-semantics)
when choosing timeouts for your source notification cadence.

## Build and verify locally

Install the tools pinned in `mise.toml`, then build Alloy from the repository root:

```sh
mise install
mise exec -- make alloy
```

Run focused tests for the shared alert contract and all six components:

```sh
mise exec -- go test -race -tags=nodocker ./internal/component/common/alertmanager ./internal/component/prometheus/alertmanager/... ./internal/service/livedebugging
```

Run the repository lint checks with `mise exec -- make lint`. The focused tests
cover native debug registration, values across all six components, HTTP retries
and redaction, and delivery with debugging enabled and unused.

The [schema-firewall integration test](integration-tests/alertmanager-schema-firewall/README.md)
exercises webhook ingestion, API v2 payload validation, multi-alert forwarding,
and destination outage recovery with real Alertmanager containers.

## Update and release

Use the repository workflows to merge upstream changes and build distributable
binaries and container images.

### Update to the latest Alloy source

1. Open **Actions** in this repository.
2. Select **Update Alloy to latest**.
3. Select **Run workflow**, leave the upstream branch as `main`, and run it.

The workflow merges the latest `grafana/alloy` source and runs race-enabled
tests for the shared alert contract and `receive`/`write` packages before it updates this repository's
`main` branch. A merge conflict or failed check leaves `main` unchanged. The
workflow summary records the exact upstream commit that was incorporated.
The workflow currently doesn't test the four custom JSON components; run the
full focused command above to cover them. The repository's `.github/workflows` directory is intentionally preserved so
upstream Grafana automation doesn't replace these custom workflows or block the
repository token from pushing the update.

This update workflow deliberately doesn't perform a complete Alloy build: that
can exhaust a standard GitHub-hosted runner. Use **Build custom release files**
after updating to perform the complete build and download the resulting binary
bundle and container image.

### Make the release files

The easiest option is **Actions** > **Build custom release files** > **Run
workflow**. When it finishes, download the `alloy-alertmanager-pipeline-*`
artifact from the workflow run.

To make the files locally, install Git, Docker with BuildKit (or Podman), GNU
tar, and `sha256sum`, then run:

```bash
git clone https://github.com/oriraz1607/alloy-alertmanager-pipeline.git
cd alloy-alertmanager-pipeline
CONTAINER_ENGINE=docker ./scripts/build-custom-release.sh
```

Use `CONTAINER_ENGINE=podman` to build with Podman. Set `VERSION` to override
the embedded version and filenames:

```bash
VERSION=v1.20.0-alertmanager-pipeline.1 \
  CONTAINER_ENGINE=podman \
  ./scripts/build-custom-release.sh
```

The script creates `dist-custom/` containing:

* `alloy-<version>-rhel-amd64.tar.gz` — the Alloy binary, example
  configuration, systemd unit, Kubernetes example, build information, and
  license.
* `alloy-alertmanager-pipeline-<version>-linux-amd64.docker-image.tar` — an
  image archive loadable with `docker load` or `podman load`.
* `SHA256SUMS` — checksums for both files.

Verify and load the results with:

```bash
cd dist-custom
sha256sum --check SHA256SUMS
docker load --input alloy-alertmanager-pipeline-*-linux-amd64.docker-image.tar
```

The produced binary is Linux AMD64 and dynamically linked against glibc. The
packaging workflow smoke-tests it in Red Hat UBI 8. The container image itself
uses Alloy's upstream Ubuntu runtime image.

## Upstream project and contributing

This project is based on [Grafana Alloy](https://github.com/grafana/alloy) and
retains its [Apache 2.0 license](LICENSE).
Refer to the [upstream Alloy documentation](https://grafana.com/docs/alloy/latest/)
for the base collector and the local component references above for this fork's
Alertmanager additions.

For installation examples, refer to the [custom bundle guide](packaging/custom/README.md).
For development conventions, refer to the [contributing guide](docs/developer/contributing.md).
