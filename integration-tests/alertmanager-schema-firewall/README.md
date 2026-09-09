# Alertmanager schema-firewall integration test

This environment proves that an Alertmanager generic webhook is accepted by
`prometheus.alertmanager.receive`, converted to Alloy's typed alert value, and
emitted by `prometheus.alertmanager.write` as an Alertmanager v2 alerts API
request. A schema-validating proxy is the only route from Alloy to the
destination Alertmanager.

## Prerequisites

- Docker with the Compose plugin, or Podman with `podman-compose`
- `curl`
- `python3`
- a current local Alloy binary at `../../build/alloy`

Build and run:

```bash
make alloy
integration-tests/alertmanager-schema-firewall/test/run-tests.sh
```

The script starts fresh containers, runs positive and negative firewall
controls, exercises single- and multi-alert forwarding, tests destination
unavailability and recovery, and writes logs and captured payloads to
`evidence/`.

The destination-unavailable case is asynchronous: the receive component has
accepted the collection once the writer's bounded queue accepts it. Delivery
failures then cause writer retries. After Alertmanager B is restarted, the test
requires the queued alert to arrive.
