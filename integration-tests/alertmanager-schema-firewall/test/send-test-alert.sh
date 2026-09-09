#!/usr/bin/env bash
set -euo pipefail

name="${1:-AlloyForwardingTest}"
run_id="${2:-manual-$(date +%s)}"
starts_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ends_at="$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)"

python3 - "$name" "$run_id" "$starts_at" "$ends_at" <<'PY' |
import json, sys
name, run_id, starts_at, ends_at = sys.argv[1:]
print(json.dumps([{
    "labels": {"alertname": name, "instance": "integration-test", "source": "alertmanager-a", "test_group": run_id, "test_run_id": run_id},
    "annotations": {"summary": "End-to-end Alloy forwarding test"},
    "startsAt": starts_at,
    "endsAt": ends_at,
    "generatorURL": "http://integration-test.local/alloy"
}]))
PY
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @- http://127.0.0.1:19093/api/v2/alerts
