#!/usr/bin/env bash
set -euo pipefail

test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$test_dir"
if command -v docker >/dev/null 2>&1; then
  compose=(docker compose -f docker-compose.yml)
elif command -v podman >/dev/null 2>&1 && command -v podman-compose >/dev/null 2>&1; then
  compose=(podman-compose -f docker-compose.yml)
else
  echo "FAIL: Docker Compose or Podman Compose is required" >&2
  exit 1
fi
run_id="run-$(date +%s)-$$"

cleanup() {
  mkdir -p evidence
  "${compose[@]}" logs --no-color >evidence/docker-compose.log 2>&1 || true
  for service in alloy alertmanager-a schema-firewall alertmanager-b; do
    "${compose[@]}" logs --no-color "$service" >"evidence/$service.log" 2>&1 || true
  done
  curl -fsS http://127.0.0.1:19094/api/v2/alerts >evidence/alertmanager-b-alerts.json 2>/dev/null || true
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
wait_http() {
  local url="$1"
  for _ in $(seq 1 60); do curl -fsS "$url" >/dev/null 2>&1 && return 0; sleep 1; done
  fail "timed out waiting for $url"
}
has_alert() {
  local name="$1"
  curl -fsS http://127.0.0.1:19094/api/v2/alerts | python3 -c 'import json,sys; name=sys.argv[1]; data=json.load(sys.stdin); raise SystemExit(0 if any(a.get("labels",{}).get("alertname")==name for a in data) else 1)' "$name"
}
wait_alert() {
  local name="$1"
  for _ in $(seq 1 60); do has_alert "$name" && return 0; sleep 1; done
  fail "destination did not contain $name"
}

rm -rf evidence
mkdir -p evidence
"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" up -d --build --wait
wait_http http://127.0.0.1:19093/-/ready
wait_http http://127.0.0.1:19094/-/ready
wait_http http://127.0.0.1:18080/health
wait_http http://127.0.0.1:12345/-/ready

echo "Test 1: firewall accepts and forwards a valid v2 payload"
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @test/fixtures/valid-api-v2-alerts.json http://127.0.0.1:18080/api/v2/alerts >/dev/null
wait_alert FirewallPositiveControl

echo "Test 2: firewall rejects the generic webhook envelope"
status="$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data-binary @test/fixtures/invalid-generic-webhook.json http://127.0.0.1:18080/api/v2/alerts)"
[[ "$status" == 422 ]] || fail "generic envelope returned HTTP $status"
has_alert MustNeverReachDestination && fail "rejected alert reached destination"

echo "Test 3: firewall rejects the wrong path"
status="$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data '{}' http://127.0.0.1:18080/webhook)"
[[ "$status" == 403 ]] || fail "wrong path returned HTTP $status"

echo "Tests 4 and 5: full Alloy path and payload transformation"
test/send-test-alert.sh AlloyForwardingTest "$run_id" >/dev/null
wait_alert AlloyForwardingTest
python3 - "$run_id" <<'PY'
import json, sys
body = json.load(open("evidence/firewall-last-accepted-body.json"))
assert isinstance(body, list) and body
assert all("labels" in alert for alert in body)
assert any(alert["labels"].get("test_run_id") == sys.argv[1] for alert in body)
for forbidden in ("receiver", "status", "groupLabels", "commonLabels", "alerts"):
    assert all(forbidden not in alert for alert in body)
PY

echo "Test 6: destination outage causes retries and eventual recovery"
"${compose[@]}" stop alertmanager-b >/dev/null
test/send-test-alert.sh AlloyRetryTest "$run_id-retry" >/dev/null
for _ in $(seq 1 30); do
  "${compose[@]}" logs --no-color schema-firewall 2>&1 | grep -q 'destination_unavailable' && break
  sleep 1
done
"${compose[@]}" logs --no-color schema-firewall 2>&1 | grep -q 'destination_unavailable' || fail "firewall did not observe destination failure"
"${compose[@]}" start alertmanager-b >/dev/null
wait_http http://127.0.0.1:19094/-/ready
wait_alert AlloyRetryTest

echo "Test 7: multiple alerts in one notification group survive forwarding"
starts_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ends_at="$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)"
python3 - "$run_id-group" "$starts_at" "$ends_at" <<'PY' |
import json, sys
group, starts, ends = sys.argv[1:]
print(json.dumps([{
  "labels": {"alertname": name, "instance": "integration-test", "source": "alertmanager-a", "test_group": group, "test_run_id": group},
  "startsAt": starts, "endsAt": ends
} for name in ("AlloyForwardingTestA", "AlloyForwardingTestB")]))
PY
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @- http://127.0.0.1:19093/api/v2/alerts >/dev/null
wait_alert AlloyForwardingTestA
wait_alert AlloyForwardingTestB

"${compose[@]}" logs --no-color schema-firewall | grep -q 'ALLOW POST /api/v2/alerts' || fail "missing ALLOW evidence"
"${compose[@]}" logs --no-color schema-firewall | grep -q 'DENY POST /api/v2/alerts' || fail "missing schema DENY evidence"
"${compose[@]}" logs --no-color schema-firewall | grep -q 'DENY POST /webhook' || fail "missing path DENY evidence"
echo "PASS: all Alertmanager schema-firewall tests completed"
