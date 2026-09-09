import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request
from jsonschema import Draft202012Validator, FormatChecker

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("schema-firewall")

destination = os.environ.get("DESTINATION_URL", "http://alertmanager-b:9093/api/v2/alerts")
evidence_path = Path(os.environ.get("EVIDENCE_PATH", "/tmp/last-accepted-body.json"))
schema = json.loads(Path("alertmanager-v2-alerts.schema.json").read_text())
validator = Draft202012Validator(schema, format_checker=FormatChecker())


def decision(result, reason, destination_status="-"):
    timestamp = datetime.now(timezone.utc).isoformat()
    log.info("%s %s %s %s reason=%s destination_status=%s", timestamp, result, request.method, request.path, reason, destination_status)


@app.get("/health")
def health():
    return jsonify(status="ok")


@app.route("/api/v2/alerts", methods=["POST"])
def alerts():
    if request.mimetype != "application/json":
        decision("DENY", "content_type_not_allowed")
        return jsonify(error="Content-Type must be application/json"), 403
    try:
        body = request.get_json(force=False, silent=False)
    except Exception:
        decision("DENY", "invalid_json")
        return jsonify(error="invalid JSON"), 400

    errors = sorted(validator.iter_errors(body), key=lambda error: list(error.path))
    if errors:
        decision("DENY", "schema_validation_failed")
        return jsonify(error="schema validation failed", detail=errors[0].message), 422

    encoded = json.dumps(body, indent=2, sort_keys=True).encode()
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_bytes(encoded + b"\n")
    Path("/tmp/last-accepted-body.json").write_bytes(encoded + b"\n")
    try:
        response = requests.post(destination, data=encoded, headers={"Content-Type": "application/json"}, timeout=5)
    except requests.RequestException as error:
        decision("ALLOW", "destination_unavailable")
        return jsonify(error=str(error)), 502
    decision("ALLOW", "schema_valid", response.status_code)
    return Response(response.content, status=response.status_code, content_type=response.headers.get("Content-Type", "text/plain"))


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def deny(path):
    decision("DENY", "method_or_path_not_allowed")
    return jsonify(error="only POST /api/v2/alerts is allowed"), 403


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
