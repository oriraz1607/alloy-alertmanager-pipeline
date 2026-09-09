# Alloy Alertmanager pipeline bundle

This bundle contains a custom Linux AMD64 Grafana Alloy binary with these
Alertmanager community components:

- `prometheus.alertmanager.receive`
- `prometheus.alertmanager.transform`
- `prometheus.alertmanager.http`
- `prometheus.alertmanager.http_receive`
- `prometheus.alertmanager.decode`
- `prometheus.alertmanager.write`

Together they support strongly typed Alertmanager alerts inside Alloy and a
configuration-defined JSON schema and HTTP path at an Alloy-to-Alloy boundary.
Start Alloy with `--feature.community-components.enabled` to use them.

## Load and run the container image

The release includes a Docker-compatible image archive next to this bundle.
Load it with:

```bash
docker load -i alloy-alertmanager-pipeline-<VERSION>-linux-amd64.docker-image.tar
```

Use the image name printed by `docker load`. Mount your Alloy configuration and
enable the community components when starting it:

```bash
docker run --rm \
  -v "$PWD/config.alloy:/etc/alloy/config.alloy:ro" \
  -p 12345:12345 \
  localhost/alloy-alertmanager-pipeline:<VERSION> \
  run \
  --feature.community-components.enabled \
  --storage.path=/var/lib/alloy/data \
  /etc/alloy/config.alloy
```

Replace the example port with the listener configured in `config.alloy`.

## Install on a RHEL-compatible host

```bash
sudo useradd --system --home-dir /var/lib/alloy --shell /sbin/nologin alloy
sudo install -m 0755 alloy /usr/local/bin/alloy
sudo install -d -m 0755 /etc/alloy /var/lib/alloy/data
sudo chown -R alloy:alloy /var/lib/alloy
sudo install -m 0644 config.alloy /etc/alloy/config.alloy
sudo install -m 0644 alloy.service /etc/systemd/system/alloy.service
sudo systemctl daemon-reload
sudo systemctl enable --now alloy
```

Edit `/etc/alloy/config.alloy` before starting the service. The example listens
on all interfaces at port 9095 and sends alerts to
`http://alertmanager:9093`. Protect the listener with network controls or mTLS.

The source Alertmanager can use `alertmanager-source-example.yml` as a starting
point. Set `send_resolved: true` so resolved alerts reach the destination.

Check the service with:

```bash
systemctl status alloy
journalctl -u alloy -f
```

The queue and alert refresh state are held in memory and do not survive an
Alloy restart.
