#!/bin/sh
# promtool expects native rule files. Test the groups from the deployable CRDs.
set -eu
cd "$(dirname "$0")"
workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT
for role in source central; do
  sed -n '/^spec:$/,$p' "$role-prometheus-rule.yaml" | sed '1d;s/^  //' > "$workdir/$role.rules.yaml"
done
cp rules.test.yaml "$workdir/"
promtool check rules "$workdir/source.rules.yaml" "$workdir/central.rules.yaml"
promtool test rules "$workdir/rules.test.yaml"
