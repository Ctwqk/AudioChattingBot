#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_CONSTRUCTURE_ROOT="$(cd "$SCRIPT_DIR/../../../k8s-constructure" && pwd)"
CLUSTER_DEPLOY_SCRIPT="$K8S_CONSTRUCTURE_ROOT/scripts/deploy-offloaded-services.sh"

cat >&2 <<EOF
[compat] offload_to_macs.sh is now a compatibility wrapper.
[compat] Prefer using:
[compat]   $CLUSTER_DEPLOY_SCRIPT
EOF

exec "$CLUSTER_DEPLOY_SCRIPT" "$@"
