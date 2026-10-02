#!/usr/bin/env bash
#
# verify.sh
# ---------
# Quick read-only health check of Hai Platform on the existing cluster:
# nodes, hai-platform pods, services, ingress, and the public entrypoint.

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
NAMESPACE="${NAMESPACE:-hai-platform}"

"${SSH[@]}" "set -e
  echo '===== nodes ====='
  sudo kubectl get nodes -o wide
  echo
  echo '===== hai-platform pods ====='
  sudo kubectl get pods -n $NAMESPACE -o wide
  echo
  echo '===== services ====='
  sudo kubectl get svc -n $NAMESPACE
  echo
  echo '===== ingress ====='
  sudo kubectl get ingress -n $NAMESPACE 2>/dev/null || true
  echo
  echo '===== hai-cli whoami ====='
  sudo hai-cli whoami 2>&1 || true
"

exit 0