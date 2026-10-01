#!/usr/bin/env bash
#
# destroy.sh
# ----------
# Tear down Hai Platform from the existing cluster by running
# `terraform destroy` on host 103. This runs `hai-up down` (the null_resource
# destroy provisioner) and removes the kubeconfig/metallb resources that
# Terraform manages.
#
# Note: it does NOT remove the MicroK8s cluster or the VMs.

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
TF_DIR="/opt/terraform/hai-platform"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

"${SSH[@]}" 'cd '"$TF_DIR"' && export PATH=/snap/bin:$PATH && terraform init -input=false >/dev/null 2>&1 && terraform destroy -auto-approve' \
  || die "terraform destroy failed on $HOST"

log "Done. Hai Platform torn down."
exit 0