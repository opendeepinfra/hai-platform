#!/usr/bin/env bash
#
# destroy.sh
# ----------
# Tears down the Kubernetes (MicroK8s) cluster on Multipass VMs that was
# created by create.sh. Uses `terraform destroy` (which runs the destroy-time
# provisioners: unmount + multipass delete --purge on every node).
#
# Usage:
#   ./destroy.sh

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
TF_DIR="/opt/terraform/k8s-ha"

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

log "Restore: destroy Multipass VMs via Terraform on $HOST"
"${SSH[@]}" "cd $TF_DIR && export PATH=/snap/bin:\$PATH && terraform destroy -auto-approve" \
  || die "terraform destroy failed; check the remote host state"

log "Post-destroy verification"
"${SSH[@]}" 'export PATH=/snap/bin:$PATH; multipass list 2>/dev/null || echo "(no multipass VMs left)"' \
  || true

echo
log "Destroy complete. Environment restored (cluster VMs gone, /nfs-shared left in place)."
exit 0