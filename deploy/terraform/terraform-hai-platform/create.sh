#!/usr/bin/env bash
#
# create.sh
# ---------
# Deploys Hai Platform onto an EXISTING MicroK8s cluster (on host 103) by
# driving Terraform over SSH. References src/microk8s_up_hai_platfrom.sh but
# adapted so that:
#   * kubeconfig is a correct, workable one (apiserver 10.205.52.154:16443),
#     replacing the stale foreign config on the host;
#   * MetalLB + an IPAddressPool are provisioned for the LoadBalancer;
#   * node names are mapped to the actual VM node names (not fireflyer-xxxx);
#   * `hai-up up` runs on host 103 (which reaches the VM subnet directly).
#
# Usage:
#   ./create.sh                      # defaults (fireflyer@192.168.100.103)
#   HOST=user@ip ./create.sh
#   TF_ADMIN_TOKEN=... ./create.sh   # pass the admin bearer token
#   TF_CLUSTER_CA_B64=... ./create.sh
#
# The two credentials (admin bearer token, base64 CA) can be supplied via
# env vars, terraform.tfvars in this directory, or -var on the command line.
# They are NOT hard-coded.

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
SCP=(scp -o BatchMode=yes -o ConnectTimeout=10)
TF_VERSION="${TF_VERSION:-1.9.8}"
TF_DIR="/opt/terraform/hai-platform"   # staging dir on host 103

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
err() { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; }
die() { err "$*"; exit 1; }

# ---------------------------------------------------------------------------
log "0) Sanity: SSH to $HOST"
"${SSH[@]}" 'echo OK; sudo -n true 2>/dev/null && echo "passwordless sudo OK" || echo "WARN: sudo may prompt"' >/dev/null \
  || die "Cannot reach $HOST over SSH"

# ---------------------------------------------------------------------------
log "1) Ensure hai-up / hai-cli present"
"${SSH[@]}" 'which hai-up hai-cli >/dev/null 2>&1 && echo "hai-up/hai-cli: present" || echo "WARN: hai-up/hai-cli missing (run install_hai_cli.sh first)"'

# ---------------------------------------------------------------------------
log "2) Ensure Terraform on $HOST if missing"
if ! "${SSH[@]}" 'which terraform >/dev/null 2>&1'; then
  "${SSH[@]}" "set -e
    cd /tmp
    ARCH=\$(uname -m); case \"\$ARCH\" in x86_64|amd64) A=amd64;; aarch64|arm64) A=arm64;; *) echo unknown; exit 1;; esac
    F=terraform_${TF_VERSION}_linux_\$A.zip
    curl -fsSLo \$F https://releases.hashicorp.com/terraform/${TF_VERSION}/\$F
    unzip -o \$F -d /tmp/tfbin >/dev/null
    sudo install -m 0755 /tmp/tfbin/terraform /usr/local/bin/terraform
    rm -f \$F; rm -rf /tmp/tfbin"
fi
echo -n "Terraform: "; "${SSH[@]}" 'terraform version | head -n1'

# ---------------------------------------------------------------------------
log "3) Stage main.tf on $HOST"
"${SSH[@]}" "sudo mkdir -p $TF_DIR && sudo chown -R \$(id -u):\$(id -g) $TF_DIR"
"${SCP[@]}" main.tf "$HOST:$TF_DIR/main.tf"

# ---------------------------------------------------------------------------
# Build a tfvars file with the credentials (from env var or this directory).
# ---------------------------------------------------------------------------
TFVARS_LOCAL="./terraform.tfvars"
VAR_FILE=""
admin_token="${TF_ADMIN_TOKEN:-${ADMIN_TOKEN:-}}"
ca_b64="${TF_CLUSTER_CA_B64:-${CLUSTER_CA_B64:-}}"

if [ -f "$TFVARS_LOCAL" ]; then
  log "Using local $TFVARS_LOCAL for vars."
  "${SCP[@]}" "$TFVARS_LOCAL" "$HOST:$TF_DIR/terraform.tfvars"
  VAR_FILE="-var-file=terraform.tfvars"
elif [ -n "$admin_token" ] && [ -n "$ca_b64" ]; then
  log "Building terraform.tfvars from env vars."
  {
    echo "admin_token  = \"$admin_token\""
    echo "cluster_ca_b64 = \"$ca_b64\""
  } > /tmp/tf_hai.tfvars
  "${SCP[@]}" /tmp/tf_hai.tfvars "$HOST:$TF_DIR/terraform.tfvars"
  rm -f /tmp/tf_hai.tfvars
  VAR_FILE="-var-file=terraform.tfvars"
else
  die "No credentials found. Provide terraform.tfvars in this dir OR set TF_ADMIN_TOKEN + TF_CLUSTER_CA_B64 env vars."
fi

# ---------------------------------------------------------------------------
log "4) terraform init + apply on $HOST"
"${SSH[@]}" "cd $TF_DIR && export PATH=/snap/bin:\$PATH && terraform init -input=false"
"${SSH[@]}" "cd $TF_DIR && export PATH=/snap/bin:\$PATH && terraform apply -auto-approve $VAR_FILE"

echo
log "Done. Hai Platform deployed to the existing cluster."
echo "Pod/service status was printed by the verify step above."
exit 0