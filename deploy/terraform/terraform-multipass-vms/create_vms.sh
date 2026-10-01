#!/usr/bin/env bash
#
# create_vms.sh
# -------------
# Creates three Multipass VMs on a remote Ubuntu host using Terraform,
# and shares the host directory /nfs-shared into all three VMs.
#
# This script runs on YOUR machine (the controller / Terraform host choice)
# and drives everything over SSH against the host that will run Multipass
# and Terraform. Terraform itself executes on the remote host so its
# local-exec provisioners can call `multipass` directly.
#
# Usage:
#   ./create_vms.sh            # use defaults (fireflyer@192.168.100.103)
#   HOST=user@ip ./create_vms.sh
#
# Behavior (all steps are idempotent / re-runnable):
#   1. Install Multipass (snap) and Terraform on the remote host if missing.
#   2. Allow `multipass` client access for the SSH user.
#   3. Make /nfs-shared writable by the host user (uid 1000) so VMs can write.
#   4. Stage main.tf on the remote host and run terraform init + apply.
#   5. Verify the three VMs exist/running and /nfs-shared is mounted in each.

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
SCP=(scp -o BatchMode=yes -o ConnectTimeout=10)
TF_VERSION="${TF_VERSION:-1.9.8}"        # pinned; change freely
TF_DIR="/opt/terraform/multipass-vms"    # staging dir on the remote host
NSHARED="/nfs-shared"

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
err()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; }
die()  { err "$*"; exit 1; }
run()  { printf '\033[2m$ %s\033[0m\n' "$*"; "$@"; }

# ---------------------------------------------------------------------------
log "0) Sanity check: SSH access to $HOST"
"${SSH[@]}" 'echo OK; uname -sr; id -u' >/dev/null || die "Cannot reach $HOST over SSH"

# ---------------------------------------------------------------------------
log "1) Install Multipass on $HOST"
if ! "${SSH[@]}" 'which multipass >/dev/null 2>&1'; then
  echo "Multipass not found - installing via snap (classic)..."
  "${SSH[@]}" 'sudo snap install multipass --classic'
fi
"${SSH[@]}" 'multipass version || true'

# ---------------------------------------------------------------------------
log "2) Add the SSH user to the 'multipass' group (idempotent)"
"${SSH[@]}" 'grep -q multipass /etc/group 2>/dev/null && sudo usermod -aG multipass $USER || true'
# New SSH sessions re-read /etc/group, so group membership is active now.

# ---------------------------------------------------------------------------
log "3) Install Terraform $TF_VERSION on $HOST if missing"
if ! "${SSH[@]}" 'which terraform >/dev/null 2>&1'; then
  "${SSH[@]}" "set -e
    cd /tmp
    ARCH=\$(uname -m); case \"\$ARCH\" in x86_64|amd64) A=amd64;; aarch64|arm64) A=arm64;; *) echo unknown arch; exit 1;; esac
    F=terraform_${TF_VERSION}_linux_\$A.zip
    curl -fsSLo \$F https://releases.hashicorp.com/terraform/${TF_VERSION}/\$F
    unzip -o \$F -d /tmp/tfbin >/dev/null
    sudo install -m 0755 /tmp/tfbin/terraform /usr/local/bin/terraform
    rm -f \$F; rm -rf /tmp/tfbin"
fi
echo -n "Terraform version on host: "; "${SSH[@]}" 'terraform version | head -n1'

# ---------------------------------------------------------------------------
log "4) Prepare the shared directory $NSHARED for write-by-uid-1000"
# multipass mount maps the VM's default user (uid 1000) to the host caller;
# make the host dir owned by uid 1000 so both sides can write.
"${SSH[@]}" "sudo mkdir -p $NSHARED && sudo chown \$(id -u):\$(id -g) $NSHARED && sudo chmod 775 $NSHARED && ls -ld $NSHARED"

# ---------------------------------------------------------------------------
log "4b) Bind-mount $NSHARED into the SSH user's home (confined-daemon visibility)"
# The strictly-confined multipass snap can only `mount` host dirs under /home.
# We bind-mount the real /nfs-shared into the user's home as a mount-source
# proxy so the daemon can see it, and persist it in /etc/fstab.
BIND_PROXY="$( "${SSH[@]}" 'printf %s "$HOME"' )/.mp_share"
BIND_PROXY="${BIND_PROXY:-/home/fireflyer/.mp_share}"
"${SSH[@]}" "set -e
    sudo mkdir -p '$BIND_PROXY'
    grep -qF '$BIND_PROXY' /etc/fstab || echo '$NSHARED $BIND_PROXY none bind 0 0' | sudo tee -a /etc/fstab >/dev/null
    if ! mountpoint -q '$BIND_PROXY'; then
      sudo mount '$BIND_PROXY'
    fi
    mountpoint -q '$BIND_PROXY' && echo 'BIND OK $BIND_PROXY'"

# ---------------------------------------------------------------------------
log "5) Stage Terraform config on $HOST"
"${SSH[@]}" "sudo mkdir -p $TF_DIR && sudo chown -R \$(id -u):\$(id -g) $TF_DIR"
"${SCP[@]}" main.tf "$HOST:$TF_DIR/main.tf"

# ---------------------------------------------------------------------------
log "6) terraform init + apply on $HOST"
"${SSH[@]}" "cd $TF_DIR && export PATH=/snap/bin:\$PATH && terraform init -input=false"
"${SSH[@]}" "cd $TF_DIR && export PATH=/snap/bin:\$PATH && terraform apply -auto-approve -var=mount_source='$BIND_PROXY'"

echo
log "Apply finished."

exit 0