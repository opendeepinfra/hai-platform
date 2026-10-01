#!/usr/bin/env bash
#
# verify_vms.sh
# -------------
# Reports the current state of the Multipass VMs and the shared directory.

set -euo pipefail
HOST="${HOST:-fireflyer@192.168.100.103}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
"${SSH[@]}" 'export PATH=/snap/bin:$PATH
echo "== multipass list =="; multipass list
echo
echo "== /nfs-shared on host =="; ls -ld /nfs-shared
echo
for vm in vm-01 vm-02 vm-03; do
  echo "== $vm /nfs-shared =="
  multipass exec "$vm" -- ls -ld /nfs-shared 2>&1 || true
done
echo
echo "== write test from vm-01 =="
multipass exec vm-01 -- sh -c "echo hello-from-vm01 > /nfs-shared/probe.txt && cat /nfs-shared/probe.txt" 2>&1 || true
echo "== host sees it =="
ls -l /nfs-shared/probe.txt 2>&1 || true
'