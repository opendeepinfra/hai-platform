#!/usr/bin/env bash
#
# verify.sh
# ---------
# Reports the current state of the Multipass VMs and the K8S (MicroK8s)
# cluster: VM list, shared /nfs-shared, and cluster node status from the
# master.

set -euo pipefail
HOST="${HOST:-fireflyer@192.168.100.103}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
"${SSH[@]}" 'export PATH=/snap/bin:$PATH
echo "== multipass list =="; multipass list
echo
echo "== /nfs-shared on host =="; ls -ld /nfs-shared
echo
for vm in k8s-master k8s-slave01 k8s-slave02 k8s-slave03; do
  echo "== $vm /nfs-shared =="
  multipass exec "$vm" -- ls -ld /nfs-shared 2>&1 || true
done
echo
echo "== cluster nodes (from master) =="
multipass exec k8s-master -- sudo microk8s kubectl get nodes -o wide 2>&1 || \
  echo "(cluster not reachable / MicroK8s not online)"
'