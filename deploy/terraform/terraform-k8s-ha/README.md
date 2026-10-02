# terraform-k8s-ha

Create a Kubernetes (MicroK8s) cluster on Multipass VMs using Terraform:

- **k8s-master** — MicroK8s master, initializes the cluster
- **k8s-slave01 / 02 / 03** — MicroK8s slave / worker nodes, join the cluster

All four VMs share the host directory `/nfs-shared` on
`fireflyer@192.168.100.103`.

## Layout

| File          | Purpose                                                            |
|---------------|--------------------------------------------------------------------|
| `main.tf`     | Terraform config: VMs, shared mount, MicroK8s install + join     |
| `create.sh`   | Local driver: provisioning + `terraform apply` (idempotent)      |
| `destroy.sh`  | Local driver: `terraform destroy` to tear the cluster down       |
| `verify.sh`   | Local driver: report VM / cluster state                          |
| `.gitignore`  | Exclude local Terraform caches                                  |

## Design

This builds on the `terraform-multipass-vms` pattern. Terraform runs on the
host that runs Multipass (not on your machine), so its `local-exec`
provisioners can call the `multipass` CLI directly. The `hashicorp/null`
provider wraps the CLI.

### Confined multipass daemon (the important fix)

The strictly-confined multipass snap daemon can only read host directories
**under `/home`** (the `home` interface), not arbitrary root paths like
`/nfs-shared`. So `create.sh`:

1. bind-mounts `/nfs-shared` into the SSH user's home as `.mp_share`
   (persisted in `/etc/fstab`),
2. uses that home path as the `multipass mount` **source** while the VM still
   sees `/nfs-shared`.

UID 1000 (host `fireflyer`, VM user `ubuntu`) maps to `default`, so both
sides can read/write the share.

### MicroK8s cluster bootstrap

The `null_resource.master` provisioner initializes the cluster and writes a
fresh `microk8s join` token to `/nfs-shared/.k8s_join`. Each
`null_resource.slave` provisioner (count = 3) reads that token and joins the
cluster. Resource `depends_on` ensures the master is ready before slaves
join. A final `null_resource.verify` reports `microk8s kubectl get nodes`.

## Usage

Run from your machine (drives the remote host over SSH):

```bash
./create.sh                 # defaults: fireflyer@192.168.100.103
HOST=user@ip ./create.sh    # different host

./verify.sh                 # check VM + cluster state
./destroy.sh                # tear the cluster down (restores environment)
```

`create.sh` installs Multipass and Terraform on the remote host if missing,
so it is safe to run from a fresh machine.

## Requirements

- Your machine: `ssh`, `scp`, `bash`
- Remote host: passwordless `sudo` for the SSH user, ~80 GB free disk