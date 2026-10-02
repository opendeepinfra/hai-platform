# Terraform + Multipass: Three VMs sharing a directory

This directory contains a self-contained task that creates **three Multipass
virtual machines** on a remote Ubuntu host using **Terraform**, and shares the
host directory `/nfs-shared` into all three VMs.

## Layout

| File | Purpose |
|------|---------|
| `main.tf` | Terraform config. Creates the VMs and the shared mount via the `null` provider + `local-exec` wrapping the `multipass` CLI. |
| `create_vms.sh` | Driver script: provisions Multipass + Terraform on the remote host, runs `terraform apply`, then shares `/nfs-shared`. Run this from your machine. |
| `destroy_vms.sh` | Restore script: `terraform destroy` (unmounts, deletes the VMs). |
| `verify_vms.sh` | Reports VM state and tests the shared directory. |

## Design

* **Controller vs. host.** Terraform runs on the host that runs Multipass
  (`fireflyer@192.168.100.103`) so its `local-exec` provisioners call
  `multipass` directly. The shell scripts here are thin SSH drivers run from
  your workstation.
* **Provider.** The official registry has no first-party *Multipass* provider,
  so it uses `hashicorp/null` with `local-exec`+`destroy` provisioners around
  `multipass launch / mount / unmount / delete`. This is simple and reliable.
* **Sharing.** `/nfs-shared` on the host is `multipass mount`-ed into each VM.
  It is owned by uid 1000 with mode 775 so both the host user and the VM's
  default user (also uid 1000) can read/write the same files.
* **Defaults.** 3 VMs `vm-01..03`, image `22.04`, 2 CPUs / 2 GB / 10 GB each.
  Overridable by editing the `vms` variable in `main.tf`.

## Usage

Defaults target `fireflyer@192.168.100.103`. Override with `HOST=user@ip`.

```bash
cd terraform-multipass-vms

# Create (installs tools if needed, runs apply, shares and verifies)
./create_vms.sh

# Inspect state / test the shared dir
./verify_vms.sh

# Tear down (restore environment)
./destroy_vms.sh
```

## Requirements

* Passwordless SSH (`ssh fireflyer@192.168.100.103`) and passwordless `sudo`
  on the remote host.
* Remote host: Ubuntu (22.04), `snap`, ~a few GB free disk/RAM.
* Internet access on the remote host (snap install and Terraform download).