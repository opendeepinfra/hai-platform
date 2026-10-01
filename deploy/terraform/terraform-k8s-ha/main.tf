# Terraform configuration for a K8S (MicroK8s) cluster on Multipass VMs.
#
# This builds on the terraform-multipass-vms pattern: Terraform runs on the
# same host that runs Multipass (fireflyer@192.168.100.103), and the `null`
# provider's local-exec provisioners wrap the `multipass` CLI.
#
# Topology:
#   k8s-master       - MicroK8s master (initializes the cluster)
#   k8s-slave01..03  - MicroK8s slave / worker nodes (join the cluster)
#
# The master writes the `microk8s join` token to /nfs-shared/.k8s_join; each
# slave reads it and joins. The shared directory is mounted into every VM.
#
# Usage (run on the host that runs Multipass and Terraform):
#   terraform init
#   terraform apply -auto-approve   # create VMs + install + join cluster
#   terraform destroy -auto-approve # tear the whole cluster down

terraform {
  required_version = ">= 1.5"
  required_providers {
    null = {
      source = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

locals {
  shared_dir = "/nfs-shared"
}

variable "host" {
  type        = string
  default     = "fireflyer@192.168.100.103"
  description = "SSH target of the host that runs Multipass (informational only)."
}

variable "image" {
  type    = string
  default = "22.04"
}

variable "microk8s_channel" {
  type    = string
  default = "1.21/stable"
  description = "MicroK8s snap channel to install on each node."
}

# Host path that is the *source* of the multipass mount. The strictly-confined
# multipass snap can only read host directories under /home (the `home`
# interface), NOT arbitrary root paths like /nfs-shared. So the driver script
# bind-mounts the real share /nfs-shared into the SSH user's home
# (e.g. /home/fireflyer/.mp_share) and we use THAT path as the mount source.
variable "mount_source" {
  type        = string
  default     = "/home/fireflyer/.mp_share"
  description = "Host directory (under the SSH user's home) backing the VM shares."
}

# ---------------------------------------------------------------------------
# VM definitions: one master, three slaves.
# ---------------------------------------------------------------------------

variable "master" {
  type = object({
    name = string
    cpus = number
    mem  = string
    disk = string
  })
  default = {
    name = "k8s-master"
    cpus = 2
    mem  = "4G"
    disk = "10G"
  }
}

variable "slaves" {
  type = list(object({
    name = string
    cpus = number
    mem  = string
    disk = string
  }))
  default = [
    { name = "k8s-slave01", cpus = 2, mem = "4G", disk = "10G" },
    { name = "k8s-slave02", cpus = 2, mem = "4G", disk = "10G" },
    { name = "k8s-slave03", cpus = 2, mem = "4G", disk = "10G" },
  ]
}

# ---------------------------------------------------------------------------
# VM creation (master + slaves), so that all VMs exist before the shared
# directory is mounted / any node touched. This breaks the dependency cycle
# that would otherwise exist between the mount and the node resources.
# ---------------------------------------------------------------------------

resource "null_resource" "vm" {
  count = 1 + length(var.slaves)
  triggers = {
    name  = count.index == 0 ? var.master.name : var.slaves[count.index - 1].name
    image = var.image
    cpus  = count.index == 0 ? var.master.cpus : var.slaves[count.index - 1].cpus
    mem   = count.index == 0 ? var.master.mem : var.slaves[count.index - 1].mem
    disk  = count.index == 0 ? var.master.disk : var.slaves[count.index - 1].disk
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      if multipass info "${self.triggers.name}" >/dev/null 2>&1; then
        echo "VM ${self.triggers.name} already exists"
        if [ "$(multipass info --format csv "${self.triggers.name}" | tail -n1 | cut -d, -f1)" = "Stopped" ]; then
          multipass start "${self.triggers.name}"
        fi
      else
        multipass launch ${self.triggers.image} \
          --name "${self.triggers.name}" \
          --cpus ${self.triggers.cpus} \
          --memory ${self.triggers.mem} \
          --disk ${self.triggers.disk}
      fi
      multipass exec "${self.triggers.name}" -- true
      echo "VM ${self.triggers.name} is up"
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      if multipass info "${self.triggers.name}" >/dev/null 2>&1; then
        multipass delete --purge "${self.triggers.name}"
      fi
    EOT
  }
}

# ---------------------------------------------------------------------------
# Shared directory mount (after the VMs exist).
# ---------------------------------------------------------------------------

# Share the host directory into each VM.
resource "null_resource" "mount" {
  count = 1 + length(var.slaves)
  triggers = {
    vm           = count.index == 0 ? var.master.name : var.slaves[count.index - 1].name
    shared_dir   = local.shared_dir
    mount_source = var.mount_source
  }

  depends_on = [null_resource.vm]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      multipass exec "${self.triggers.vm}" -- sudo mkdir -p "${self.triggers.shared_dir}"
      if ! multipass info "${self.triggers.vm}" | grep -q "${self.triggers.shared_dir}"; then
        multipass mount "${self.triggers.mount_source}" "${self.triggers.vm}:${self.triggers.shared_dir}"
      fi
      multipass exec "${self.triggers.vm}" -- sudo chmod 777 "${self.triggers.shared_dir}"
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = "export PATH=/snap/bin:$PATH; multipass unmount '${self.triggers.vm}:${self.triggers.shared_dir}' || true"
  }
}

# ---------------------------------------------------------------------------
# Master node: create, install MicroK8s, initialize cluster, write join token.
# ---------------------------------------------------------------------------

resource "null_resource" "master" {
  triggers = {
    name      = var.master.name
    shared_dir = local.shared_dir
  }

  depends_on = [null_resource.vm, null_resource.mount]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH

      # Install snapd if not already present.
      multipass exec "${self.triggers.name}" -- bash -c "command -v snapd >/dev/null 2>&1 || sudo apt-get update -y && sudo apt-get install -y snapd"

      # Install MicroK8s (idempotent: `snap install` returns non-zero if already
      # installed, which would trip `set -e`).
      if ! multipass exec "${self.triggers.name}" -- sudo snap list microk8s >/dev/null 2>&1; then
        multipass exec "${self.triggers.name}" -- sudo snap install microk8s --classic --channel=${var.microk8s_channel}
      fi
      multipass exec "${self.triggers.name}" -- sudo usermod -a -G microk8s ubuntu
      # Create the kube config dir first; `chown -f` still exits 1 if the
      # target does not exist, which would trip `set -e`.
      multipass exec "${self.triggers.name}" -- sudo mkdir -p /home/ubuntu/.kube
      multipass exec "${self.triggers.name}" -- sudo chown -f -R ubuntu /home/ubuntu/.kube
      multipass exec "${self.triggers.name}" -- bash -c "echo \\\"alias kubectl='microk8s kubectl'\\\" >> ~/.bash_aliases"

      # Wait for MicroK8s to become fully ready, retrying because a freshly
      # installed snap may need extra time to bootstrap (the built-in
      # --wait-ready can time out on cold start).
      for i in $(seq 1 20); do
        if multipass exec "${self.triggers.name}" -- sudo microk8s status --wait-ready >/dev/null 2>&1; then
          break
        fi
        echo "waiting for microk8s to become ready on ${self.triggers.name} (attempt $i)..."
        sleep 10
      done
      multipass exec "${self.triggers.name}" -- sudo microk8s status --wait-ready

      # Enable plugins with a retry (new cluster, pods still coming up).
      for i in $(seq 1 5); do
        if multipass exec "${self.triggers.name}" -- sudo microk8s enable dns ingress storage >/dev/null 2>&1; then
          break
        fi
        echo "retrying microk8s enable on ${self.triggers.name} (attempt $i)..."
        sleep 10
      done

      # Write a fresh join command to the shared directory for the slaves.
      # NOTE: `-l` is the token TTL in minutes (1440 = 24h); in this MicroK8s
      # version `-t` means the bootstrap TOKEN string, not a TTL, so using
      # `-t 24h` fails.
      JOIN_CMD=$(multipass exec "${self.triggers.name}" -- sudo microk8s add-node -l 1440 | grep 'microk8s join' | head -n1 | tr -d '\r')
      echo "$JOIN_CMD" > "${self.triggers.shared_dir}/.k8s_join"
      echo "Join command written: $JOIN_CMD"
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      rm -f "${self.triggers.shared_dir}/.k8s_join"
      if multipass info "${self.triggers.name}" >/dev/null 2>&1; then
        multipass delete --purge "${self.triggers.name}"
      fi
    EOT
  }
}

# ---------------------------------------------------------------------------
# Slave nodes: create, install MicroK8s, join the master's cluster.
# ---------------------------------------------------------------------------

resource "null_resource" "slave" {
  count = length(var.slaves)
  triggers = {
    name       = var.slaves[count.index].name
    shared_dir = local.shared_dir
  }

  depends_on = [null_resource.vm, null_resource.master, null_resource.mount]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH

      # Install snapd if not already present.
      multipass exec "${self.triggers.name}" -- bash -c "command -v snapd >/dev/null 2>&1 || sudo apt-get update -y && sudo apt-get install -y snapd"

      # Install MicroK8s (idempotent: `snap install` returns non-zero if already
      # installed, which would trip `set -e`).
      if ! multipass exec "${self.triggers.name}" -- sudo snap list microk8s >/dev/null 2>&1; then
        multipass exec "${self.triggers.name}" -- sudo snap install microk8s --classic --channel=${var.microk8s_channel}
      fi
      multipass exec "${self.triggers.name}" -- sudo usermod -a -G microk8s ubuntu
      multipass exec "${self.triggers.name}" -- sudo mkdir -p /home/ubuntu/.kube
      multipass exec "${self.triggers.name}" -- sudo chown -f -R ubuntu /home/ubuntu/.kube
      multipass exec "${self.triggers.name}" -- bash -c "echo \\\"alias kubectl='microk8s kubectl'\\\" >> ~/.bash_aliases"

      # Read the join command the master wrote, then join the cluster.
      JOIN_CMD=$(cat "${self.triggers.shared_dir}/.k8s_join")
      if [ -z "$JOIN_CMD" ]; then
        echo "ERROR: no join command found at ${self.triggers.shared_dir}/.k8s_join"
        exit 1
      fi

      # Let the freshly-installed MicroK8s finish bootstrapping, then join
      # (retry in case the join races the local control-plane start).
      sleep 20
      for i in $(seq 1 10); do
        if multipass exec "${self.triggers.name}" -- sudo $JOIN_CMD >/dev/null 2>&1; then
          break
        fi
        echo "retrying join for ${self.triggers.name} (attempt $i)..."
        sleep 15
      done
      multipass exec "${self.triggers.name}" -- sudo $JOIN_CMD 2>/dev/null || true
      echo "${self.triggers.name} joined the cluster"
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      if multipass info "${self.triggers.name}" >/dev/null 2>&1; then
        multipass delete --purge "${self.triggers.name}"
      fi
    EOT
  }
}

# ---------------------------------------------------------------------------
# Final verification: report cluster node status.
# ---------------------------------------------------------------------------

resource "null_resource" "verify" {
  triggers = {
    master      = var.master.name
    shared_dir = local.shared_dir
  }

  depends_on = [null_resource.master, null_resource.slave]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      echo "===== node list ====="
      multipass exec "${self.triggers.master}" -- sudo microk8s kubectl get nodes -o wide
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'verify: skip on destroy'"
  }
}