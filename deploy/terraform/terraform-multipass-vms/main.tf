# Terraform configuration for managing Multipass VMs on the host
# where `terraform` itself is executed (fireflyer@192.168.100.103).
#
# The `null` provider's local-exec provisioners wrap the `multipass` CLI,
# which must be present in PATH (under /snap/bin/ on Ubuntu).
#
# Usage (run on the host that runs Multipass):
#   terraform init
#   terraform apply -auto-approve
#   terraform destroy -auto-approve   # to tear the VMs down

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
  # Directory the VMs see (and which exists on the host as the real share).
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

variable "vms" {
  type = list(object({
    name = string
    cpus = number
    mem  = string
    disk = string
  }))
  default = [
    { name = "vm-01", cpus = 2, mem = "2G", disk = "10G" },
    { name = "vm-02", cpus = 2, mem = "2G", disk = "10G" },
    { name = "vm-03", cpus = 2, mem = "2G", disk = "10G" },
  ]
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

# Create each Multipass VM.
resource "null_resource" "vm" {
  count = length(var.vms)
  triggers = {
    name       = var.vms[count.index].name
    shared_dir = local.shared_dir
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      if multipass info "${var.vms[count.index].name}" >/dev/null 2>&1; then
        echo "VM ${var.vms[count.index].name} already exists"
        if [ "$(multipass info --format csv "${var.vms[count.index].name}" | tail -n1 | cut -d, -f1)" = "Stopped" ]; then
          multipass start "${var.vms[count.index].name}"
        fi
        exit 0
      fi
      multipass launch ${var.image} \
        --name "${var.vms[count.index].name}" \
        --cpus ${var.vms[count.index].cpus} \
        --memory ${var.vms[count.index].mem} \
        --disk ${var.vms[count.index].disk}
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      multipass unmount "${self.triggers.name}:${self.triggers.shared_dir}" 2>/dev/null || true
      if multipass info "${self.triggers.name}" >/dev/null 2>&1; then
        multipass delete --purge "${self.triggers.name}"
      fi
    EOT
  }
}

# Share the host directory into each VM.
resource "null_resource" "mount" {
  count = length(var.vms)
  triggers = {
    vm           = var.vms[count.index].name
    shared_dir   = local.shared_dir
    mount_source = var.mount_source
  }

  depends_on = [null_resource.vm]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      export PATH=/snap/bin:$PATH
      # Ensure the target directory exists inside the VM.
      multipass exec "${self.triggers.vm}" -- sudo mkdir -p "${self.triggers.shared_dir}"
      # Re-mount if not already mounted (multipass info lists mounts).
      if ! multipass info "${self.triggers.vm}" | grep -q "${self.triggers.shared_dir}"; then
        multipass mount "${self.triggers.mount_source}" "${self.triggers.vm}:${self.triggers.shared_dir}"
      fi
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = "export PATH=/snap/bin:$PATH; multipass unmount '${self.triggers.vm}:${self.triggers.shared_dir}' || true"
  }
}