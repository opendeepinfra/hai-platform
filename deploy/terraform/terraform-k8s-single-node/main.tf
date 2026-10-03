# Terraform configuration —— 把 host 103 初始化成 **单机 K8s 全节点（control-plane + worker）+ GPU**。
#
# 与同目录另外三套的关系：
#   terraform-multipass-vms / terraform-k8s-ha / terraform-hai-platform
#     是「4 台 Multipass VM + MicroK8s + 平台」的老路径（VM 里没有 GPU，也直通不了）。
#   本模块是「路线 A」：**不用 VM**，直接把 103 本机变成单节点集群，从而用上本机的
#     Tesla V100-SXM2-16GB（PCI 82:00.0，驱动 570 / CUDA 12.8，已装 nvidia-container-toolkit）。
#
# 执行方式（沿用既有约定）：Terraform **跑在 host 103 上**，
#   ./create.sh 会把 main.tf 与 files/ 同步到 /opt/terraform/k8s-single-node，
#   然后在 103 上 terraform init + apply；所有 local-exec 都是"在 103 本机执行"。
#
# 前置事实（2026-10-03 实测）：
#   * 103 原本是某 Sealos 集群的 GPU worker：kubelet v1.29.9 + containerd 2.3.3 在跑，
#     但控制面 apiserver.cluster.local(10.103.97.2:6443) 已不可达 → 步骤 02 先复位；
#   * containerd 已带 nvidia 运行时与 CDI 规格，但默认运行时是 runc → 步骤 03 修正；
#   * 平台**不**给任务 Pod 申请 nvidia.com/gpu，GPU 靠 NVIDIA_VISIBLE_DEVICES 限卡
#     → 必须把 CRI 默认运行时设为 nvidia，任务容器才能看到 /dev/nvidia*。
#
# 使用：
#   ./create.sh --preflight-only   # 干跑：只执行只读前置检查
#   ./create.sh                    # 完整初始化
#   ./verify.sh                    # 只读验收（含 GPU 冒烟 Pod）
#   ./destroy.sh                   # 拆集群（保留驱动/containerd/Docker 容器）

terraform {
  required_version = ">= 1.5"
  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

# ---------------------------------------------------------------------------
# 变量
# ---------------------------------------------------------------------------

variable "host" {
  type        = string
  default     = "fireflyer@192.168.100.103"
  description = "运行 Terraform 与承载单节点集群的主机（仅信息性；Terraform 就跑在这台机器上）。"
}

variable "node_ip" {
  type        = string
  default     = "192.168.100.103"
  description = "apiserver 对外地址（advertiseAddress / certSAN）。"
}

variable "node_name" {
  type        = string
  default     = ""
  description = "K8s 节点名；留空则用 hostname（当前为 fireflyer-0003）。"
}

variable "kubernetes_version" {
  type        = string
  default     = "v1.29.9"
  description = "目标 K8s 版本，必须与本机 kubeadm/kubelet 一致。"
}

variable "image_repository" {
  type        = string
  default     = "registry.aliyuncs.com/google_containers"
  description = <<-EOT
    控制面镜像仓库。默认用阿里云镜像（实测 103 拉 kube-apiserver 约 23s，
    registry.k8s.io 同一镜像约 67s，且 dl.k8s.io 下载极慢）。
    阿里云仓库会把 coredns/coredns 压平成 coredns —— kubeadm 用它生成的镜像名正好匹配。
  EOT
}

variable "pod_subnet" {
  type        = string
  default     = "10.244.0.0/16"
  description = "Pod 网段。刻意避开 VM 集群的 calico 10.1.0.0/16。"
}

variable "service_subnet" {
  type        = string
  default     = "10.96.0.0/12"
  description = "Service 网段。刻意避开 VM 集群的 10.152.183.0/24。"
}

variable "bridge_subnet" {
  type        = string
  default     = "10.244.0.0/24"
  description = "bridge CNI 给本节点 Pod 分配的网段（单节点用 pod_subnet 的第一个 /24）。"
}

variable "bridge_cni_version" {
  type        = string
  default     = "0.3.1"
  description = "conflist 的 cniVersion。Ubuntu 22.04 的 containernetworking-plugins 是 0.9.1，用 0.3.1；换成 v1.x 插件可改 1.0.0。"
}

variable "cni_plugin_source" {
  type        = string
  default     = "apt"
  description = "CNI 插件来源：apt（containernetworking-plugins）或 tarball。"
}

variable "cni_plugins_tarball" {
  type        = string
  default     = ""
  description = "cni_plugin_source=tarball 时的下载地址（如 https://github.com/containernetworking/plugins/releases/download/v1.4.0/cni-plugins-linux-amd64-v1.4.0.tgz）。"
}

variable "mars_prefix" {
  type        = string
  default     = "hai"
  description = "平台节点标签前缀（hai-up 的 MARS_PREFIX）。"
}

variable "mars_group" {
  type        = string
  default     = "training"
  description = "本节点在平台里的分组标签值（<前缀>_mars_group）。"
}

variable "pause_image" {
  type        = string
  default     = "registry.k8s.io/pause:3.9"
  description = "CRI sandbox 镜像；替换掉旧 drop-in 里不可达的 sealos.hub:5000/pause:3.9。"
}

variable "default_runtime" {
  type        = string
  default     = "nvidia"
  description = "CRI 默认运行时（nvidia）。平台任务不申请 nvidia.com/gpu，只有默认 nvidia 才能看到 GPU。"
}

variable "containerd_dropin" {
  type        = string
  default     = "/etc/containerd/conf.d/99-nvidia.toml"
  description = "containerd GPU 配置落点（config.toml 已 imports conf.d/*.toml）。"
}

variable "restart_containerd" {
  type        = bool
  default     = true
  description = "配置变化后是否重启 containerd。注意 Docker 与 k8s 共用同一个 containerd。"
}

variable "allow_docker_restart" {
  type        = bool
  default     = false
  description = "containerd 重启后若 docker CLI 不可用，是否自动重启 dockerd（会重启所有容器，含 RustFS）。"
}

variable "install_kubectl" {
  type        = bool
  default     = true
  description = "是否安装与集群版本匹配的 kubectl（本机自带的 kubectl 是 v1.36，与 1.29 偏差过大）。"
}

variable "kubectl_path" {
  type        = string
  default     = "/usr/local/bin/kubectl-1.29"
  description = "匹配版本 kubectl 的安装路径（不会覆盖系统 /usr/local/bin/kubectl）。"
}

variable "kubectl_urls" {
  type        = list(string)
  default     = ["https://dl.k8s.io", "https://cdn.dl.k8s.io"]
  description = <<-EOT
    kubectl 下载源基址列表（会拼成 <base>/release/<version>/bin/linux/<arch>/kubectl），
    每个源限时 90s。实测 103 上 dl.k8s.io 极慢（500s 只下到 6MB），因此这只是"尽力而为"：
    全部失败时自动降级使用系统 kubectl 并在日志里 WARN，不会让 apply 卡死。
    内网/离线环境可改用 kubectl_local_path。
  EOT
}

variable "kubectl_local_path" {
  type        = string
  default     = ""
  description = "103 上已存在的 kubectl 二进制路径（如 /root/kubectl-1.29）；设置后优先直接安装它，不联网。"
}

variable "kubeconfig_path" {
  type        = string
  default     = "/root/.kube/hai-single.conf"
  description = "本集群的 kubeconfig 路径（**不覆盖** /root/.kube/config，后者仍指向 VM 集群）。"
}

variable "reset_stale_node" {
  type        = bool
  default     = true
  description = "是否先做陈旧 Sealos 节点复位（停 kubelet / kubeadm reset / 清 CNI 残留）。"
}

variable "install_device_plugin" {
  type        = bool
  default     = true
  description = "是否部署 NVIDIA k8s-device-plugin（暴露 nvidia.com/gpu）。"
}

variable "device_plugin_image" {
  type        = string
  default     = "nvcr.io/nvidia/k8s-device-plugin:v0.17.1"
  description = "device plugin 镜像；103 的 containerd 已缓存该 tag，故 IfNotPresent。"
}

variable "gpu_smoke_image" {
  type        = string
  default     = "ubuntu:20.04"
  description = "GPU 冒烟 Pod 用的镜像（本机 Docker 已有，可离线导入 containerd）。"
}

variable "gpu_smoke_import_from_docker" {
  type        = bool
  default     = true
  description = "冒烟镜像优先从本机 Docker 导入（离线可用）。"
}

variable "min_free_disk_gb" {
  type        = number
  default     = 40
  description = "前置检查要求的最小可用磁盘。"
}

variable "min_mem_gb" {
  type        = number
  default     = 8
  description = "前置检查要求的最小内存。"
}

variable "restore_containerd_dropin" {
  type        = bool
  default     = true
  description = "destroy 时是否把 containerd drop-in 还原为 03 步骤的备份。"
}

# ---------------------------------------------------------------------------
# 公共环境变量（传给 files/*.sh）
# ---------------------------------------------------------------------------

locals {
  common_env = {
    NODE_IP                      = var.node_ip
    KUBE_VERSION                 = var.kubernetes_version
    IMAGE_REPOSITORY             = var.image_repository
    SERVICE_SUBNET               = var.service_subnet
    POD_SUBNET                   = var.pod_subnet
    BRIDGE_SUBNET                = var.bridge_subnet
    BRIDGE_CNI_VERSION           = var.bridge_cni_version
    MARS_PREFIX                  = var.mars_prefix
    MARS_GROUP                   = var.mars_group
    PAUSE_IMAGE                  = var.pause_image
    DEFAULT_RUNTIME              = var.default_runtime
    DROPIN_PATH                  = var.containerd_dropin
    RESTART_CONTAINERD           = tostring(var.restart_containerd)
    ALLOW_DOCKER_RESTART         = tostring(var.allow_docker_restart)
    RESET_STALE_NODE             = tostring(var.reset_stale_node)
    INSTALL_KUBECTL              = tostring(var.install_kubectl)
    KUBECTL_PATH                 = var.kubectl_path
    KUBECTL_URLS                 = join(" ", var.kubectl_urls)
    KUBECTL_LOCAL_PATH           = var.kubectl_local_path
    KUBECONFIG_PATH              = var.kubeconfig_path
    CNI_PLUGIN_SOURCE            = var.cni_plugin_source
    CNI_PLUGINS_TARBALL          = var.cni_plugins_tarball
    INSTALL_DEVICE_PLUGIN        = tostring(var.install_device_plugin)
    DEVICE_PLUGIN_IMAGE          = var.device_plugin_image
    GPU_SMOKE_IMAGE              = var.gpu_smoke_image
    GPU_SMOKE_IMPORT_FROM_DOCKER = tostring(var.gpu_smoke_import_from_docker)
    RESTORE_CONTAINERD_DROPIN    = tostring(var.restore_containerd_dropin)
    MIN_FREE_DISK_GB             = tostring(var.min_free_disk_gb)
    MIN_MEM_GB                   = tostring(var.min_mem_gb)
  }
  # 每个资源只把"自己真正依赖的变量"放进 triggers：
  # 这样改 image_repository 不会连带重建 containerd 配置（避免多一次 containerd 重启）。
  base_triggers = {
    node_ip      = var.node_ip
    node_name    = var.node_name
    kube_version = var.kubernetes_version
  }
}

# ---------------------------------------------------------------------------
# 01 只读前置检查（不做任何修改；可用 create.sh --preflight-only 单独跑）
# ---------------------------------------------------------------------------

resource "null_resource" "preflight" {
  triggers = merge(local.base_triggers, {
    min_free_disk_gb = tostring(var.min_free_disk_gb)
    min_mem_gb       = tostring(var.min_mem_gb)
  })

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/01-preflight.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'preflight: nothing to destroy'"
  }
}

# ---------------------------------------------------------------------------
# 02 复位陈旧 Sealos 节点残留（停 kubelet / kubeadm reset / 清旧 CNI）
# ---------------------------------------------------------------------------

resource "null_resource" "stale_node_reset" {
  triggers = merge(local.base_triggers, {
    reset_stale_node = tostring(var.reset_stale_node)
  })

  depends_on = [null_resource.preflight]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/02-reset-stale-node.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'stale_node_reset: nothing to destroy'"
  }
}

# ---------------------------------------------------------------------------
# 03 containerd GPU 运行时（默认运行时 = nvidia，修正 sandbox_image）
# ---------------------------------------------------------------------------

resource "null_resource" "containerd_gpu" {
  triggers = {
    dropin_path               = var.containerd_dropin
    pause_image               = var.pause_image
    default_runtime           = var.default_runtime
    restart_containerd        = tostring(var.restart_containerd)
    allow_docker_restart      = tostring(var.allow_docker_restart)
    restore_containerd_dropin = tostring(var.restore_containerd_dropin)
  }

  depends_on = [null_resource.stale_node_reset]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/03-configure-containerd-gpu.sh"
    environment = local.common_env
  }

  # destroy：把 drop-in 还原为备份并重启 containerd。
  # 注意：destroy-time provisioner 只能引用 self.triggers，不能引用 var.*。
  provisioner "local-exec" {
    when    = destroy
    command = "bash ${path.module}/files/91-destroy-containerd.sh"
    environment = {
      DROPIN_PATH               = self.triggers.dropin_path
      RESTORE_CONTAINERD_DROPIN = self.triggers.restore_containerd_dropin
      RESTART_CONTAINERD        = self.triggers.restart_containerd
    }
  }
}

# ---------------------------------------------------------------------------
# 04 kubeadm init —— 单机全节点（去污点 + 打分组标签）
# ---------------------------------------------------------------------------

resource "null_resource" "kubeadm_init" {
  triggers = merge(local.base_triggers, {
    image_repository   = var.image_repository
    pod_subnet         = var.pod_subnet
    service_subnet     = var.service_subnet
    bridge_subnet      = var.bridge_subnet
    bridge_cni_version = var.bridge_cni_version
    mars_prefix        = var.mars_prefix
    mars_group         = var.mars_group
    kubectl_path       = var.kubectl_path
    kubectl_urls       = join(",", var.kubectl_urls)
    kubectl_local_path = var.kubectl_local_path
    kubeconfig_path    = var.kubeconfig_path
    install_kubectl    = tostring(var.install_kubectl)
  })

  depends_on = [null_resource.containerd_gpu]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/04-kubeadm-init.sh"
    environment = local.common_env
  }

  # destroy：拆集群（保留驱动 / containerd / Docker 容器）。
  provisioner "local-exec" {
    when    = destroy
    command = "bash ${path.module}/files/90-destroy-cluster.sh"
    environment = {
      NODE_IP            = self.triggers.node_ip
      NODE_NAME          = self.triggers.node_name
      KUBE_VERSION       = self.triggers.kube_version
      MARS_PREFIX        = self.triggers.mars_prefix
      KUBECTL_PATH       = self.triggers.kubectl_path
      KUBECONFIG_PATH    = self.triggers.kubeconfig_path
      IMAGE_REPOSITORY   = self.triggers.image_repository
      SERVICE_SUBNET     = self.triggers.service_subnet
      POD_SUBNET         = self.triggers.pod_subnet
      BRIDGE_SUBNET      = self.triggers.bridge_subnet
      BRIDGE_CNI_VERSION = self.triggers.bridge_cni_version
      MARS_GROUP         = self.triggers.mars_group
    }
  }
}

# ---------------------------------------------------------------------------
# 05 CNI（bridge + host-local + portmap，零镜像依赖）
# ---------------------------------------------------------------------------

resource "null_resource" "cni_bridge" {
  triggers = merge(local.base_triggers, {
    pod_subnet          = var.pod_subnet
    service_subnet      = var.service_subnet
    bridge_subnet       = var.bridge_subnet
    cni_plugin_source   = var.cni_plugin_source
    cni_plugins_tarball = var.cni_plugins_tarball
    bridge_cni_version  = var.bridge_cni_version
  })

  depends_on = [null_resource.kubeadm_init]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/05-install-cni-bridge.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "sudo rm -f /etc/cni/net.d/10-hai-bridge.conflist; echo 'cni_bridge: removed'"
  }
}

# ---------------------------------------------------------------------------
# 06 NVIDIA device plugin（暴露 nvidia.com/gpu）
# ---------------------------------------------------------------------------

resource "null_resource" "gpu_plugin" {
  triggers = merge(local.base_triggers, {
    mars_prefix           = var.mars_prefix
    install_device_plugin = tostring(var.install_device_plugin)
    device_plugin_image   = var.device_plugin_image
  })

  depends_on = [null_resource.cni_bridge]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/06-install-gpu-plugin.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'gpu_plugin: DaemonSet 由 90-destroy-cluster.sh 一并清理'"
  }
}

# ---------------------------------------------------------------------------
# 07 验收（含 GPU 冒烟 Pod）
# ---------------------------------------------------------------------------

resource "null_resource" "verify" {
  triggers = merge(local.base_triggers, {
    image_repository             = var.image_repository
    gpu_smoke_image              = var.gpu_smoke_image
    gpu_smoke_import_from_docker = tostring(var.gpu_smoke_import_from_docker)
  })

  depends_on = [null_resource.gpu_plugin]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/07-verify.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'verify: nothing to destroy'"
  }
}

# ---------------------------------------------------------------------------
# 输出
# ---------------------------------------------------------------------------

output "node_name" {
  value       = var.node_name != "" ? var.node_name : "(hostname)"
  description = "K8s 节点名。"
}

output "kubeconfig_command" {
  value       = "export KUBECONFIG=${var.kubeconfig_path}   # 注意：不是 /root/.kube/config（那是 VM 集群）"
  description = "在本机使用本集群的方式。"
}

output "next_steps" {
  value = <<-EOT
    1) 验证集群：   ./verify.sh                （或 kubectl-hai --kubeconfig=${var.kubeconfig_path} get nodes）
    2) 看 GPU：     kubectl-hai get node -o custom-columns=NAME:.metadata.name,GPU:.status.capacity.nvidia\.com/gpu
    3) 接平台：     README.md「接平台」一节（hai-up 用本 kubeconfig、NODE_GPUS=1、HAS_RDMA_HCA_RESOURCE=0）
    4) 拆集群：     ./destroy.sh
  EOT
}
