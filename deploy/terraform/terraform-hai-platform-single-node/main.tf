# Terraform configuration —— 把 Hai Platform 部署到 **103 单节点 K8s 集群**
# （由 deploy/terraform/terraform-k8s-single-node 创建的那个集群，带 V100）。
#
# 这是一个**独立模块**：与本目录下另一套 `terraform-hai-platform`（部署到 4 台 Multipass VM
# 的 MicroK8s 集群）互不干扰：
#
#   | 维度 | terraform-hai-platform（老） | 本模块（新） |
#   | --- | --- | --- |
#   | 集群 | MicroK8s on 4 VMs | kubeadm 单节点 on 103 本机 |
#   | kubeconfig | /root/.kube/config | /root/.kube/hai-single.conf |
#   | 共享盘 | /nfs-shared/hai-platform | /nfs-shared/**hai-single**/hai-platform |
#   | 节点 | k8s-master/slave01..03 | fireflyer-0003（单节点全节点） |
#   | GPU | 无（NODE_GPUS=0） | **1 块 V100（NODE_GPUS=1）** |
#   | 访问 | MetalLB 10.205.52.200 + 宿主 nginx 反代 | MetalLB **192.168.100.150**（Mac 直连，无需反代） |
#
# 执行方式（与其它模块一致）：Terraform 跑在 host 103 上，`./create.sh` 负责 staging 与 apply。
#
# 用法：
#   ./create.sh --preflight-only   # 只读前置检查（含隔离性检查）
#   ./create.sh                    # 部署（导入镜像 → MetalLB → hai-up → 验收 → π/GPU 任务测试）
#   ./verify.sh                    # 只读验收
#   ./destroy.sh                   # 卸载

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
  description = "运行 Terraform 的主机（信息性；Terraform 就跑在这台机器上）。"
}

variable "kubeconfig_path" {
  type        = string
  default     = "/root/.kube/hai-single.conf"
  description = "单节点集群的 kubeconfig（terraform-k8s-single-node 生成）。"
}

variable "node_name" {
  type        = string
  default     = "fireflyer-0003"
  description = "单节点集群里唯一的节点名（同时作为 training/manager 节点）。"
}

variable "task_namespace" {
  type        = string
  default     = "hai-platform"
  description = "平台与任务的 namespace（在单节点集群里，与 VM 集群的同类 namespace 不冲突）。"
}

variable "shared_fs_root" {
  type        = string
  default     = "/nfs-shared/hai-single"
  description = <<-EOT
    本实例的共享根目录。one/hai-up.sh 固定用 $${SHARED_FS_ROOT}/hai-platform 作为平台目录，
    因此这里必须与 VM 平台的 /nfs-shared **不同**，否则会共用 postgres 数据目录/redis/workspace。
  EOT
}

variable "platform_image" {
  type        = string
  default     = "registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:f2cb559"
  description = "all-in-one 平台镜像（同时用作平台 Pod 镜像与任务 worker 镜像）。需已存在于宿主 Docker。"
}

variable "base_image" {
  type        = string
  default     = ""
  description = "任务基础镜像；留空则等于 platform_image。"
}

variable "train_image" {
  type        = string
  default     = ""
  description = "训练镜像；留空则等于 platform_image。"
}

variable "node_gpus" {
  type        = number
  default     = 1
  description = "**本机实际 GPU 数**。平台按 DB 的 host.gpu_num 分卡；单卡必须是 1（老环境默认 4 会分错卡）。"
}

variable "training_group" {
  type        = string
  default     = "training"
  description = "训练分组名（节点标签 <mars_prefix>_mars_group）。"
}

variable "jupyter_group" {
  type        = string
  default     = "jupyter_cpu"
  description = "jupyter 分组名（单节点不启用 jupyter 节点，仅保留配置）。"
}

variable "manager_nodes" {
  type        = string
  default     = "fireflyer-0003"
  description = "平台 manager 所在节点（单节点就是本机）。"
}

variable "mars_prefix" {
  type        = string
  default     = "hai"
  description = "节点标签前缀。"
}

variable "hai_server_addr" {
  type        = string
  default     = "192.168.100.150"
  description = "平台自身地址（写进 override.toml 的 postgres/redis/api_server host）。应与 metallb_ip_range 一致。"
}

variable "ingress_host" {
  type        = string
  default     = "hai-platform-svc.hai-platform.svc.cluster.local"
  description = <<-EOT
    studio/jupyter 使用的主机名（会写进 Ingress 的 host 与 [jupyter.ingress_host]）。
    **必须是 DNS 名，不能是 IP** —— 否则 `hai-up` 建 Ingress 时被 apiserver 拒绝：
      The Ingress "hai-platform-ingress-studio" is invalid: spec.rules[0].host: must be a DNS name, not an IP address
    这里用集群内部服务名（集群内可解析）。浏览器要访问 studio 时，在 Mac 上把它指向 VIP 即可：
      echo "192.168.100.150 hai-platform-svc.hai-platform.svc.cluster.local" | sudo tee -a /etc/hosts
    Web 主入口不受影响，直接用 http://192.168.100.150/ 。
  EOT
}

variable "ingress_class" {
  type        = string
  default     = "nginx"
  description = "Ingress class（本集群未装 ingress controller；该 Ingress 只是静态声明，服务走 LoadBalancer IP）。"
}

variable "metallb_version" {
  type        = string
  default     = "v0.14.9"
  description = "MetalLB 版本（speaker v0.14.9 镜像本机 containerd 已缓存）。"
}

variable "metallb_ip_range" {
  type        = string
  default     = "192.168.100.150/32"
  description = "MetalLB 地址池：必须是 Mac/103 同网段的空闲 IP，这样平台地址从 Mac 直接可达。"
}

variable "postgres_user" {
  type    = string
  default = "root"
}

variable "postgres_password" {
  type    = string
  default = "root"
}

variable "redis_password" {
  type    = string
  default = "root"
}

variable "user_info" {
  type        = string
  default     = "haiadmin:10020:123456"
  description = "平台账号，格式 name:uid:token（单引号内不要有空格）。"
}

variable "root_user" {
  type        = string
  default     = "haiadmin"
  description = "管理员用户名（任务脚本会放在该用户的工作区下）。"
}

variable "bff_admin_uid" {
  type    = number
  default = 10000
}

variable "has_rdma_hca_resource" {
  type        = number
  default     = 0
  description = "是否给任务加 rdma/hca 资源请求。本机虽已缓存 rdma-shared-device-plugin 镜像但未部署，保持 0。"
}

variable "min_free_disk_gb" {
  type    = number
  default = 30
}

variable "run_task_tests" {
  type        = bool
  default     = true
  description = "是否执行 π 任务与 GPU 任务端到端测试（会真实占用集群几分钟）。"
}

variable "task_test_version" {
  type        = string
  default     = "1.0"
  description = "改这个值可强制下一次 apply 重跑任务测试。"
}

variable "reset_data" {
  type        = bool
  default     = false
  description = <<-EOT
    是否在执行 hai-up 前**清空**本实例的 db/redis 目录。
    默认 false（安全）：db 目录里已有数据时保留并 WARN —— 老模块是无条件 rm -rf，
    结果"改一个配置就清库"。首次部署时目录本来就是空的，无需置 true；
    确实要重装（丢弃全部用户/任务）时再设 true。
  EOT
}

# ---------------------------------------------------------------------------
# 公共环境变量 / triggers
# ---------------------------------------------------------------------------

locals {
  base_image  = var.base_image != "" ? var.base_image : var.platform_image
  train_image = var.train_image != "" ? var.train_image : var.platform_image

  common_env = {
    KUBECONFIG_PATH       = var.kubeconfig_path
    NODE_NAME             = var.node_name
    TASK_NAMESPACE        = var.task_namespace
    PLATFORM_NAMESPACE    = var.task_namespace
    SHARED_FS_ROOT        = var.shared_fs_root
    MARS_PREFIX           = var.mars_prefix
    TRAINING_GROUP        = var.training_group
    JUPYTER_GROUP         = var.jupyter_group
    TRAINING_NODES        = var.node_name
    MANAGER_NODES         = var.manager_nodes
    NODE_GPUS             = tostring(var.node_gpus)
    HAS_RDMA_HCA_RESOURCE = tostring(var.has_rdma_hca_resource)
    INGRESS_CLASS         = var.ingress_class
    INGRESS_HOST          = var.ingress_host
    HAI_SERVER_ADDR       = var.hai_server_addr
    METALLB_VERSION       = var.metallb_version
    METALLB_IP_RANGE      = var.metallb_ip_range
    PLATFORM_IMAGE        = var.platform_image
    BASE_IMAGE            = local.base_image
    TRAIN_IMAGE           = local.train_image
    POSTGRES_USER         = var.postgres_user
    POSTGRES_PASSWORD     = var.postgres_password
    REDIS_PASSWORD        = var.redis_password
    USER_INFO             = var.user_info
    ROOT_USER             = var.root_user
    BFF_ADMIN_UID         = tostring(var.bff_admin_uid)
    MIN_FREE_DISK_GB      = tostring(var.min_free_disk_gb)
    RESET_DATA            = tostring(var.reset_data)
  }
}

# ---------------------------------------------------------------------------
# 01 只读前置检查（含"绝不动 VM 平台数据"的隔离性检查）
# ---------------------------------------------------------------------------

resource "null_resource" "preflight" {
  triggers = {
    kubeconfig_path = var.kubeconfig_path
    node_name       = var.node_name
    shared_fs_root  = var.shared_fs_root
    node_gpus       = tostring(var.node_gpus)
    platform_image  = var.platform_image
    metallb_ip      = var.metallb_ip_range
    # 脚本本体也进触发器：否则改了 files/*.sh 再 apply，Terraform 认为"无变化"而不重跑该步
    script_sha = filemd5("${path.module}/files/01-preflight.sh")
  }

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
# 02 导入平台镜像到 k8s containerd（本机无内网 registry）
# ---------------------------------------------------------------------------

resource "null_resource" "image_import" {
  triggers = {
    platform_image = var.platform_image
    script_sha     = filemd5("${path.module}/files/02-import-image.sh")
  }

  depends_on = [null_resource.preflight]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/02-import-image.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'image_import: 镜像保留（任务 worker 也在用）'"
  }
}

# ---------------------------------------------------------------------------
# 03 MetalLB：给平台 Service 提供 LAN 可达的 LoadBalancer IP
# ---------------------------------------------------------------------------

resource "null_resource" "metallb" {
  triggers = {
    version    = var.metallb_version
    ip_range   = var.metallb_ip_range
    script_sha = filemd5("${path.module}/files/03-metallb.sh")
  }

  depends_on = [null_resource.image_import]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/03-metallb.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'metallb: 保留（可复用；彻底清理见 README）'"
  }
}

# ---------------------------------------------------------------------------
# 04 生成 config.sh 并执行 hai-up up + 本环境必需的加固
# ---------------------------------------------------------------------------

resource "null_resource" "hai_up" {
  triggers = {
    config = sha256(join("", [
      var.task_namespace, var.shared_fs_root, var.node_name,
      tostring(var.node_gpus), var.hai_server_addr, var.ingress_host,
      local.base_image, local.train_image, var.user_info,
    ]))
    # destroy provisioner 只能引用 self.triggers，所以这些值也要进 triggers
    kubeconfig_path = var.kubeconfig_path
    node_name       = var.node_name
    task_namespace  = var.task_namespace
    shared_fs_root  = var.shared_fs_root
    postgres_user   = var.postgres_user
    script_sha      = filemd5("${path.module}/files/04-hai-up.sh")
  }

  depends_on = [null_resource.metallb]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/04-hai-up.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "bash ${path.module}/files/90-destroy.sh"
    environment = {
      KUBECONFIG_PATH = self.triggers.kubeconfig_path
      NODE_NAME       = self.triggers.node_name
      TASK_NAMESPACE  = self.triggers.task_namespace
      SHARED_FS_ROOT  = self.triggers.shared_fs_root
      POSTGRES_USER   = self.triggers.postgres_user
      PURGE_DATA      = "false"
    }
  }
}

# ---------------------------------------------------------------------------
# 05 验收：Pod/Service/LB、hai-cli、DB 里的 host.gpu_num
# ---------------------------------------------------------------------------

resource "null_resource" "verify" {
  triggers = {
    run        = "verify-${null_resource.hai_up.id}"
    script_sha = filemd5("${path.module}/files/05-verify.sh")
  }

  depends_on = [null_resource.hai_up]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/05-verify.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'verify: nothing to destroy'"
  }
}

# ---------------------------------------------------------------------------
# 06 π 任务端到端（平台真的能跑任务）
# ---------------------------------------------------------------------------

resource "null_resource" "pi_task_test" {
  count = var.run_task_tests ? 1 : 0

  triggers = {
    version    = var.task_test_version
    namespace  = var.task_namespace
    script_sha = filemd5("${path.module}/files/06-pi-task.sh")
  }

  depends_on = [null_resource.verify]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/06-pi-task.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'pi_task_test: nothing to destroy'"
  }
}

# ---------------------------------------------------------------------------
# 07 GPU 任务端到端（**本模块的核心价值**：任务 Pod 里能看到 V100）
# ---------------------------------------------------------------------------

resource "null_resource" "gpu_task_test" {
  count = var.run_task_tests ? 1 : 0

  triggers = {
    version    = var.task_test_version
    namespace  = var.task_namespace
    gpus       = tostring(var.node_gpus)
    script_sha = filemd5("${path.module}/files/07-gpu-task.sh")
  }

  depends_on = [null_resource.pi_task_test]

  provisioner "local-exec" {
    command     = "bash ${path.module}/files/07-gpu-task.sh"
    environment = local.common_env
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'gpu_task_test: nothing to destroy'"
  }
}

# ---------------------------------------------------------------------------
# 输出
# ---------------------------------------------------------------------------

output "platform_url" {
  value       = "http://${var.hai_server_addr}:8080/"
  description = "平台 UI 入口（studio 直连 8080；Mac 直连，无需隧道/反代）。:80 只跑 haproxy 的 API 前缀路由。"
}

output "credentials" {
  value       = "用户：${split(":", var.user_info)[0]}    token：${split(":", var.user_info)[2]}"
  description = "平台登录凭据。"
}

output "data_dir" {
  value       = "${var.shared_fs_root}/hai-platform"
  description = "本实例的数据目录（与 VM 平台的 /nfs-shared/hai-platform 隔离）。"
}

output "kubectl_hint" {
  value       = "kubectl-hai -n ${var.task_namespace} get pods,svc   # 单节点集群"
  description = "查看平台资源的方式。"
}

output "next_steps" {
  value = <<-EOT
    1) 浏览器打开 http://${var.hai_server_addr}:8080/ （账号见 credentials；:80 是 haproxy 的 API 路由，"/" 会返回 503 属正常）
    2) 提交 GPU 任务：在平台里选择分组 ${var.training_group}，任务容器会拿到 NVIDIA_VISIBLE_DEVICES=${join(",", [for i in range(var.node_gpus) : tostring(i)])}
    3) 命令行提交：hai-cli python <脚本> -- --nodes 1 -g ${var.training_group} --name demo -f
       （注意 hai-cli 的配置：HFAI_CLIENT_CONFIG=<文件> 可指定独立配置，避免覆盖其它平台的 ~/.hfai/conf.yml）
    4) 只读验收：./verify.sh    卸载：./destroy.sh
  EOT
}
