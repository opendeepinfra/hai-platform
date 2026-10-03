# deploy/terraform · 测试环境编排（Multipass → MicroK8s → Hai Platform）

这套 Terraform 用来从零搭出 [docs/haiplatform/workspace/test-environment.md](../../docs/haiplatform/workspace/test-environment.md)
描述的那套环境（host `103` 上的 4 台 Multipass VM + MicroK8s + MetalLB + Hai Platform）。

> **来源**：从 `hai-install` 仓库原样复制（目录名保持一致，便于与那边交叉对照）。
> 复制时**剔除了** `.terraform/`（下载的 provider 二进制，约 16MB/个）、`*.tfstate*`、
> 以及 `terraform-hai-platform/terraform.tfvars`（含真实 admin token，本地私有）。
> 需要凭据时用 `terraform.tfvars.example` 或环境变量传入。

## 1. 三层结构（按顺序执行）

```
① terraform-multipass-vms    建 3~4 台 Multipass VM（Ubuntu 22.04）+ 共享目录挂载
        │                     产物：k8s-master / k8s-slave01~03
        ▼
② terraform-k8s-ha           在 VM 上装 MicroK8s、组集群、装 MetalLB
        │                     产物：apiserver 10.205.52.154:16443 + LB 地址池
        ▼
③ terraform-hai-platform     把 Hai Platform 部署到「已存在」的集群上
                             产物：namespace hai-platform、StatefulSet hai-platform-0、
                                   LoadBalancer VIP 10.205.52.200、π 冒烟通过
```

| 目录 | 职责 | 入口脚本 |
| --- | --- | --- |
| [terraform-multipass-vms](terraform-multipass-vms/) | 创建/销毁 VM 与共享盘挂载 | `create_vms.sh` / `destroy_vms.sh` / `verify_vms.sh` |
| [terraform-k8s-ha](terraform-k8s-ha/) | 在 VM 上建 MicroK8s 集群 | `create.sh` / `destroy.sh` / `verify.sh` |
| [terraform-hai-platform](terraform-hai-platform/) | 把平台部署到已有集群 | `create.sh` / `destroy.sh` / `verify.sh` |
| [terraform-k8s-single-node](terraform-k8s-single-node/) | **路线 A（新）**：不用 VM，把 host `103` 本机初始化成单机 K8s 全节点 + GPU（V100） | `create.sh` / `verify.sh` / `destroy.sh` |

四个目录的 `README.md` 是各自最详细的说明，**先读它们**。

> **为什么多出第四套**：上面三层的节点是 Multipass VM，而 V100 在 **宿主机** 上，
> Multipass（QEMU 驱动）不支持 PCI/GPU 直通，所以 VM 集群永远拿不到显卡
> （实测：VM 内 `lspci` 无 NVIDIA 设备、集群无 `nvidia.com/gpu`）。
> `terraform-k8s-single-node` 直接把 103 变成单节点集群，从而用上本机 V100；
> 它与上面三层**互不干扰**（独立 kubeconfig `/root/.kube/hai-single.conf`、独立网段
> `10.244.0.0/16` + `10.96.0.0/12`），可以并存或替换。

## 2. 凭据（都不入库）

`terraform-hai-platform` 需要两项：

| 变量 | 含义 | 传入方式 |
| --- | --- | --- |
| `admin_token` | 集群 cluster-admin 的 Bearer token | `TF_ADMIN_TOKEN=... ./create.sh`，或 `terraform.tfvars`（gitignore） |
| `cluster_ca_b64` | 集群 CA 的 base64 | `TF_CLUSTER_CA_B64=... ./create.sh`，或 `terraform.tfvars` |

```bash
cd terraform-hai-platform
cp terraform.tfvars.example terraform.tfvars   # 填入真实值，然后
./create.sh
# 或者不建 tfvars：
TF_ADMIN_TOKEN=... TF_CLUSTER_CA_B64=... ./create.sh
```

其余变量（IP、节点名、分组、密码等）都在 `main.tf` 里带默认值，默认值就是 103 测试环境的取值。
`postgres_password` / `redis_password` 的默认值是平台自身的默认密码（`root`），不是本环境专有秘密。

## 3. 使用与风险

```bash
# 只读健康检查（随时可跑）
./verify.sh

# ⚠️ create.sh 会执行 rm -rf db/* 与 rm -rf redis/*：这是「重装」而不是「重启」，
#    会清空所有用户/任务/token。执行前请确认。
./create.sh

# 卸载（保留 VM 与集群）
./destroy.sh
```

已知前置（详见 test-environment.md §5.3）：

- host 103 需预装 `terraform`（v1.9.8）、`hai-up`、`hai-cli`，且可免密 sudo；
- `releases.hashicorp.com` 从 103 不可达 → 需离线 provider mirror
  （`/opt/terraform/plugins` + `~/.terraformrc`）；
- terraform 本身在 host 103 上执行（通过 `null_resource` + SSH 驱动），不是在开发机上跑。

## 4. 与本次 workspace 特性的关系

镜像构建、部署旁路（registry 无凭据）、RustFS 对象存储、`[cloud.storage]` 配置与冒烟/E2E 脚本
都在 [docs/haiplatform/scripts/](../../docs/haiplatform/scripts/)；本目录只负责「把集群和平台搭起来」。
