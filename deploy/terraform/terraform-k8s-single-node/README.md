# terraform-k8s-single-node

把 **host `103` 本机** 初始化成一台 **单机 K8s 全节点（control-plane + worker）+ GPU 就绪** 的机器，
作为「路线 A」：不再用 4 台 Multipass VM（VM 里没有、也直通不了 V100），而是直接在 103 上跑集群，
从而用上本机的 **Tesla V100-SXM2-16GB**。

> 与同目录另外三套的分工：
>
> | 目录 | 做什么 |
> | --- | --- |
> | `terraform-multipass-vms` / `terraform-k8s-ha` / `terraform-hai-platform` | 老路径：4 台 Multipass VM → MicroK8s → 平台（**无 GPU**） |
> | **本目录** | 新路径：103 裸机单节点 K8s + GPU（本次交付），**不含**平台部署 |
>
> 平台部署仍由 `terraform-hai-platform` 承担，需要按下文「接平台」一节换 kubeconfig / 节点名 / `NODE_GPUS`。

---

## 0. 为什么需要"复位"这台机器

2026-10-03 实测：103 **原本就是某个 Sealos 集群的 GPU worker**，节点侧全是现成的，但控制面已失联：

| 项 | 实测值 |
| --- | --- |
| GPU | Tesla V100-SXM2-16GB（`0000:82:00.0`），驱动 570.211.01 / CUDA 12.8，`nvidia-smi` 正常 |
| GPU 运行时 | nvidia-container-toolkit 1.19.1；CDI 规格 `/etc/cdi/nvidia.yaml`、`/var/run/cdi/nvidia.yaml` 已生成 |
| kubelet | v1.29.9，active + enabled，监听 10250 |
| containerd | v2.3.3（与 Docker 共用！`dockerd --containerd=/run/containerd/containerd.sock`） |
| 已有组件镜像 | `nvcr.io/nvidia/k8s-device-plugin:v0.17.1`、`nfd/node-feature-discovery:v0.17.2`、cilium v1.13.4、metallb speaker、higress gateway、kube-proxy、coredns、pause 等 14 个 |
| 控制面 | `apiserver.cluster.local → 10.103.97.2:6443`，**不可达**（kubelet 一直在刷 `i/o timeout`，节点早已从集群消失） |
| 遗留 | `/etc/hosts` 里的 `sealos.hub` / `lvscare.node.ip`、`/etc/cni/net.d/05-cilium.conf`、`/root/.kube/config`（指向 **VM 集群**，不能覆盖） |

所以本模块不是"从零装 K8s"，而是**复位陈旧节点 → 起单节点控制面 → 把 GPU 接对**。

---

## 1. 布局

| 文件 | 用途 |
| --- | --- |
| `main.tf` | 变量 + 7 个 `null_resource`（preflight / 复位 / containerd GPU / kubeadm init / CNI / device plugin / 验收）+ 输出 |
| `files/01-preflight.sh` | **只读**前置检查（GPU、版本、端口、网段冲突、swap、Docker 影响面、可达性） |
| `files/02-reset-stale-node.sh` | 停陈旧 kubelet、`kubeadm reset`、清旧 CNI 与 `KUBE-*`/`CILIUM_*`/`CALI-*` 链（**不动 DOCKER\***） |
| `files/03-configure-containerd-gpu.sh` | 重写 `/etc/containerd/conf.d/99-nvidia.toml`（默认运行时 = `nvidia`、修正 `sandbox_image`），校验后重启 containerd |
| `files/04-kubeadm-init.sh` | 装匹配版本 kubectl → `kubeadm init` → kubeconfig 副本 → 去污点 + 打分组标签 |
| `files/05-install-cni-bridge.sh` | 装 `containernetworking-plugins`，写 `10-hai-bridge.conflist`，等节点 Ready |
| `files/06-install-gpu-plugin.sh` | 部署 NVIDIA device plugin（`nvidia.com/gpu` 进节点容量） |
| `files/07-verify.sh` | 验收：节点/组件/容量 + **GPU 冒烟 Pod**（验证平台形态的 GPU 通路） |
| `files/90-destroy-cluster.sh` / `files/91-destroy-containerd.sh` | `destroy` 时拆集群 / 回滚 containerd 配置 |
| `create.sh` / `verify.sh` / `destroy.sh` | 开发机侧入口（SSH 驱动 103；Terraform 本身跑在 103 上） |
| `terraform.tfvars.example` | 变量示例（默认值即 103 现状，通常无需覆盖） |

---

## 2. 用法

```bash
cd deploy/terraform/terraform-k8s-single-node

# ① 干跑：只读前置检查，不改系统（推荐先跑）
./create.sh --preflight-only

# ② 只出 plan
./create.sh --plan

# ③ 完整初始化（约 3–6 分钟；会自动 approve）
./create.sh

# ④ 验收（含 GPU 冒烟 Pod）
./verify.sh              # 或 ./verify.sh --no-smoke
./verify.sh --keep       # 保留冒烟 Pod 供取证

# ⑤ 拆除（保留 NVIDIA 驱动 / containerd / Docker 容器）
./destroy.sh -y
```

环境变量覆盖：`HOST`（默认 `fireflyer@192.168.100.103`）、`TF_DIR`（默认 `/opt/terraform/k8s-single-node`）。

在 103 上直接操作本集群（注意 **不是** `/root/.kube/config`，那个仍指向 VM 集群）：

```bash
export KUBECONFIG=/root/.kube/hai-single.conf
kubectl-hai get nodes -o wide
kubectl-hai get node -o custom-columns=NAME:.metadata.name,GPU:.status.capacity.nvidia\.com/gpu
```

---

## 3. 它到底改了 103 的什么（变更清单 / 回滚）

| 变更 | 位置 | 回滚方式 |
| --- | --- | --- |
| 停掉陈旧 kubelet、`kubeadm reset` | systemd / `/etc/kubernetes` | `./destroy.sh` 或手工 `systemctl start kubelet` |
| 删除 `/etc/hosts` 的 4 行 Sealos 遗留 | `/etc/hosts` | 备份在 `/etc/hosts.hai-single.<时间戳>.bak` |
| 清理旧 CNI 配置与网卡、`KUBE-*`/`CILIUM_*`/`CALI-*` 链 | `/etc/cni/net.d`、`ip link`、iptables | `kubeadm reset` 后重新 init 即可；**DOCKER\* 链从未被触碰** |
| 重写 containerd GPU drop-in | `/etc/containerd/conf.d/99-nvidia.toml` | 备份 `99-nvidia.toml.bak.<时间戳>`，`destroy` 默认还原 |
| 重启 containerd | systemd | 无需回滚；配置本身可还原 |
| 新装匹配版本 kubectl | `/usr/local/bin/kubectl-1.29`、软链 `kubectl-hai` | `destroy` 删除软链；二进制可留 |
| 新增 kubeconfig 副本 | `/root/.kube/hai-single.conf`、`~/.kube/hai-single.conf` | `destroy` 删除 |
| 新建单节点集群 | `/etc/kubernetes`、`/var/lib/kubelet` | `./destroy.sh` |

**不会动**：NVIDIA 驱动与 toolkit、containerd 本体、Docker 容器（RustFS 等）、`/root/.kube/config`、
4 台 Multipass VM 与上面跑着的现有平台、`/nfs-shared`。

---

## 4. 网段与端口规划（刻意避让）

| 用途 | 本模块取值 | 必须避开的既有网段 |
| --- | --- | --- |
| Pod | `10.244.0.0/16`（bridge 用第一个 `/24`） | VM 集群 calico `10.1.0.0/16` |
| Service | `10.96.0.0/12` | VM 集群 service `10.152.183.0/24` |
| 主机网卡 | `192.168.100.0/24` | Multipass 桥 `10.205.52.0/24`、Docker `172.17/172.18`、ZeroTier `10.135/10.171` |

前置检查会用 `ipaddress` 做重叠判定，冲突直接 `apply` 失败。

端口：控制面用 `6443 / 2379 / 2380 / 10257 / 10259`（实测全空闲）。
宿主 nginx 已占 **80**（还有 8090/8092/3004），所以本模块**不装 ingress controller**；
平台接入方式见下一节。

---

## 5. GPU 是怎么接上的（关键，和平台强相关）

Hai Platform **不给任务 Pod 申请 `nvidia.com/gpu`**：

* `k8s/v1_api.py` 的 `get_node_resource()` 只设 `cpu` / `memory`（可选 `rdma/hca`）；
* GPU 由 `experiment_manager/manager/init_manager.py` 注入 `NVIDIA_VISIBLE_DEVICES` 来"限卡"；
* 节点卡数写在平台 DB 的 `host.gpu_num`（由 `one/hai-up.sh` 的 `NODE_GPUS` 决定）。

因此**光装 device plugin 不够**——任务容器必须由运行时直接拿到 `/dev/nvidia*`。本模块的做法：

1. **CRI 默认运行时 = `nvidia`**（`containerd` 的 `default_runtime_name`）→ 所有 Pod 都由
   `nvidia-container-runtime` 起，`NVIDIA_VISIBLE_DEVICES` 由平台注入（`0` / `0,1` …）；
2. `enable_cdi = true` + 已有的 CDI 规格，兼容 CDI 路径；
3. 同时部署 **NVIDIA device plugin**（镜像本机已缓存），让 `nvidia.com/gpu` 出现在节点容量里，
   标准 GPU 工作负载也能调度；
4. `files/07-verify.sh` 的冒烟 Pod **刻意不申请 GPU 资源**、只设 `NVIDIA_VISIBLE_DEVICES=0`，
   这正是平台任务 Pod 的形态 —— 它通过，才说明平台能用到卡。

> ⚠️ 两个已知的"平台侧"硬编码，接平台前必须处理（不在本模块范围内）：
> * `k8s/v1_api.py:21` `node_gpu_num()` 硬编码返回 **8**（注释"强制是8"），
>   `base_model/base_task.py:49` 在 `assigned_gpus` 为空时会用它 → 单卡机器要改成真实卡数；
> * `HAS_RDMA_HCA_RESOURCE` 代码默认 `'1'`，会给所有任务加 `rdma/hca` 请求（没有 RDMA device plugin
>   就 Pending）——`hai-up.sh` 默认 `0`，保持一致即可。

---

## 6. 接平台（已由 `../terraform-hai-platform-single-node` 实现）

**平台已经部署完成**，用的就是本目录的兄弟模块
[`terraform-hai-platform-single-node`](../terraform-hai-platform-single-node/)（独立 kubeconfig、独立数据目录
`/nfs-shared/hai-single`、MetalLB LAN VIP `192.168.100.150`）。直接用它即可：

```bash
cd ../terraform-hai-platform-single-node && ./create.sh      # 部署（含 π / GPU 任务测试）
cd ../terraform-hai-platform-single-node && ./verify.sh      # 只读验收
```

它内部自动处理了下面这些与本集群强相关的点（本模块只负责"集群 + GPU 就绪"）：

| 关注点 | 取值 / 处理 |
| --- | --- |
| `KUBECONFIG` | `/root/.kube/hai-single.conf`（**不要**用 `/root/.kube/config`，那是 VM 集群）；hai-up 会把它拷进平台 Pod 供 launcher/manager 建任务 Pod |
| `TRAINING_NODES` / `MANAGER_NODES` | `fireflyer-0003` |
| `JUPYTER_NODES` | `" "`（单节点刻意没有 jupyter 节点；hai-up 用 `:=` 会把**空值**换成假节点，空格则数组为空） |
| `NODE_GPUS` | **1**（本机只有 1 块 V100；老环境默认 4 会分错卡） |
| `HAS_RDMA_HCA_RESOURCE` | `0`（要启用 RDMA 再装 mellanox rdma-shared-device-plugin） |
| `SHARED_FS_ROOT` | `/nfs-shared/hai-single`（与 VM 平台的 `/nfs-shared/hai-platform` 完全隔离） |
| Service | 平台 Service 是 `type: LoadBalancer` → 由平台模块安装 MetalLB 提供 LAN VIP |

> ⚠️ **LoadBalancer 与单控制面节点**：kubeadm 会给控制面节点打上
> `node.kubernetes.io/exclude-from-external-load-balancers`，任何 LoadBalancer 实现都会据此**拒绝在该节点通告** LB IP
> （症状：Service 有 `EXTERNAL-IP`，但局域网内 ARP 无人应答、外部完全不可达）。
> 本模块的 `04-kubeadm-init.sh` 已在 init 后**自动移除该标签**；若你的集群是更早版本创建的，
> 手工执行：`kubectl-hai label node <node> node.kubernetes.io/exclude-from-external-load-balancers-`

---

## 7. 已知风险 / 未验证项

1. **containerd 与 Docker 共用**：改 GPU 配置需重启 containerd。运行中的容器（shim 独立）不会被杀，
   但 `docker` CLI 可能需要 `systemctl restart docker`（**会重启容器，RustFS 中断**），
   所以默认 `allow_docker_restart = false`，交给人工判断。
2. **单节点无 HA**：控制面与工作负载同机；`./destroy.sh` 即整集群下线。
3. **CNI 是最小实现**：bridge + host-local + portmap，**没有 NetworkPolicy**；需要策略/与原 Sealos
   环境一致时可换 Cilium（`cilium` 与 `operator` 镜像本机已缓存，但需把 `sealos.hub:5000/...` 标签
   retag 成 `quay.io/...`，且 Cilium 1.13 官方支持到 k8s 1.27，属未验证组合）。
4. **kubectl 版本**：本机 `/usr/local/bin/kubectl` 是 v1.36，与 1.29 控制面偏差超出官方 skew；
   本模块会尽力装一个 `/usr/local/bin/kubectl-1.29`。注意 **103 上 `dl.k8s.io` 下载极慢**
   （实测 500s 只下到 6MB），所以下载是"限时 90s + 多源 + 失败降级用系统 kubectl"的尽力而为逻辑，
   不会卡住 apply；离线环境用 `kubectl_local_path` 指定二进制。
   `kubectl-hai` 是**包装脚本**（固定 `KUBECONFIG` 指向本集群），因此即使降级到系统 kubectl 也可用。
5. **镜像源**：默认 `registry.aliyuncs.com/google_containers`（实测拉 `kube-apiserver` 约 23s，
   `registry.k8s.io` 约 67s）。换回官方仓库用 `image_repository = "registry.k8s.io"`。
6. **本模块不含平台部署**（平台接入见 §6，由 `terraform-hai-platform` 改造后执行）。

---

## 8. 首次实测记录（2026-10-03，103 实机）

**结果**：`terraform apply` 成功，`terraform plan` 复查 **No changes**（状态与实机一致）。

| 时间点 | 事件 |
| --- | --- |
| 12:43 | `./create.sh --preflight-only` 9/9 全绿（只读，系统未变） |
| 12:46 | 第 1 次 apply：`02 复位` 成功（`kubeadm reset` 因旧 Sealos pod 的 CRI 超时报错，已有 `\|\| warn` 兜底）；`03 containerd` 成功（默认运行时 = nvidia，Docker 自动重连、RustFS 存活）；`04` 卡在 kubectl 下载 → 失败 |
| 12:57 | 诊断：`dl.k8s.io` 546s 只下到 6.8MB；阿里云镜像拉 `kube-apiserver` 23s、`registry.k8s.io` 67s |
| 13:01 | 第 2 次 apply：kubectl 降级到系统 v1.36；`kubeadm init` **2m9s 成功**；`05 CNI` 失败（apt 包把插件装在 `/usr/lib/cni`，不在 `/opt/cni/bin`） |
| 13:05 | 第 3 次 apply：CNI 补齐 → 节点 **Ready**（26s）；device plugin 11s 后 `nvidia.com/gpu = 1`；GPU 冒烟 Pod 通过 |
| 13:07 | `./verify.sh` 独立复跑：节点 Ready、控制面 8 个 Pod Running、`GPU_CAP=1`、冒烟 Pod 内 `nvidia-smi` 看到 V100 |
| 13:12 | `terraform plan` → `No changes` |

**验收输出（节选）**

```
NAME             STATUS   ROLES           AGE   VERSION   INTERNAL-IP       CONTAINER-RUNTIME
fireflyer-0003   Ready    control-plane   4m31s v1.29.9   192.168.100.103   containerd://2.3.3

NAME             GPU_CAP   GPU_ALLOC
fireflyer-0003   1         1

# 冒烟 Pod（不申请 nvidia.com/gpu，只注入 NVIDIA_VISIBLE_DEVICES=0 —— 平台任务 Pod 的形态）
GPU 0: Tesla V100-SXM2-16GB (UUID: GPU-fe16a16e-d671-0aa5-37ab-c199473a8943)
--- /dev ---
/dev/nvidia-uvm  /dev/nvidia-uvm-tools  /dev/nvidia0  /dev/nvidiactl
```

**同机既有环境未受影响**（同一时间点复核）：4 台 Multipass VM 集群仍 `Ready`（`kubectl --kubeconfig=/root/.kube/config`），
`hai-platform-0` Running 16h，RustFS 容器 Up，`/root/.kube/config` 未被覆盖。

**测试中发现并修掉的 5 个问题**（均已回写代码）：
1. `dl.k8s.io` 慢到会卡死 apply → kubectl 下载改为限时/多源/可离线/失败降级；
2. `registry.k8s.io` 慢 → 默认镜像源换阿里云（`coredns/coredns` 会被阿里云压平成 `coredns`，与 kubeadm 生成的名字一致）；
3. Ubuntu 的 `containernetworking-plugins` 装到 `/usr/lib/cni` → 补齐到 `/opt/cni/bin`；
4. 反复 apply 会堆重复的 `/etc/hosts`、containerd 备份 → 改为复用已有备份；
5. macOS bash 下 `${VAR}（中文）` 会被误解析成变量名（`unbound variable`）→ 所有脚本给变量加花括号。
