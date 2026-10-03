# terraform-hai-platform-single-node

把 **Hai Platform 部署到 103 单节点 K8s 集群**（由 [`terraform-k8s-single-node`](../terraform-k8s-single-node/) 创建，
带一块 **Tesla V100-SXM2-16GB**）。这是一个**独立模块**，与部署到 4 台 Multipass VM 的
[`terraform-hai-platform`](../terraform-hai-platform/) 并存、互不干扰。

## 0. 与老模块的关系（为什么必须独立）

| 维度 | `terraform-hai-platform`（VM 集群） | **本模块（单节点）** |
| --- | --- | --- |
| 集群 | MicroK8s on k8s-master/slave01..03 | kubeadm 单节点 on `fireflyer-0003` |
| kubeconfig | `/root/.kube/config` | `/root/.kube/hai-single.conf` |
| 数据目录 | `/nfs-shared/hai-platform` | `/nfs-shared/**hai-single**/hai-platform` |
| GPU | 无（`NODE_GPUS=0`） | **1 块 V100（`NODE_GPUS=1`）** |
| 访问入口 | MetalLB `10.205.52.200` + 宿主 nginx 反代（Mac 不可直达） | MetalLB **`192.168.100.150`**，Mac 直连 |
| 任务测试 | π（CPU） | π（CPU）+ **GPU 探针（V100）** |

> `one/hai-up.sh` 固定用 `${SHARED_FS_ROOT}/hai-platform` 作为平台目录，所以只要把
> `shared_fs_root` 换成 `/nfs-shared/hai-single`，两套平台的数据（postgres/redis/workspace）
> 就完全隔离。**模块内置了拒绝执行的检查**：`shared_fs_root=/nfs-shared` 或目标目录等于
> `/nfs-shared/hai-platform` 时直接报错。

## 1. 部署链路

```
./create.sh
  ├─ 01 preflight     只读：集群/GPU/containerd 运行时/镜像/空闲 VIP/隔离性/磁盘
  ├─ 02 image import  docker save → ctr -n k8s.io images import（约 5GB）
  ├─ 03 metallb       MetalLB v0.14.9 + IPAddressPool 192.168.100.150/32 + L2Advertisement
  ├─ 04 hai-up        写 config.sh → hai-up up → 权限加固 → override.toml 修补（pull 策略 + namespace 合法性）
  │                   → 补 Pod 内 kubeconfig 的 config 名 → patch 平台 StatefulSet（IfNotPresent + 清空容器内 JUPYTER_GROUP）
  │                   → 等 Pod Running → redis 自检 → 重启 launcher/k8swatcher
  ├─ 05 verify        Pod/Service/EXTERNAL-IP、hai-cli whoami/nodes、DB 里 host.gpu_num=1
  ├─ 06 pi task       numpy 版 Monte-Carlo 求 π，误差 < 5e-4
  └─ 07 gpu task      **任务 Pod 内 nvidia-smi 看到 V100**
```

## 2. 用法

```bash
cd deploy/terraform/terraform-hai-platform-single-node

./create.sh --preflight-only   # 先干跑（只读）
./create.sh                    # 部署（预计 5~12 分钟：镜像导入 + hai-up + 两个任务）
./verify.sh                    # 只读验收
./verify.sh --tasks            # 验收 + 重跑 π/GPU 任务
./destroy.sh -y                # 卸载（保留数据目录；--purge 一并删数据）
```

部署完成后：

| 项 | 值 |
| --- | --- |
| Web 入口 | `http://192.168.100.150:8080/`（studio；Mac 直连，**不需要** SSH 隧道或 nginx 反代）。`:80` 只跑镜像内 haproxy 的 `/query/` `/operating/` `/ugc/` `/monitor_v2/` 前缀路由，访问 `http://192.168.100.150/` 返回 **503 属正常**（haproxy 无 `default_backend`） |
| 账号 | `haiadmin` / `123456` |
| 数据目录 | `/nfs-shared/hai-single/hai-platform` |
| 集群操作 | `kubectl-hai -n hai-platform get pods,svc` |

## 3. 关键配置（都是踩过的坑）

| 配置 | 值 | 原因 |
| --- | --- | --- |
| `kubeconfig_path` | `/root/.kube/hai-single.conf` | hai-up 会把它复制进平台 Pod 供 launcher/manager 建任务 Pod；写成 `/root/.kube/config` 会把任务建到 VM 集群去 |
| `node_gpus` | `1` | 平台**不用** k8s 的 `nvidia.com/gpu`，而是按 DB `host.gpu_num` 分卡并注入 `NVIDIA_VISIBLE_DEVICES`；写成 4 会把不存在的卡分给任务 |
| `JUPYTER_NODES` | `" "`（脚本里写死） | `one/hai-up.sh` 用 `: ${JUPYTER_NODES:=<假节点>}`，**空值会被替换成一个不存在的节点**并写进 `host` 表；空格则数组为空、不产生 jupyter 节点。单节点不能让同一节点同时进 training 与 jupyter（hai-up 会报 duplicated nodes） |
| `has_rdma_hca_resource` | `0` | 代码默认 `1` 会给所有任务加 `rdma/hca` 请求；本机未部署 RDMA device plugin，任务会 Pending |
| `hai_server_addr` / `ingress_host` | `hai_server_addr` = MetalLB VIP；`ingress_host` 必须是 **DNS 名**（默认集群内部服务名） | `hai_server_addr` 写进 `override.toml` 的 postgres/redis/`launcher.api_server`，必须稳定可达；`ingress_host` 会进 Ingress 的 `host`，apiserver 会拒绝 IP（`must be a DNS name, not an IP address`）。**浏览器看到的 studio 地址不取 `ingress_host`**，而由 04-G 的 `BFF_ADDR` 决定（见下） |
| 平台页面的 `bffURL` | `${HAI_SERVER_ADDR}:8080`（04 步骤 G patch 容器 env `BFF_ADDR`） | `one/hai-up.sh:531-532` 把 `BFF_ADDR` 设成 `INGRESS_HOST`（集群内部名），studio 再把它写进页面 `window.haiConfig.bffURL`；浏览器于是请求 `http://hai-platform-svc.hai-platform.svc.cluster.local/proxy/s?endPoint=…`——而部署机的 `/etc/hosts` 往往已把该内部名指向 **103**（宿主 nginx → **VM 平台**），登录代理因此打到另一个平台并返回 **403**（实测：同请求发到 `http://192.168.100.150:8080/proxy/s` 返回 `success:1`，发到内部名返回 403）。改成 VIP:8080 后页面与 API 同源，不依赖任何 DNS 改写 |
| `image_pull_policy` | `IfNotPresent`（04 步骤自动写进 override.toml） | 镜像里 `core.toml` 是 `Always`；本机没有 registry 凭据，会 ImagePullBackOff |
| `task_namespaces_by_role` | `hai-platform`（04 步骤自动补） | 镜像里是厂商占位名，`k8s_watcher` 会 watch 一个不存在的 namespace，症状是节点注册不上、`/query/node/list` 500 |
| 平台 Pod 的 `imagePullPolicy` | `IfNotPresent`（04 步骤 G 直接 patch StatefulSet） | `one/hai-up.sh:510` 把平台 StatefulSet **写死** `imagePullPolicy: Always`，而本机没有内网 registry、镜像只是 `docker save → ctr import` 进本地 containerd → 平台 Pod 永远 `ImagePullBackOff`（`failed to resolve reference … not found`）。`override.toml` 里的 `image_pull_policy` 只管**任务/manager** Pod，管不到平台自己 |
| Pod 内 kubeconfig 文件名 | 04 步骤 F 额外补一份 `${HAI_DIR}/kubeconfig/config` | `one/hai-up.sh:513` 硬编码容器/manager 的 `KUBECONFIG=/root/.kube/config`，而 `create_kubeconfig()`（`one/hai-up.sh:812`）按**原文件名**拷贝——本实例用的是 `/root/.kube/hai-single.conf`，所以挂进 Pod 的 `/root/.kube/` 里只有 `hai-single.conf`，不补 `config` 则 launcher/manager 建任务直接失败 |
| `task_namespaces_by_role.external` | 04 步骤 E 改写成合法 TOML | `one/hai-up.sh:385` 生成的是 `external = '<ns>'-external`（单引号字符串后跟裸字符）——**非法 TOML**，平台组件合并 `override.toml` 时会解析失败（仓库内 `one/one_etc/core.toml:53` 已是对的，属模板笔误） |
| 容器内 `JUPYTER_GROUP` | 置空（04 步骤 G patch 平台 StatefulSet 的 env） | 本部署**刻意没有 jupyter 节点**（`JUPYTER_NODES=" "`），而镜像里的 `one/entrypoint.sh` 是 `set -e` 且只判断 `JUPYTER_GROUP != ""`，于是执行 `kubectl label nodes   hai_mars_group=jupyter_cpu`（没有节点名）→ 非 0 退出 → 平台容器**启动 1 秒即死、CrashLoopBackOff**。置空容器内的 `JUPYTER_GROUP` 即跳过该分支；平台运行时用的是 `override.toml` 的 `[jupyter] shared_node_group_prefix`，镜像内也没有任何 Python 读这个环境变量（已 grep 核实）。仓库 `one/entrypoint.sh` 已同步修好判空，但已构建的镜像不会因此改变，故运行期兜底 |
| hai-cli 配置 | `HFAI_CLIENT_CONFIG=/tmp/hai-single-hfai/conf.yml` | 103 上 `~/.hfai/conf.yml` 现在指向 VM 平台，冒烟测试不能覆盖它 |

## 4. GPU 任务是怎么验的

平台的 GPU 通路是：`调度器（host.gpu_num=1）→ assigned_gpus → init_manager 注入 NVIDIA_VISIBLE_DEVICES → containerd 默认运行时 nvidia 挂载 /dev/nvidia*`。

`files/gpu_task.py` 在**任务容器内**同时打印：

```
GPU_INFO NVIDIA_VISIBLE_DEVICES=None CUDA_VISIBLE_DEVICES=None
GPU_ENV MARSV2_RANK='0' NODE_NAME='fireflyer-0003'
GPU_DEVS /dev/nvidia-uvm,/dev/nvidia-uvm-tools,/dev/nvidia0,/dev/nvidiactl
GPU_SMI GPU 0: Tesla V100-SXM2-16GB (UUID: GPU-fe16a16e-...)
GPU_QUERY Tesla V100-SXM2-16GB, 16384 MiB, 570.211.01
GPU_RESULT GPU 0: Tesla V100-SXM2-16GB (UUID: GPU-fe16a16e-...)
TASK_RUNNER:EXIT_OK
```

> `NVIDIA_VISIBLE_DEVICES=None` 是**预期**的，不要当成失败：`init_manager.py:193-200` 把
> `NVIDIA_VISIBLE_DEVICES=<assigned_gpus>` 写进 **pod spec**（containerd 的 nvidia 运行时据此挂载
> `/dev/nvidia*`），而任务容器起来后 `marsv2/entrypoints/system_scope.sh:24` 会
> `unset NVIDIA_VISIBLE_DEVICES`（"unset for running no error"）——所以用户在任务里读不到它。
> 判据应以 `/dev/nvidia0` + `nvidia-smi` 为准。

terraform 以「任务 `succeeded` + `TASK_RUNNER:EXIT_OK` + 出现 `GPU_RESULT`」为通过判据。

## 5. 与 VM 平台的隔离边界（实测口径）

`destroy.sh` 只删本实例的 namespace，**不动**：

* `/nfs-shared/hai-platform`（VM 平台的 db/redis/workspace）；
* `/root/.kube/config`（仍指向 VM 集群）、`~/.hfai/conf.yml`（VM 平台的 hai-cli 配置）；
* 4 台 Multipass VM 与上面的老平台；
* 单节点集群本身（要拆集群用 `terraform-k8s-single-node/destroy.sh`）。

## 6. 已知限制

1. **单节点 = 无 HA**：控制面、平台 Pod、任务 Pod 都在同一台机器上。
2. **不启用 jupyter 分组**：单节点无法同时属于 training 与 jupyter（见 §3），需要 jupyter
   任务时得再加节点，或改成 jupyter 单组（把 `training_group`/`jupyter_group` 互换）。
3. **无 ingress controller**：`hai-up` 会建一个 `ingressClassName: nginx` 的 Ingress，
   本集群没有 controller，该对象只是静态声明；访问走 LoadBalancer IP。**平台 UI 在 `:8080`**；
   `:80` 是镜像内 haproxy，只按前缀路由 API（`one/thirdparty_conf/haproxy.cfg` **没有 `default_backend`**，
   所以 `http://VIP/` 返回 503 是正常现象，不要据此判断平台没起来）。
4. **MetalLB L2 占用 LAN IP**：`192.168.100.150` 需与局域网 DHCP 不冲突（部署前 preflight 会 ping 探测）。
5. **未在本模块内安装 metrics-server / ingress-nginx**：FreeLens 的 CPU/内存曲线、按域名访问
   studio 需要额外组件，可按需再加。

---

## 7. 实测记录（2026-10-03，103 实机）

**结果**：`terraform apply` 全绿（`Apply complete! Resources: 2 added, 0 changed, 2 destroyed`），
`terraform plan` 复查 **No changes**；平台 Pod 稳定 `1/1 Running 且 Ready`，`./verify.sh` **验收通过**，
π 任务与 GPU 任务都真的在任务 Pod 里跑完（`chain=finished job=succeeded`），
**浏览器登录链路**（页面 `bffURL` + studio `/proxy/s`）也已在 Mac 侧实测返回 `success:1`。

| 时间点 | 事件 |
| --- | --- |
| 13:27–13:38 | 第 1 次 apply：`preflight / image_import / metallb / hai_up` 落地，但平台 Pod 一直 `ImagePullBackOff`（StatefulSet 写死 `imagePullPolicy: Always`）→ `verify` 失败（HTTP 503 + Pod 未 Running） |
| 13:41–13:48 | 第 2 次 apply（修掉 ①②③ 后）：`verify` **通过**；π 任务其实已提交并跑出 `PI_RESULT`（task 1），但 `task_lib.sh` 的 `hcli_submit` 把整张表当 task id 返回 → 状态轮询失败（脚本 bug，不是平台问题）；同时暴露容器 1 秒即死（`JUPYTER_NODES=" "` + `entrypoint.sh` 的 `set -e`） |
| 13:48–13:50 | 第 3 次 apply（修掉 ④⑤ 后）：π 任务 **task 34**、GPU 任务 **task 35** 均 `job=succeeded`；`./verify.sh` 独立复跑通过；`terraform plan` → No changes |
| 13:52–13:57 | 浏览器点 Sign in 报 **403**：页面 `bffURL` 是集群内部名，在 Mac 的 `/etc/hosts` 里指向 103（宿主 nginx → **VM 平台**），登录代理打到了另一个平台（实测同请求发到内部名 403、发到本实例 `:8080` `success:1`）→ 修 ⑥⑦ |
| 13:57–14:05 | 第 4 次 apply（修掉 ⑥⑦ 后）：`BFF_ADDR` 指回 `192.168.100.150:8080`，`verify` 首次带上 **D2** 且全绿；Mac 侧复核 `window.haiConfig.bffURL = http://192.168.100.150:8080`、`/proxy/s` 登录返回 `success:1` |

**关键证据（原样摘录）**

```
==> B. 等 hai-platform-0 Running 且 Ready
[ OK ] hai-platform-0 Running 且 Ready
==> D. HTTP 可达性
      GET http://192.168.100.150:8080/ -> 200（studio，平台 UI）
      GET http://192.168.100.150/query/user/whoami -> 404（haproxy :80 → query-server）
==> D2. 浏览器登录链路（页面 haiConfig.bffURL + studio /proxy/s）
[ OK ] bffURL 指向本实例的 studio（浏览器与页面同源）
[ OK ] 浏览器登录链路通（studio /proxy/s → access_token 创建成功）
==> F. 平台 DB 里的节点/GPU 注册
      fireflyer-0003|1|gpu|training|training
[ OK ] host.gpu_num(fireflyer-0003) = 1（= NODE_GPUS）
==> G. k8s 侧 GPU 容量
      fireflyer-0003   1         1
[ OK ] 验收通过：平台已部署在单节点集群上，地址 http://192.168.100.150
```

```
# 页面配置（Mac 侧）
window.haiConfig = {...,"bffURL":"http://192.168.100.150:8080","wsURL":"ws://192.168.100.150:8080",
                    "jupyterURL":"http://hai-platform-svc...","clusterServerURL":"http://192.168.100.150",...}
# 同一个登录请求（Mac 侧，走页面同源）
POST http://192.168.100.150:8080/proxy/s?endPoint=/operating/user/access_token/create
  -> {"success":1,"result":{...,"access_token":"ACCESS-...","expire_at":"3000-01-01T00:00:00"}}
```

> `jupyterURL` 仍是集群内部名：本部署没有 jupyter 节点，用不到；若以后加节点，
> 需要把 `ingress_host` 换成浏览器可解析的 DNS 名并给平台加上 `/` 与 `/proxy/` 的路由。

```
# π 任务（task 34）
[ OK ] π 任务通过：pi=3.1416904000 error=9.774641020676711e-05
# GPU 任务（task 35）
GPU 0: Tesla V100-SXM2-16GB (UUID: GPU-fe16a16e-d671-0aa5-37ab-c199473a8943)
GPU_QUERY Tesla V100-SXM2-16GB, 16384 MiB, 570.211.01
TASK_RUNNER:EXIT_OK
[ OK ] GPU 任务通过（job=succeeded）
```

**踩到并修掉的 7 个坑**（细节见 §3 表与脚本注释）

1. 平台 StatefulSet 写死 `imagePullPolicy: Always`（`one/hai-up.sh:510`）→ 本地导入的镜像永远拉不到 → 04-G 改 `IfNotPresent`；
2. Pod 内 `KUBECONFIG=/root/.kube/config` 与拷进去的 `hai-single.conf` 不同名（`one/hai-up.sh:513` / `:812`）→ 04-F 补一份 `config`；
3. `external = '<ns>'-external` 是非法 TOML（`one/hai-up.sh:385`）→ 04-E 归一化（仓库模板已同步修好）；
4. `one/entrypoint.sh` 的 `set -e` + `JUPYTER_NODES=" "` → `kubectl label nodes` 缺节点名报错、容器启动 1 秒即死 → 04-G 置空容器内 `JUPYTER_GROUP`（仓库 `one/entrypoint.sh` 已同步修好判空）；
5. `task_lib.sh` 的 `hcli_submit` 把提交输出整张表写进 stdout，污染 task id → 回显改走 stderr；
6. **浏览器登录打到另一个平台（403）**：页面 `window.haiConfig.bffURL` 取自容器 env `BFF_ADDR ← INGRESS_HOST`（`one/hai-up.sh:531-532`），即集群内部名；而部署机 `/etc/hosts` 常把该内部名指向 103（宿主 nginx → VM 平台），于是 studio 的 `/proxy/s` 登录请求落到 **VM 平台**并返回 403 → 04-G 把 `BFF_ADDR` 改成本实例的 `${HAI_SERVER_ADDR}:8080`（页面与 API 同源，不依赖 DNS 改写）。`05-verify.sh` 新增 **D2** 步专门守这条链路（同时校验页面 bffURL 与 `/proxy/s` 登录）；
7. **验收对着正在终止的旧 Pod 跑**：StatefulSet 是 `OrderedReady`，删掉旧 Pod 后新 Pod 要等旧 Pod 完全消失才创建，而旧 Pod 终止期间 `phase` 仍是 `Running` → 只判 phase 会读到**修补前**的配置（实测 D2 因此误报）→ `lib.sh` 新增 `wait_pod_ready`（要求 Ready=True + UID 变化），04-H 与 05-B 改用它。

> 另外：`main.tf` 各步骤的 `triggers` 现在包含 `files/*.sh` 的 `filemd5` —— 改了脚本再 `apply`
> 会**真的重跑那一步**；此前只有变量进触发器，改了脚本 Terraform 会认为「无变化」而不执行。
