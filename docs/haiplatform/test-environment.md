# HAI Platform · 192.168.100.103 测试环境说明

本文档记录 **192.168.100.103 上这套真实测试环境**的拓扑、访问方式、部署流程、冒烟验证、已固化修复与故障恢复步骤。

与 [`workspace-server-test-cases.md`](workspace-server-test-cases.md) §2 的关系：

| | 本文档 | 用例集 §2 |
| --- | --- | --- |
| 性质 | **真实环境**，已部署并通过冒烟 | 抽象设定，供用例编写时假设 |
| 拓扑 | Multipass VM + MicroK8s + MetalLB + LoadBalancer | 单机 `localfs` / 真实 OSS 两条理想路径 |
| 用途 | 部署、联调、回归、排障 | 用例的环境前提与数据准备 |

> 联调 `hai-cli workspace` 服务端时，**以本文档的环境为准**；用例集 §2 的 `/tmp/hai-test/*` 路径是另一套独立的最小环境。

---

## 1. 环境拓扑

### 1.1 宿主机与虚拟化

```
你的 Mac (tongxiaojun)
  │  ✅ 能 ping 通 192.168.100.103
  │  ❌ 不能直接访问 VM 网段 10.205.52.x
  ▼
宿主机 103  fireflyer@192.168.100.103  (hostname: fireflyer-0003)
  │  NVMe /dev/nvme0n1p2  938G
  │  网桥 mpqemubr0 = 10.205.52.1/24
  │  NFS 导出 /nfs-shared → 10.205.52.1:/nfs-shared
  ▼
4 台 Multipass VM (Ubuntu 22.04 LTS, MicroK8s v1.21.13, containerd 1.4.13)
```

| VM | IP | vCPU | 内存 | 磁盘 | K8s 角色 | 平台分组 |
| --- | --- | --- | --- | --- | --- | --- |
| `k8s-master` | 10.205.52.154 | 10 | 8 GiB | 29 G | apiserver | （manager，不参与计算） |
| `k8s-slave01` | 10.205.52.222 | 2 | 3.9 GiB | 29 G | worker | `training` |
| `k8s-slave02` | 10.205.52.16 | 2 | 3.9 GiB | **9.6 G** ⚠️ | worker | `jupyter_cpu` |
| `k8s-slave03` | 10.205.52.213 | 2 | 3.9 GiB | 29 G | worker | `training` |

> ⚠️ `k8s-slave02` 磁盘未扩容（其余节点已按 README 扩到 29 G）。它承担 jupyter 计算，冷拉镜像时可能 `no space left on device`。

### 1.2 Kubernetes 与平台

| 项 | 值 |
| --- | --- |
| apiserver | `https://10.205.52.154:16443` |
| 命名空间 | `hai-platform` |
| 平台 Pod | `hai-platform-0`（StatefulSet，运行在 `k8s-master`） |
| 镜像 | `registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:7589fb1` |
| Service | `hai-platform-svc`，`LoadBalancer`，VIP **`10.205.52.200`** |
| Service 端口 | `5432` postgres · `6379` redis · `80` web · `8080` studio |
| MetalLB | `metallb-system`，地址池 `10.205.52.200-10.205.52.210` |
| 共享盘 | `/nfs-shared/hai-platform`（hostPath，经 NFS 由 103 导出给 VM） |

Pod 内常驻进程（由 supervisord 管理）：

```
postgres 12 · redis · haproxy · studio(hai-studio)
launcher.py · k8s_watcher.py · scheduler.py
uvicorn_server.py ×4（8081 query / 8082 operating / 8083 ugc / 8084 monitor）
```

---

## 2. 访问方式

### 2.1 浏览器（从你的 Mac）

| 入口 | 地址 | 实测 |
| --- | --- | --- |
| **推荐** | `http://192.168.100.103:8090/` | ✅ HTTP 200 |
| 备用 | `http://192.168.100.103/` | ✅ HTTP 200 |
| 域名方式 | `http://hai-platform-svc.hai-platform.svc.cluster.local/` | ✅（需 `/etc/hosts` 指向 103） |

**登录凭据：`haiadmin` / `123456`**

前置条件（Mac 侧）：

```bash
# /etc/hosts —— 已配置
192.168.100.103 hai-platform-svc.hai-platform.svc.cluster.local

# 代理例外需覆盖（否则请求会被本地代理劫持成 ERR_EMPTY_RESPONSE）
192.168.0.0/16  10.0.0.0/8  172.16.0.0/12  127.0.0.1  localhost  *.local
```

> ⚠️ **务必用 80 或 8090 端口的 103 地址访问**。前端会把 `window.haiConfig.bffURL` 注入为 `http://hai-platform-svc.hai-platform.svc.cluster.local`（80 端口），若页面来自其他 origin，登录请求会变成跨域；虽然服务端 CORS 已放行，但浏览器侧仍易受代理干扰。

### 2.2 `hai-cli`（在 host 103 上）

```bash
ssh fireflyer@192.168.100.103

# 必须用 fireflyer 身份：/root 下没有 .hfai/conf.yml，root 运行必然报"缺少 token"
sudo -u fireflyer hai-cli whoami      # 显示 haiadmin / 10020
sudo -u fireflyer hai-cli nodes       # 4 个节点及分组
sudo -u fireflyer hai-cli list        # 任务列表
```

配置文件：`/home/fireflyer/.hfai/conf.yml` → `token: ACCESS-…` + `url: http://10.205.52.200`

### 2.3 kubectl / FreeLens

Mac 到 VM 网段不可达，必须走 SSH 隧道：

```bash
# 1) 开隧道（保持常驻；FreeLens 是长驻应用）
ssh -f -N \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  -L 127.0.0.1:16443:10.205.52.154:16443 \
  fireflyer@192.168.100.103

# 2) 接入 kubeconfig
cd /Users/tongxiaojun/github/hai-install
KUBECONFIG="$HOME/.kube/config:$PWD/k8s-master-kubeconfig.yaml" \
  kubectl config view --flatten > /tmp/kube-merged.yaml
kubectl --kubeconfig=/tmp/kube-merged.yaml config use-context microk8s   # 必需
cp /tmp/kube-merged.yaml "$HOME/.kube/config" && chmod 600 "$HOME/.kube/config"

# 3) 验证
kubectl get nodes
```

FreeLens 默认读 `~/.kube/config`；也可直接导入 `/tmp/kube-merged.yaml`。

> 顺序：**先开隧道，再启 FreeLens**。隧道断开时重开即可，FreeLens 会自动重连。

### 2.4 网络可达性矩阵

| 地址 | Mac | host 103 | 集群内 Pod |
| --- | --- | --- | --- |
| `192.168.100.103:{80,8090}` | ✅ | ✅ | ✅ |
| `10.205.52.200`（LB VIP） | ❌ | ✅ | ✅ |
| `10.205.52.154:16443`（apiserver） | ❌ | ✅ | ✅ |

---

## 3. 部署与卸载

编排在 **`hai-install` 仓库**（Gitee），不在本仓库：

| 目录 | 职责 |
| --- | --- |
| `terraform-hai-platform/` | 在**已有**集群上部署/卸载 HAI Platform |
| `terraform-k8s-ha/` | 建 MicroK8s 集群 |
| `terraform-multipass-vms/` | 建 Multipass VM |

```bash
cd /Users/tongxiaojun/github/hai-install/terraform-hai-platform

./create.sh      # 部署：装 terraform → staging → terraform apply（含 MetalLB、hai-up up、verify、π 冒烟）
./verify.sh      # 只读健康检查
./destroy.sh     # 卸载
```

**前置条件**：host 103 上已装 `terraform`（v1.9.8）、`hai-up`、`hai-cli`（`/usr/local/bin`），且可免密 sudo。

> ⚠️ `create.sh` 会执行 `rm -rf db/*` 与 `rm -rf redis/*` —— 这是**重装而非重启**，会清空所有用户/任务/token。执行前请确认。

---

## 4. 冒烟验证：π 计算任务

环境是否健康，用一个最小 Monte-Carlo 求 π 任务验证即可。

### 4.1 脚本

```bash
# 位于共享盘，worker 可直接挂载
TASK_DIR=$(ls -d /nfs-shared/hai-platform/workspace/haiadmin/jupyter/notebooks/pi_np_* | tail -1)
```

脚本要点：**只用 numpy**（镜像内有 numpy 1.22.3，**没有 torch**），分块采样控制内存，结果同时打印并落盘到同目录 `pi_output.txt`（worker pod 随任务结束被删除，落盘可避免与日志保留竞争）。

### 4.2 提交与观察

```bash
ssh fireflyer@192.168.100.103

TASK_DIR=$(ls -d /nfs-shared/hai-platform/workspace/haiadmin/jupyter/notebooks/pi_np_* | tail -1)

# 提交（必须以 fireflyer 身份）
sudo -u fireflyer hai-cli python "$TASK_DIR/pi_np.py" \
     -- --nodes 1 -g training --name pi_smoke

# 状态：chain_status 是 running|finished|failed|stopped
#       pod 级 status 才是权威成功信号（finished 对失败的链同样返回 finished）
sudo -u fireflyer hai-cli status <TASK_ID>
sudo -u fireflyer hai-cli logs   <TASK_ID>
```

### 4.3 实测结果（2026-10-01，任务 4）

```
PI_INFO    rank=0 python=3.8.10 numpy=1.22.3 total=100000000 chunk=5000000
PI_SAMPLES 100000000
PI_INSIDE  78537221
PI_RESULT  3.1414888400
PI_ERROR   0.0001038136
PI_ELAPSED 3.4s
TASK_RUNNER:EXIT_OK
```

**通过判据**：`hai-cli status` 的 pod 状态为 `succeeded`（退出码 0），且 `|PI_RESULT − π| < 0.0005`。上例误差 1.04e-4，1 亿采样在 2 vCPU 上耗时 3.4 秒。

---

## 5. 环境中已固化的修复

以下问题都在本环境的搭建与联调中实际发生过，并已修复。**按修复所在层级分类**，因为它们生效方式不同。

### 5.1 源码层（`hai-platform` 仓库，分支 `feature/hai-cli-workspace-server-design`）

| 提交 | 修复内容 |
| --- | --- |
| `203ca3b` | ① `one/one_etc/core.toml`：`image_pull_policy` 由 `Always` 改 `IfNotPresent`（tag 引用配 Always 会让每次启动都回源解析，registry 不通即 `ImagePullBackOff`，即使镜像已缓存）<br>② `core.toml`：`[launcher.task_namespaces_by_role]` 由占位符 `poly-hpp` 改 `hai-platform`（`k8s_watcher.py` 用它决定监视哪些命名空间，占位符导致监视不存在的 ns → 节点全部注册不上、leader election 报 `namespaces "poly-hpp" not found`）<br>③ `marsv2/entrypoints/entrypoint.sh`：`ulimit -n 204800` 后接 `&&`，失败即中断整条链 → 用户脚本从不执行（表现为**无任何输出、exit 1**）。改为 `2>/dev/null \|\| true` |
| `50c9566` | ④ 32 个 `db_schemas/*.sql` 幂等化（`IF NOT EXISTS` / `create type` 包 DO 块 / `create trigger` 前置 `drop trigger if exists`）+ `init_postgresql.sh` 改为**每次启动全量重放** + 新增生成脚本 `idempotentize.py`<br>根因：原逻辑「`task_ng`/`user` 存在即整体跳过初始化」，导致已部署的库**永远收不到新迁移** → `032.table_host_flags` 未应用 → `host.flags` 缺失 → k8swatcher `UndefinedColumn` 崩溃 |

> ⚠️ **这两次提交尚未进入运行中的镜像**（当前仍是 `:7589fb1`）。要真正生效需重新构建并推送新 tag。

### 5.2 编排层（`hai-install` 仓库，提交 `32c19a6`）

| # | 问题 | 根因 |
| --- | --- | --- |
| 1 | 数据库从未被真正清空 | `sudo rm -rf db/*` 的通配符在**提权前**由 `fireflyer` 展开，而 `db` 目录为 `0700` 属主 uid 107，列不出内容 → glob 不展开 → 退化成 `rm -rf 'dir/*'` 静默空操作。改为 `sudo sh -c 'rm -rf …'` |
| 2 | postgres 拒绝启动 | 加固用的 `chmod -R a+rwX` 把数据目录刷成 0777，postgres 要求 0700/0750 → CrashLoop。加固后补 `chmod 700 db` |
| 3 | π 测试死等 20 分钟 | 轮询 grep `"state"`，但 `hai-cli status -j` **根本没有该键**（只有 `chain_status` 与 pod `status`）→ 永远判不出终态 → apply 必然失败 |
| 4 | 成功被误判 | `chain_status=finished` 对**失败**的链同样返回 finished；改用 pod `status == succeeded` |
| 5 | task id 抓成时间戳 | `grep -oE '[0-9]{4,}'` 抓到 workspace 路径里的时间戳；新库 task id 很短（从 1 开始）。改用表格行正则 |
| 6 | `$$` 转义 | Terraform 的转义是 `$${VAR}`；裸 `$$VAR` 被 shell 当 `$$`(PID)+VAR，导致 override 加固被静默跳过、日志打印成 `(179942i/120)` |
| 7 | 内嵌 π 脚本 | `import torch` —— 镜像**没有 torch**，必然 `ModuleNotFoundError`；改 numpy 并落盘结果 |

### 5.3 节点本地（手工操作，重建节点会丢失）

| 项 | 位置 | 说明 |
| --- | --- | --- |
| containerd nofile 提升 | `slave01/02/03` 的 `/var/snap/microk8s/current/args/containerd-env` | `ulimit -n 65536` → `1048576`。容器 nofile 硬上限继承自 containerd，65536 使 `ulimit -n 204800` 必然失败（源码修复 ③ 的另一半保障） |
| `pause:3.1` 沙箱镜像 | `master`、`slave03` | 该镜像曾被 GC，而 `k8s.gcr.io`/`registry.k8s.io` 经代理不可达 → **无法创建任何新 pod**。从可达镜像源取 amd64 manifest 组装 OCI tar 后 `microk8s.ctr images import` |
| terraform provider mirror | host 103 `/opt/terraform/plugins` + `~/.terraformrc` | `releases.hashicorp.com` 从 103 不可达（HTTP 000），需离线 provider |
| `override.toml` 覆盖 | `/nfs-shared/hai-platform/override.toml` | 运行时兜底写入 `image_pull_policy = 'IfNotPresent'` 与 `[launcher.task_namespaces_by_role] internal/external = 'hai-platform'`（`hai-install` 侧已可自动写入） |

> `proj_conf.py` 的合并顺序是 `core.toml → scheduler.toml → extension.toml → override.toml`，**override 优先级最高**，因此 §5.3 的运行时覆盖能立刻生效，无需重建镜像。

---

## 6. 已知脆弱点

| # | 脆弱点 | 影响 | 建议 |
| --- | --- | --- | --- |
| 1 | **平台把 postgres/redis 配到 LoadBalancer VIP `10.205.52.200`** | VM 重启后 MetalLB 重新宣告 VIP 期间，k8swatcher 连 DB 超时 → 线程崩溃 → supervisord 反复重启 → **节点列表为空**，任务无法调度 | 平台内部组件改用 ClusterIP；或给 k8swatcher 加启动退避 |
| 2 | `k8s-slave02` 磁盘仅 9.6 G | jupyter 节点冷拉镜像可能 `no space left on device` | `multipass set local.k8s-slave02.disk=30G`（需短暂停机） |
| 3 | 宿主机 103 根分区曾达 **99%** | 写满会拖垮整个平台（`/nfs-shared` 与 `/` 同分区） | 清理 `/home`（曾占 351 G）或扩容 |
| 4 | Ingress class 不匹配 | Ingress 声明 `nginx`，MicroK8s 控制器实为 `--ingress-class=public` → `LB:80` 返回 503（不影响浏览器入口，走 nginx→:8080） | 改 `ingress_class = "public"` |
| 5 | `verify.sh` 用 root 跑 `hai-cli whoami` | 必然报「缺少 token」 | 改 `sudo -u fireflyer hai-cli whoami` |
| 6 | 镜像内无 torch | 依赖 torch 的参考脚本必然失败 | 统一改用 numpy（已固化进 π 脚本） |
| 7 | outbound 依赖代理 | `k8s.gcr.io` / `releases.hashicorp.com` 不可达；registry.cn-hangzhou 可达 | 关键镜像/二进制需预置 |

---

## 7. 故障恢复 Runbook

### 7.1 VM 重启后（最常见）

```bash
ssh fireflyer@192.168.100.103

# ① 网络与 VM
multipass list                                   # 4 台 Running
ip -br addr show mpqemubr0                       # UP 10.205.52.1/24

# ② 等 apiserver（VM 重启后约 1~2 分钟自愈）
until sudo kubectl get nodes --request-timeout=8s >/dev/null 2>&1; do echo waiting; sleep 15; done
sudo kubectl get nodes                           # 4/4 Ready

# ③ MetalLB 必须 4/4，否则 LB VIP 无人宣告
sudo kubectl -n metallb-system get pods          # 期望全部 1/1
for p in 80 8080 5432 6379; do
  timeout 5 bash -c "cat </dev/null >/dev/tcp/10.205.52.200/$p" && echo "$p OK" || echo "$p FAIL"
done

# ④ 平台 pod
sudo kubectl -n hai-platform get pod hai-platform-0   # 1/1 Running

# ⑤ 关键：k8swatcher 是否自愈（它会因 DB 超时崩溃循环）
sudo kubectl -n hai-platform exec hai-platform-0 -- supervisorctl status k8swatcher
sudo kubectl -n hai-platform exec hai-platform-0 -- \
  redis-cli -a root -n 0 exists nodes_df_pickle       # 期望 1

# ⑥ 节点列表
sudo -u fireflyer hai-cli nodes                  # 4 个节点 + 正确分组
```

**判据链**：`nodes_df_pickle = 1` 是节点列表恢复的**直接证据**。若为 0，说明 k8swatcher 的 node watcher 尚未跑起来 —— 检查 §7.3。

### 7.2 平台 Pod 起不来

```bash
sudo kubectl -n hai-platform logs hai-platform-0 --tail=40
```

| 日志关键字 | 原因 | 处理 |
| --- | --- | --- |
| `data directory … has invalid permissions` | `db` 目录不是 0700 | `sudo chmod 700 /nfs-shared/hai-platform/db` |
| `Failed to create pod sandbox … pause:3.1` | 沙箱镜像缺失 | 导入 `pause:3.1` OCI tar（见 §5.3） |
| `ImagePullBackOff` | registry 不可达且策略为 Always | 确认 `override.toml` 的 `image_pull_policy='IfNotPresent'`，并确认镜像已缓存 |

### 7.3 节点列表为空（`hai-cli nodes` 空表）

这是**最典型的复合故障**，按链路自下而上排查：

```
k8swatcher 崩溃循环 ──▶ watcher 线程未启动 ──▶ 无人写 nodes_df_pickle ──▶ /query/node/list 返回空
        ▲
        └── 根因通常是连不上 DB（VIP 未宣告 / 代理不通）
```

```bash
P="sudo kubectl -n hai-platform exec hai-platform-0 --"

# 1) k8swatcher 在崩溃循环吗？（uptime 很短 = 是）
$P supervisorctl status k8swatcher

# 2) 崩溃原因
$P bash -c "grep -aE 'OperationalError|Connection timed out|Traceback' /high-flyer/log/k8swatcher_0.log | tail -5"

# 3) Pod 内能否连到 DB VIP
$P bash -c 'for p in 5432 6379; do timeout 5 bash -c "cat </dev/null >/dev/tcp/10.205.52.200/$p" && echo "$p OK" || echo "$p FAIL"; done'

# 4) VIP 通了之后，等 k8swatcher 下一次重启自愈（约 20~40 秒）
$P redis-cli -a root -n 0 exists nodes_df_pickle
```

### 7.4 任务无输出且 exit 1

**几乎总是 entrypoint 的 `ulimit` 中断**（源码修复 ③ 之前的症状）。确认方式：

```bash
# 容器内 nofile 硬上限
sudo kubectl -n hai-platform exec hai-platform-0 -- sh -c 'ulimit -Hn'   # 65536 即未修
```

处理：修 `containerd-env` 的 `ulimit -n`（§5.3）或应用源码修复 ③ 后重建镜像。

---

## 8. 关键路径速查

| 用途 | 路径 |
| --- | --- |
| 平台运行时配置（优先级最高） | `/nfs-shared/hai-platform/override.toml` |
| 平台服务端日志 | `/nfs-shared/hai-platform/log/`（`k8swatcher_0.log`、`launcher_0.log`、`operating_0.log`、`studio.log`…） |
| 任务日志（按 task id 分目录） | `/nfs-shared/hai-platform/workspace/log/haiadmin/<task_id>/` |
| 任务工作目录（worker 挂载） | `/nfs-shared/hai-platform/workspace/haiadmin/jupyter/notebooks/` |
| postgres 数据目录 | `/nfs-shared/hai-platform/db`（**必须 0700**） |
| redis 数据目录 | `/nfs-shared/hai-platform/redis` |
| 数据库迁移文件 | 镜像内 `/high-flyer/code/multi_gpu_runner_server/db_schemas/*.sql` |
| 迁移执行器 | 镜像内 `deploy/dbs/files/init_postgresql.sh` |
| Terraform 编排 | `/Users/tongxiaojun/github/hai-install/terraform-hai-platform/` |
| Terraform staging（host 103） | `/opt/terraform/hai-platform/` |

## 9. 数据库 schema 变更须知

当前机制（提交 `50c9566` 起）：`init_postgresql.sh` 在**每次容器启动**按文件名顺序重放全部 `db_schemas/*.sql`。

因此**新增迁移必须幂等**，否则会阻断平台启动：

```sql
create table if not exists "t" (...);
create index if not exists "i" on "t" (...);
alter table "t" add column if not exists "c" int;

-- PG 12 不支持 create type if not exists
do $$ begin
  create type "ty" as enum ('a','b');
exception when duplicate_object then null;
end $$;

-- PG 12 不支持 create trigger if not exists
drop trigger if exists "tg" on "t";
create trigger "tg" before insert on "t" for each row execute procedure f();
```

写完后用仓库内的生成/校验脚本确认：

```bash
python3 idempotentize.py --check    # 列出仍非幂等的文件；无输出即全部合规
```

> 提交 `50c9566` 之前的镜像采用「表存在即跳过」逻辑，**新迁移永远不会被应用**；若仍在跑旧镜像，任何 DDL 变更都需要人工执行。
