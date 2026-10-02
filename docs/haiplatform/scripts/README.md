# hai-cli workspace · 运维与验证脚本

本目录收录「部署 + 验证 `hai-cli workspace` 链路」用到的脚本，全部来自 192.168.100.103
测试环境的实际使用（已在真实环境跑通）。配合 [test-environment.md](../workspace/test-environment.md)、
[workspace-dataflow.md](../workspace/workspace-dataflow.md)、[workspace-server-task-list.md](../workspace/workspace-server-task-list.md) 阅读。

> **密钥策略**：本目录**不含任何真实凭据**。对象存储 AK/SK 一律从环境变量传入；
> 103 环境里的真实取值在 `/nfs-shared/hai-platform/override.toml` 的 `[cloud.storage]`。

## 0. 前置条件（103 测试环境）

| 依赖 | 说明 |
| --- | --- |
| host 103 | `fireflyer` 免密 sudo；已装 `terraform` / `docker` / `multipass`；可免密 sudo `kubectl` |
| MicroK8s 集群 | 4 台 Multipass VM（`k8s-master` + 3 worker），MetalLB 提供 VIP `10.205.52.200` |
| 镜像构建 assets | `$HOME/build-assets/`：`kubectl`、`decode-protobuf-camel`、`ambient.tar.gz`、`fountain.tar.gz`、`hai-studio-*.tar.gz`（外网受限，必须预置） |
| 对象存储 | RustFS 容器（`deploy_rustfs.sh` 拉起），bucket `hai-platform-private` / `hai-platform-public` |
| 客户端 | `hai-cli`（`haiworkspace` 插件）已安装；`~/.hfai/conf.yml` 里有 token 与平台地址 |

## 1. 脚本清单

| 脚本 | 用途 | 用法 | 是否需要凭据 |
| --- | --- | --- | --- |
| [build_hai.sh](build_hai.sh) | 构建镜像 + hai-cli wheels（离线：打 Dockerfile 补丁 + assets 命名上下文 + buildx） | `bash build_hai.sh <tag> [--no-cache]` | 否 |
| [patch_dockerfile.py](patch_dockerfile.py) | 把 Dockerfile 里对外网的下载（kubectl / decode-protobuf-camel / ambient / fountain / studio）改成从 `assets` 复制，并给 apt 加重试、钉 setuptools 版本（幂等） | `python3 patch_dockerfile.py <Dockerfile>` | 否 |
| [deploy_rustfs.sh](deploy_rustfs.sh) | 用 docker 拉起 RustFS（S3 兼容）并建 bucket | `RUSTFS_AK=.. RUSTFS_SK=.. bash deploy_rustfs.sh` | **是** |
| [config_cloud_storage.sh](config_cloud_storage.sh) | 幂等写入 `override.toml` 的 `[cloud.storage]` / `[cloud.storage.service]`，并同步 `manager_image` | `RUSTFS_AK=.. RUSTFS_SK=.. bash config_cloud_storage.sh` | **是** |
| [redeploy_local.sh](redeploy_local.sh) | registry 无凭据时的部署旁路：`docker save` → `multipass transfer` → `microk8s.ctr images import` → 更新 StatefulSet 并把 `imagePullPolicy` 设为 `IfNotPresent` → 重建 Pod | `bash redeploy_local.sh <tag>` | 否 |
| [smoke_ugc.sh](smoke_ugc.sh) | `/ugc/*` 接口冒烟 8 项（含枚举串 + `text/plain` + `{"file_list":...}` 兼容形态） | `bash smoke_ugc.sh [base_url]` | 复用本机 `~/.hfai/conf.yml` 的 token |
| [e2e_workspace.sh](e2e_workspace.sh) | 7 个子命令端到端 19 项（init/push/diff/list/pull/download/remove -f/remove） | `bash e2e_workspace.sh [1\|2\|3\|all]` | 同上 |
| [rustfs_check.py](rustfs_check.py) | RustFS S3 语义验证（tagging / 分片 / list / delete / STS 探测） | `RFS_AK=.. RFS_SK=.. python3 rustfs_check.py` | **是** |

`build_hai.sh` / `redeploy_local.sh` 均支持用环境变量覆盖默认路径（`REPO`、`BUILD_ROOT`、
`ASSETS`、`REGISTRY`、`NS`、`VM`、`OVERRIDE` 等），脚本头部有说明。

## 2. 一次完整的部署 + 验证流程

```bash
# ── 一次性准备 ───────────────────────────────────────────────
# ① 对象存储（凭证自备）
RUSTFS_AK=<ak> RUSTFS_SK=<sk> bash deploy_rustfs.sh
# ② 让平台指向它（同时把 manager_image 指到即将部署的 tag）
RUSTFS_AK=<ak> RUSTFS_SK=<sk> bash config_cloud_storage.sh

# ── 每次改代码后 ─────────────────────────────────────────────
# ③ 构建（tag 建议用短 commit hash）
bash build_hai.sh $(git -C ~/hai-platform rev-parse --short HEAD)
# ④ 部署（无 registry 凭据时的旁路）
bash redeploy_local.sh <tag>
# ⑤ 冒烟 + 端到端
bash smoke_ugc.sh http://10.205.52.200          # 期望 PASS=8 FAIL=0
bash e2e_workspace.sh all                       # 期望 PASS=19 FAIL=0
```

> `e2e_workspace.sh` 在 stage 2 前会**刻意等 32s**：`cluster_files/list` 有 30s 缓存（FR-04），
> 不等待的话「集群侧新增文件 → pull」会被旧列表挡住而误判失败。

## 3. 手工验证（不跑脚本时）

```bash
ssh fireflyer@192.168.100.103
sudo kubectl -n hai-platform get pod -o wide
sudo kubectl -n hai-platform exec hai-platform-0 -- supervisorctl status
sudo -u fireflyer -- env HOME=/home/fireflyer hai-cli nodes
sudo -u fireflyer -- env HOME=/home/fireflyer hai-cli list

# 工作区侧
cd /tmp/wsdemo2 && hai-cli workspace init demo2 -p s3   # provider 必须与 service 端一致
hai-cli workspace push && hai-cli workspace diff && hai-cli workspace list
sudo kubectl -n hai-platform exec hai-platform-0 -- \
  ls -la /nfs-shared/hai-platform/workspace/hfai/haiadmin/workspaces/demo2
```

## 4. 排障速查

| 症状 | 首查 |
| --- | --- |
| Pod `ImagePullBackOff` | 镜像没导入集群 containerd：先跑 `redeploy_local.sh <tag>`；确认 StatefulSet 的 `imagePullPolicy=IfNotPresent` |
| `/ugc/*` 返回 500 且日志有 `await wasn't used with future` | Redis 连接池被启动期抖动污染（历史问题，已在镜像内修）；确认用的是含修复的镜像 |
| 客户端报 `get_sts_token returns non oss data` | 本地 `workspace.yml` 的 `provider` 与服务端 `[cloud.storage].provider` 不一致（接 RustFS 要 `-p s3`） |
| `push` 成功但 bucket 里看不到散文件 | 正常：默认 zip 模式，bucket 里是 `<本地目录名>.zip`，散文件在共享盘 |
| 刚 push 完 `pull` 看不到新文件 | `cluster_files/list` 的 30s 缓存，等一会儿或换 subpath |
| 接口返回 `CLOUD_STORAGE_NOT_CONFIGURED` | `override.toml` 缺 `[cloud.storage]`（用 `config_cloud_storage.sh` 生成），或 pod 没重启 |

---

## 5. `hai-cli env`（haienv）—— 联调脚本

配合 [env-server-design.md](../env/env-server-design.md)、[env-server-test-cases.md](../env/env-server-test-cases.md)、
[env-server-test-report.md](../env/env-server-test-report.md) 阅读。

| 脚本 | 用途 | 用法 | 备注 |
| --- | --- | --- | --- |
| [patch_env_override.py](patch_env_override.py) | 幂等写入运行时配置：`env_path`（→ `env_root`）+ `env_push_enabled*` / `env_name_regex` | `sudo python3 patch_env_override.py` | 103 上 `env_path=/nfs-shared/hai-platform/workspace` |
| [mount_env_root.sh](mount_env_root.sh) | 把 `env_root` 挂进**任务容器**（默认任务只挂 `.../workspace/{user}`，共享的 `hfai_envs` 不在其下） | `bash mount_env_root.sh` | 生产应走 `/operating/mount_point/create` |
| [env_fixture.py](env_fixture.py) | 造测试用 env（真实 `haienv` 包写 `venv.db` + 可用的 `activate` + 探针包 `haienv_probe_unique`） | `sudo python3 env_fixture.py --env-root … --user … --name … [--clean]` | 替代需要 conda/CUDA 的 `haienv create` |
| [build_cli_local.sh](build_cli_local.sh) | 在**目标机**上直接构建并安装 `hai-cli` / `haienv` / `haiworkspace` wheel（免 docker） | `REPO=<源码> HAI_VERSION=<tag> bash build_cli_local.sh` | 含 wheel 自检（`hfai/conf/utils.py` 等必须在内）；版本号取自 `HAI_VERSION` 或源目录 git HEAD |
| [package_cli_for_host.sh](package_cli_for_host.sh) | **推荐的打包安装入口**：在仓库根按当前提交 `git archive` 干净源码 → 传到目标机 → 构建安装 → 归档 wheel + `SHA256SUMS` + `MANIFEST.txt` | `bash package_cli_for_host.sh [user@host]` | 保证「安装的包 == 某个提交」；产物在目标机 `~/hai-cli-wheels/<short>/` |
| [deploy_pod_dev.sh](deploy_pod_dev.sh) | **联调快通道**：把源码 tar 进运行中的 `hai-platform-0`（`/high-flyer/code/multi_gpu_runner_server`）并重启 `ugc_server` | `bash deploy_pod_dev.sh` | 只覆盖服务端代码；**任务侧/manager 仍需重建镜像** |
| [smoke_env.sh](smoke_env.sh) | env 接口冒烟 20 项（API-11/API-13 正常 / 边界 / 幂等 / 越界 / 鉴权 / 注册表反序列化 / `source haienv`） | `bash smoke_env.sh http://10.205.52.200` | 期望 `PASS=20 FAIL=0` |
| [e2e_env.sh](e2e_env.sh) | 端到端：fixture → `hai-cli env push` → 注册 → 任务内 `source haienv` + 探针 import | `bash e2e_env.sh` | 期望 `PASS=16 FAIL=0`；需**已部署含 env 实现的镜像** |
| [verify_env.sh](verify_env.sh) | 一键验证：L1 单元 + 客户端单测 + L2 冒烟 + N3 幂等 + 回滚演练 + L3 E2E + workspace 回归 | `bash verify_env.sh`（`SKIP_E2E=1` / `SKIP_REG=1` / `SKIP_DRILL=1` 可裁剪） | 每步日志落在 `/tmp/verify_env_<step>.log`；103 实测 `PASS=8 FAIL=0` |
| [check_env_idempotent.sh](check_env_idempotent.sh) | **N3 幂等自检**：首次预检 `_0` → 模拟「已上传未注册」→ 重试必须复用同一路径 → 补登记 → 重复注册记录数不增长 | `bash check_env_idempotent.sh [base_url]` | 期望 `PASS=5 FAIL=0` |
| [env_rollback_drill.sh](env_rollback_drill.sh) | **回滚演练**：一级（关开关 → 三条写入路径全被拒 → `venv.db` 无改动 → 恢复）；`DRILL_L2=1` 追加二级（注释两条路由 → 404 → 恢复） | `bash env_rollback_drill.sh [base_url]`；`DRILL_L2=1 CLIENT_ENV_PATH=.. CLIENT_ENV_NAME=.. bash …` | 期望 `PASS=8`（一级）/ `PASS=13`（含二级）；关停 4–5s |
| [env_metrics.sh](env_metrics.sh) | **最小看板**：抓 `/metrics` 汇总 env 请求量/成功率/注册耗时 P50·P95·P99/失败 reason，并做 5% 阈值判定 | `bash env_metrics.sh [metrics_url]` | 默认走 `kubectl exec` 抓 pod 内 `8083/metrics`；`WARN_RATE=` 可调 |
| [env_alerts.yml](env_alerts.yml) | **告警规则（配置即代码）**：注册/预检失败率 > 5%、写失败、读失败（N3）、P99 > 500ms | `kubectl -n <ns> apply -f env_alerts.yml` | 103 无 Prometheus/Grafana → 未 apply；与 `env_metrics.sh` 同指标名 |

> `patch_dockerfile.py` 还负责两件与本特性无关但必要的事：①把构建期 apt 源从 `archive.ubuntu.com`
> 换成 `mirrors.aliyun.com`（103 上前者不可达，构建会卡在 `apt-get update`）；②7 条替换规则都带 `skip_if` 标记，
> 补丁**幂等**（重复对同一 Dockerfile 执行不会报 `PATCH_FAILED`）。

### 5.1 一次完整的 env 联调 + 验证流程

```bash
ssh fireflyer@192.168.100.103

# ① 运行时配置 + 共享盘/任务容器挂载
sudo python3 ~/hai-platform/docs/haiplatform/scripts/patch_env_override.py
bash ~/hai-platform/docs/haiplatform/scripts/mount_env_root.sh

# ② 服务端：联调快的用 deploy_pod_dev.sh；要跑任务侧 E2E 必须重建镜像
bash build_hai.sh <tag> && bash redeploy_local.sh <tag>

# ③ 客户端：构建并安装带 env push 的 hai-cli（在仓库根执行，按当前提交打包）
#    注意：直接跑 build_cli_local.sh 会用「目标机工作树的 git HEAD」当版本号，
#    内容靠 rsync 同步时版本号会落后；推荐用 package_cli_for_host.sh
bash docs/haiplatform/scripts/package_cli_for_host.sh fireflyer@192.168.100.103

# ④ 用例：一条命令跑完全部（L1 单元 + 客户端单测 + L2 接口 + L3 E2E + workspace 回归）
bash ~/hai-platform/docs/haiplatform/scripts/verify_env.sh

# 或分步执行
sudo kubectl -n hai-platform exec hai-platform-0 -- sh -c \
  "cd /high-flyer/code/multi_gpu_runner_server && MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config \
   python3 -m pytest tests/env/test_env_registry.py -q"
cd ~/hai-platform && HAIENV_PATH=$(mktemp -d) python3 -m pytest tests/env/test_client_push.py -q
bash ~/hai-platform/docs/haiplatform/scripts/smoke_env.sh http://10.205.52.200
bash ~/hai-platform/docs/haiplatform/scripts/e2e_env.sh
```

### 5.2 env 排障速查

| 症状 | 首查 |
| --- | --- |
| `env push` 报 `No such command 'workspace'` | 子进程走到了插件二进制 `haiworkspace`，命令里不能再带 `workspace` 词（`_build_push_cmd` 已按可执行文件分支处理） |
| `env push` 报 `ModuleNotFoundError: hfai.conf.utils` | 客户端 wheel 构建时缺 `astunparse`（`client/install.sh` 中途失败）；用 `build_cli_local.sh` 重建（含自检） |
| `env push` 报 `上传venv失败` 且日志里是 `haienv workspace push` | E13 未修（客户端过旧） |
| `update_cluster_venv` 返回 `ENV_REGISTRY_NOT_WRITABLE` | `env_root/<user>` 对平台账号（root）不可写：`sudo chmod 777` 该目录 |
| `register_cluster_venv` 返回 `PATH_ESCAPE` | `path` 不在 `env_root/<user>/` 之下（例如误传他人目录或 `/tmp/...`） |
| 任务内报 `no valid env found` 且 `$HAIENV_PATH=/hf_shared/...` | 跑任务的是 **manager 容器**，用的是 `manager_image`：需重建镜像并确认 `override.toml` 的 `manager_image` 已同步到新 tag |
| 任务内 `$HAIENV_PATH` 目录不存在 | 任务容器没挂 `env_root`：跑 `mount_env_root.sh`（storage 表里的 Directory 挂载） |
| `env push` 上传后 stage2 报 `download <key> failed: ... 404` | 客户端把集群路径当成对象 key 前缀了（C-6）：确认服务端 API-11 返回 `cloud_path`、且客户端已升级（`--env_remote_path` 必须是 `{group}/shared/hfai_envs/...`） |
| 任务里 `source haienv` 报 `<prefix>/activate: No such file or directory` | C-7：ENV 上传排除了 `activate`；确认 `workspace_api.push` 的 ENV 分支 `exclude_list = []` |
| `haienv create` 打印 `WARNING: 检测到的 nvcc 均不在平台基线 11.x 内` | 这是**提示不是错误**（CUDA 不影响 conda 环境本身）；确认本机是否有可用的 11.x nvcc（会依次看 `nvcc` 与 `/usr/local/cuda/bin/nvcc`），或调 `HAIENV_CUDA_VERSION_RE`；需要恢复硬门禁就设 `HAIENV_CUDA_STRICT=1` |
| `haienv create` 提示 python 版本与集群基线不一致 | 默认取当前解释器（如 3.10），集群基础环境是 3.8：加 `-p 3.8`（基线可用 `HAIENV_CLUSTER_PY` 调整） |
| `haienv create` 提示未加 `--no_extend` | 只有在平台镜像/开发容器里 `extend` 才有意义（继承平台基础环境）；裸机上请加 `--no_extend` |
| `env push` 明明改了环境却提示「数据已同步」 | 本地 env 目录与集群落盘目录是同一个（共享盘）；E2E 必须把本地放在共享盘之外（`e2e_env.sh` 默认 `/tmp/hai-env-e2e`） |
| 启动日志没有 `env path check` | `api/register/implement.py` 的 ugc 段缺 `startup_env_check` 注册，或看的是别的 server 日志 |

---

## 6. `hai-cli images`（用户自定义镜像）—— 实施、联调与验证

> 配合阅读：[images-server-decisions.md](../images/images-server-decisions.md)（S0 决策冻结与文档不一致裁决） ·
> [images-server-design.md](../images/images-server-design.md) ·
> [images-server-test-cases.md](../images/images-server-test-cases.md)（§5 E2E-01 是端到端判据）

**实施状态见 [images-server-test-report.md](../images/images-server-test-report.md)**（落地文件、L1/L2/L3 实测、AC 对照）。

| 脚本 | 用途 | 用法 | 是否需要凭据 |
| --- | --- | --- | --- |
| [probe_images.sh](probe_images.sh) | `images` **现状基线**探测（只读、幂等）：命令面 / 4 条接口 HTTP 码 / `train_image` 行数 / 运行面前置 / 两个 `AttributeError` 复现 | `bash probe_images.sh [base_url]` | 复用本机 `~/.hfai/conf.yml` 的 token；节点探测需 `multipass` |
| [patch_image_override.py](patch_image_override.py) | 幂等写入运行时配置：`[cloud.storage.service].image_path` + `[image]` 全节（含 R-2 的 `containerd_socket` / `runtime_bin_dir` / `image_mount_root`） | `sudo python3 patch_image_override.py` | 否 |
| [image_fixture.sh](image_fixture.sh) | 造测试用**自定义镜像**：在平台基础镜像上加 `/hfai_image_probe.txt`（内容可区分），tag 成 `registry/<group>/demo:v1` 并 `docker save` 到镜像共享根 | `bash image_fixture.sh [镜像名] [输出 tar]` | 否（用本机 docker 与当前部署镜像） |
| [smoke_images.sh](smoke_images.sh) | **L2 接口冒烟**：API-15/16/17/18 正常/边界/幂等/错误码 + DB 副作用核对 + 运行面前置 | `bash smoke_images.sh http://10.205.52.200` | 复用 token；DB 校验需 `kubectl` |
| [e2e_images.sh](e2e_images.sh) | **L3 端到端（AC-01）**：`images load` → `images list` → 用自定义镜像提交任务 → 日志出现镜像内探针 → initContainer 证据 → 删除后提交被拒 | `bash e2e_images.sh` | 同上；需已部署含本特性的镜像 |

**现状基线（实施前，2026-10-02，103）**：`probe_images.sh` → `PASS=4 FAIL=6`（详见分析 §9）。
**实施后期望**：`load` / `update_status` / `delete` → 200；`train_image` 的行出现在 `images list` 里；
`link_hfai_image.sh` 存在且已被 `storage` 种子登记；节点前置满足；两个 `AttributeError` 消失；
`smoke_images.sh` `FAIL=0`、`e2e_images.sh` `FAIL=0`。
**端到端验收以 [用例 §5 E2E-01](../images/images-server-test-cases.md) 为准**——必须产出**可区分的任务输出**，
只看接口 200 **不构成通过**（分析 §5 的教训：S6「逻辑可用但永不通过」）。

> **脚本实现陷阱（供后续扩展脚本的人参考）**：本脚本开启 `set -o pipefail`，**不能**写
> `cmd | grep -q PATTERN` 做判定 —— `grep -q` 命中后立即退出会让上游收到 `SIGPIPE(141)`，
> 管道整体被判为失败，从而出现「**匹配到了却走 else**」的假 PASS（本脚本第一版就踩了这个坑，
> 把两个 `AttributeError` 误报成 PASS）。统一改为 `out="$(cmd 2>&1)"; grep -q PATTERN <<<"$out"`。

### 6.1 images 排障速查

| 症状 | 首查 |
| --- | --- |
| `images load` 抛 `AttributeError: 'UserImage' object has no attribute 'async_load'` | 客户端未升级（审计 **C-3**）：`client/model/user_impl/default.py` 是否已补方法、wheel 是否已重装 |
| `images load/delete` 返回 `{"success":0,"msg":"Not Found"}` | 服务端未注册路由（**I3**）：`api/register/implement.py` 的 `ugc` 区块、`api.resource.image` 是否被显式导入 |
| `images list` 的「用户自定义镜像」永远为空 | `server_model/user_impl/user_image/default.py:16` 的 `'user_images': []` 是否已改为真实查询（**I2**）；`train_image` 是否真有行 |
| 接口 500 且日志含 numpy 编码错误 | `user_images` 出口未归一化（**I8**）：`task_id` 需 `int(...)`、时间列需转字符串 |
| 同一镜像显示的 `status` 与预期相反（成了「最旧为准」） | `a_find_user_group_images` 是否已按 `updated_at **DESC**` 返回（**I7**） |
| 任务提交报「不存在镜像 … 或镜像仍在加载」但 `images list` 里看不到 | `user_images` 空（**I2**）+ 无 `status='loaded'` 行（**I4**）——服务端指引当前**必然误导**（契约 K5） |
| `images load` 报 `PATH_ESCAPE` | `image_tar` 不在 `[cloud.storage.service].image_path` 之下（常见：误传客户端本机路径） |
| pod 卡在 `Init`，日志 `sh: /marsv2/scripts/link_hfai_image.sh: not found` | **I16**：脚本不存在，或未在 `one/hai-up.sh` 的 `storage` 种子里登记（`init_manager.py:358` 引用了它） |
| initContainer `ImagePullBackOff` | **I17②**：`registry.high-flyer.cn/google_containers/busybox:latest` 不可达；把 `[image].load_helper_image` 改为节点已有镜像 |
| pod 因 `/data_local` 挂载失败 | **I17①**：节点无 `/data_local` 且 hostPath **未指定 `type`** → kubelet 不创建；部署侧创建或改 `DirectoryOrCreate`（OPS-04） |
| 灰度关闭后 `images list` 也报错 | 开关只应作用于 `load/update_status/delete`；`list` 与内建镜像路径**必须不受影响**（OPS-02 / CMP-03） |

