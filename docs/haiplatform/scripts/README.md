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
| [build_cli_local.sh](build_cli_local.sh) | 在宿主机直接构建并安装 `hai-cli` / `haienv` / `haiworkspace` wheel（免 docker） | `bash build_cli_local.sh` | 含 wheel 自检（`hfai/conf/utils.py` 等必须在内） |
| [deploy_pod_dev.sh](deploy_pod_dev.sh) | **联调快通道**：把源码 tar 进运行中的 `hai-platform-0`（`/high-flyer/code/multi_gpu_runner_server`）并重启 `ugc_server` | `bash deploy_pod_dev.sh` | 只覆盖服务端代码；**任务侧/manager 仍需重建镜像** |
| [smoke_env.sh](smoke_env.sh) | env 接口冒烟 19 项（API-11/API-13 正常 / 边界 / 幂等 / 越界 / 鉴权 / 注册表反序列化 / `source haienv`） | `bash smoke_env.sh http://10.205.52.200` | 期望 `PASS=19 FAIL=0` |
| [e2e_env.sh](e2e_env.sh) | 端到端：fixture → `hai-cli env push` → 注册 → 任务内 `source haienv` + 探针 import | `bash e2e_env.sh` | 期望 `PASS=12 FAIL=0`；需**已部署含 env 实现的镜像** |
| [verify_env.sh](verify_env.sh) | 一键验证：L1 单元 + 客户端单测 + L2 冒烟 + L3 E2E + workspace 回归 | `bash verify_env.sh`（`SKIP_E2E=1` / `SKIP_REG=1` 可裁剪） | 每步日志落在 `/tmp/verify_env_<step>.log` |

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

# ③ 客户端：构建并安装带 env push 的 hai-cli
bash ~/hai-platform/docs/haiplatform/scripts/build_cli_local.sh

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
| 启动日志没有 `env path check` | `api/register/implement.py` 的 ugc 段缺 `startup_env_check` 注册，或看的是别的 server 日志 |

