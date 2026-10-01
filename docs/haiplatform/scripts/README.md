# hai-cli workspace · 运维与验证脚本

本目录收录「部署 + 验证 `hai-cli workspace` 链路」用到的脚本，全部来自 192.168.100.103
测试环境的实际使用（已在真实环境跑通）。配合 [test-environment.md](../test-environment.md)、
[workspace-dataflow.md](../workspace-dataflow.md)、[workspace-server-task-list.md](../workspace-server-task-list.md) 阅读。

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
