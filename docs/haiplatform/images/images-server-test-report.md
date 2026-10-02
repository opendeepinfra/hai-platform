# HAI Platform · `hai-cli images`（用户自定义镜像）实现与 103 实测报告

> **文档定位**：需求/设计/用例/Checklist 之后的**实测记录**（对应 `env` 特性的
> [env-server-test-report.md](../env/env-server-test-report.md)）。只写「实际落地了什么、跑出什么结果、
> 发现并修了什么缺陷、哪些没验证」，不重复需求与设计条文。
>
> **被测版本**：`feature/hai-cli-images-server-design` @ `5f844b4`（103 工作树与之逐字节一致）；
> 平台镜像 `registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:f2cb559`
> （tag 为**构建迭代号**：受 `ARG HAI_VERSION` 的 buildx 缓存键影响，迭代期间保持同一 tag 以复用缓存，
> 内容以构建时的 git HEAD 为准）；客户端 `hai-cli f2cb559`（`~/hai-cli-wheels/f2cb559/`）。
>
> **判定纪律**：每个结论都给出**可复核命令与输出**；「接口 200」不构成通过（AC-01 的判据是
> **任务日志里出现由镜像内容决定的输出**）。

---

## 0. 结论速览

| 里程碑 | 判定 | 证据 |
| --- | --- | --- |
| **M1 控制面闭环** | ✅ 达成 | L1 `33 passed`；L2 `PASS=44 FAIL=0`；启动自检 `image path check: OK` |
| **M2 运行面闭环** | ✅ 达成 | L3 E2E：自定义镜像任务 `succeeded`，日志出现镜像内探针内容（AC-01/AC-08/AC-09） |
| **M3 可上线** | 🟡 部分 | 迁移幂等（§5）、一级回滚演练（§7.3）、4 指标可抓取（§7.3）、workspace/env 回归零回归（§7.2）均通过；**未做**：性能压测（PERF-01/02）、生产三级灰度与看板/告警（OPS-06/REL-02/03/OBS-04/05）、上线后观察（POST-*） |

**本次实现期共发现并修复 9 个缺陷**（§6），其中 4 个只可能被 103 实机发现：
① Python 3.8 注解（`list[dict]`）导致服务端导入期崩溃；
② `load` 响应契约（三段 URL）；
③ runc/glibc 交互（helper 镜像的 `/bin/sh` 被宿主 libc 覆盖后无法启动）；
④ **长 init（镜像导入）被 `unschedulable` 看门狗打断**（新风险 **I19**）。

---

## 1. 环境与部署

| 项 | 取值 | 证据 |
| --- | --- | --- |
| 平台镜像 | `registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:f2cb559` | `kubectl -n hai-platform get statefulset hai-platform -o jsonpath='{.spec.template.spec.containers[0].image}'` |
| 平台 pod | `hai-platform-0` 1/1 Running；`ugc_server` RUNNING（:8083） | `supervisorctl status` |
| 客户端 | `hai-cli f2cb559`（`hai` / `haienv` / `haiworkspace` 同步升级） | `hai-cli --version`；`pip show hai` |
| 启动自检 | `image path check: OK (image_root=/nfs-shared/hai-platform/workspace/image, loader_backend=register, enabled=True)` | `/high-flyer/log/ugc_0.log`（ugc_server 启动时） |
| 部署前置自检 | `check_images_preflight.sh` → `PASS=30 FAIL=0 WARN=0` | §8 脚本 |

**运行时配置（`/nfs-shared/hai-platform/override.toml`，由 `patch_image_override.py` 幂等写入）**：

```toml
[cloud.storage.service]
image_path = '/nfs-shared/hai-platform/workspace/image'
[image]
enabled = true
enabled_groups = ['hfai']
loader_backend = 'register'
load_helper_image = 'docker.io/library/busybox:latest'
data_local_path = '/data_local'
containerd_socket = '/var/snap/microk8s/common/run/containerd.sock'
runtime_bin_dir = '/snap/microk8s/current/bin'
runtime_lib_dir = '/usr/lib/x86_64-linux-gnu'
image_mount_root = '/nfs-shared/hai-platform/workspace/image'
```

> **与文档的偏差（已在[决策记录](images-server-decisions.md) §4.2 登记）**：103 的平台 StatefulSet
> 只挂了 `.../workspace`，没挂 `.../image`，所以 `image_path` 取到 workspace 之下。

---

## 2. 落地文件清单

**服务端（控制面）**

| 文件 | 改动 |
| --- | --- |
| `api/resource/image/default.py` | 4 个具名桩 → 真实接入层（保持函数名，ADR-I1）；`startup_image_check` 自检钩子 |
| `api/register/implement.py` | 显式 `import api.resource.image`；注册 `load`/`update_status`/`delete` 三条路由；挂 startup 自检 |
| `server_model/user_impl/user_image/implement.py` | 领域层 4 方法（`async_load`/`async_report_image_status`/`async_delete`/`async_get_user_images`）+ 状态常量单点 |
| `server_model/user_impl/user_image/default.py` | `user_images` 由 `[]` 改为真实查询（修 I2） |
| `server_model/selector/train_image_selector.py` | `updated_at DESC`（修 I7）+ 出口归一化（修 I8）+ 幂等 upsert + 状态回报/软删 SQL + 写后刷缓存 |
| `conf/utils.py` | `FileType.IMAGE`、`get_image_root()`、`IMAGE_NAME_RE`、`derive_image_name()`（**不自动补 tag**） |
| `cloud_storage/service/context.py` | `[image]` 配置面、灰度开关、`image_self_check()` |
| `cloud_storage/service/errors.py` | `IMAGE_TAR_NOT_FOUND` / `IMAGE_NOT_FOUND` / `IMAGE_NAME_CONFLICT` / `ILLEGAL_TRANSITION` |
| `cloud_storage/utils.py` | `get_base_path` 的 `FileType.IMAGE` 分支 |
| `server_model/user_data/table_config.py` | `TrainImageTable.columns` 追加 `message` / `user_name` |
| `image_metrics.py`（新增） | 4 个指标（+ `image_update_status_total`） |

**数据面**

| 文件 | 改动 |
| --- | --- |
| `db_schemas/035.table_train_image_alter.sql`（新增） | 幂等加列 + 唯一索引改 `(shared_group, image_tar)`（fail-soft） |

**运行面**

| 文件 | 改动 |
| --- | --- |
| `marsv2/scripts/link_hfai_image.sh`（新增，I16） | 只读 env、幂等、失败可见；`ctr_run()` 用宿主 loader 显式运行 ctr；docker 兜底 |
| `experiment_manager/manager/init_manager.py` | helper 镜像 / `data_local` 可配置；`/data_local` 改 `DirectoryOrCreate`；**仅 initContainer** 挂 containerd socket / ctr / 宿主 glibc / 镜像共享根（R-2） |
| `experiment_manager/manager/check_unschedulable.py` | 放行「initContainer 正在运行」的 pod（**I19**，长导入不再被判 unschedulable） |
| `one/hai-up.sh` | `storage` 挂载种子登记 `link_hfai_image.sh`（HC-08） |
| `one/one_etc/core.toml` | 新增 `[image]` 与 `image_path` |

**客户端**

| 文件 | 改动 |
| --- | --- |
| `base_model/base_user_modules/default.py` | `IUserImage` 补 `async_load`/`async_delete`（修 C-3） |
| `client/model/user_impl/default.py` | 两个方法指向 `train_image/{load,delete}`；`load` 支持 `image`/`force` |
| `client/api/image_api.py` | 失败打印服务端 `msg` + 退出码 1（修 I10）；`list` 缺 `result` 时降级（R-8） |
| `client/commands/hfai_image.py` | `load` 新增 `-i/--image` 与 `--force`；`list` 防御性取值；`delete` 文案明确「不回收存储」 |

**测试与运维脚本（`docs/haiplatform/scripts/`）**：`patch_image_override.py`、`image_fixture.sh`、
`smoke_images.sh`、`e2e_images.sh`、`check_images_preflight.sh`（均新增），
`deploy_pod_dev.sh`（PATHS 补 `image_metrics.py`）、`patch_dockerfile.py`（pip 源钉到 tuna）。

---

## 3. L1 单元测试（NFR-04）

```bash
sudo kubectl -n hai-platform exec hai-platform-0 -- sh -c \
  "cd /high-flyer/code/multi_gpu_runner_server && MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config \
   python3 -m pytest tests/images -q"
# → 33 passed, 4 warnings in 2.26s
```

> ⚠️ 必须带 `MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config`；不带会因 `CONF` 缺 database 段在收集期报
> `AttributeError: database`。

覆盖：TC-U01~TC-U12（路径单点/命名派生/白名单/越界与软链/状态机矩阵/组隔离/出口归一化/DESC 排序/
启动自检）+ 附录 A.1 的三段 URL 契约 + link 脚本静态契约；全部用 fake 替身，不连 DB/k8s/registry。

---

## 4. L2 接口契约冒烟

```bash
bash docs/haiplatform/scripts/smoke_images.sh http://10.205.52.200
# → PASS=44 FAIL=0
```

要点（每条都同时校验响应体与 DB 副作用）：

- API-17 返回 `user_images` 真实行，6 字段齐备，`mars_images` 零改动；
- API-15 旧形态（只发 `image_tar`）可用，服务端派生 `image=demo`；同 tar 重复 load 仍 1 行、状态不倒退；
- API-15 负例：`PATH_ESCAPE` / `IMAGE_TAR_NOT_FOUND` / 名字含 `/` → `INVALID_PARAM`，且**零新增行**；
- 入参承载：query / `text/plain` JSON / `application/json` 三者等价；
- API-16：伪造 `task_id` → `FORBIDDEN`；`loaded → processing` → `ILLEGAL_TRANSITION`；
- API-18：非 3 段 → `INVALID_PARAM`；跨组 → `FORBIDDEN`；重复删除 `deleted:0`；
  删除后无 `loaded` 行，但 `list` 仍返回该 `deleted` 行（服务端不隐藏，HC-04）；
- 缺 token → 403 且响应体含 `success=0`。

---

## 5. 数据库与迁移（AC-12 / OPS-03）

```bash
sudo kubectl -n hai-platform exec -i hai-platform-0 -- \
  psql -U root -d mars_db -v ON_ERROR_STOP=1 < db_schemas/035.table_train_image_alter.sql   # 第 1 轮
# 再执行一次（第 2 轮）
```

第 2 轮输出（幂等证据）：

```
NOTICE:  column "message" of relation "train_image" already exists, skipping
NOTICE:  column "user_name" of relation "train_image" already exists, skipping
NOTICE:  index "train_image_image_uindex" does not exist, skipping
NOTICE:  relation "train_image_group_tar_uindex" already exists, skipping
```

`\d train_image` 结果：新增 `message` / `user_name`（`not null default ''`）、
唯一索引 `train_image_group_tar_uindex UNIQUE (shared_group, image_tar)`、触发器
`trigger_update_train_image_updated_at` 仍在。

---

## 6. 实现期发现并修复的缺陷

| # | 现象 | 根因 | 修复 | 发现方式 |
| --- | --- | --- | --- | --- |
| D1 | `ugc_server` 启动即崩：`TypeError: 'type' object is not subscriptable` | 新增代码用了 `-> list[dict]`（PEP 585），而镜像内是 **Python 3.8** | 两个模块补 `from __future__ import annotations` | 自检发现（预判 + pod 内 `python3 -c 'print(list[dict])'` 复现） |
| D2 | L2 步骤 6 失败：`load` 响应的 `image` 是裸名 | 契约（附录 A.1）要求**完整三段 URL** | `_load_result` 返回 URL，裸名另以 `image_name` 返回 | L2 冒烟 |
| D3 | 热部署后 `ugc_server` 崩：`No module named 'image_metrics'` | `deploy_pod_dev.sh` 的 PATHS 白名单漏了新文件 | PATHS 补 `image_metrics.py` | 热部署实测 |
| D4 | initContainer 立刻退出（exit 1）：`libdl.so.2: cannot open shared object file` | 宿主 `ctr` 动态链接，helper 镜像 busybox 自带 glibc 缺 `libdl` | 挂宿主 glibc 目录（第一版） | 节点 `ctr run` 等价复现 |
| D5 | initContainer 立刻退出：`/bin/sh: ... version 'GLIBC_2.38' not found` | 第一版把宿主 glibc 挂到 `/lib/x86_64-linux-gnu`，覆盖了 busybox **自己**的 libc（Debian trixie/2.41） | 改为挂到 **`/host-lib`**，脚本用 `/host-lib/ld-linux-x86-64.so.2 --library-path` 显式运行 ctr（`runtime_loader_file` 随之移除） | 真实任务 pod |
| D7 | 任务链每 ~110s 重启一次，最终才成功（task 27→36） | 1 GB tar 首次导入 >1 分钟，而 `manager.unschedulable_timeout_Ms=1`(60s)；`check_unschedulable` 把 Pending+`Initialized=False` 的 pod 判成 BUILDING → `STOP_CODE.UNSCHEDULABLE(33)` | `check_unschedulable` 放行「initContainer 正在运行」的 pod（**新风险 I19**） | L3 E2E（任务耗时异常） |
| D8 | 镜像构建在 `pip install jupyterlab_hai_platform_ext` 处 `ReadTimeoutError` | 该行 pip 未走镜像源，103 直连 `files.pythonhosted.org` 超时 | `patch_dockerfile.py` 把该行也钉到 tuna 源并加 `--retries/--timeout` | 构建失败日志 |
| D9 | `k8s-slave02` 缺 helper 镜像 busybox | 节点镜像清单不一致（slave01/03 有） | 节点侧 `microk8s.ctr images pull`（纳入 `check_images_preflight.sh` 检查项） | 部署前置自检 |

**I19 的处置建议（写进 OPS 手册）**：生产上「首次导入大 tar」的窗口内，任务不应被判 unschedulable。
本次已在 `check_unschedulable` 中放行 initContainer 运行中的 pod；若部署方希望保留更激进的重启策略，
可改为按 `manager.unschedulable_timeout_Ms` 与镜像体积联合配置。

---

## 7. 端到端（L3，AC-01）与回归

### 7.1 L3 E2E：控制面 → 运行面全链路（**PASS=26 FAIL=0**）

```bash
E2E_PURGE_IMAGE=1 bash docs/haiplatform/scripts/e2e_images.sh http://10.205.52.200
# → E2E_IMAGES 结果: PASS=26 FAIL=0
```

本轮**刻意先把镜像从三个节点的 containerd 里删掉**，强制走一次真实导入（否则 link 脚本会
「已存在，跳过」而绕过运行面最关键的代码路径）：

```
--- 1.0) 强制重新导入：从三个节点的 containerd 删除 registry.high-flyer.cn/hfai/demo:v1
PASS | k8s-slave01: 镜像已清除      PASS | k8s-slave02: 镜像已清除      PASS | k8s-slave03: 镜像已清除
```

**初次尝试即成功，任务链没有任何重启**（`task_id=38` 从第一次轮询到终态都是 38）——这正是 I19 修复
要保证的性质。关键证据：

| 判据 | 实测输出 |
| --- | --- |
| 控制面 load | `镜像已登记，状态：loaded`；DB 行 `demo:v1 …/demo.tar loaded 0`；`path == image_tar` |
| 列表可见（修 I2） | `images list` 出现 `registry.high-fly… loaded hfai demo.tar` |
| 任务真实运行（**AC-01**） | pod `succeeded`、`exit_code=0`，日志：`PROBE_FILE_CONTENT= HFAI_CUSTOM_IMAGE_PROBE image=registry.high-flyer.cn/hfai/demo:v1 built=2026-10-02T11:39:51Z`、`IMAGE_PROBE=images-load-ok` |
| 可区分性 | 该探针文件**只存在于自定义镜像里**（夹具由平台基础镜像 + `RUN printf > /hfai_image_probe.txt` 构造） |
| 运行面 link（**AC-08**） | initContainer 日志：`镜像资产: …/demo.tar (957 MiB)` → `导入: … -> registry.high-flyer.cn/hfai/demo:v1 (namespace=k8s.io)` → `unpacking … done` → `OK: registry.high-flyer.cn/hfai/demo:v1` |
| helper 镜像（I17②） | initContainer `image=docker.io/library/busybox:latest`（节点已有，无 `ImagePullBackOff`） |
| init env（E2E-07） | initContainer 的 `HFAI_IMAGE_WEKA_PATH == train_image.path` |
| 删除闭环（K5） | `images delete` → `deleted=1`；`list -a` 可见 `deleted` 行；再提交任务被拒且**原话保留**：`用户所在的组 [hfai] 不存在镜像 [registry.high-flyer.cn/hfai/demo:v1] 或镜像仍在加载, 请使用命令 \`hfai images list\` 检查` |
| 无 registry（**AC-09**） | 集群内无 registry service；`getent hosts registry.high-flyer.cn` → `198.18.0.77`（不可达）；全程 `register` 后端 |

> **一次真实的失败→修复记录**：修复 I19 之前，同样的场景下任务链每 ~110s 被
> `STOP_CODE.UNSCHEDULABLE(33)` 重启（task 27→36），直到某次 incarnation 的导入恰好跑完才成功。
> 修复后首次尝试即成功。

### 7.2 回归（AC-10）：**全部零回归**

| 脚本 | 结果 |
| --- | --- |
| `smoke_ugc.sh`（workspace 接口 8 项） | `PASS=8 FAIL=0` |
| `e2e_workspace.sh all`（7 子命令 19 项） | `PASS=19 FAIL=0` |
| `smoke_env.sh`（env 接口 20 项） | `PASS=20 FAIL=0` |
| `e2e_env.sh`（env 端到端 16 项，含任务内 `source haienv` + 探针 import） | `PASS=16 FAIL=0` |

内建镜像路径未受影响：`mars_images` 结构与语义零改动（CMP-03），内建镜像任务照常。

### 7.3 可观测（OBS-01）与一级回滚（RB-01 / OPS-01/02 / CFG-06）

```bash
kubectl -n hai-platform exec hai-platform-0 -- sh -c "curl -s http://127.0.0.1:8083/metrics | grep ^image_"
# image_load_total{code="",result="ok"} 3.0
# image_load_duration_seconds_bucket{backend="register",le="0.075"} 3.0 ...
# （另有 image_list_rows / image_link_failed_total / image_update_status_total）
```

一级回滚演练（改 `override.toml` → 重启 `ugc_server`）：

| 步骤 | 实测 |
| --- | --- |
| 置 `[image].enabled=false` 后 `load` | `{"success":0,"code":"FEATURE_DISABLED","msg":"镜像功能未开放"}`（HTTP 200，非 500） |
| 同态 `delete` | `{"success":0,"code":"FEATURE_DISABLED",...}` |
| 同态 `list`（及内建镜像路径） | `{"success":1,"result":{"mars_images":[...]}}` —— **不受影响** |
| 恢复 `enabled=true` + 重启 | `list` 正常；写入路径恢复 |

> 一级回滚**不会打断已 `loaded` 行上的线上任务**（任务只读 `train_image.status`，与开关无关）；
> 三级回滚（逆迁移）按设计**不做**：迁移只加不删（设计 §9.3）。

---

## 8. 复现命令

```bash
# 0) 配置 + 部署前置
sudo python3 ~/hai-platform/docs/haiplatform/scripts/patch_image_override.py
bash ~/hai-platform/docs/haiplatform/scripts/check_images_preflight.sh      # 期望 FAIL=0

# 1) 构建与部署（本环境无 registry 凭据 → 走 docker save + ctr import 旁路）
bash ~/hai-platform/docs/haiplatform/scripts/build_hai.sh <tag>
bash ~/hai-platform/docs/haiplatform/scripts/redeploy_local.sh <tag>

# 2) 客户端（按提交打包安装）
bash docs/haiplatform/scripts/package_cli_for_host.sh fireflyer@192.168.100.103

# 3) 夹具（自定义镜像 tar，含可区分探针）
bash ~/hai-platform/docs/haiplatform/scripts/image_fixture.sh

# 4) L1 / L2 / L3
sudo kubectl -n hai-platform exec hai-platform-0 -- sh -c \
  "cd /high-flyer/code/multi_gpu_runner_server && MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config \
   python3 -m pytest tests/images -q"
bash ~/hai-platform/docs/haiplatform/scripts/smoke_images.sh http://10.205.52.200
E2E_PURGE_IMAGE=1 bash ~/hai-platform/docs/haiplatform/scripts/e2e_images.sh http://10.205.52.200
```
