# hai-cli 客户端 / 服务端实现现状审计

> **文档定位**：对 `hai-cli`（`hfai`）**客户端命令面**与**服务端接口面**的一次横向、代码级盘点，回答一个问题——
> 「哪些功能已经实现，哪些还不完整」。
>
> **与同目录文档的关系**：
> - [workspace/hai-cli-workspace-analysis.md](workspace/hai-cli-workspace-analysis.md)、[env/hai-cli-env-analysis.md](env/hai-cli-env-analysis.md) 是**单特性深挖**（`workspace` / `env`）；
> - 本文是**全局横切审计**：把客户端全部子命令、服务端全部路由放在一张表上做「调用 ↔ 注册」差分，并把散落在各特性文档里的缺口收敛成一份清单。
> - 三者结论一致，可互为佐证。
>
> **审计基线**：`b866c10`（2026-10-02）。审计时工作区含未提交改动：`docs/haiplatform/env/`（新增）与 `docs/haiplatform/README.md`（修改）。
>
> **方法**：只读代码审计，未启动服务、未修改任何文件。三条主线：
> 1. **AST 级核对**：把 `api/register/implement.py` 注册的 83 条路由逐一解析到处理函数定义，确认「无注册但函数缺失」；
> 2. **全量差分**：抓取 `client/**`、`plugins/**`、`base_model/**` 中所有 `mars_url()/...` 调用点，与注册表做程序化差集；
> 3. **桩可达性判定**：对每个 `default.py` 中返回 `not implemented` 的函数，检查是否被同名 `implement.py` 覆盖、是否被注册为路由。
>
> **已验证 / 未验证**：本文所有「完整/桩/缺失」判定均来自代码；**端到端可用性**部分（§6.2）引用仓库内已记录的实测结果（`workspace-server-task-list.md` §8），本文未复跑。

---

## 0. 结论速览

| 维度 | 判定 | 关键数字 |
| --- | --- | --- |
| 客户端核心命令面 | ✅ 基本完整 | 15 个核心子命令 + 2 个插件命令；缺陷见 §3.4（C-1~C-11） |
| 客户端 `workspace` 插件 | ✅ 主链路已实现并端到端验证 | 7 子命令；枚举字符串化贯穿 |
| 客户端 `env`（haienv）插件 | ⚠️ 本地完整、**跨端缺失** | 6 个子命令全本地可用；**无 `push`** |
| 服务端路由 | ✅ 处理函数齐全，无「空注册」 | **83 条**（operating 35 / query 33 / ugc 13 / monitor 2） |
| 服务端 workspace（`/ugc/*`） | ✅ 9 条全部真实实现（非桩） | service 层 + DB + provider 齐备 |
| 服务端 P1 与部分能力 | ❌ 桩未注册 / 无实现 | 33 个 `not implemented` 桩中，32 个未被覆盖 |
| 前后端对接 | ⚠️ **7 条客户端调用没有对应路由** | 6 条有桩未注册 + 1 条完全无实现 |
| 生产可用性的最大未知数 | ❓ 私有 `custom.py` 是否存在并补齐了这些路由 | 本仓 0 个 `custom.py` |

**一句话结论**：客户端与「任务主链路 + workspace」服务端都已成型；不完整集中在 **① 7 条客户端调用缺服务端路由、② env 上传链路整条缺失、③ workspace 的 P1（配额/审计/用量）与崩溃恢复正确性、④ 少量客户端硬 bug**。

---

## 1. 审计范围

| 侧 | 纳入范围 | 排除 |
| --- | --- | --- |
| 客户端 | `client/**`（含 `commands/`、`api/`、`model/`、`remote/`）、`plugins/haiworkspace/**`、`plugins/haienv/**`、`base_model/base_user_modules/**` | `docs/**`、`exporter/**` |
| 服务端 | `api/**`、`cloud_storage/**`、`server_model/task_impl/**`、`one/one_etc/core.toml`、`uvicorn_server.py` | `scheduler/**`（仅抽查桩）、`monitor/**`、`fetion/**` |
| 前端/Hub | —— | 不在本次范围（但服务端有大量「仅供前端/管理工具」的路由，见 §5 反向观察） |

---

## 2. 代码结构与三条关键约定

### 2.1 客户端 = 1 个 CLI + 2 个插件

```
client/                     包名 hfai，可执行文件 hai-cli / hfai
├── hfai_cli.py             CLI 入口：注册 15 个核心子命令 + 动态插件子命令
├── commands/               命令层（click），负责参数、展示、退出码
├── api/                    HTTP 调用层，统一走 api_utils.async_requests
├── model/                  客户端侧 User 及其子模块（配额/存储/镜像/制品…）
└── remote/                 任务内 hfai.remote 远程执行（非 CLI 命令）

plugins/haiworkspace/       → hfai workspace ...（独立 wheel）
plugins/haienv/             → hfai env ... / haienv ...（独立 wheel）
```

插件装配：`client/commands/utils.py:29-39` 声明 `haiworkspace`/`haienv` 两个插件；`client/hfai_cli.py:71-83` 为每个插件注册一个壳命令，实际执行 `os.system("{plugin_path} {argv[2:]}")`。**因此插件是独立进程**，`sys.argv[0]` 在插件内是插件自身的可执行文件（这一点导致 §3.3 的缺陷）。

### 2.2 服务端 = 1 个 FastAPI app + 4 个 server group + 2 个旁路宿主

| 项 | 说明 |
| --- | --- |
| App | `api/app.py:43` 唯一 FastAPI 实例（中间件、异常改写、Prometheus） |
| 路由注册点 | `api/register/implement.py` **全部 83 条**，按 `SERVER` 环境变量分组门控（`:23` operating / `:67` ugc / `:90` query / `:133` monitor） |
| Server group | `operating`(35) · `query`(33) · `ugc`(13) · `monitor`(2)；启动项见 `one/supervisord.conf` |
| 旁路宿主 | `uvicorn_server.py:24-31`：`cloud-storage` → `cloud_storage.api:app`（旧无前缀实现，900 行）；`log-forest` → `log_forest_server:app` |
| 事件钩子 | `api/register/implement.py:86-87` 仅在 ugc 组注册 `startup_recover` / `shutdown_workers` |

### 2.3 ⚠️ 必须理解的「三层文件」约定

每个 API 包采用 `default.py` + `implement.py` + `custom.py`：

```
xxx/
├── default.py     开源基线：真实逻辑 或 大量 {'success':1,'msg':'not implemented'} 桩
├── implement.py   from .default import *  +  from .custom import *  +  本仓真实实现
└── custom.py      私有部署覆盖层 —— 本仓不存在
```

`custom.py` 由 `base_model/utils.py:50-61` 的 `CustomFinder` 兜底：找不到时**注入一个空模块**，使 `from .custom import *` 不报错。触发条件（`base_model/utils.py:33-43`）为路径命中 `SERVER_CODE_DIR`（默认 `/high-flyer/code/multi_gpu_runner_server`）或路径含 `hfai/client`、`hfai/base_model`。

> **推论**：本仓的每一个 `not implemented` 桩都有三种可能含义，判定时必须区分——
>
> | 含义 | 判别方式 | 本文标注 |
> | --- | --- | --- |
> | A. 本仓确实没实现，且无人调用 | 未被 `implement.py` 覆盖 + 未注册 + 客户端不调用 | 「死桩」 |
> | B. 本仓没有，但私有 `custom.py` 可能实现 | 同上，但**客户端会调用**或**已注册** | 「缺失（可能由私有层提供）」 |
> | C. 本仓已实现，`default.py` 的桩被 `implement.py` 遮蔽 | 同名函数在 `implement.py` 中存在 | 「完整」 |

---

## 3. 客户端功能清单

### 3.1 核心命令（`client/hfai_cli.py:52-66` 注册 15 个）

| 命令 | 实现位置 | 状态 | 调用的服务端接口 |
| --- | --- | --- | --- |
| `init` | `client/commands/hfai_init.py:38-71` | ✅ | `POST /operating/user/access_token/create` |
| `python` / `bash` / `exec` | `client/commands/hfai_python.py:127-139` | ✅（见缺陷 C-6） | `POST /operating/task/create` |
| `run` | `client/commands/hfai_experiment.py:153-188` | ✅ | `/operating/task/create` + artifact 映射 |
| `status` | `client/commands/hfai_experiment.py:40-52` | ✅ | `POST /query/task` |
| `describe` | `client/commands/hfai_experiment.py:65-71` | ✅ | 同上 |
| `list` | `client/commands/hfai_experiment.py:194-214` | ✅ | `POST /query/task/list` |
| `logs`（默认） | `client/commands/hfai_experiment.py:106-142` | ✅ | `POST /query/task/log` |
| `logs -c`（容器日志） | `client/api/experiment_api.py:531-534` | ❌ | `POST /query/task/container_log` —— **服务端未注册** |
| `stop`（含 `--succeeded/--failed`） | `client/commands/hfai_experiment.py:222-236` | ✅ | `POST /operating/task/stop`（`TASK_OP_CODE` 含 `succeed`） |
| `ssh` | `client/commands/hfai_experiment.py:243-258` | ✅ | `POST /query/task/ssh_ip` |
| `nodes` | `client/commands/hfai_nodes.py:7-13` | ✅ | `POST /query/node/list` |
| `whoami` | `client/commands/hfai_whoami/implement.py:35-47` | ✅ | `POST /query/user/info` + `quota/list` + `access_token/list` |
| `images list` | `client/api/image_api.py:6-13` | ✅ | `POST /ugc/user/train_image/list` |
| `images load` | `client/api/image_api.py:16-25` | ❌ **AttributeError** | 见缺陷 C-3 |
| `images delete` | `client/api/image_api.py:27-35` | ❌ **AttributeError** | 见缺陷 C-3 |
| `artifact set/get/list/remove/map/unmap/showmapped` | `client/commands/hfai_artifact.py`、`client/api/artifact_api.py` | ✅ | 6 条 artifact 路由均已注册 |

**未注册到 CLI 的死代码**

| 文件 | 说明 |
| --- | --- |
| `client/commands/hfai_sync.py:10-20` | 定义了 `sync` 命令（`os.system(rsync)`），但从未被 import、未 `add_command`，完全不可达 |

**文档有、代码没有的命令**

| 文档引用 | 现状 |
| --- | --- |
| `docs/_sources/cli/cluster.rst.txt:4` → `hfai.client.commands.hfai_monitor:monitor` | ❌ `client/commands/hfai_monitor.py` 不存在 |
| 同上 `:12` → `hfai_prof:prof` | ❌ 不存在 |
| 同上 `:16` → `hfai_validate:validate` | ❌ 不存在（且其服务端 `/operating/{node,task}/validate` 也是未注册桩） |
| 同上 `:20` → `hfai_version:version` | ❌ 不存在（仅 `--version` 选项可用） |
| `docs/_sources/cli/ugc.rst.txt:5` → `hfai.client.commands.custom.hfai_workspace:workspace` | 已注释；实际由插件 `haiworkspace` 提供 |

### 3.2 插件 `hfai workspace`（`plugins/haiworkspace/**`）

命令注册：`plugins/haiworkspace/haiworkspace/client/cli.py:18-24` → `init / push / pull / download / diff / list / remove`。

| 子命令 | 命令层 | 业务层 | 状态 |
| --- | --- | --- | --- |
| `init <name>` | `client/command.py:22-33` | `client/workspace_api.py:16-72` | ⚠️ 功能可用，但传枚举（缺陷 C-1） |
| `push` | `client/command.py:36-69` | `client/workspace_api.py:108-151` → `workspace_util.py:319-454` | ✅ 主链路完整 |
| `pull` | `client/command.py:72-96` | `workspace_api.py:154-172` → `workspace_util.py:457-554` | ✅ |
| `download <remote_path>` | `client/command.py:99-128` | 复用 `pull`（`command.py:119`） | ⚠️ `required=True` 与 `default='checkpoint'` 冲突（缺陷 C-4） |
| `diff` | `client/command.py:131-144` | `workspace_api.py:175-186` | ✅ |
| `list` | `client/command.py:147-156` | `workspace_api.py:189-204` | ✅ |
| `remove <name> [-f files]` | `client/command.py:159-177` | `workspace_api.py:207-226` | ✅（含 `..` 路径校验） |

**调用的服务端接口（9 条，与 `api/register/implement.py:73-83` 一一对应）**

| 接口 | 客户端调用点 | Body 形态 |
| --- | --- | --- |
| `POST /query/user/info` | `workspace_api.py:44` | query |
| `POST /ugc/get_sts_token` | `workspace_util.py:127` | query |
| `POST /ugc/set_sync_status` | `workspace_util.py:142` | query |
| `POST /ugc/get_sync_status` | `workspace_util.py:150` | query |
| `POST /ugc/delete_files` | `workspace_util.py:161` | `{"file_list":{"files":[...]}}` |
| `POST /ugc/cloud/cluster_files/list` | `workspace_util.py:181` | `{"file_list":{"files":[...]}}` |
| `POST /ugc/sync_to_cluster` | `workspace_util.py:241` | `{"file_list":{"files":[...]}}` |
| `GET /ugc/sync_to_cluster/status` | `workspace_util.py:253` | — |
| `POST /ugc/sync_from_cluster` | `workspace_util.py:277` | `{"file_infos":{"files":[{...}]}}` |
| `GET /ugc/sync_from_cluster/status` | `workspace_util.py:290` | — |

> 客户端固定用 `aiohttp data=<json str>` 发送 → `Content-Type: text/plain`；服务端在 `cloud_storage/service/compat.py:104-127` 手工解析原始 Body 来吸收该形态。

### 3.3 插件 `hfai env`（`plugins/haienv/**`）

命令注册：`plugins/haienv/haienv/client/cli.py:19-22` → `create / list / remove / config`；`config` 下 `show / clear / append`（`client/command.py:115/135/153`）。

| 子命令 | 实现 | 状态 | 是否联网 |
| --- | --- | --- | --- |
| `create <name>` | `client/command.py:27-45` → `client/api.py:16-133` | ⚠️ 可用但硬编码（缺陷 C-7） | 否（本地 conda） |
| `list [-u] [-a] [-o json]` | `client/command.py:48-94` → `client/api.py:136-145` | ✅ | 否 |
| `remove <name>` | `client/command.py:97-104` → `client/api.py:158-170` | ✅ | 否 |
| `config show` | `client/command.py:115-132` | ✅ | 否 |
| `config clear` | `client/command.py:135-150` | ✅ | 否 |
| `config append` | `client/command.py:153-170` | ✅ | 否 |
| **`push`（上传 venv 到集群）** | **不存在** | ❌ 缺失 | —— |

**`haienv` 的四种调用形态**（同一套本地数据模型）：① `hfai env <sub>`；② `haienv <sub>`（`plugins/haienv/haienv/haienv` 自造 `/tmp/haienv` 引导脚本）；③ `source haienv <name>`（同一脚本的另一分支，环境加载器）；④ 进程内 `import haienv; haienv.set_env(...)`（`haienv/haienv.py:20-102`）。

**上传链路的唯一实现**在插件之外：`client/api/venv_api.py:10-46` 的 `push_venv`——**全仓无调用方**、未被 `client/api/__init__.py` 导出。其失败链路见缺陷 C-8。

### 3.4 客户端缺陷汇总

| ID | 缺陷 | 证据 | 影响 |
| --- | --- | --- | --- |
| **C-1** | **`FileType` 枚举字符串化**：`class FileType(str, Enum)`（`conf/utils.py:22-34`）经 f-string 插值产出字面量 `FileType.WORKSPACE`，而非 `workspace` | `workspace_api.py:67,101,184-185,196,217`；`workspace_util.py:127,142,150,161,181,241,277`；`venv_api.py:25` | 服务端 `normalize_enum`（`cloud_storage/service/compat.py:25-53`，默认开）能兜住；`legacy_param_compat=false` 时全部 400 |
| **C-2** | **`file_type == FileType.ENV` 比较恒为 False** | `plugins/haiworkspace/haiworkspace/client/workspace_api.py:125` | env 上传分支客户端侧永不命中，打印「不支持的file_type」 |
| **C-3** | **`hfai images load/delete` 抛 AttributeError**：调用 `user.image.async_load/async_delete`，但客户端 `UserImage` 只有 `async_get` | `client/api/image_api.py:23,34`；`client/model/user_impl/default.py:5-8`；接口 `base_model/base_user_modules/default.py:20-22` | 两个子命令完全不可用 |
| **C-4** | `download` 的 `required=True` 与 `default='checkpoint'` 冲突，`remote_path == ''` 判断永不成立 | `plugins/haiworkspace/haiworkspace/client/command.py:100,115` | 默认值失效，仅提示信息问题 |
| **C-5** | 未知 provider **静默回退 `MockApi`**（返回空结果但 `success:1`） | `plugins/haiworkspace/haiworkspace/client/workspace_util.py:311-315` | 可能「看起来成功但没上传」 |
| **C-6** | `hfai python` 的 workspace 自动 push 只在 **external 构建**下存在；内部构建被 `patch_client.py` 裁掉 | `client/commands/hfai_python.py:171`；`client/patch_client.py` | 内部模式需手工 `workspace push` 或手写 workspace URI |
| **C-7** | `haienv create` 硬编码只接受 CUDA 11.1/11.3、需交互 `input()` 确认、`__IS_HF_ENV__` 占位符从未替换 | `plugins/haienv/haienv/client/command.py:41-43`；`client/api.py:34-36,88-118`；`client/script.py:122` | 新镜像上不可用；无法自动化；生成的 `activate` 语义错误 |
| **C-8** | `push_venv` 三重失败链：① `sys.argv[0]` 拼出 `haienv workspace push`（未知子命令）② `--file_type {FileType.ENV}` 字面量 ③ 目标路由未注册且 `result['path']` 无 None 防御 | `client/api/venv_api.py:22,23,24,25` | env 上传不可用 |
| **C-9** | 插件非自包含：`workspace_util.py` 依赖的 `.api_config/.api_utils/.utils/.provider` 只在打包时由 `install.sh` 拷入 | `plugins/haiworkspace/install.sh:7-10` | 从源码直接运行/测试会 `ModuleNotFoundError` |
| **C-10** | `hfai sync` 死代码 | `client/commands/hfai_sync.py:10-20` | 不可达 |
| **C-11** | `haienv` 包装脚本：`[[ "$2" -ne "-u" ]]` 用算术运算符比较字符串；`cat <<EOF >> $prog` 追加而非截断 | `plugins/haienv/haienv/haienv:5,30` | `-u` 参数校验不可靠；`/tmp/haienv` 残留会累积 |

---

## 4. 服务端功能清单

### 4.1 路由总览

| Server group | 路由数 | 门控位置 | 主要用途 |
| --- | --- | --- | --- |
| `operating` | 35 | `api/register/implement.py:23` | 任务生命周期、用户/配额/权限管理、节点/挂载点运维 |
| `query` | 33 | `:90` | 任务/用户/集群/存储查询 |
| `ugc` | 13 | `:67` | nodeport、train_image、**workspace 9 条** |
| `monitor` | 2 | `:133` | 性能时序、用户存储 |
| （不分组） | 3 | `api/app.py:280,283` + 条件 `/swagger/*` | metrics / 健康检查 / swagger |

**AST 级核对结果**：83 条注册引用的处理函数**全部存在**，无 `ImportError`/`AttributeError` 型空注册；**没有任何已注册路由指向 `not implemented` 桩**。

### 4.2 已完整实现（本仓真实逻辑）

| 域 | 内容 | 证据 |
| --- | --- | --- |
| 任务生命周期 | `create/resume/stop/suspend/tag/untag/share/unshare/fail/priority/group/service_control/restart_log` | `api/task/experiment/implement.py`、`api/training.py:16` |
| 任务查询 | `query/task`、`list`、`log`、`sys_log`、`log/search`、`ssh_ip`、`overview`、`time_range`、`on_node`、`artifact/*` | `api/query/optimized/task/implement.py`、`api/task/experiment/implement.py` |
| 用户/配额/权限 | `user/info`、`quota/list`、`training_quota/*`、`access_token/*`、`artifact/*`、`nodeport/*` | `api/user/**`、`api/query/optimized/user.py` |
| 集群/节点/存储 | `node/list`、`client_overview`、`host_info` CRUD、`storage/get_by_task`、`mount_point/*` | `api/resource/cluster/implement.py`、`api/resource/storage/implement.py` |
| **workspace `/ugc/*` 9 条** | STS、同步状态、集群文件分页、双向同步、删除 —— 全部真实实现 | `api/resource/cloud_storage/default.py:40-182` → `cloud_storage/service/**`；DB 层 `server_model/user_impl/aio_user_db/default.py:34-119` |
| workspace 任务侧 | `<provider>://<group>/<user>/workspaces/<name>` 解析、提交期存在性校验、pod 挂载项生成 | `server_model/task_impl/workspace_resolver.py`、`server_model/task_impl/runtime_mounts/default.py:20-55`、`api/operation/implement.py:245-249` |
| 崩溃恢复 / 进程池回收 | 心跳 + `SET NX` 互斥 + startup/shutdown 钩子 | `cloud_storage/service/recovery.py`、`api/register/implement.py:86-87` |
| env 运行时消费 | `HAIENV_PATH` 注入 + `source haienv <name> [-u owner]` | `server_model/task_impl/single_task_impl.py:60-61,150-158` |

### 4.3 未实现 / 桩（按可达性分类）

全仓 `default.py` 中返回 `'not implemented'` 的函数共 **33 个**，其中**仅 1 个**（`task_sys_log_api`）被 `implement.py` 同名覆盖。其余 **32 个**按可达性分为三类：

| 类别 | 函数（所在文件） | 客户端是否调用 | 是否有路由 |
| --- | --- | --- | --- |
| **A. 客户端会调用但未注册**（→ 见 §5） | `swap_memory`(`api/task/swap/default.py:3`)、`update_cluster_venv`(`api/resource/storage/default.py:9`)、`haiprof_task`(`api/task/experiment/default.py:24`)、`validate_task`(`:10`)、`validate_nodes`(`:17`)、`task_container_log_api`(`:46`) —— 共 6 个 | ✅ | ❌ |
| **B. 无任何入口（死桩 / 私有扩展点）** | `create_task`(`api/task/experiment/default.py:3`，已被 `create_task_v2` 取代)、`create_task_base_queue`、`switch_schedule_zone`、`checkpoint_api`、`syslog_api`、`hfai_image_load/update_status/list/delete`(4)、`clone_dataset*`(3)、`get_user_monitor_info`、`get_user_weka_usage`、`get_external_user_storage_usage`、`handle_user_usage_exceed`、external-user API(6)、`get_user_community_info`、`get_task_distribute_api`、`set_external_user_cloud_storage_quota` —— 共 25 个 | ❌ | ❌ |
| **C. 审计子系统** | `run_audit`(`cloud_storage/audit/default.py:10`) —— 共 1 个 | ❌ | ❌ |

> 类别 B 的桩多数是**有意留出的私有覆盖接缝**（见 §2.3 含义 B），不应一律视为缺陷；但 `checkpoint_api`、`syslog_api`、`get_task_distribute_api` 等连前端入口都没有，属真正的死代码。
> workspace 的 P1 需求（API-10 配额 / API-11 env 上传 / API-12 用量）对应函数分别落在 A、B 两类中。

**完全无实现**（连桩都没有）：`/operating/rerun_task` —— 客户端 `client/api/experiment_api.py:87` 会调用，全仓无任何服务端代码。

### 4.4 workspace provider 支持度

| provider | 状态 | 证据 |
| --- | --- | --- |
| `oss` | ✅ 完整（AssumeRole + 前缀策略） | `cloud_storage/provider/oss.py` |
| `s3` / `rustfs` | ⚠️ 可用但**安全降级**：`get_access_token` 下发**静态 AK/SK、不限制 prefix** | `cloud_storage/provider/s3.py:276-293` |
| `mock` | 桩，全部返回空；`get_access_token` 返回 `{}` | `cloud_storage/provider/mock.py` |
| `localfs` | ❌ **文件不存在**（设计 ADR-8 要求） | `cloud_storage/service/context.py:168-170` 显式报错 |

**接线与风险**：`oss`、`s3`/`rustfs` 正常；`localfs` 直接抛 `CLOUD_STORAGE_NOT_CONFIGURED`；**其他任意字符串静默降级为 `MockApi` 且仍返回 `success:1`**（`cloud_storage/service/context.py:168-174`）——典型的「假成功」。
另：`cloud_storage/provider/__init__.py:1-2` 在导入期强制依赖 `oss2`/`aliyunsdkcore`，违背设计 ADR-11 的「惰性 provider」初衷（`cloud_storage/utils.py:27`）。

### 4.5 服务端缺陷

| ID | 级别 | 缺陷 | 证据 |
| --- | --- | --- | --- |
| **S-1** | 高 | **缺通用 `Exception` 处理器**：只注册了 `StarletteHTTPException` / `WorkspaceError` / `RequestValidationError`。凡未被包成 `WorkspaceError` 的异常都返回**裸 500 文本、无 `success` 字段**，破坏客户端「先断言 `success` in result」的契约（CON-3）。典型触发点：写库、查库、`FileInfo(**item)` 遇非 dict | `api/app.py:211-256`；`api/resource/cloud_storage/default.py:68,82,152` |
| **S-2** | 高 | **崩溃恢复不正确**（5 个子问题）：① 恢复不改写旧 instance 的 param 快照 → 每次重启重传同一任务；② 恢复锁 TTL 300s 从不主动释放；③ `_stale` 默认 600s 造成 10 分钟恢复盲区（pull 快照还写死 `instance: None`）；④ `execute_*` 接收 `index` 却不下传、内部按 token 重算 → token 变化时写到另一个 index，客户端轮询原 index 得 `NOT_FOUND_INDEX`；⑤ 跨版本快照形态互踩（旧宿主写 `file_list={"files":[...]}`，新恢复当 list 用 → 去下载名为 `files` 的对象） | `cloud_storage/service/recovery.py:33,72-79,108,113,115,145-152`；`sync_to_cluster.py:63,73,178-181,195`；`sync_from_cluster.py:68,86,241,252-263` |
| **S-3** | 中 | **SQL 字符串拼接（注入面）**：`/query/service_task/list` 的多字段过滤、tag 列表、`user_name`、`artifact_name/version/page`、`start_time/end_time` 全部 f-string 直插 | `api/query/optimized/service_task/implement.py:22-79`；`api/task/experiment/implement.py:181-183,193,251-260`；`api/query/optimized/task/implement.py:253-272` |
| **S-4** | 中 | **挂载点安全校验是空实现**：`security_check(mount_point)` 函数体只有 `pass`，且 `create_mount_point` 调用了它但结果被忽略 | `api/resource/storage/default.py:25-27`；`api/resource/storage/implement.py:158` |
| **S-5** | 中 | 少量逻辑错误：`return HTTPException(403, ...)` 未 `raise`（越权时变 500）；artifact map 的 f-string 引用内置 `input` 导致提示恒为 "input"；`service_task.create` 的 `except` 在 `task` 非空时继续返回 `success:1` | `api/task/experiment/implement.py:315,227`；`api/task/service_task/implement.py:148-159` |
| **S-6** | 中 | **权限语义冲突**：`/ugc/user/nodeport/{create,delete}`、`/query/user/nodeport/list` 要求 internal role，但客户端由普通用户凭自身 token 直接调用；`api/task/port.py:12` 留有 `TODO(role)` | `api/task/port.py:12-16,32,43`；`client/model/user_impl/implement.py:28-43` |
| **S-7** | 中 | **灰度开关覆盖不全**：`check_feature_enabled` 只在 3 个接口调用（sync_to_cluster / sync_from_cluster / delete_files）；`get_sts_token`、`set_sync_status`、`get_sync_status`、`cluster_files/list` 可绕过 `enabled/enabled_users/enabled_groups` | `cloud_storage/service/sync_to_cluster.py:43`、`sync_from_cluster.py:48`、`delete.py:23` vs `api/resource/cloud_storage/default.py:40,55,77,89` |
| **S-8** | 中 | **两套并行 workspace 实现**：新 `cloud_storage/service/**`（被 `/ugc/*` 使用）与旧 `cloud_storage/api.py`（900 行，被 `cloud-storage` 宿主使用，且自带 audit 线程与恢复逻辑）。语义已出现漂移：旧实现有 `create_quota_df()` 刷新配额，新实现读不到配额直接兜底 102400MB | `cloud_storage/api.py:31-70,465-467` vs `cloud_storage/service/sync_from_cluster.py:35-42` |
| **S-9** | 低 | 审计子系统是桩：`run_audit` 直接跳过，且只在旧宿主启动；`SEC-07 审计`实际仅剩 `delete.py:53-55` 一行日志 | `cloud_storage/audit/default.py:10`；`api/resource/cloud_storage/default.py:187-188` |
| **S-10** | 低 | `api/app.py:51` 引用的 `api/openapi_specification/user_api.yaml` **在本仓不存在**（swagger 文档加载可能失败） | `api/app.py:46-58` |
| **S-11** | 低 | 其余：`api/register/default.py:12` 直接 `os.environ['SERVER']`（未设置即 `KeyError`）；`post_process_cluster_df` 为恒等函数；`check_sidecar_get_err` 恒返回 `None`；`query/task/container_monitor_stats/list` 的 try-import 失败会 NameError；关机时进程池 `shutdown()` 无 `cancel_futures` 会阻塞 | `api/register/default.py:11-12`；`api/resource/cluster/default.py:2-3`；`api/operation/default.py:34-35`；`api/query/optimized/task/implement.py:18-21,361`；`cloud_storage/utils.py:88-94` |

---

## 5. 客户端调用 ↔ 服务端注册 缺口矩阵

对客户端全部 `mars_url()/...` 调用点与 `api/register/implement.py` 注册表做程序化差集，**7 条客户端调用在本仓没有路由**（另有 `/monitor_v2/*` 2 条属独立监控服务，不在本仓）：

| # | 客户端调用点 | 服务端现状 | 用户可见后果 |
| --- | --- | --- | --- |
| 1 | `POST /operating/rerun_task`<br>`client/api/experiment_api.py:87` | **全仓无任何实现**（连桩都没有） | 任务重跑不可用 |
| 2 | `POST /operating/task/validate`<br>`client/api/experiment_api.py:520` | 桩 `api/task/experiment/default.py:10`，未注册 | `validate` 类功能 404 |
| 3 | `POST /operating/node/validate`<br>`client/api/experiment_api.py:504` | 桩 `api/task/experiment/default.py:17`，未注册 | 同上 |
| 4 | `POST /operating/task/haiprof`<br>`client/api/haiprof_api.py:7` | 桩 `api/task/experiment/default.py:24`，未注册 | 性能剖析提交失败 |
| 5 | `POST /query/task/container_log`<br>`client/api/experiment_api.py:533` | 桩 `api/task/experiment/default.py:46`（返回空串），未注册 | `hfai logs -c` 404 |
| 6 | `POST /ugc/swap_memory`<br>`client/api/swap_api.py:13` | 桩 `api/task/swap/default.py:3`，未注册 | 任务内 `set_swap_memory()` 失败 |
| 7 | `POST /ugc/update_cluster_venv`<br>`client/api/venv_api.py:22` | 桩 `api/resource/storage/default.py:9`，未注册 | venv 上传链路断点 |

> **保留意见**：第 2–7 条**可能由生产环境的私有 `api/register/custom.py` 补注册**（本仓 `custom.py` 数量为 0，无法验证）。第 1 条 `rerun_task` 连桩都没有，私有实现也只能从零写。
>
> **反向观察**：注册表中另有 **41 条路由客户端从不调用**（如 `task/share`、`task/resume`、`task/fail`、`user/create`、`node/host_info/*`、`service_task/*`、`training_quota/list_all` 等），它们是**前端 Hub / 管理工具 / 私有代码**的接口——这属于正常分工，不是缺陷。（已确认这 41 条在 `client/**`、`plugins/**`、`base_model/**` 中均无调用点。）

---

## 6. `workspace` 特性端到端判定

### 6.1 判定矩阵

| 环节 | 客户端 | 服务端 | 判定 |
| --- | --- | --- | --- |
| `init` 写 `.hfai/workspace.yml` + 登记同步状态 | ✅ | ✅ | **可用** |
| `push`（本地 → bucket → 集群） | ✅ | ✅ | **可用**（实测通过） |
| `diff` / `list` | ✅ | ✅ | **可用** |
| `pull` / `download` | ✅ | ✅ | **可用** |
| `remove <name>` / `remove -f <file>` | ✅ | ✅ | **可用** |
| 任务侧 `oss://` 解析 + 挂载 | —— | ✅ | **可用**（提交期校验通过） |
| 任务 pod 内 `cd {workspace}` 并执行用户脚本 | —— | ⚠️ | **未验证**（见 §6.3 #1） |
| `hai-cli python` 自动 push 联动 | ⚠️ | —— | **内部构建不可用**（见 C-6） |

### 6.2 仓库内已记录的实测结果

引用 [workspace/workspace-server-task-list.md](workspace/workspace-server-task-list.md) §8（v1 最小闭环，2026-10-01 部署并实测）：

| 验证 | 结果 |
| --- | --- |
| `/ugc/*` 接口冒烟 | **8/8 PASS**（含枚举串 + `text/plain` + `{"file_list":{...}}` 兼容形态） |
| 7 个子命令 E2E | **19/19 PASS**（两个镜像各跑通一次） |
| 任务侧 `s3://...` 解析 | 通过（正确解析为集群路径并写入 `code_file`/`workspace`） |
| 提交期存在性校验 | 通过（未 push 时返回可读错误，不产生「已创建但立即失败」） |

同处记录 6 项实测暴露并已修复的阻断问题（Redis 异步池污染、`finalize_status` 写成 async 导致状态悬挂、部分删除误软删、zip 进度提前上报、`add_runtime_mounts` 访问 `user` 导致 SIGABRT、manager pod 看不到共享盘导致误判「尚未同步」）。

### 6.3 已知限制（来源于同文档 §8.4，本次审计复核代码一致）

1. **任务容器内运行未验证** —— `oss://` 解析、提交期校验、挂载项生成均通过，但 pod 内 `cd {workspace}` 执行用户脚本的最终确认尚未完成；
2. **平台自带客户端的 workspace 自动联动不可用** —— 内部模式客户端经 `patch_client.py` 裁掉了 `client/commands/hfai_python.py:171` 的 external 分支，需手工 push 或手写 workspace URI；
3. **STS 最小权限降级** —— `s3`/RustFS 无 STS 角色扮演，下发静态 AK/SK，SEC-02 的「最小权限前缀」未实现；
4. **registry 推送无凭据** —— 部署走本机 import 到 containerd 的旁路；
5. **宿主机上存在未收编的 rustfs 实例**（端口 9000，默认凭据）—— 与环境内 19000 端口的容器并存，建议尽快停用；
6. **P1 未实现** —— FR-17/18/21、API-10/11/12；`cloud_storage_quota` 访问器未加，`sync_from_cluster` 用 100 GiB 兜底。

---

## 7. `env`（haienv）特性端到端判定

| # | 场景 | 客户端 | 服务端 | 判定 |
| --- | --- | --- | --- | --- |
| S1 | 开发容器内 `haienv create` → 任务指定 `HF_ENV_NAME` | ✅ | ✅ | **可用**（env 直接建在共享盘） |
| S2 | 代码内 `haienv.set_env()` | ✅ | — | **可用**（需 `HAIENV_PATH` 可达） |
| S3 | `hfai env list`（本用户） | ✅ | — | **可用** |
| S4 | `hfai env list -u <他人>` | ⚠️ | — | 仅共享盘可见时可用（无路径校验） |
| S5 | 集群外建 env → 推到集群 | ❌ 无命令 + 枚举 bug | ❌ 端点未注册 | **不可用** |
| S6 | 推送后任务 `source haienv <name>` | — | ❌ 无注册表写入 | **不可用** |
| S7 | 推送 `extend=True` 的环境 | ❌ 客户端明确拒绝 | — | 设计上不支持 |

**根因不止「缺端点」，还有路径约定不一致**：

| 环节 | 路径 | 证据 |
| --- | --- | --- |
| 数据面（上传落盘） | `{env_path}/{group}/shared/hfai_envs/{user}/{name}`，`env_path=/hf_shared` | `cloud_storage/utils.py:469-474`；`one/one_etc/core.toml:113` |
| 运行时（搜索根） | `dirname(HAIENV_PATH) = /hf_shared/hfai_envs`，再遍历其下**用户目录**找 `venv.db` | `server_model/task_impl/single_task_impl.py:61`；`plugins/haienv/haienv/haienv:45-90` |

两者相差一层 `{group}/shared`，因此**即使上传成功，`source haienv` 也发现不了该环境**。补齐需要三件事：客户端 `env push` 入口（含 `.value` 修复与可执行文件解析）、`env_root` 路径对齐、上传后写 `venv.db` 注册表（可复用镜像内 `haienv` 包的 `Haienv.insert`）。设计见 [env/env-server-design.md](env/env-server-design.md)，规模约 6.5 人日，不新增 Postgres 表。

---

## 8. 不完整清单（按优先级）

| 优先级 | ID | 侧 | 事项 | 证据 | 影响面 |
| --- | --- | --- | --- | --- | --- |
| **P0** | C-3 | 客户端 | `images load/delete` 抛 AttributeError | `client/api/image_api.py:23,34` | 2 个子命令完全不可用 |
| **P0** | S-1 | 服务端 | 缺通用 `Exception` 处理器，异常返回裸 500 无 `success` | `api/app.py:211-256` | 破坏客户端全局契约 |
| **P0** | S-2 | 服务端 | 崩溃恢复 5 项正确性问题 | `cloud_storage/service/recovery.py` 等 | 生产每次重启必踩 |
| **P0** | §5#1 | 服务端 | `/operating/rerun_task` 完全无实现 | `client/api/experiment_api.py:87` | 重跑任务不可用 |
| **P1** | C-1 / C-2 | 客户端 | `FileType` 枚举字符串化；env 分支恒不命中 | `conf/utils.py:22-34` 等 | 依赖服务端兼容层兜底 |
| **P1** | §5#2-7 | 服务端 | 6 条客户端调用只有桩、未注册 | 见 §5 表 | 6 个功能 404（除非私有层补齐） |
| **P1** | §7 | 双端 | env 上传链路整体缺失 + 路径约定不一致 | 见 §7 | 跨端 env 不可用 |
| **P1** | S-4 | 服务端 | 挂载点安全校验为空实现 | `api/resource/storage/default.py:25-27` | 安全 |
| **P1** | S-3 | 服务端 | 多处 SQL 字符串拼接 | 见 S-3 证据 | 安全 |
| **P2** | S-6 | 服务端 | nodeport 接口要求 internal role 与 `/ugc` 语义冲突 | `api/task/port.py:12-16` | 外部用户必 401 |
| **P2** | S-7 | 服务端 | 灰度开关覆盖不全（4 个接口可绕过） | 见 S-7 证据 | 灰度发布风险 |
| **P2** | S-8 | 服务端 | 两套并行 workspace 实现语义漂移 | `cloud_storage/api.py` vs `service/` | 维护风险 |
| **P2** | C-6 | 客户端 | `hfai python` 内部构建不自动 push workspace | `client/commands/hfai_python.py:171` | 使用体验 |
| **P2** | C-7 / C-11 | 客户端 | `haienv create` 硬编码 + 包装脚本缺陷 | 见 C-7/C-11 | 新镜像不可用 |
| **P3** | S-9 / S-10 / S-11 | 服务端 | 审计桩、swagger yaml 缺失、若干小逻辑问题 | 见 §4.5 | 次要 |
| **P3** | C-4 / C-5 / C-9 / C-10 | 客户端 | 默认值冲突、Mock 静默兜底、插件非自包含、死代码 | 见 §3.4 | 次要 |
| **P3** | —— | 文档 | `validate`/`monitor`/`prof`/`version` 四个命令文档有、代码无 | `docs/_sources/cli/cluster.rst.txt:4-20` | 文档与实现不一致 |
| **P3** | S-* | 服务端 | workspace P1（API-10/11/12、配额、审计、用量） | `docs/.../workspace-server-task-list.md` §8.4 | 已知排期外 |
| **P3** | §4.4 | 服务端 | `localfs` provider 缺失 | `cloud_storage/service/context.py:168-170` | 测试/单机部署路径 |

---

## 9. 建议的收口顺序

1. **修客户端硬 bug（成本最低）**：C-3（补 `async_load/async_delete` 或下线命令）、C-1（统一 `.value`，可给 `FileType` 加 `__str__ = str.__str__` 一次性解决）、C-2。
2. **给 7 条缺失路由一个明确态度**：要么在开源层注册并实现（至少 `rerun_task`），要么在 `api/register/implement.py` 里显式声明「由私有 `custom.py` 提供」，避免使用者误以为功能存在。
3. **加通用 `Exception` 处理器**（S-1），保证任何路径都返回带 `success` 的 JSON。
4. **修崩溃恢复**（S-2 的 5 个子项）——这是生产重启后必然暴露的问题。
5. **env 链路**：按 [env/env-server-design.md](env/env-server-design.md) 补 `env push` + `env_root` 路径对齐 + `venv.db` 注册。
6. **安全收口**：SQL 参数化（S-3）、`security_check` 落地（S-4）、`s3` provider 的 STS/前缀策略（§6.3 #3）。
7. **文档对齐**：删除或标注 `docs/_sources/cli/cluster.rst.txt` 中不存在的 4 个命令。

---

## 附录 A · 关键证据索引

| 主题 | 文件:行 |
| --- | --- |
| CLI 入口与命令注册 | `client/hfai_cli.py:52-66` |
| 插件装配 | `client/commands/utils.py:29-39`、`client/hfai_cli.py:71-83` |
| 服务端路由唯一注册点 | `api/register/implement.py:23,67,90,133` |
| 三层文件约定 | `base_model/utils.py:33-61` |
| 客户端 HTTP 契约 | `client/api/api_utils.py:48-60` |
| 客户端兼容层（枚举/Content-Type/Body 外壳） | `cloud_storage/service/compat.py:25-127` |
| workspace 9 条接口接入层 | `api/resource/cloud_storage/default.py:40-182` |
| workspace 领域层 | `cloud_storage/service/{sts,cluster_files,status,sync_to_cluster,sync_from_cluster,delete,transfer,recovery,context}.py` |
| workspace DB 访问 | `server_model/user_impl/aio_user_db/default.py:34-119` |
| workspace 任务侧解析/挂载 | `server_model/task_impl/workspace_resolver.py`、`server_model/task_impl/runtime_mounts/default.py:20-55` |
| env 运行时消费 | `server_model/task_impl/single_task_impl.py:60-61,150-158` |
| env 数据面路径 | `cloud_storage/utils.py:469-474`、`one/one_etc/core.toml:97-137` |
| 客户端 venv 上传死代码 | `client/api/venv_api.py:10-46` |
| haienv 本地实现 | `plugins/haienv/haienv/client/{api,command,model,script,sqlite_dict}.py`、`plugins/haienv/haienv/haienv` |
| workspace 客户端实现 | `plugins/haiworkspace/haiworkspace/client/{cli,command,workspace_api,workspace_util}.py` |
| 服务端异常处理器 | `api/app.py:211-256` |
| workspace 实测记录 | [workspace/workspace-server-task-list.md](workspace/workspace-server-task-list.md) §8 |
| env 逆向分析 | [env/hai-cli-env-analysis.md](env/hai-cli-env-analysis.md) |

## 附录 B · 无法确认项

| # | 项 | 说明 |
| --- | --- | --- |
| B1 | 私有 `api/register/custom.py` 是否实际注册了 §5 的 6 条路由 | 本仓 0 个 `custom.py`，无法验证；判定为「本仓缺失」而非「生产缺失」 |
| B2 | `api/app.py:51` 的 swagger yaml 缺失是否导致启动失败 | 取决于 `swagger-ui-py` 的行为，本机未安装该依赖 |
| B3 | nodeport 接口要求 internal role 是否有意为之 | `api/task/port.py:12` 留有待确认 TODO |
| B4 | `from .custom import *` 在非标准目录下是否真的阻断启动 | 取决于部署时 `SERVER_CODE_DIR` 的取值（标准部署目录 `/high-flyer/code/multi_gpu_runner_server` 命中，不阻断） |
| B5 | 任务 pod 内 workspace 实际运行 | 仓库记录为「未验证」（§6.3 #1） |

## 附录 C · 复现命令

```bash
# 1) 路由总数与分组统计
python3 - <<'PY'
import re
from collections import Counter
s = open('api/register/implement.py').read()
r = re.findall(r"app\.(post|get)\('([^']+)'\)", s)
print('routes:', len(r), Counter(p.split('/')[1] for m, p in r))
PY

# 2) 客户端调用 vs 服务端注册 差集
python3 - <<'PY'
import re, glob
reg = set(re.findall(r"app\.(?:post|get)\('([^']+)'\)", open('api/register/implement.py').read()))
called = set()
for p in glob.glob('client/**/*.py', recursive=True) + glob.glob('plugins/**/*.py', recursive=True):
    for m in re.finditer(r"mars_url\(\)\}(/[A-Za-z0-9_/{}\.\-]+)", open(p, errors='replace').read()):
        called.add(m.group(1).rstrip('?'))
norm = lambda p: re.sub(r'\{[^}]*\}', '{}', p)
print(sorted(p for p in called if norm(p) not in {norm(x) for x in reg}))
PY

# 3) 找出所有 not implemented 桩
grep -rn "not implemented" --include="*.py" api/ cloud_storage/ | grep -v __pycache__

# 4) 枚举字符串化实测（Python 3.11+ 同样成立）
python3 -c "
from enum import Enum
class FileType(str, Enum): ENV='env'
print(f'{FileType.ENV}', 'FileType.ENV' == FileType.ENV)"
```

---

*本文档由代码审计生成，审计过程未修改任何源文件；引用仓库内既有实测结论处已标注出处。*
