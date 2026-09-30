# HAI Platform · `hai-cli workspace` 服务端实现需求说明书

| 项目 | 内容 |
| --- | --- |
| 文档 | 服务端实现需求说明书（Requirements Specification） |
| 特性 | `hai-cli workspace`（工作区同步：init / push / pull / download / diff / list / remove） |
| 版本 | v1.0（首版，对应客户端插件 `haiworkspace` 现有实现，客户端**不做破坏性改动**） |
| 前置分析 | 见 [hai-cli-workspace-analysis.md](hai-cli-workspace-analysis.md)（同目录） |
| 配套文档 | [程序设计](workspace-server-design.md) · [功能测试用例](workspace-server-test-cases.md) · [Checklist](workspace-server-checklist.md) |
| 需求编号 | `FR-*` 功能、`API-*` 接口、`NFR-*` 非功能、`SEC-*` 安全、`OPS-*` 运维、`COMP-*` 兼容、`CON-*` 约束 |

---

## 1. 背景与目标

### 1.1 背景

`hai-cli workspace` 让用户在**本地**（开发机 / 个人 PC）编辑代码，一条命令同步到萤火集群，再提交训练任务运行。当前仓库状态：

- **客户端完整**：`plugins/haiworkspace/`（约 1 000 行）实现了本地状态文件、目录 diff、`.hfignore`、zip 打包、OSS 分片上传/下载、任务提交前自动 push 等全部能力。
- **服务端缺失**：客户端调用的 9 个 `/ugc/*` 接口中，仅 `/ugc/cloud/cluster_files/list` 在 `api/register/implement.py:73` 有路由注册，且指向 `api/resource/cloud_storage/default.py:24-25` 的 `return []` 桩；其余 8 个接口无路由、无实现。DB 方法（`set_sync_status` / `get_sync_status` / `downloaded_files.*`）、`[cloud.storage]` 配置、任务侧 `oss://` 工作区解析同样缺失。
- **可直接复用的资产**：`cloud_storage/`（约 1 900 行）已实现 STS 签发、Redis 阶段状态、进程池并发、断点续传、对象 tagging、路径校验、配额校验等**领域逻辑**；`db_schemas/010/011` 两张表已定义；`conf/utils.py` 的 `FileType` / `SyncStatus` / `FileInfo` / `list_local_files_inner` 双端共享。

### 1.2 目标

**G1** 在不修改客户端语义的前提下，补齐服务端实现，使 `hai-cli workspace` 的 7 个子命令在开源栈上端到端可用。
**G2** 服务端逻辑可被两类宿主复用：① 单机 `one` 部署的 `ugc-server`（进程内调用）；② 独立部署的 `cloud-storage` 服务（HTTP 调用）。二者共享同一领域层，行为一致。
**G3** 具备灰度、回滚、可观测与崩溃恢复能力，满足生产可用标准。
**G4** 交付可执行的测试用例集与上线 Checklist，需求-用例-检查项三者可追溯。

### 1.3 非目标（Out of Scope）

- 不实现 `dataset` / `doc` / `pypi` / `website` 类型的业务闭环（复用同一领域层即可，但本期不验收）。
- 不实现 `hai venv` / `haienv push`（`/ugc/update_cluster_venv` 仅登记为 P1）。
- 不实现前端页面、不计费、不做跨集群复制（OSS 中转已隐式支持）。
- 不重构 `cloud_storage/api.py` 的既有对外契约（仅做内部抽层，保持无前缀路由兼容，见 `COMP-03`）。

---

## 2. 角色与场景

| 角色 | 场景 |
| --- | --- |
| 平台用户（local 侧） | 在本地目录 `hai-cli workspace init` → 编辑代码 → `push` → 提交任务；训练产出后 `diff` / `pull` / `download` 取回结果；`list` 查看状态；`remove` 清理 |
| 平台用户（集群内开发容器） | 集群共享盘上的代码无需 push（`MOUNT_LIST` 命中），直接用 `oss://` 以外的本地路径提交任务 |
| 平台管理员 | 设置用户云存储配额；查看同步任务积压与失败；清理 bucket 过期对象 |
| 平台自身（任务调度） | 解析任务 schema 中的 `oss://<group>/<user>/workspaces/<name>`，映射为集群路径并挂载进容器 |

**主场景（P0）**：用户在本地 `push` → 服务端把对象下发到集群 `<workspace_path>/<group>/<user>/workspaces/<name>` → 用户提交任务，任务在该路径下启动 → 训练写出的 checkpoint 由 `pull` 或 `download` 取回本地。

---

## 3. 功能需求（FR）

### 3.0 需求总览

| ID | 名称 | 优先级 | 关联接口 | 主要证据/来源 |
| --- | --- | --- | --- | --- |
| FR-01 | 申请对象存储临时凭证（STS） | P0 | API-01 | `client/.../workspace_util.py:108-121` |
| FR-02 | 写入同步状态 | P0 | API-02 | `workspace_util.py:124-131`；`workspace_api.py:67` |
| FR-03 | 查询同步状态列表 | P0 | API-03 | `workspace_util.py:134-142`；`workspace_api.py:91-105,189-204` |
| FR-04 | 列出集群侧文件（分页） | P0 | API-04 | `workspace_util.py:155-185` |
| FR-05 | 触发「bucket → 集群」同步 | P0 | API-05 | `workspace_util.py:208-243` |
| FR-06 | 查询同步进度/阶段 | P0 | API-06/08 | `workspace_util.py:188-205` |
| FR-07 | 触发「集群 → bucket」同步 | P0 | API-07 | `workspace_util.py:246-280` |
| FR-08 | 删除集群侧工作区/文件 | P0 | API-09 | `workspace_api.py:207-226` |
| FR-09 | 对象命名与 tagging 规范 | P0 | — | `workspace_util.py:389-406` |
| FR-10 | zip 分发语义（打包上传/落盘解压） | P0 | API-05 | `workspace_util.py:357-361`；`cloud_storage/api.py:279-285,669-688` |
| FR-11 | 断点续传与失败重试 | P0 | API-05/07 | `cloud_storage/api.py:619-763` |
| FR-12 | 同步状态机与双写持久化（Redis + PG） | P0 | API-02/06/08 | `db_schemas/010,011` |
| FR-13 | 服务重启后的任务恢复 | P0 | — | `cloud_storage/api.py:31-69` |
| FR-14 | 客户端参数兼容层 | P0 | 全部 | 分析报告 F2/F3 与 §3.1 约束 C2/C3 |
| FR-15 | 任务侧 `oss://` 工作区解析 | P0 | — | `client/commands/hfai_python.py:203`；`server_model/task_impl/code/default.py:5-13` |
| FR-16 | 任务侧工作区挂载 | P0 | — | `server_model/task_impl/runtime_mounts/default.py:1-25` |
| FR-17 | 配额与资源限额 | P1 | API-07/01 | `cloud_storage/api.py:459-475` |
| FR-18 | 管理员配额设置接口 | P1 | API-10 | `api/resource/cloud_storage/default.py:8-9` |
| FR-19 | 配置装载与启动自检 | P0 | — | `conf/proj_conf/default.py:10-85` |
| FR-20 | 传输进度上报（供 CLI 进度条） | P0 | API-06/08 | `workspace_util.py:198-204` |
| FR-21 | 审计与过期对象回收 | P1 | — | `cloud_storage/audit/default.py:4-11` |

### 3.1 硬约束（CON）

| ID | 约束 | 说明 |
| --- | --- | --- |
| CON-1 | **客户端不可破坏性变更** | 现有 `haiworkspace` 插件（含其内置的 `api_utils.py` / `conf/utils.py` 副本）必须能直接与新版服务端互通；允许客户端做**向后兼容**的缺陷修复（见 `COMP-01`） |
| CON-2 | **请求方法/路径固定** | 9 个接口的 HTTP 方法、路径、查询参数名、Body 外壳形状由客户端固定，服务端不得调整 |
| CON-3 | **响应必须含 `success` 字段** | 客户端 `async_requests` 先断言 `'success' in result`，再断言 `result['success'] in [1]`（`client/api/api_utils.py:106-108`）。任何响应（含 4xx/5xx 与校验失败）都不得返回裸 `{"detail": ...}` |
| CON-4 | **Body 的 Content-Type 为 `text/plain`** | 客户端用 `aiohttp` 的 `data=<json str>` 发送（`workspace_util.py:151,178,234,270`），aiohttp 会设置 `Content-Type: text/plain; charset=utf-8`；FastAPI 仅在 `application/json` 时才自动解析 Body，因此**服务端必须手工读取并解析原始 Body** |
| CON-5 | **枚举参数的字符串化** | 客户端会把 `FileType` / `SyncDirection` / `SyncStatus` 枚举成员直接插入查询串，实际值为 `FileType.WORKSPACE` / `SyncDirection.PUSH` / `SyncStatus.STAGE1_RUNNING`（分析报告 F2）。服务端必须归一化（FR-14） |
| CON-6 | **路径参数不做 URL 编码** | `local_path` / `cluster_path` 直接拼进查询串；含 `&`、`#`、空格、中文时可能被截断或破坏后续参数。服务端只能把它们当作**展示性元数据**，严禁参与路径推导 |
| CON-7 | **鉴权凭据在查询串** | 所有接口以 `?token=<mars token>` 鉴权；新增 `Authorization: Bearer` 为可选增强，不得移除查询串支持 |
| CON-8 | **状态接口响应时延** | 客户端轮询超时 10 s（`workspace_util.py:190-193`），状态查询必须在 1 s 内返回（P99） |
| CON-9 | **不改动既有表结构即可上线** | MVP 不得新增/修改 `user_sync_status`、`user_downloaded_files` 的列；新增表为 P1。**已由《[数据库支撑性审计](workspace-server-db-audit.md)》逐字段核实**：两张表 + 两个枚举可直接支撑 P0（枚举标签 6/6、9/9 完全一致）；同时须遵守该审计 §4 的三条 DB 访问层硬约束，并注意本仓库**无自动迁移框架**（`deploy/dbs/files/init_postgresql.sh` 仅在空库执行 DDL） |
| CON-10 | **多 worker 安全** | `one/one_etc/core.toml:3` 配置 `ugc = 2`，即 ugc-server 有 2 个 uvicorn worker 进程，任何「启动即执行」的逻辑必须幂等 |

### 3.2 接口级功能需求

#### FR-01 申请对象存储临时凭证（`/ugc/get_sts_token`）

- 输入：`token`、`name`（工作区名）、`file_type`、`ttl_seconds`。
- 行为：以 token 解析出的 `(user_name, shared_group)` 与 `name`、`file_type` 推导出该用户可读写的前缀，向对象存储申请**最小权限**临时凭证。
- 输出：`{'success': 1, '<provider>': {'endpoint', 'access_key_id', 'access_key_secret', 'security_token', 'bucket'}}`；`<provider>` 必须等于客户端配置的 provider 名（默认 `oss`）。
- 约束：凭证有效期夹在 `[900, 43200]` 秒；授权范围只能是 `<bucket>/<cloud_base_path>/*`；不得签发可访问他人目录的凭证。
- 失败：返回 `{'success': 0, 'msg': ...}`（HTTP 200 或 4xx 均可，但必须具备 `success` 字段）。

#### FR-02 写入同步状态（`/ugc/set_sync_status`）

- 输入：`token, file_type, name, direction, status, local_path, cluster_path`（后 4 项需归一化）。
- 行为：`upsert` 到 `user_sync_status`；`direction=push` 写 `push_status/last_push`，`direction=pull` 写 `pull_status/last_pull`；`local_path`/`cluster_path` 为空字符串时**保留原值**（不得清空）。
- 行为：`name` 或 `file_type` 非法时返回 `success=0`；用户不存在或 token 失效返回 401/403 且带 `success` 字段。
- 幂等：相同参数重复调用结果一致。

#### FR-03 查询同步状态（`/ugc/get_sync_status`）

- 输入：`token, file_type, name`（`name='*'` 表示列出该用户该类型下的全部工作区）。
- 输出：`{'success': 1, 'data': [ {'name', 'local_path', 'cluster_path', 'push_status', 'last_push', 'pull_status', 'last_pull'} ... ]}`。
- **无记录时必须返回 `data: []` 且 `success=1`**（客户端据此打印「没找到工作区」并终止，`workspace_api.py:102-105`）。
- 时间字段格式 `'%Y-%m-%d %H:%M:%S'`；空状态返回 `None`/空串，客户端直接渲染。
- 排序：`updated_at DESC`，保证 `list` 输出稳定。

#### FR-04 列出集群侧文件（`/ugc/cloud/cluster_files/list`）

- 输入：`token, name, file_type, no_checksum, no_hfignore, recursive=True, page, size` + Body `{"file_list": {"files": ["<子路径>", ...]}}`。
- 输出：分页结果 `{'items': [FileInfo...], 'total': int, 'page': int, 'size': int, 'pages': int}`。
- 语义：`items[i]` 至少含 `path`（相对工作区根）、`size`、`last_modified`，`no_checksum=false` 时含 `md5`。
- **`items` 与 `total` 必须真实有效**：桩实现返回空列表会让客户端把集群目录判为空并触发全量重传（分析报告 F1）。
- 分页：`size` 上限 1000（超出截断为 1000）；`total` 为去重后的文件总数；翻页期间文件被删除时返回可重试错误而非错误的空页。
- 性能：10 万文件的目录，首次列目录 ≤ 30 s，重复查询走缓存 ≤ 1 s。

#### FR-05 触发 bucket → 集群 同步（`/ugc/sync_to_cluster`）

- 输入：`token, name, file_type, no_zip` + Body `{"file_list": {"files": [...]}}`。
- 行为：把对象存储中的这些对象下载到 `<workspace_path>/<group>/<user>/workspaces/<name>`（集群文件系统）。
- 输出：`{'success': 1, 'msg': ..., 'index': '<sha256>', 'dst_path': '<集群路径>'}`；`index` 必须返回（客户端有兜底重算，但不应依赖）。
- 幂等：同一 `index` 在 `RUNNING` 期间重复提交返回 `success=1` + 「上一次同步正在进行中」，不得启动第二份任务。
- 空文件列表：立即置 `status=finished` 并返回。
- 权限与属主：新建目录 `chown` 给目标用户；从 tagging 恢复 `filemode`。
- 失败：参数/路径非法→`success=0` + `msg`；执行期失败通过状态接口暴露。

#### FR-06 / FR-08 查询同步进度（`/ugc/sync_to_cluster/status`、`/ugc/sync_from_cluster/status`）

- 输入：`token, index`（GET）。
- 输出：`{'success': 1, 'status': 'init'|'running'|'finished'|'failed', 'msg': <int 或 str>}`。
- **`status='running'` 时 `msg` 必须可被 `int()` 解析**（客户端 `int(result['msg'])`，`workspace_util.py:199`）；建议直接返回已传字节数（JSON number）。
- `status='failed'` 时 `msg` 为可读错误原因（客户端 `raise Exception(result['msg'])`）。
- 终态保留期 ≥ 1800 s（客户端默认 `--sync_timeout 1800`）。
- 不存在的 index：返回 `{'success': 0, 'msg': '不存在的index'}`（HTTP 400 可接受，但必须带 `success`）。

#### FR-07 触发 集群 → bucket 同步（`/ugc/sync_from_cluster`）

- 输入：`token, name, file_type` + Body `{"file_infos": {"files": [FileInfo...]}}`。
- 行为：把集群文件上传到对象存储，写入 `size/md5/source=cluster/filemode` tagging，并登记 `user_downloaded_files`。
- 输出：`{'success': 1, 'index': ...}`。
- 校验：每个 `path` 不得为绝对路径、不得含 `..`；**符号链接指向工作区之外的必须拒绝**（SEC-03）。
- 配额：累计上传量 + 本次 > `cloud_storage_quota.download` 时返回 403 + `success=0` + 明确提示（`cloud_storage/api.py:467-475`）。

#### FR-09 对象命名与 tagging 规范

| 项 | 规范 |
| --- | --- |
| 对象 key | 工作区：`<group>/<user>/workspaces/<name>/<相对路径>`；env：`<group>/shared/hfai_envs/<user>/<name>/<相对路径>` |
| tagging | `size=<bytes>&source=<client|cluster>&filemode=<octal>&md5=<md5>[&expire_at=<'%Y-%m-%d %H:%M:%S'>]` |
| 用途 | `md5` 用于跳过重复传输；`filemode` 用于两端恢复权限；`expire_at` 供审计回收；`source` 标记生产者 |
| 兼容 | 两端必须共用同一份 `conf/utils.py`/provider 语义；tagging 缺失时按「无元数据」降级处理，不得报错 |

#### FR-10 zip 分发语义

- 客户端默认 `no_zip=false`：把本次待传文件打成一个 `<workspace_name>.zip` 上传到 `<cloud_base_path>/<workspace_name>.zip`。
- 服务端识别 `.zip` 结尾的对象：先下载到 `<cluster_base_path>/.hfai/`，解压到 `<cluster_base_path>`，逐个文件 `chown`，最后删除临时 zip（`cloud_storage/api.py:281-285,669-688`）。
- `.hfai/` 目录及 `.hfai/*.zip` 不得被计入工作区文件列表（客户端已过滤，服务端也要过滤）。
- `no_zip=true` 时按普通对象逐文件落盘。

#### FR-11 断点续传与失败重试

- 单文件传输失败重试 10 次（指数/固定 1 s 间隔），全部失败后任务置 `failed` 并记录首个错误。
- 分片阈值与分片大小 100 MB，并发 4（`conf/utils.py:17`、`cloud_storage/api.py:306-319,508-524`）。
- 对象 tagging 的 `md5` 与本地一致时跳过传输（断点续传去重）。
- 重试不得造成重复写 DB 记账或重复 `chown` 报错。

#### FR-12 状态机与双写持久化

- 过程态（Redis）：`init → running → finished | failed`；key 形如 `{PROVIDER}:{sync_to_cluster|sync_from_cluster}:{index}:{status|progress|param:<instance>}`。
- 长期态（PostgreSQL，`user_sync_status`）：`init → stage1_running → stage1_finished → stage2_running → finished`，失败分支 `stage1_failed` / `stage2_failed`。
- 方向语义（**易错点**）：`push` = 本地→bucket（stage1，客户端执行）+ bucket→集群（stage2，服务端执行）；`pull` = 集群→bucket（stage1，服务端执行）+ bucket→本地（stage2，客户端执行）。
- `progress` 以 hash 存储「对象 key → 已传字节」，状态接口求和返回；任务结束删除。
- 服务端写库失败不得影响数据传输，但必须计数并告警（`cloud_storage_db_failure_total`）。

#### FR-13 崩溃恢复

- 服务启动时扫描 `{PROVIDER}:*:*:param:*`，对属于本实例的未完成任务以 `force=True` 重新提交。
- **多 worker 必须互斥**：同一 pod 的多个 worker 进程只能有一个执行恢复（`CON-10`）。
- 恢复必须幂等：已完成的文件通过 tagging `md5` 跳过；重复 `chown`/`makedirs` 不得失败。

#### FR-14 客户端参数兼容层

服务端必须接受并归一化以下三种「客户端实际形态」，且与「规范形态」行为一致：

| 参数 | 规范形态 | 客户端实际形态（必须兼容） | 归一化规则 |
| --- | --- | --- | --- |
| `file_type` | `workspace` | `FileType.WORKSPACE` | 取最后一个 `.` 之后的部分并小写；同时接受大写 |
| `direction` | `push` | `SyncDirection.PUSH` | 同上 |
| `status` | `stage1_running` | `SyncStatus.STAGE1_RUNNING` | 同上 |
| Body | `{"files": [...]}` | `{"file_list": {"files": [...]}}` | 兼容两种外壳：优先取 `<name>` 包裹键，缺失时按裸体解析 |
| Body 传输 | `application/json` | `text/plain; charset=utf-8` | 忽略 Content-Type，直接 `json.loads(body)`（`CON-4`） |
| 未知参数 | — | 可能新增 | 忽略未知查询参数，不返回 422 |

- 归一化必须是**开关可控**的（`cloud.storage.service.legacy_param_compat`，默认 `true`），便于在客户端修复后收紧。
- 归一化失败时必须返回 `success=0` + 明确提示，而不是 422。

#### FR-15 任务侧 `oss://` 工作区解析

- 当任务 schema 的 `spec.workspace` 形如 `<provider>://<group>/<user>/workspaces/<name>` 时，服务端必须解析为集群真实路径 `{workspace_path}/<group>/<user>/workspaces/<name>`。
- 解析后必须校验：结果在 `workspace_path` 之内（防穿越）、`<group>/<user>` 与提交任务的用户一致（防越权引用他人工作区）。
- 目录不存在时的行为：返回明确错误 `workspace 尚未同步到集群，请先执行 hai-cli workspace push`（P0）；P1 支持自动从 bucket 拉取一次。
- 该解析必须对用户透明：`MARSV2_TASK_WORKSPACE` 与环境变量中的 `workspace` 都使用解析后的集群路径（`server_model/task_impl/single_task_impl.py:290-291`）。

#### FR-16 任务侧工作区挂载

- 若解析出的集群路径未被既有挂载点（`storage` 表）覆盖，必须为 pod 追加挂载项：`host_path = mount_path = <集群路径>`，`mount_type = DirectoryOrCreate`，`read_only = False`，`name = 'workspace-path'`（`server_model/task_impl/runtime_mounts/default.py:15-22`）。
- 挂载路径必须与解析路径**完全一致**，保证 `cd {code_dir}` 与 `MARSV2_TASK_WORKSPACE` 语义不变。
- 去重：同一 pod 不得出现重复 `mount_path`。

#### FR-17 配额与资源限额（P1）

| 限额 | 默认 | 行为 |
| --- | --- | --- |
| pull（集群→bucket）累积容量 | `quota.cloud_storage_quota.download`，默认 100 GB | 超限返回 403 + 已用/申请/限额明细 |
| 单次请求文件数 | 10 000 | 超出返回 `success=0` |
| 单次请求总大小 | 1 TiB | 超出返回 `success=0` |
| 用户并发同步任务 | 10（超出后共享池） | 复用 `WorkerPools.max_pools` |
| 单批文件数 | 客户端固定 50 | 服务端不低于 50，且不得依赖该值 |

#### FR-18 管理员配额设置接口（P1）

- 目标：让管理员可为**外部用户**设置/调整云存储配额，供 FR-17 的 pull 限额校验使用。
- 接口：`POST /ugc/set_cloud_storage_quota`（API-10），参数 `token`、`user_name`、`quota_mb`、`expire_time`（可选）。
- 行为：写入 `quota` 表 `resource = 'cloud_storage_quota'`（`insert ... on conflict ("user_name","resource") do update`），并记录变更日志（可复用 `external_quota_change_log`）。
- 权限：仅 `ops` / `platform` 组（复用 `api/depends.get_internal_api_user_with_token(allowed_groups=[...])`）。
- 输出：`{'success': 1, 'user_name': ..., 'quota_mb': ...}`；非管理员返回 403 + `success=0`。
- 失败模式：目标用户不存在 → `success=0`；`quota_mb` 非法（负值/非整数）→ `INVALID_PARAM`。

#### FR-19 配置装载与启动自检

- 必须支持从 `$MARSV2_MANAGER_CONFIG_DIR` 的 TOML 装载 `[cloud.storage]`（`conf/proj_conf/default.py:10-35`），敏感项支持 RSA 解密（`:53-61`）。
- 必填键缺失时：启动日志给出**逐项**缺失清单；相关接口返回 `{'success': 0, 'msg': '云存储未配置: 缺少 xxx'}`，不得抛 500 裸异常。
- 自检项：`provider`、`endpoint`、`access_key_id`、`access_key_secret`、`uid`、`role_arn`、`private_bucket`、`service.workspace_path`、`service.breakpoint_info_path`。

#### FR-20 传输进度上报

- 每个对象传输过程中，按已传字节更新 Redis `progress` hash；CLI 进度条据此前进（`workspace_util.py:198-204`）。
- 进度只能单调不减（同一 index 内）。
- 进度更新频率需限频（建议 ≥ 1 s/次或 ≥ 1% 变更），避免高频写 Redis。

#### FR-21 审计与过期对象回收（P1）

- 周期任务：① 删除 `expire_at < now` 的对象；② 统计每用户 bucket 用量并暴露指标；③ 清理残留的 `.hfai/*.zip`；④ 把超时（> 24 h）未结束的 Redis 任务标记为 `failed`。
- 单实例执行（Redis 锁），可开关（`RUN_AUDIT`）。
- 本期允许仍为 no-op，但必须保留接口与开关，并在 Checklist 中登记为 P1。

---

## 4. 接口需求清单（API）

> 完整字段、示例、错误码见[程序设计 §4](workspace-server-design.md)。此处只登记契约要点与优先级。

| ID | 方法 | 路径 | 鉴权 | 幂等 | 优先级 |
| --- | --- | --- | --- | --- | --- |
| API-01 | POST | `/ugc/get_sts_token` | token→user | 是 | P0 |
| API-02 | POST | `/ugc/set_sync_status` | token→user | 是 | P0 |
| API-03 | POST | `/ugc/get_sync_status` | token→user | 是 | P0 |
| API-04 | POST | `/ugc/cloud/cluster_files/list` | token→user | 是 | P0 |
| API-05 | POST | `/ugc/sync_to_cluster` | token→user | 是（index 去重） | P0 |
| API-06 | GET | `/ugc/sync_to_cluster/status` | token→user | 是 | P0 |
| API-07 | POST | `/ugc/sync_from_cluster` | token→user | 是（index 去重） | P0 |
| API-08 | GET | `/ugc/sync_from_cluster/status` | token→user | 是 | P0 |
| API-09 | POST | `/ugc/delete_files` | token→user | 是 | P0 |
| API-10 | POST | `/ugc/set_cloud_storage_quota` | token→admin | 是 | P1 |
| API-11 | POST | `/ugc/update_cluster_venv` | token→user | 否 | P1 |
| API-12 | GET | `/ugc/cloud_storage/usage` | token→user | 是 | P1 |

**统一约定**

- 鉴权：`?token=<mars token>`；由 `AioUserSelector.from_token` 解析出用户，`username = user.user_name`、`group = user.shared_group`、`userid = user.user_id`。
- **忽略**客户端可能传来的 `username` / `group` / `userid`（防越权）。
- 成功：`{'success': 1, ...}`；业务失败：`{'success': 0, 'msg': ..., 'code': '<ERROR_CODE>'}`。
- 校验失败（含 FastAPI 自动校验）：必须由全局异常处理器改写为带 `success` 的响应（`CON-3`）。
- 所有响应携带 `client-version` 响应头（已有中间件，`api/app.py:158-162`）。

---

## 5. 非功能需求（NFR）

| ID | 类别 | 指标 |
| --- | --- | --- |
| NFR-01 | 时延 | 状态查询 P99 ≤ 100 ms；`get_sts_token` P99 ≤ 1 s；`get_sync_status` P99 ≤ 200 ms；`sync_to_cluster` 提交（不含传输）P99 ≤ 500 ms |
| NFR-02 | 吞吐 | 单实例 ≥ 200 MB/s（与 bucket 带宽相关）；10 000 文件 / 10 GB 工作区，push 端到端 ≤ 10 min（内网、100 MB/s 带宽下） |
| NFR-03 | 规模 | 单工作区 ≤ 200 000 文件、≤ 1 TB；单次请求 ≤ 10 000 文件 |
| NFR-04 | 可用性 | 接口可用性 ≥ 99.9%；单文件失败不影响同批其他文件的状态可观测性 |
| NFR-05 | 幂等与一致性 | 相同 `index` 的重复提交不产生第二份传输任务；DB 与 Redis 状态最终一致（允许 300 s 内不一致） |
| NFR-06 | 可观测 | 每个接口有 QPS/错误率指标；每个同步任务有耗时、文件数、字节数、失败计数指标；关键路径日志带 `uuid`/`user`/`name`/`index` |
| NFR-07 | 可维护 | 领域层与宿主层解耦；单测覆盖率 ≥ 70%（领域层 ≥ 85%） |
| NFR-08 | 兼容 | 支持「老客户端（枚举串 + text/plain body）」与「修复后客户端」两种形态同时在线（`COMP-01/02`） |
| NFR-09 | 部署 | 兼容 `one` 单机部署与 `cloud-storage` 独立部署；不新增必须的中间件（复用既有 PG/Redis/对象存储） |
| NFR-10 | 资源 | 每进程 worker 数可配（默认 4），同步任务内存占用与文件大小无关（流式/分片） |

---

## 6. 安全需求（SEC）

| ID | 需求 | 验收要点 |
| --- | --- | --- |
| SEC-01 | 身份只来自 token | 请求中出现的 `username`/`group` 一律忽略；伪造这些参数不能访问他人数据 |
| SEC-02 | STS 最小权限 | 凭证只能读写 `<bucket>/<group>/<user>/workspaces/<name>/*`；TTL ∈ [900, 43200]；不得使用长期密钥下发 |
| SEC-03 | 路径穿越与符号链接 | 所有落盘/上传路径经 `check_is_subpath` + `Path.resolve()` 校验；`..`、绝对路径、指向工作区外的符号链接一律拒绝 |
| SEC-04 | 越权访问 | 用他人 `name` 或他人 `index` 访问时返回 403 + `success=0`；`index` 校验必须绑定用户 |
| SEC-05 | 敏感信息 | 长期 AK/SK 不得出现在响应、日志、异常栈；`token` 在访问日志中脱敏（已有）；STS 凭证不入库 |
| SEC-06 | 资源保护 | 文件数/总大小/并发任务/单批大小限额；`page.size` 上限；防 `list` 接口放大（缓存 + 限页） |
| SEC-07 | 删除保护 | 删除整个工作区必须经客户端二次确认；服务端对空 `file_list` 的「删全量」语义必须显式打审计日志（含操作人/IP） |
| SEC-08 | 审计 | 关键操作（删工作区、签发 STS、超配额拒绝）写审计日志，保留 ≥ 90 天 |

---

## 7. 运维需求（OPS）

| ID | 需求 |
| --- | --- |
| OPS-01 | 灰度开关：服务端按用户/组白名单开启该特性（`cloud.storage.service.enabled_users/groups`），未开启用户调用返回 `success=0` + 提示 |
| OPS-02 | 配置热更：`[cloud.storage]` 变更后重启生效，重启不中断已完成任务（状态在 Redis/PG） |
| OPS-03 | 回滚：关闭开关即可停止服务；已上传对象与 PG 记录保留，回滚不产生脏数据 |
| OPS-04 | 监控告警：任务失败率 > 5%（5 min）、同步任务积压 > 100、bucket 用量 > 90%、`cloud_storage_db_failure_total` > 0 触发告警 |
| OPS-05 | 日志：新增独立日志目录文件 `ugc_0.log` 已有，同步任务日志需含 `index` 便于检索；保留 ≥ 7 天 |
| OPS-06 | 容量：`breakpoint_info_path` 需可写且按进程隔离；提供清理脚本处理残留断点文件 |
| OPS-07 | 变更窗口：DDL（如启用 P1 新表）必须可在线执行且可回滚 |

---

## 8. 兼容性需求（COMP）

| ID | 需求 | 说明 |
| --- | --- | --- |
| COMP-01 | 兼容 `FileType.X` / `SyncDirection.X` / `SyncStatus.X` 形态 | 见 FR-14；上线后老客户端无需升级即可用 |
| COMP-02 | 兼容 Body 外壳两种形态与 `text/plain` | 见 FR-14 与 `CON-4` |
| COMP-03 | 不破坏 `cloud-storage` 独立部署 | 既有无前缀路由（`/get_sts_token`、`/sync_to_cluster`…）及其 `dependencies=[Depends(validate_user_token)]`、`Page[FileInfo]` 响应模型保持可用 |
| COMP-04 | 双端语义一致 | 客户端内置的 `conf/utils.py`、`cloud_storage/provider` 副本与服务端同版本；md5、`.hfignore`、zip 打包/解包结果必须逐字节一致 |
| COMP-05 | 响应字段只增不减 | 新字段追加返回，老客户端忽略未知字段仍可用 |
| COMP-06 | DB 向后兼容 | 不修改既有列语义；新增列/表必须带默认值且可空 |

---

## 9. 依赖与假设

| 项 | 说明 | 风险 |
| --- | --- | --- |
| PostgreSQL | 既有 `mars_db`，含 `user_sync_status` / `user_downloaded_files`；审计确认字段、枚举、触发器、主键均满足 P0 | 表已存在，无需迁移（`CON-9`）。剩余风险：① 线上库可能与 `db_schemas` 漂移（私有层历史变更）→ 上线前用审计 §9.1 核对；② 配额口径（累计 vs 净占用）与 push 方向用量不可见属**语义缺口**，须产品确认（审计 §5 G3/G4） |
| 自动迁移 | 平台**无** alembic / schema 版本机制；`db_schemas/` 新文件只对新库生效 | P1 的任何 DDL 必须人工迁移并留痕（审计 §7、Checklist DB-03~DB-05） |
| Redis | 既有实例，用于过程态 | Redis 抖动会导致状态查询失败；需重试 + 明确错误 |
| 对象存储 | 阿里云 OSS（`provider=oss`）；必须有可用的 RAM Role | 无 OSS 环境时需 `localfs`/`mock` provider 供测试（设计文档 §9.3） |
| 集群共享存储 | `service.workspace_path` 所在文件系统，需被任务 pod 挂载 | 路径未挂载会导致任务拿不到代码（FR-16） |
| 客户端版本 | 现有 `haiworkspace` 插件 | 客户端缺陷（枚举串）由服务端兼容层兜底，长期仍需客户端修复 |
| haproxy/supervisord | `/ugc/` → `127.0.0.1:8083` 已配置 | 若独立部署 cloud-storage，需要新增 ACL/路由 |

**假设**：`user.shared_group` 与客户端 `init` 时拿到的 `user_shared_group` 同源且稳定（`server_model/user/implement.py:67`）；若管理员调整用户组，历史工作区路径需人工迁移（本期不自动化）。

---

## 10. 验收标准（Definition of Done）

1. **端到端**：在真实客户端（未升级）+ 本设计实现的服务端上，以下流程全部通过且无报错：
   `workspace init` → `push` → `diff`（无差异）→ `list`（显示状态）→ 提交任务并在集群路径下看到代码 → `pull`（取回集群新文件）→ `download <subpath>` → `remove -f <file>` → `remove`。
2. **增量正确性**：连续两次 `push`，第二次上传字节数为 0；改 1 个文件后 `push`，仅传输该文件（`--no_zip` 下可精确计数）。
3. **兼容性**：`file_type=FileType.WORKSPACE`、`Content-Type: text/plain`、`{"file_list": {...}}` 三种「客户端实际形态」全部被正确服务（`FR-14`）。
4. **安全**：SEC-01 ~ SEC-08 全部有对应用例通过；用他人 token/name/index 访问一律 403。
5. **恢复**：任务执行中 `kill -9` 服务进程，重启后任务自动续跑并在状态接口可见 `finished`。
6. **多 worker**：`ugc=2` 下启动不产生重复恢复；同一 index 不产生重复传输。
7. **可观测**：指标与日志在压测中完整可查，无敏感信息泄漏。
8. **文档**：本文档 4 件套齐备；需求 ID 100% 被测试用例覆盖（追溯矩阵无空洞）。

---

## 11. 需求追溯矩阵

| 需求 | 设计章节 | 测试用例 | Checklist 项 |
| --- | --- | --- | --- |
| FR-01 | 设计 §4.1、§6.2 | TC-A01~A05, TC-H02 | DEV-05, SEC-02 |
| FR-02 | 设计 §4.2、§7.2 | TC-A06~A10, TC-G01~G04 | DEV-06 |
| FR-03 | 设计 §4.3、§7.2 | TC-A11~A15, TC-B03, TC-B04 | DEV-06 |
| FR-04 | 设计 §4.4、§8.4 | TC-A16~A21, TC-C05, TC-I02 | DEV-07 |
| FR-05 | 设计 §4.5、§5.3、§8.2 | TC-A22~A27, TC-C01~C12 | DEV-08, DEV-09 |
| FR-06 | 设计 §4.6、§7.3 | TC-A28~A32, TC-G05~G10 | DEV-10 |
| FR-07 | 设计 §4.7、§8.3 | TC-A33~A38, TC-D01~D08 | DEV-11 |
| FR-08 | 设计 §4.9 | TC-A39~A43, TC-E01~E06 | DEV-12 |
| FR-09 | 设计 §8.1 | TC-C13~C16, TC-H06 | DEV-13 |
| FR-10 | 设计 §5.4 | TC-C17~C20 | DEV-14 |
| FR-11 | 设计 §5.5 | TC-C21~C24, TC-F05 | DEV-15 |
| FR-12 | 设计 §7 | TC-G01~G12 | DEV-16 |
| FR-13 | 设计 §5.6 | TC-F01~F04 | DEV-17 |
| FR-14 | 设计 §6.3 | TC-A44~A50, TC-J01~J06 | DEV-04 |
| FR-15 | 设计 §10.1 | TC-K01~K06 | DEV-18 |
| FR-16 | 设计 §10.2 | TC-K07~K10 | DEV-19 |
| FR-17 | 设计 §9.4 | TC-D07, TC-H07~H09 | DEV-20 |
| FR-18 | 设计 §4.10 | TC-A51~A53 | DEV-21（P1） |
| FR-19 | 设计 §9.1 | TC-A54~A56, TC-J07 | DEV-02, CFG-01 |
| FR-20 | 设计 §7.4 | TC-G11, TC-C23 | DEV-16 |
| FR-21 | 设计 §11 | TC-L05~L08 | DEV-22（P1） |
| NFR-01~04 | 设计 §12.1 | TC-I01~I08 | PERF-01~03 |
| NFR-05/06 | 设计 §12.2 | TC-F06, TC-G12, TC-L01~L04 | OBS-01~03 |
| NFR-07~10 | 设计 §13 | TC-J08, TC-I09 | DOC-01, DEP-01 |
| SEC-01~08 | 设计 §12.3 | TC-H01~H12 | SEC-01~08 |
| OPS-01~07 | 设计 §9.5、§13.4 | TC-J09~J12 | OPS-01~07 |
| COMP-01~06 | 设计 §6.3、§13 | TC-J01~J10 | CMP-01~04 |
| API-11（env 链路，P1） | 设计 §4.10 | TC-J14 | DEV-21（P1） |
| API-12（用量自查，P1） | 设计 §4.10、§9.4 | TC-J13 | DEV-21（P1） |

> **追溯权威性说明**：上表按**功能需求（FR）聚合**，粒度为「一组用例」。接口级（`API-*`）与非功能（`NFR-*` / `OPS-*`）的**逐条反向映射以《[功能测试用例](workspace-server-test-cases.md)》§12 为权威**；该文档 §12.4 记录了本表与用例文档之间的差异（D-2、D-5~D-9），并已按「覆盖更全的一侧为准」在用例文档中逐条细化（例如 NFR-08 → TC-J01~J04、NFR-09 → TC-I10/TC-A56、NFR-10 → TC-I05/I08/F07、OPS-06/07 → TC-J08）。若两处再次不一致，以用例文档 §12 为准并回改本表。
