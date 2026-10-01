# HAI Platform · `hai-cli workspace` 服务端 · 需求梳理与实施任务列表

| 项目 | 内容 |
| --- | --- |
| 文档 | 需求梳理 + 实施/测试任务列表（执行视图） |
| 需求基线 | [workspace-server-requirements.md](workspace-server-requirements.md) v1.0（FR-01~21 / API-01~12 / NFR / SEC / OPS / COMP / CON） |
| 设计基线 | [workspace-server-design.md](workspace-server-design.md) v1.0（§3 文件清单 / §4 契约 / §5 领域层 / §6 兼容层 / §15 ADR / §17 WBS） |
| 用例基线 | [workspace-server-test-cases.md](workspace-server-test-cases.md)（182 条功能用例 + 12 条 TC-DB + 8 个 E2E 场景 = 202 项；分层 L1~L4） |
| 检查基线 | [workspace-server-checklist.md](workspace-server-checklist.md)（GATE/ENV/CFG/DB/DEV/UT/E2E/SEC/PERF/OBS/OPS/TASK/CMP/DOC+DEP/REL/RB/POST/ACC；组数表述见 §7 #3） |
| DB 基线 | [workspace-server-db-audit.md](workspace-server-db-audit.md)（G1~G6 缺口、3 条访问层硬约束） |
| 目标环境 | `fireflyer@192.168.100.103`（Multipass + MicroK8s + MetalLB，见 [test-environment.md](test-environment.md)） |
| 范围 | 本期实现 **P0**（FR-01~16、19、20 + API-01~09）；P1（FR-17/18/21 + API-10~12）单列，不阻塞 P0 |

---

## 0. 一页摘要

**要做什么**：客户端插件 `plugins/haiworkspace/`（约 1000 行）已完整，但服务端 9 个 `/ugc/*` 接口里只有 1 个被注册且是 `return []` 桩。本任务把服务端补齐，使 `hai-cli workspace` 的 `init / push / pull / download / diff / list / remove` 在开源栈上端到端可用，并在 103 测试环境上验证。

**四块拼图**（缺一不可）：

1. `/ugc/*` 9 条路由注册 + token→用户解析（不得信任客户端传的 `username`/`group`）。
2. `user_sync_status` / `user_downloaded_files` 的 DB 读写方法（表已存在，**P0 零 DDL**）。
3. `[cloud.storage]` 配置装载与启动自检（当前 `core.toml` 完全没有该段）。
4. 任务侧 `oss://<group>/<user>/workspaces/<name>` 解析 + 工作区挂载（当前 `parse_code_cmd` 原样返回、`add_runtime_mounts` 是 `pass`）。

**关键约束**：客户端**不做破坏性改动**，服务端必须吸收三种「客户端实际形态」——枚举串（`FileType.WORKSPACE`）、`Content-Type: text/plain` 的 JSON Body、`{"file_list":{...}}` 外壳。

**零 DDL 上线**：两张表 + 两个 PG 枚举（`file_type` 6/6、`sync_status` 9/9 标签完全一致）已由 DB 审计逐字段核实可直接支撑 P0。

**工作量**：P0 关键路径 ≈ 18 人日（设计 §17：S1→S2→S3→S4→S5→S6→S8→S10→S12），另有测试/压测/灰度约 4.5 人日。

**三个生产必答题**：多 worker 恢复互斥（ADR-5）、终态 TTL ≥ 客户端超时 1800 s（ADR-6）、任务侧 `oss://` 解析与挂载（FR-15/16）。

---

## 1. 现状核对（本仓库实测）

> 以下为在 `feature/hai-cli-workspace-server-design` 分支（HEAD `7589fb1` / 本地 `50c9566`）上实际 grep/read 的结论，不是文档转述。

| # | 能力 | 现状（实测） | 差距 |
| --- | --- | --- | --- |
| 1 | `/ugc/*` 路由 | `api/register/implement.py:67-73` 只注册 5 条，其中与 workspace 相关的仅 `/ugc/cloud/cluster_files/list`，指向 `api/resource/cloud_storage/default.py:24-25` 的 `return []` | **缺 8 条路由**；已注册的那条是**危险桩**（客户端会判定「集群目录为空」→ 全量重传，F1） |
| 2 | 接口实现 | `api/resource/cloud_storage/default.py` 中 9 个同名函数**全部** `'not implemented'` | 需全部替换为真实实现 |
| 3 | DB 读写 | 全仓 `grep user_sync_status\|user_downloaded_files --include=*.py` = **0 命中**（仅 `db_schemas/010,011` 有 DDL） | 缺 `set_sync_status` / `get_sync_status` / `soft_delete_sync_status` / `insert_downloaded_file` / `get_usage_in_mb` |
| 4 | 领域层 | `cloud_storage/service/` **不存在**；逻辑全在 `cloud_storage/api.py`（~900 行，进程内直连 FastAPI） | 需抽 `service/` 供双宿主复用（ADR-1/11/12） |
| 5 | 配置 | `one/one_etc/core.toml` **无** `[cloud.storage]` 段；`conf/proj_conf/default.py` 只有 `decrypt_message("CONF.cloud.storage.access_key_id")` 等解密钩子 | 需新增配置模板 + 自检 `check_cloud_storage_config()` |
| 6 | 任务侧解析 | `server_model/task_impl/code/default.py` 直接 `return task_schema.spec.workspace`，无 `oss://` 解析 | 需新增 `workspace_resolver.py` |
| 7 | 任务侧挂载 | `server_model/task_impl/runtime_mounts/default.py` 函数体是 `pass` | 需追加 `workspace-path` 挂载项 |
| 8 | 配额访问器 | `UserQuota` 只有扁平 `quota(resource) -> int`；全仓 `cloud_storage_quota` 仅 1 处调用（`cloud_storage/api.py:467`）+ 1 个桩 | 需补 `UserQuotaExtras.cloud_storage_quota` |
| 9 | Provider | `cloud_storage/provider/` 只有 `oss.py` / `mock.py`，**无 `localfs.py`**；`MockApi` 签名与调用点不兼容（F5） | 需新增 `localfs`（测试/CI 必需，ADR-8）+ 修 `mock` 签名 |
| 10 | 全局异常 | `api/app.py:211` 有 `StarletteHTTPException` 处理器，**无** `RequestValidationError` 处理器 | 需补，保证 4xx/5xx 响应体都含 `success`（CON-3） |
| 11 | 客户端 | `plugins/haiworkspace/haiworkspace/client/` 完整；9 个接口调用点齐全 | **不改语义**；仅允许向后兼容的加固（P1） |

---

## 2. 需求梳理

### 2.1 功能需求（FR）

| ID | 名称 | 优先级 | 接口 | 核心验收点 |
| --- | --- | --- | --- | --- |
| FR-01 | 申请对象存储临时凭证（STS） | P0 | API-01 | provider 键名匹配；TTL 夹取 `[900,43200]`；policy 前缀 = `<bucket>/<cloud_base_path>/*`；不得签发可访问他人目录的凭证 |
| FR-02 | 写入同步状态 | P0 | API-02 | upsert 到 `user_sync_status`；`push` 写 `push_status/last_push`、`pull` 写 `pull_status/last_pull`；**空路径保留原值**；幂等 |
| FR-03 | 查询同步状态列表 | P0 | API-03 | 7 字段；时间格式 `%Y-%m-%d %H:%M:%S`；**无记录必须 `data:[]` + `success:1`**；`name='*'` 列全部；`updated_at DESC` |
| FR-04 | 列出集群侧文件（分页） | P0 | API-04 | `items/total` 必须真实（桩返回空列表会导致全量重传）；`size ≤ 1000`；10 万文件首次 ≤ 30 s、重复 ≤ 1 s |
| FR-05 | 触发 bucket → 集群 同步 | P0 | API-05 | 落盘到 `{workspace_path}/{group}/{user}/workspaces/{name}`；`index` 必须返回；同 index RUNNING 期间不启动第二份；空列表立即 `finished`；新建目录 `chown` + 从 tagging 恢复 `filemode` |
| FR-06 | 查询同步进度/阶段 | P0 | API-06/08 | `running` 时 `msg` **必须可 `int()`**；`failed` 时 `msg` 为可读原因；不存在 index → `success:0`；**index 绑定用户**（SEC-04） |
| FR-07 | 触发 集群 → bucket 同步 | P0 | API-07 | 写 `size/md5/source=cluster/filemode` tagging + 登记 `user_downloaded_files`；拒绝绝对路径/`..`/越界软链；超配额 403 |
| FR-08 | 删除集群侧工作区/文件 | P0 | API-09 | 空 `file_list` = 删整区（须显式审计 + 软删 DB 行）；逐个 `check_is_subpath`；幂等 |
| FR-09 | 对象命名与 tagging 规范 | P0 | — | key：`<group>/<user>/workspaces/<name>/<相对路径>`；tagging 缺失时降级不报错 |
| FR-10 | zip 分发语义 | P0 | API-05 | `.zip` 先落 `<cluster_base>/.hfai/` → 解压 → 逐文件 `chown` → 删临时 zip；`.hfai/` 不得计入文件列表（双端都要过滤） |
| FR-11 | 断点续传与失败重试 | P0 | API-05/07 | 单文件重试 10 次（1 s 间隔）；分片阈值/大小 100 MB、并发 4；tagging `md5` 命中则跳过；重试不重复记账 |
| FR-12 | 状态机与双写持久化 | P0 | API-02/06/08 | Redis（过程态）+ PG（长期态）双写；方向语义易错：`push` = 本地→bucket（客户端）+ bucket→集群（服务端）；DB 写失败不得中断传输但须计数告警 |
| FR-13 | 服务重启后的任务恢复 | P0 | — | 扫 `{PROVIDER}:*:*:param:*`；**多 worker 互斥**（心跳 + pod 锁）；恢复幂等 |
| FR-14 | 客户端参数兼容层 | P0 | 全部 | 枚举串归一化（开关 `legacy_param_compat`，默认 `true`）；`text/plain` Body 手工解析；`{"file_list":{...}}` 外壳；未知参数忽略不报 422；失败返回 `success=0` 而非 422 |
| FR-15 | 任务侧 `oss://` 工作区解析 | P0 | — | 解析为集群真实路径；校验在 `workspace_path` 内 + `<group>/<user>` 与提交人一致；目录不存在给明确错误文案；对用户透明（`MARSV2_TASK_WORKSPACE` 用解析后路径） |
| FR-16 | 任务侧工作区挂载 | P0 | — | `host_path = mount_path = 集群路径`，`mount_type='DirectoryOrCreate'`，`read_only=False`，`name='workspace-path'`；同 pod 不重复 `mount_path` |
| FR-17 | 配额与资源限额 | P1 | API-07/01 | pull 累积 100 GB（超限 403 + 明细）；单请求 10 000 文件；单请求 1 TiB；并发 10；单批 ≥ 50 |
| FR-18 | 管理员配额设置接口 | P1 | API-10 | 写 `quota` 表 `resource='cloud_storage_quota'`（`on conflict do update`）；仅 `ops`/`platform` 组 |
| FR-19 | 配置装载与启动自检 | P0 | — | 缺失键**逐项**打印；接口返回 `CLOUD_STORAGE_NOT_CONFIGURED`，**不抛 500、不影响 ugc 其他接口** |
| FR-20 | 传输进度上报 | P0 | API-06/08 | Redis `progress` hash 按对象累加；进度单调不减；写限频（≥1 s 或 ≥1%） |
| FR-21 | 审计与过期对象回收 | P1 | — | 4 个子任务 + 单实例锁 + `RUN_AUDIT` 开关；本期允许 no-op 但保留接口与开关 |

### 2.2 接口清单（API）

| ID | 方法 | 路径 | 鉴权 | 幂等 | 优先级 | 客户端调用点 |
| --- | --- | --- | --- | --- | --- | --- |
| API-01 | POST | `/ugc/get_sts_token` | token→user | 是 | P0 | `workspace_util.py:114` |
| API-02 | POST | `/ugc/set_sync_status` | token→user | 是 | P0 | `workspace_util.py:129` |
| API-03 | POST | `/ugc/get_sync_status` | token→user | 是 | P0 | `workspace_util.py:137` |
| API-04 | POST | `/ugc/cloud/cluster_files/list` | token→user | 是 | P0 | `workspace_util.py:168` |
| API-05 | POST | `/ugc/sync_to_cluster` | token→user | 是（index 去重） | P0 | `workspace_util.py:228` |
| API-06 | GET | `/ugc/sync_to_cluster/status` | token→user | 是 | P0 | `workspace_util.py:240` |
| API-07 | POST | `/ugc/sync_from_cluster` | token→user | 是（index 去重） | P0 | `workspace_util.py:264` |
| API-08 | GET | `/ugc/sync_from_cluster/status` | token→user | 是 | P0 | `workspace_util.py:277` |
| API-09 | POST | `/ugc/delete_files` | token→user | 是 | P0 | `workspace_util.py:148` |
| API-10 | POST | `/ugc/set_cloud_storage_quota` | token→admin | 是 | P1 | 无（管理端） |
| API-11 | POST | `/ugc/update_cluster_venv` | token→user | 否 | P1 | `client/api/venv_api.py:22` |
| API-12 | GET | `/ugc/cloud_storage/usage` | token→user | 是 | P1 | 无 |

**统一约定**：鉴权走查询串 `?token=<mars token>`，由服务端解析出 `user_name/shared_group/user_id`，**忽略**客户端传来的 `username`/`group`/`userid`；成功 `{'success':1,...}`，失败 `{'success':0,'msg':...,'code':...}`；所有响应带 `client-version` 头。

**错误码表**：`INVALID_PARAM`/`INVALID_BODY`/`PATH_ESCAPE`/`CLOUD_STORAGE_NOT_CONFIGURED`/`FEATURE_DISABLED`/`TOO_MANY_FILES`/`PAYLOAD_TOO_LARGE`/`CLIENT_RETRY` → HTTP 200；`UNAUTHORIZED` 401；`FORBIDDEN`/`QUOTA_EXCEEDED` 403；`NOT_FOUND_INDEX` 400；`INTERNAL_ERROR` 500。

### 2.3 硬约束（CON）

| ID | 约束 | 实现要点 |
| --- | --- | --- |
| CON-1 | 客户端不可破坏性变更 | 仅允许向后兼容缺陷修复 |
| CON-2 | 请求方法/路径/查询参数名/Body 外壳固定 | 服务端不得调整 |
| CON-3 | **响应必须含 `success`** | 客户端先断言 `'success' in result` 再断言 `== 1`；含 4xx/5xx 与校验失败，**不得返回裸 `{"detail": ...}`** |
| CON-4 | **Body 的 Content-Type 是 `text/plain`** | 客户端用 aiohttp `data=<json str>`；必须 `json.loads(await request.body())` 手工解析 |
| CON-5 | 枚举参数字符串化 | 实际值是 `FileType.WORKSPACE` / `SyncDirection.PUSH` / `SyncStatus.STAGE1_RUNNING`，必须归一化 |
| CON-6 | 路径参数不做 URL 编码 | `local_path`/`cluster_path` 只当**展示性元数据**，严禁参与路径推导；截断 2047 |
| CON-7 | 鉴权凭据在查询串 | 不得移除查询串 token 支持 |
| CON-8 | 状态接口响应时延 | 轮询超时 10 s，状态查询须 1 s 内返回（P99） |
| CON-9 | 不改动既有表结构即可上线 | P0 零 DDL；新增表列均为 P1 且须人工迁移 |
| CON-10 | 多 worker 安全 | `one/one_etc/core.toml:3` `ugc = 2`，任何「启动即执行」逻辑必须幂等 |

> ⚠️ 最容易踩的四条：**CON-3**（缺 `success` 客户端直接断言失败）、**CON-4**（不手工解析 Body 必 422）、**CON-5**（枚举串不归一化查库查不到）、**CON-10**（`ugc=2` 下恢复逻辑跑两遍）。

### 2.4 非功能需求（NFR）

| ID | 类别 | 指标 |
| --- | --- | --- |
| NFR-01 | 时延 | status P99 ≤ 100 ms；`get_sts_token` ≤ 1 s；`get_sync_status` ≤ 200 ms；`sync_to_cluster` 提交 ≤ 500 ms |
| NFR-02 | 吞吐 | 单实例 ≥ 200 MB/s；10 000 文件 / 10 GB 工作区 push 端到端 ≤ 10 min |
| NFR-03 | 规模 | 单工作区 ≤ 200 000 文件 / ≤ 1 TB；单次请求 ≤ 10 000 文件 |
| NFR-04 | 可用性 | 接口可用性 ≥ 99.9% |
| NFR-05 | 幂等一致性 | 同 index 不产生第二份任务；DB/Redis 允许 300 s 内不一致 |
| NFR-06 | 可观测 | 每接口 QPS/错误率；每任务耗时/文件数/字节数/失败数；关键路径日志带 `uuid/user/name/index` |
| NFR-07 | 可维护 | 领域层与宿主层解耦；单测覆盖 ≥ 70%（领域层 ≥ 85%） |
| NFR-08 | 兼容 | 老客户端与新客户端形态可同时在线 |
| NFR-09 | 部署 | 兼容 `one` 单机与 `cloud-storage` 独立部署；不新增必需中间件 |
| NFR-10 | 资源 | worker 数可配（默认 4）；内存占用与文件大小无关（流式/分片） |

### 2.5 安全需求（SEC）

| ID | 需求 | 验收要点 |
| --- | --- | --- |
| SEC-01 | 身份只来自 token | 伪造 `username`/`group` 不能访问他人数据 |
| SEC-02 | STS 最小权限 | 只能读写 `<bucket>/<group>/<user>/workspaces/<name>/*`；TTL ∈ [900,43200] |
| SEC-03 | 路径穿越与符号链接 | `check_is_subpath` + `Path.resolve()`；`..`、绝对路径、越界软链一律拒绝 |
| SEC-04 | 越权访问 | 用他人 `name`/`index` → 403 + `success=0`；`index` 校验绑定用户 |
| SEC-05 | 敏感信息 | 长期 AK/SK 不入响应/日志/异常栈；STS 凭证不入库 |
| SEC-06 | 资源保护 | 文件数/总大小/并发/单批限额；`page.size` 上限；list 缓存 + 限页 |
| SEC-07 | 删除保护 | 空 `file_list` 删全量必须显式审计日志（操作人/IP） |
| SEC-08 | 审计 | 删工作区、签发 STS、超配额拒绝留痕 ≥ 90 天 |

### 2.6 运维需求（OPS）

`OPS-01` 灰度开关（`enabled_users`/`enabled_groups`）· `OPS-02` 配置热更重启生效且不中断已完成任务 · `OPS-03` 回滚只需关开关 · `OPS-04` 监控告警（失败率 >5%、积压 >100、用量 >90%、DB 失败计数 >0）· `OPS-05` 日志含 `index`、保留 ≥7 天 · `OPS-06` 断点目录可写且按进程隔离 · `OPS-07` DDL 变更可在线执行并可回滚。

### 2.7 兼容性需求（COMP）

`COMP-01` 兼容枚举串形态 · `COMP-02` 兼容 Body 外壳与 `text/plain` · `COMP-03` 不破坏 `cloud-storage` 独立部署的无前缀路由 · `COMP-04` 双端 `conf/utils.py` 语义一致（md5/`.hfignore`/zip 逐字节一致）· `COMP-05` 响应字段只增不减 · `COMP-06` DB 向后兼容。

### 2.8 数据库（复用既有表，P0 零 DDL）

复用表：`user_sync_status`（PK `(user_name, file_type, name)`，冲突目标即 upsert 键）、`user_downloaded_files`（PK `(file_path, file_md5)`）、`quota`（`resource='cloud_storage_quota'`，自由字符串无需 DDL）、`storage`（挂载去重）、`user_all_groups`（灰度白名单）。

**DB 访问层三条硬约束**（违反即 SQL 报错，`db/mars_db.py` patch 了 `Connection.execute`）：

1. **禁止 `%s::type`** —— `%s::file_type` 会被 SQLAlchemy 正则截成 `:p` + `1::file_type`；需要转换写 `CAST(%s AS file_type)`。
2. **带参数 SQL 中字面 `%` 必须写 `%%`** —— `like 'abc%'` 会参数错位。
3. **参数只能传 tuple，枚举只能传 `.value`** —— 传 dict 报错；传 `SyncStatus.INIT` 会得到 `SyncStatus.INIT` 字面量。

**能力缺口**：G1 无同步任务表（P0 用 Redis 终态 TTL 1800 s + 心跳兜底）· G2 无 `group_name/provider/remote_path` 列（P0 每次由 token 重新推导）· **G3 配额语义未定义**（`user_downloaded_files` 是追加型日志，按 `sum(file_size)` 会「只增不减」永久限流 → 必须显式定口径）· G4 push 方向用量对 DB 不可见 · G5 缺聚合索引（P1）· G6 无审计表（P1）。

**迁移现状**：`init_postgresql.sh` 只在**空库**执行 DDL（`task_ng`/`user` 存在即整体跳过），仓库无 alembic → 任何新增 DDL 对既有库都**不会自动生效**，必须人工迁移并留痕。

### 2.9 既有缺陷（F）

| ID | 说明 | 本次是否必修 |
| --- | --- | --- |
| F1 | 服务端路由/DB/配置均在私有层；唯一注册的是 `return []` 桩 | ✅ 本任务核心 |
| F2 | `FileType` 枚举被 f-string 插值成 `FileType.WORKSPACE` | ✅ 服务端兼容层兜底（P0） |
| F3 | Body 外壳/`Body(FileList)` 语义不一致 → 422 | ✅ 兼容层兜底 |
| F4 | 客户端 `remote` 路径不被服务端采用，组名/用户名变化会失联 | ⚠️ P0 每次重新推导 + 前缀不匹配拒绝写入 |
| F5 | 非 oss provider 静默降级且 `MockApi` 签名不兼容 | ✅ 新增 `localfs` + 修 `mock` 签名（ADR-8） |
| F6 | push 不清理集群侧孤儿文件 | ⛔ P1 显式 prune（ADR-7） |
| F7 | `env` 上传链路不可用 | ⛔ P1 |
| F8 | `get_sts_token` bucket 选择绕过 `get_bucket_name` | ✅ DEV-05 判定标准已要求修正 |
| F9 | `RUNNING_TASKS_GAUGE` 无 `try/finally`、`bucket_usage_size` 无写入点 | ✅ 随 S8/DEV-22 修 |
| F10 | 客户端用法/文档问题 | ⛔ P1（CMP-06） |

---

## 3. 任务列表

### 3.1 总览

| 阶段 | 任务 | 依赖 | 估时 | 对应 Checklist | 状态 |
| --- | --- | --- | --- | --- | --- |
| S0 | 基线与环境准备（分支、构建、部署、配置、DB 核对） | — | 0.5 d | GATE/ENV/CFG/DB | ☐ |
| S1 | 领域层抽离与配置 | — | 1.5 d | DEV-01/02/03 | ☐ |
| S2 | 兼容层与路由注册 | S1 | 1 d | DEV-04/23 | ☐ |
| S3 | 状态与数据层 | S1 | 1.5 d | DEV-06/16 | ☐ |
| S4 | 列目录（`cluster_files/list`） | S2, S3 | 1 d | DEV-07 | ☐ |
| S5 | bucket → 集群 | S3, S4 | 3 d | DEV-08/09/10/13/14 | ☐ |
| S6 | 集群 → bucket | S3, S5 | 2 d | DEV-11/15/20 | ☐ |
| S7 | 删除与审计 | S3 | 0.5 d | DEV-12 | ☐ |
| S8 | 恢复与并发 | S5, S6 | 1.5 d | DEV-17 | ☐ |
| S9 | 任务侧 `oss://` 与挂载 | S1 | 1 d | DEV-18/19 | ☐ |
| S10 | 端到端联调（真实 CLI，7 子命令） | S1–S9 | 3 d | E2E-01~17 | ☐ |
| S11 | 测试与压测 | S1–S10 | 3 d | UT/PERF/SEC | ☐ |
| S12 | 灰度与上线 | S1–S11 | 1.5 d | REL/RB/POST/ACC | ☐ |
| P1-1 | 配额管理接口（API-10） | S6 | 0.5 d | DEV-21 | ☐ |
| P1-2 | 审计回收实现 | S6, S7 | 1.5 d | DEV-22 | ☐ |
| P1-3 | env 链路（API-11） | S5 | 0.5 d | — | ☐ |
| P1-4 | 客户端加固（`.value`/`quote()`/provider 映射） | 独立 | 0.5 d | CMP-06 | ☐ |

**关键路径**：S0 → S1 → S2 → S3 → S4 → S5 → S6 → S8 → S10 → S12。
**可并行**：S9（任务侧）与 S5/S6 无耦合；S11 的单测可与 S5~S8 同步编写。

---

### 3.2 任务明细

#### S0 · 基线与环境准备（0.5 d）

| 项 | 内容 |
| --- | --- |
| 目标 | 确定代码落点与构建/部署通路，避免开发到一半发现镜像进不去 |
| 交付物 | ① 统一代码分支；② 103 上可重复执行的构建脚本（`docker build` + `one/build_cli.sh`）；③ `override.toml` 增补 `[cloud.storage]`；④ DB 核对结果记录 |
| 关键动作 | ① 确认 3 个 checkout 的关系（本地 `~/github/hai-platform`、会话工作区 `opendeepinfra/hai-platform`、103 `~/hai-platform` 与 `~/hai-platform-clean`）并选定唯一真源；② 用 DB 审计 §9.1 的 6 段 SQL 核对线上库（两张表、两个枚举 6/9、触发器、索引）；③ 决定测试 provider（`localfs` 必跑 / 真实 OSS 需 AK-SK——**103 可达 `oss-cn-hangzhou.aliyuncs.com:443`，但当前无任何 `[cloud.storage]` 配置**） |
| 验收 | `sudo -u fireflyer hai-cli whoami` 正常；`hai-cli nodes` 4 节点；新镜像 tag 可部署并回滚；DB 核对表登记完毕 |
| 风险 | 103 上镜像构建 ~4.7 GB、耗时数十分钟；`create.sh` 会清空 db/redis（**重装而非重启**） |

#### S1 · 领域层抽离与配置（1.5 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `cloud_storage/service/{__init__,context,errors}.py`；`[cloud.storage]` 配置模板 + `check_cloud_storage_config()` |
| 修改 | `cloud_storage/__init__.py`（去掉 `from .api import *`，改导出 `app`，ADR-11）；`uvicorn_server.py`（`cloud_storage.api:app`）；`conf/proj_conf/default.py` 或 `context.py` 加自检 |
| 要点 | **惰性单例**：`get_cloud_api()` / `get_provider()` / `get_status_recorder()` / `get_worker_pools()`，把 `cloud_storage/utils.py:30-46` 的模块级副作用改成函数级；`import cloud_storage.service` 在**无配置环境**下不得抛异常，且**不注册路由、不注册 `on_event`** |
| 验收 | DEV-01/02/03：`grep -rn "from fastapi" cloud_storage/service/` 为空；`python -c "import cloud_storage.service"` 无配置不报错；`cloud-storage` 独立部署 7 条无前缀路由回归通过 |
| 需求 | FR-19、ADR-1/11/12、NFR-09 |

#### S2 · 兼容层与路由注册（1 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `cloud_storage/service/compat.py`（`normalize_enum` / `parse_json_body`）；`api/depends/implement.py` 的 `get_ugc_user(request)` |
| 修改 | `api/register/implement.py`（`if 'ugc' in REG_SERVERS` 分支 +9 条路由）；`api/app.py`（`WorkspaceError` + `RequestValidationError` 处理器） |
| 要点 | ① 枚举归一化剥前缀 `^(?:[A-Za-z_]\w*\.)+` 后大小写不敏感匹配，开关 `legacy_param_compat` 可关；② Body：忽略 Content-Type，空体→`{}`，非法 JSON→`INVALID_BODY`，兼容 `file_list`/`file_infos` 外壳与裸 `files`；③ 鉴权只认 token，**忽略** `username`/`group`；④ 校验失败保持 422 但响应体必须带 `success/code/msg` |
| 验收 | DEV-04/23：`text/plain` + `{"file_list":{...}}` 可解析；未知查询参数不 422；全站 4xx/5xx 都有 `success`，无裸 `{"detail":...}` |
| 需求 | FR-14、CON-2/3/4/5/7、COMP-01/02/05 |

#### S3 · 状态与数据层（1.5 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `server_model/user_impl/user_downloaded_files/{__init__,default,implement}.py`；`UserQuotaExtras.cloud_storage_quota` |
| 修改 | `AioUserDbExtras` / `UserDbExtras`（`set_sync_status` / `get_sync_status` / `soft_delete_sync_status`）；`module_imports`；`User`（挂 `downloaded_files`）；`cloud_storage/utils.py`（`StatusRecorder.a_set_nx` / `a_hincrby` / `a_get_hash_keys`） |
| 要点 | upsert 用 PK `(user_name, file_type, name)`；`coalesce(nullif(excluded.local_path,''), 旧值)` 实现「空值不覆盖」；列名 `push_status/last_push` 与 `pull_status/last_pull` 走**白名单**选择，不得拼接；时间用 `to_char(...,'YYYY-MM-DD HH24:MI:SS')`；`deleted_at is null` + `updated_at desc`；**严格遵守 2.8 的三条 DB 硬约束** |
| 验收 | DEV-06/16 + TC-DB-01~05：写入后 7 字段正确；空 `local_path` 不覆盖；`data:[]` + `success:1`；DB 写失败不中断传输且计入 `cloud_storage_db_failure_total` |
| 需求 | FR-02/03/12、CON-9、G3 口径定义 |

#### S4 · 列目录（1 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `cloud_storage/service/cluster_files.py`：`list_cluster_files_page(user, name, file_type, subpaths, no_checksum, no_hfignore, page, size)` |
| 修改 | `cloud_storage/utils.py`：`paginate()` 拆为 `list_files_cached()` + 薄 `paginate()`（保持旧 `Page` 返回，COMP-03 回归） |
| 要点 | `items/total` 必须真实（**这是 F1 的直接修复**）；`size` 截断 `[1,1000]`；30 s 缓存；`.hfai/` 与 `.hfai/*.zip` 过滤；翻页期间文件被删返回 `CLIENT_RETRY` 而非错误空页；`offset ≥ total` 时 `items=[]` 但 `total` 真实 |
| 验收 | DEV-07 + TC-A16~A21：10 万文件首次 ≤ 30 s、重复 ≤ 1 s；空目录返回 `items:[]` + `total:0` 且 `success:1` |
| 需求 | FR-04、NFR-01/02/03、SEC-06 |

#### S5 · bucket → 集群（3 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `cloud_storage/service/sync_to_cluster.py`（`submit_to_cluster` / `execute_to_cluster` / `wait_to_cluster`）；`cloud_storage/service/status.py`（`get_phase` / `phase_to_api`）；`cloud_storage/provider/localfs.py` |
| 修改 | `cloud_storage/provider/{__init__,mock}.py`（`PROVIDERS` 注册表 + `build_provider` + `mock` 签名修为 `(*args, **kwargs)`） |
| 要点 | `index = hashkey(user.token, name, file_type.value, *files)` **不排序**（ADR-10，须与客户端逐字节一致）；`set_owner(index, user)` 做 SEC-04；非 force 且 `RUNNING` → 忽略重复提交；空列表 → 立即 `finished`；`ensure_dir` 用 `makedirs + chown(uid)`；`.zip` → `.hfai/` → 解压 → 逐文件 `chown` → 删 zip；从 tagging 恢复 `filemode`；断点目录按实例隔离 `{breakpoint_info_path}/{instance_id}`；**终态 TTL 改 1800 s**（ADR-6） |
| 验收 | DEV-08/09/10/13/14 + TC-C01~C30：第二次 push 传输 0 字节；同 index 不重复启动；`.hfai/` 无残留 |
| 需求 | FR-05/06/09/10/11/20、ADR-6/10、SEC-02/03 |

#### S6 · 集群 → bucket（2 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `cloud_storage/service/sync_from_cluster.py`（`submit_from_cluster` / `wait_from_cluster`） |
| 要点 | ① `os.path.realpath(src)` 必须仍在 `cluster_base` 内（SEC-03，越界软链记失败跳过）；② 配额预检 `used_mb = await user.downloaded_files.get_usage_in_mb()`，超限 `QUOTA_EXCEEDED` 403 + 明细；③ 仅 `>1 GiB` 才 `filter_synced_files`；④ 记账 `running → finished/failed`，**只有 `finished` 计入用量**；⑤ 写 tagging `size/md5/source=cluster/filemode`；⑥ 保留 doc/pypi 的 bucket GC |
| 验收 | DEV-11/15/20 + TC-D01~D10：`pull` 后本地文件 `filemode` 与集群一致；改内容后再 pull 产生新行（记录 G3 口径） |
| 需求 | FR-07/09/11、SEC-03/06 |

#### S7 · 删除与审计（0.5 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `cloud_storage/service/delete.py`：`delete_paths(user, name, file_type, files)` |
| 要点 | 逐路径 `check_is_subpath`；目录 `rmtree`、文件 `remove`；不存在视为成功；空 `file_list` = 删整区 → **必须 INFO 审计日志（操作人/IP）** + 软删 DB 行（`deleted_at = now()`） |
| 验收 | DEV-12 + TC-E01~E06：删整区后 `list` 不再显示；越界路径返回 `PATH_ESCAPE` |
| 需求 | FR-08、SEC-07/08 |

#### S8 · 恢复与并发（1.5 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `cloud_storage/service/recovery.py`（`recover_interrupted_tasks()` + 实例心跳 + pod 锁） |
| 修改 | `WorkerPools`（实例隔离断点目录、`finish()` 幂等、`shutdown_all()`）；`api/resource/cloud_storage/default.py` 注册 `startup`/`shutdown` 钩子 |
| 要点 | `instance_id = f'{POD_NAME}-{os.getpid()}'`；心跳 `HSET cloud_storage:instances:{pod_id} {instance_id} <ts>` + `EXPIRE 120`（30 s 刷新）；恢复锁 `SET NX {PROVIDER}:recover:{pod_id} <instance_id> EX 300`；**仅认领心跳缺失的实例**；重写 `param` 为新 instance 并 `force=True` 重提交；兜底「param 写入 > `recover_stale_seconds`(600) 才认领」；提交循环 `try/finally` 修 F9 gauge 悬挂 |
| 验收 | DEV-17 + TC-F01~F08：`ugc=2` 下恢复只由一个 worker 执行；`kill -9` 后重启任务续跑并最终 `finished` |
| 需求 | FR-13、CON-10、ADR-5/12 |

#### S9 · 任务侧 `oss://` 与挂载（1 d）

| 项 | 内容 |
| --- | --- |
| 新增 | `server_model/task_impl/workspace_resolver.py`：`resolve_workspace_path(user, workspace, *, check_exists=False)` |
| 修改 | `server_model/task_impl/code/default.py`（`parse_code_cmd` **只做解析、`check_exists=False`**——manager pod 看不到共享盘，校验会误判，见下方“修订”）；`server_model/task_impl/runtime_mounts/default.py`（追加挂载） |
| 要点 | 非 URI 原样返回（回归 K-05）；scheme 必须等于配置 provider；`remote` 必须以 `{shared_group}/{user}/workspaces/` 开头（防越权引用他人工作区）；`check_is_subpath` 防穿越；不存在时文案 `workspace 尚未同步到集群，请先执行 hai-cli workspace push`（**只在提交接口 `api/operation/implement.py` 产生**，那里能看到共享盘）；挂载 `{host_path=mount_path, mount_type='DirectoryOrCreate', read_only=False, name='workspace-path'}`，与 `storage` 表既有挂载去重；**`add_runtime_mounts` 早于 `parse_code_cmd`，不得做 I/O** |
| 修订 | 2026-10-01：任务 7 `ugc_e2e3` 因 `parse_code_cmd(check_exists=True)` 在 manager pod 内 `os.path.isdir` 恒为 False 而被误判「尚未同步」并卡死，故移除该处校验（`check_exists` 仅提交接口使用） |
| 验收 | DEV-18/19 + TC-K01~K10：`MARSV2_TASK_WORKSPACE` 与 `cd {code_dir}` 都落到解析后路径 |
| 需求 | FR-15/16 |

#### S10 · 端到端联调（3 d）

| 项 | 内容 |
| --- | --- |
| 目标 | 在 103 上用**真实客户端**（未升级的 `haiworkspace` 插件）跑通 7 个子命令 |
| 用例文档 E2E-01~08（8 个端到端场景） | E2E-01 首次 push → 提交任务跑通（主场景）· E2E-02 增量 push（0 字节）· E2E-03 pull 取回 checkpoint · E2E-04 download 单个子路径 · E2E-05 多用户隔离 · E2E-06 中断恢复 · E2E-07 老客户端 + 灰度开关全链路 · E2E-08 大工作区（10 万文件，P1） |
| Checklist E2E-01~17（17 条联调项） | E2E-01 `init` · 02 首次 `push` · 03 `diff` · 04 增量 push · 05 `list` · 06 提交任务看到代码 · 07 `pull` · 08 `download <subpath>` · 09 `remove -f`/`remove` · 10 多用户隔离 · 11 中断恢复 · 12 双 worker 不重复 · 13 `--no_zip` · 14 `--no_diff/--force` · 15 `--no_checksum/--no_hfignore` · 16 大文件分片 · 17 特殊字符路径（含 emoji） |
| 每条要求 | 同时用「旧客户端形态」（枚举串 + `text/plain`）与「规范形态」各跑一遍（E2E-08 除外） |
| 验收 | 用例文档 E2E-01~07 全通过 + Checklist E2E-01~17 全通过；退出码 0、无报错；增量 push 上传字节为 0 |

> ⚠️ **两处 `E2E-*` 编号含义不同**（用例文档 8 个场景 vs Checklist 17 条联调项），引用时必须带文档前缀，见 §7 不一致 #4。

#### S11 · 测试与压测（3 d）

| 项 | 内容 |
| --- | --- |
| 单测 | UT-01 覆盖率：整体 ≥70%、`cloud_storage/service/**` ≥85%；UT-02 关键纯函数边界用例（`normalize_enum` / `parse_json_body` / `resolve_workspace_path` / `local_path_for` / `index` 计算 / 截断）；UT-04 同 index 并发提交只产生一份任务 |
| 性能 | PERF-01~03：status P99 ≤100 ms、10 万文件列目录 ≤30 s、10 000 文件 10 GB push ≤10 min |
| 安全 | TC-H01~H12（含 H12 泄漏扫描：长期 AK/SK 不出现在响应/日志） |
| 故障注入 | FI-01~FI-12（阻塞项 FI-01/03/04/06/07/08/09/10） |
| 验收 | P0 冒烟集 100% 通过；RELEASE 集阻塞项全过 |

#### S12 · 灰度与上线（1.5 d）

| 项 | 内容 |
| --- | --- |
| 步骤 | ① `enabled=false` 部署验证 ugc-server 无回归 → ② `enabled=true` + `enabled_groups=<内部组>` 内部跑通 → ③ 扩到试点用户组观察失败率/时延/用量 → ④ 全量 → ⑤ 客户端发布修复版后再保持 `legacy_param_compat` 至少 2 个大版本 |
| 回滚 | 一级 `enabled=false`；二级回滚镜像（对象/PG/Redis 保留，无脏数据）；三级单独关 `legacy_param_compat` |
| 验收 | REL/RB/POST/ACC 全过；灰度 24 h 无阻塞问题 |

### 3.3 P1 任务（不阻塞 P0）

| ID | 任务 | 交付物 | 备注 |
| --- | --- | --- | --- |
| P1-1 | 配额管理接口 | API-10 + `quota` 表写入 + 变更日志 | 依赖 S6；仅 `ops`/`platform` 组 |
| P1-2 | 审计与回收 | `run_audit` 4 子任务 + 单实例锁 + `RUN_AUDIT` 开关 + `bucket_usage_size` 指标 | 依赖 S6/S7 |
| P1-3 | env 链路 | API-11 + 客户端 `FileType.ENV` 修复 | 需先修客户端字符串化 |
| P1-4 | 客户端加固 | 枚举 `.value`、路径 `quote()`、`--breakpoint_dir`、provider 映射表支持 `localfs` | 向后兼容，独立可做 |
| P1-DB | DDL 变更 | `cloud_storage_sync_log` 表、`user_sync_status` 补列、聚合索引 | **必须人工迁移**，成对提交迁移+回滚脚本 |

---

## 4. 测试任务列表（在 103 上执行）

### 4.1 环境与构建任务

| ID | 任务 | 判定 |
| --- | --- | --- |
| T-01 | 在 103 上确认环境健康（`multipass list` 4 台、`kubectl get nodes` 4/4、MetalLB 4/4、LB VIP 端口 80/8080/5432/6379 全通、`nodes_df_pickle = 1`） | 全部通过（见 test-environment.md §7.1） |
| T-02 | 构建新镜像（`docker build` + `one/build_cli.sh`），tag 用新 commit 短 hash | 构建成功、镜像可推送到 `registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform` |
| T-03 | 部署并回归（`enabled=false`） | 平台 pod 1/1、4 节点在线、π 冒烟任务通过 |
| T-04 | 配置 `[cloud.storage]`（写入 `/nfs-shared/hai-platform/override.toml`，优先级最高） | 自检无缺失键；缺键时接口返回 `CLOUD_STORAGE_NOT_CONFIGURED` 而非 500 |
| T-05 | DB 核对（审计 §9.1 六段 SQL） | 两张表 + 枚举 6/9 + 触发器 + 索引与 `db_schemas` 一致 |
| T-06 | 构造测试数据（多用户、灰度白名单、`.hfignore`、特殊字符目录、≥1 GB 大文件、10 万文件目录） | 数据可复用（脚本化） |

> **Provider 选择**：`localfs` 路径必跑（无需云凭证）；真实 OSS 路径需 AK/SK——103 到 `oss-cn-hangzhou.aliyuncs.com:443` 网络可达，但当前环境**没有任何 `[cloud.storage]` 配置**。

### 4.1.1 两套环境的关系

| | 用例文档 §2（抽象最小环境） | 103 真实环境（test-environment.md） |
| --- | --- | --- |
| 定位 | 用例编写时的假设，可脚本化重建 | 已部署并通过冒烟，Multipass + MicroK8s + MetalLB |
| provider | `localfs`（路径 1，必跑）/ 真实 OSS（路径 2，发布前必跑） | 需新建配置；建议先 `localfs` |
| 工作区根 | `/tmp/hai-test/workspace` | `/nfs-shared/hai-platform/workspace`（**待 D6 确认**） |
| 对象存储根 | `/tmp/hai-test/localfs`（tagging 落 `<key>.__tag__.json`） | `/nfs-shared/hai-platform/localfs`（建议） |
| 断点目录 | `/tmp/hai-test/breakpoints/{instance_id}` | 需可写且非 tmpfs，按实例隔离 |
| Redis | `db=1` 隔离，键前缀 `{PROVIDER}=localfs` | 既有实例 `10.205.52.200:6379`，建议用独立 db 隔离 |
| 访问入口 | `ugc-server:8083` 或 haproxy `:80/ugc/` | `http://192.168.100.103:8090/`（浏览器）、`http://192.168.100.103:80`（BFF） |
| 测试用户 | `wstest_a/wsgrp`（主）、`wstest_b/wsgrp`（同组越权）、`wstest_c/wsgrp2`（跨组）、`wsadmin/ops`（配额管理）、`wsgray/wsgrp3`（灰度外）、`wsquota0/wsgrp`（`download=0`） | 现只有 `haiadmin`（组 `10020`）；**需按上表补建测试用户** |
| 配额 | 默认 `102400` MB；预置超限 99 GB / 零额度 0 | 同 |

> `index = sha256(token + name + file_type + *files)`，**顺序敏感、不排序**（ADR-10）；测试可用 Python/aiohttp 等价复现，并验证「客户端兜底重算 == 服务端返回」。`cluster_base` / `cloud_base` 的推导结果是断言的唯一依据。

### 4.2 P0 冒烟集（SMOKE，≤30 min）

```
TC-A01, A06, A11, A16, A22, A23, A25, A28, A29, A31, A32, A33, A39, A41, A44, A46, A47, A50, A54, A55,
TC-B01~B04, TC-C01, C02, C03, C05, C08, C13, C17, C18, C19, C21, C23, C25, C26,
TC-D01, D02, D04, D06, D07, TC-E01~E03, TC-F01, F02, F04, F06,
TC-G01, G05, G10, G12, TC-H01, H02, H04, H05, H07, H10, H12,
TC-I01, TC-J01, J03, J06, J09, J12, TC-K01, K03, K05, K07,
TC-L01, L02, L04, E2E-01, E2E-02, E2E-05, E2E-06
```

**通过标准**：100% 通过，且 E2E-01/02/05/06 的关键断言（集群侧 md5、第二次 push 0 字节、越权 403、重启后 `finished`）成立。

### 4.3 用例分组（A–L + DB，共 182 条 + 12 条）

| 组 | 主题 | 用例范围 | 条数 |
| --- | --- | --- | --- |
| A | 接口契约 | TC-A01~A56 | 56 |
| B | `init` 与状态查看 | TC-B01~B06 | 6 |
| C | push 全链路 | TC-C01~C30 | 30 |
| D | pull / download | TC-D01~D10 | 10 |
| E | remove | TC-E01~E06 | 6 |
| F | 并发、幂等、崩溃恢复 | TC-F01~F08 | 8 |
| G | 状态机与一致性 | TC-G01~G12 | 12 |
| H | 安全 | TC-H01~H12 | 12 |
| I | 性能与容量 | TC-I01~I10 | 10 |
| J | 兼容、配置与运维 | TC-J01~J14 | 14 |
| K | 任务侧 `oss://` | TC-K01~K10 | 10 |
| L | 可观测性与审计 | TC-L01~L08 | 8 |
| DB | 数据库层 | TC-DB-01~12 | 12 |

### 4.4 故障注入矩阵

覆盖 ugc-server `kill -9`、Redis 抖动/清空、PG 不可用、对象存储超时/5xx、磁盘写满、单文件失败、多 worker 抢恢复、终端 TTL 过期等（详见用例文档 §6；阻塞项 FI-01/03/04/06/07/08/09/10）。

### 4.5 性能与稳定性

| 项 | 门槛 |
| --- | --- |
| status P99 | ≤ 100 ms |
| `get_sync_status` P99 | ≤ 200 ms |
| `get_sts_token` P99 | ≤ 1 s |
| 10 万文件列目录 | 首次 ≤ 30 s，重复 ≤ 1 s |
| 10 000 文件 / 10 GB push | 端到端 ≤ 10 min |
| 稳定性 PM-6 | ≥8 h 准入，24 h 为发布标准 |

### 4.6 优先级集合与缺陷分级

| 集合 | 触发时机 | 耗时 |
| --- | --- | --- |
| SMOKE（P0 冒烟） | 每次构建/每次部署后 | ≤30 min |
| REG（P1 回归） | 每日夜间、合并主干前 | ≤4 h |
| RELEASE（发布前必跑） | 每个 RC | ≤1 天（含真实 OSS） |

**缺陷分级**：Blocker（主链路失败/数据损坏/越权/凭据泄漏/`running` 时 `msg` 不可 `int()`）→ 立即修复且发布阻断；Critical（增量失效、`cluster_files/list` 返回空、终态 TTL <1800 s、多 worker 重复恢复、配额未生效、失败态悬挂 `running`）→ 24 h 内修复且发布阻断；Major（`page.size` 截断未生效、审计未回收、指标标签缺失、P1 接口缺陷）→ 当前迭代修复，不阻塞；Minor（文档/文案/日志格式）→ backlog。

### 4.7 硬门槛（必须全部通过才可进入下一阶段）

1. **阶段门**：Checklist 明示「未通过项不得进入下一阶段」。
2. **GATE-01~06** 阶段 0 前置全过（尤其 **GATE-03 接口契约冻结**）后才开工。
3. **CFG-02** `enabled=false` 时全部 `/ugc/*` 返回 `FEATURE_DISABLED` 且其他接口零回归。
4. **CFG-04** `status_ttl_finished ≥ 1800` 且 ≥ 客户端 `--sync_timeout`。
5. **DB-02** P0 零 DDL；**DB-06** `grep -rn "%s::"` 零命中 + TC-DB-07/08 通过。
6. **DEV-23** 全站 4xx/5xx 响应体都含 `success`，无裸 `{"detail":...}`。
7. **SEC-05** 凭证泄漏扫描零命中；**SEC-09** 开关不可绕过；**SEC-01~08** 全通过无高危遗留。
8. **REL-02** 部署顺序（配置 `enabled=false` → 代码 → 冒烟其他接口 → 打开灰度 → 全量）任一步失败即停；**REL-03** P0 冒烟集 100%；**REL-04** 灰度观察 ≥24 h 且失败率 <1%。
9. **RB-01** 开关回滚 ≤5 min；**RB-04** 回滚不删两表与 bucket 对象。
10. **准出**：SMOKE 100%；RELEASE 阻塞项零未通过（性能允许 ≤5% 偏差 + 书面风险接受）；Blocker/Critical = 0；Major 关闭率 ≥90%；四类告警可演练触发；真实 OSS 至少跑通一轮 SMOKE + C01/13/17/21/24。

---

## 5. 待确认决策

| # | 决策 | 选项 / 影响 |
| --- | --- | --- |
| D1 | **代码真源与改法** | ① 在 103 的 `~/hai-platform` 直接改（用户原话「在 103 上修改并测试」）；② 在本地工作区改完 rsync 到 103 再构建（可留本地 diff 与评审）；③ 两者结合（本地改 → 推送分支 → 103 拉取）。当前 103 `~/hai-platform` 在 `7589fb1`（落后本地 `50c9566` 两个提交），且 `Dockerfile` 有未提交改动 |
| D2 | **测试 provider** | `localfs`（自包含、可重复、无需凭证）／真实 OSS（需 AK-SK，103 网络可达）／两者都跑（推荐：localfs 跑 SMOKE+REG，OSS 跑 RELEASE 阻塞项） |
| D3 | **是否重建并推送镜像** | 平台当前跑 `:7589fb1`；源码修复 `203ca3b`/`50c9566` **尚未进入镜像**。是否本次一并构建新 tag 并部署（会重启平台） |
| D4 | **验收范围** | 只做 P0（FR-01~16/19/20）／P0+P1／只做「让 7 个子命令跑通」的最小闭环（S1–S10） |
| D5 | **配额口径（G3）** | 当日累计／按 `file_path` 去重取最新／写新行时 supersede 旧行（影响 `user_downloaded_files` 语义与 FR-17 判定） |
| D6 | **`service.workspace_path` 实际取值** | 设计默认 `/nfs_shared/workspace`；103 实际共享盘是 `/nfs-shared/hai-platform/workspace`——须与线上一致 |
| D7 | **校验失败的状态码** | 见 §7 不一致 #1：TC-A50 要求 200，DEV-23 要求保持 422——必须唯一 |
| D8 | **回归通过率口径** | 见 §7 不一致 #2：REG ≥98%（用例文档）vs P1 ≥90%（Checklist ACC-02） |

---

## 6. 风险

| # | 风险 | 处置 |
| --- | --- | --- |
| R1 | `service.workspace_path` 取值与线上不一致 → 任务拿不到代码 | S0 用 `storage` 表 + 线上目录核对；纳入启动自检 |
| R2 | 私有层可能已实现同名方法，与本设计冲突 | 按 ADR-4 放 `*Extras` 基类保留覆盖能力；联调时确认 `custom.py` 现状（本仓库 0 个） |
| R3 | 集群共享存储未在所有节点挂载 | FR-16 挂载 + 节点巡检（Checklist ENV-03） |
| R4 | 客户端不做 URL 编码，`local_path` 含 `&`/空格/中文 | 服务端截断 + 容错；P1 客户端 `quote()` |
| R5 | `fastapi-pagination==0.9.1` 的 `Params(page, size)` 构造方式 | 实现前先写单测确认（TC-A16 前置） |
| R6 | 大工作区（>10 万文件）列目录慢 | 缓存 + 客户端 `--no_diff` 兜底 |
| R7 | 103 环境脆弱点（k8swatcher 因 DB VIP 抖动崩溃循环、slave02 磁盘 9.6 G、宿主机根分区曾 99%） | 测试前按 test-environment.md §7 做健康检查；避免在 slave02 上跑大数据 |
| R8 | 测试环境无 OSS 凭证 | 以 `localfs` 为主路径；真实 OSS 路径待确认 D2 |

---

## 7. 文档间不一致（需裁决）

以下 4 处是梳理三份文档时发现的**互相冲突**项，实施前必须定稿，否则会出现「按 A 写、按 B 判失败」。

| # | 冲突 | 两侧原文 | 建议 |
| --- | --- | --- | --- |
| 1 | **校验失败的 HTTP 状态码** | 用例 TC-A50：`RequestValidationError` 改写为 **HTTP 200** + `{'success':0,'code':'INVALID_PARAM'}`；Checklist DEV-23：兜底处理器**保持 422 状态码**并追加 `success/code/msg/detail` | 客户端只断言 `'success' in result`，两方案都满足它。建议**统一为 200**（与「业务失败一律 200」的整体约定一致），并回改用例/Checklist 中落败的一方 |
| 2 | **回归通过率** | 用例 §11.3：REG 通过率 **≥98%**；Checklist ACC-02：P1 通过率 **≥90%** | REG 是 SMOKE 的补集。建议统一为 **P0=100%、P1/REG ≥98%**（较严者），ACC-02 回改 |
| 3 | **Checklist 组数与契约覆盖** | 各处称「16 组」，实际列了 **17 个前缀**；另含 DOC/DEP 组共 **19 个前缀**；附录 A「接口契约快照」只覆盖 **9 个 `/ugc/*`**，不含 API-10/11/12，但 A51~A53（配额管理）与 J13/J14（usage / venv）已被用例覆盖 | 修正组数表述；把 API-10/11/12 补入附录 A，或显式声明「P1 接口不参与 GATE-03 冻结」 |
| 4 | **`E2E-*` 编号命名空间冲突** | 用例文档 `E2E-01~08` = 8 个端到端场景；Checklist `E2E-01~17` = 17 条联调项，含义不同（如前者 E2E-02 是「增量 push」，后者 E2E-02 是「首次 push」） | 引用时**必须带文档前缀**（本任务列表 §3.2 S10 与 §4.3 已按此处理） |

> 另有 1 处一致性提示（非冲突）：`TC-A51~A53`、`J13`、`J14` 覆盖了 P1 接口，但需求 §11 追溯矩阵把 `API-11`/`API-12` 标为 P1——**实现 P0 时这些用例应显式标记为「不适用」，而不是「未通过」**。
