# HAI Platform · `hai-cli workspace` 服务端 Checklist

| 项目 | 内容 |
| --- | --- |
| 文档 | 服务端实施与上线 Checklist（可勾选执行） |
| 版本 | v1.0 |
| 需求基线 | [workspace-server-requirements.md](workspace-server-requirements.md) |
| 设计基线 | [workspace-server-design.md](workspace-server-design.md) |
| 用例基线 | [workspace-server-test-cases.md](workspace-server-test-cases.md) |
| 使用方式 | 每个阶段由**责任人**逐条勾选；「证据」列必须留下可复查的痕迹（命令输出、CI 链接、截图、日志片段 ID）。未通过项不得进入下一阶段 |

**状态图例**：`☐` 未开始 · `◐` 进行中 · `☑` 通过 · `✗` 不通过（必须记录原因与处置） · `N/A` 本期不适用（需说明）

**需求映射说明**：`DEV-02`、`DEV-04`~`DEV-22` 与需求文档 §11 追溯矩阵一一对应；`SEC-01`~`SEC-08`、`OPS-01`~`OPS-07`、`PERF-01`~`PERF-03`、`OBS-01`~`OBS-03`、`CMP-01`~`CMP-04`、`DOC-01`、`DEP-01` 为**与需求 ID 同名**的检查项（需求文档 §11 直接引用）；`DEV-01/03/23`、`ENV-*`、`CFG-*`、`DB-*`、`UT-*`、`E2E-*`、`TASK-*`、`REL-*`、`RB-*`、`POST-*`、`ACC-*` 为过程性检查项（追溯矩阵的补充），在文末「补充映射」中登记。

---

## 1. 阶段 0 · 启动前置（GATE-0）

| 状态 | ID | 检查项 | 判定标准（通过条件） | 证据 | 责任 |
| --- | --- | --- | --- | --- | --- |
| ☐ | GATE-01 | 需求评审通过 | [需求说明书](workspace-server-requirements.md) 通过评审，`CON-1`~`CON-10` 无异议；§16 开放问题中 R1/R2/R3 已闭环或给出临时方案 | 评审记录链接 | PM/架构 |
| ☐ | GATE-02 | 设计评审通过 | [程序设计](workspace-server-design.md) 通过评审；ADR-1~ADR-10 已确认；§4 接口契约逐条与客户端实际行为核对过 | 评审记录链接 | 架构/后端 |
| ☐ | GATE-03 | 接口契约冻结 | 9 个 `/ugc/*` 接口的方法/路径/参数名/Body 形状/响应字段冻结，写入本文档附录 A 并锁定 | 契约快照（git tag/commit） | 后端/客户端 |
| ☐ | GATE-04 | 确认私有层现状 | 核对部署私有 `custom.py` 是否已实现同名方法（设计 §16 R2）；若已存在，明确以哪一层为准 | 代码检索记录 + 结论 | 后端 |
| ☐ | GATE-05 | 客户端版本基线 | 确认需支持的最老客户端版本；确认其发送「枚举串 + `text/plain` + 外壳」三形态（分析报告 F2/F3/CON-4） | 客户端版本清单 | 客户端/测试 |
| ☐ | GATE-06 | 测试用例评审 | [测试用例](workspace-server-test-cases.md) 通过评审；需求覆盖无空洞（反向表 100%） | 评审记录 | 测试 |

---

## 2. 阶段 1 · 环境与配置（ENV / CFG）

### 2.1 环境准备

| 状态 | ID | 检查项 | 判定标准 | 证据 | 责任 |
| --- | --- | --- | --- | --- | --- |
| ☐ | ENV-01 | PostgreSQL 可用且表齐备 | `user_sync_status`、`user_downloaded_files` 存在；`file_type`、`sync_status` 两个 enum 类型存在（`\dT+ file_type` 输出含 6 个值） | `psql -c '\d user_sync_status'` 输出 | 运维/DBA |
| ☐ | ENV-02 | Redis 可用 | `redis-cli ping` → `PONG`；`SET NX` 与 `HSET/EXPIRE` 可用（版本 ≥ 3.0） | 命令输出 | 运维 |
| ☐ | ENV-03 | 集群共享存储挂载巡检 | `service.workspace_path` 在**所有可调度节点**上均已挂载且用户可写（用测试用户在同一路径下 `touch` 验证 ≥ 3 个节点） | 节点巡检脚本输出 | 运维 |
| ☐ | ENV-04 | 对象存储可达 + RAM Role 可 Assume | 服务端能成功 `AssumeRole`（用 `stsecho`/脚本验证），返回凭证含 `SecurityToken`；bucket 存在且可读写 | 脚本输出 | 运维 |
| ☐ | ENV-05 | 测试 provider（可选） | `provider=localfs` 时 `localfs_root` 可写；`localfs` 上传/下载/tagging 冒烟通过 | 单测输出 | 开发 |
| ☐ | ENV-06 | ugc-server 多 worker 生效 | `SERVER=ugc` 启动日志显示 2 个 worker；`/api_server_status` 正常 | `ugc_0.log` 片段 | 运维 |

### 2.2 配置

| 状态 | ID | 检查项 | 判定标准 | 证据 | 责任 |
| --- | --- | --- | --- | --- | --- |
| ☐ | CFG-01 | `[cloud.storage]` 完整 | 设计 §9.1 的配置项全部就位；启动自检输出「missing: []」；`access_key_id/secret` 若为密文则 RSA 解密成功 | 启动日志 | 运维 |
| ☐ | CFG-02 | 开关默认安全 | `enabled=false` 时所有 `/ugc/*` 返回 `FEATURE_DISABLED` 且 `success=0`；ugc-server 其他接口（nodeport/train_image）无回归 | curl 输出 | 后端 |
| ☐ | CFG-03 | 断点目录 | `service.breakpoint_info_path` 存在、可写、位于**共享或本地持久盘**（非 tmpfs 更佳）；按实例隔离子目录已生效 | `ls -l` + worker 启动日志 | 运维 |
| ☐ | CFG-04 | 终态 TTL ≥ 客户端超时 | `status_ttl_finished ≥ 1800`，且 ≥ 客户端默认 `--sync_timeout` | 配置 diff | 后端 |
| ☐ | CFG-05 | 限额合理 | `max_files_per_request`、`max_bytes_per_request`、`max_page_size`、`workers` 与容量规划一致 | 配置 diff + 评审 | 架构 |

### 2.3 数据库结构核对与迁移（DB）

> 依据：[workspace-server-db-audit.md](workspace-server-db-audit.md)。结论是 **P0 无需任何 DDL**，但必须先核对线上库（本仓库无自动迁移框架，`init_postgresql.sh` 仅在空库执行 DDL）。

| 状态 | ID | 检查项 | 判定标准 | 证据 | 责任 |
| --- | --- | --- | --- | --- | --- |
| ☐ | DB-01 | 线上库结构核对 | 执行审计 §9.1 的 6 段 SQL：两张表存在；`file_type` 6 值、`sync_status` 9 值与 `conf/utils.py` 一致；两表 `updated_at` 触发器存在；主键/索引与 `db_schemas` 一致；不一致处逐条登记 | SQL 输出存档 | DBA |
| ☐ | DB-02 | P0 零 DDL 确认 | 本期不提交任何 `db_schemas/` 新文件；变更单中明确「已确认无需 DDL」 | 变更单 | 后端/DBA |
| ☐ | DB-03 | 迁移脚本规范（仅 P1） | 若采纳审计 §6 的 DDL：新增 `deploy/dbs/migrations/<date>-<name>.sql`，**全部幂等**（`if not exists`），并与回滚脚本成对提交 | 文件 + 评审记录 | 后端 |
| ☐ | DB-04 | 迁移人工执行留痕 | 由 DBA 在变更窗口执行并记录（平台无自动迁移）；预发先演练 | 执行记录 | DBA |
| ☐ | DB-05 | 迁移演练与回滚 | 预发库：执行 → 验证 → 回滚 → 再执行，三次均成功且幂等 | 演练记录 | DBA/后端 |
| ☐ | DB-06 | 绑定参数规范落地 | 代码中**不存在** `%s::type` 写法（`grep -rn "%s::" --include=*.py` 无命中）；带参数 SQL 中的字面 `%` 均为 `%%`；参数一律 tuple、枚举一律 `.value`；用例 TC-DB-07/08 通过 | grep 输出 + 用例报告 | 后端 |
| ☐ | DB-07 | 配额口径确认（缺口 G3） | 「判定用量」与「展示占用」两种口径经产品确认并写入设计（审计 §5 G3）；用例 TC-DB-05 通过 | 产品确认记录 | 产品/后端 |

---

## 3. 阶段 2 · 编码完成度（DEV / UT）

### 3.1 通用与契约

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | DEV-01 | 领域层抽离完成 | `cloud_storage/service/*` 存在；**无 FastAPI 依赖**（`grep -rn "from fastapi" cloud_storage/service/` 为空）；`PROVIDER`/`cloud_api` 改为惰性获取（`python -c "import cloud_storage.service"` 在无配置环境下不抛异常）；**导入不注册路由、不注册 `on_event`**（ADR-11/12）——仅 `SERVER=ugc` 的标准形态下 `curl /get_sts_token` 应为 404（若部署有意同时暴露旧路由，需在此登记例外并说明） | 后端 |
| ☐ | DEV-02 | 配置装载与启动自检（FR-19） | 缺失键逐项打印；接口返回 `CLOUD_STORAGE_NOT_CONFIGURED`；**不抛 500、不影响 ugc 其他接口** | 后端 |
| ☐ | DEV-03 | 双宿主兼容 | `cloud-storage` 独立部署下无前缀路由仍可工作（`/get_sts_token`、`/sync_to_cluster`、`/sync_from_cluster`、`/delete_files`、两个 status、`/list_cluster_files` 全部回归通过） | 后端 |
| ☐ | DEV-04 | 兼容层（FR-14） | ① `normalize_enum` 覆盖 `FileType.`/`SyncDirection.`/`SyncStatus.` 前缀与大小写；② `parse_json_body` 兼容 `{"file_list":{...}}`/`{"file_infos":{...}}`/裸体/空体；③ **忽略 Content-Type**（`text/plain` 可解析）；④ 未知查询参数不报错；⑤ 开关 `legacy_param_compat` 可关闭 | 后端 |
| ☐ | DEV-23 | 统一错误码与校验异常（CON-3） | `WorkspaceError` → `{'success':0,'msg','code'}`（HTTP 语义按 §4.11）；`RequestValidationError` 兜底处理器**保持 422 状态码**并追加 `success/code/msg/detail`；全站 4xx/5xx 响应体都含 `success`；**不存在裸 `{"detail":...}`** | 后端 |

### 3.2 接口实现

| 状态 | ID | 检查项（需求 → 接口） | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | DEV-05 | FR-01 → API-01 `get_sts_token` | 返回 `{'success':1,'oss':{endpoint,access_key_id,access_key_secret,security_token,bucket}}`；bucket 走 `get_bucket_name`；TTL 夹取 `[900,43200]`；policy 前缀 = `get_base_path(...)[1]` | 后端 |
| ☐ | DEV-06 | FR-02/FR-03 → API-02/03 `set/get_sync_status` | upsert 逻辑正确（含空值不覆盖旧值、`deleted_at` 复位）；`get` 返回 7 字段且时间格式 `%Y-%m-%d %H:%M:%S`；**无记录返回 `data:[]` + `success:1`**；`name='*'` 列全部 | 后端 |
| ☐ | DEV-07 | FR-04 → API-04 `cluster_files/list` | `items/total/page/size/pages` 正确；`size` 截断 ≤1000；复用 30 s 缓存 + Redis（`paginate` 拆分后**旧路由回归通过**）；`.hfai/*.zip` 被过滤；文件被删时返回 `CLIENT_RETRY`；`offset ≥ total` 时 `items=[]` 且 `total` 真实 | 后端 |
| ☐ | DEV-08 | FR-05 → API-05 `sync_to_cluster` 提交面 | `index` 与客户端兜底算法**逐字节一致**；`RUNNING` 期间重复提交返回「正在进行中」且不启动第二份；空列表立即 `finished`；`accepted/skipped` 统计正确 | 后端 |
| ☐ | DEV-09 | FR-05/FR-10 → 传输执行面 | 落盘到 `{workspace_path}/{group}/{user}/workspaces/{name}`；`.zip` 先落 `.hfai/` 再解压并删除；`chown` 到用户；从 tagging 恢复 `filemode`；`makedirs` 幂等 | 后端 |
| ☐ | DEV-10 | FR-06/FR-20 → API-06/08 status | `running` 时 `msg` 为可 `int()` 的数字；`finished` 时 `msg=''`；`failed` 时 `msg` 为可读原因；不存在 index 返回 400 + `success:0`；**校验 index 归属** | 后端 |
| ☐ | DEV-11 | FR-07 → API-07 `sync_from_cluster` | `file_infos` 落对象存储并写 tagging；`insert_downloaded_file`/`update_downloaded_file_status` 记账；`>1 GiB` 才过滤已上传；路径与软链校验通过 | 后端 |
| ☐ | DEV-12 | FR-08 → API-09 `delete_files` | 逐路径 `check_is_subpath`；目录 `rmtree`、文件 `remove`；不存在视为成功；空 `file_list` 删整区并**软删 DB 行**；记录审计日志 | 后端 |
| ☐ | DEV-13 | FR-09 → tagging 规范 | 上传写 `size/md5/source=cluster/filemode`；下载读 `filemode` 恢复权限；`md5` 命中则跳过；tagging 缺失时降级不报错 | 后端 |
| ☐ | DEV-14 | FR-10 → zip 语义 | 与客户端 `zip_dir`/`unzip_dir` 互操作（同一 `conf/utils.py`）；目录权限按 `external_attr` 恢复；`.hfai` 不入列表 | 后端 |
| ☐ | DEV-15 | FR-11 → 断点续传与重试 | 单文件重试 10 次、1 s 间隔；分片 100 MB / 4 线程；断点目录按实例隔离；重试不重复记账 | 后端 |
| ☐ | DEV-16 | FR-12/FR-20 → 状态与进度 | Redis 阶段与 PG 状态映射符合设计 §7.3；`progress` 求和单调不减、写限频；**DB 写失败不中断传输**且计数 | 后端 |
| ☐ | DEV-17 | FR-13 → 崩溃恢复 | 实例心跳（30 s 刷新 / 120 s 过期）、`recover:{pod}` `SET NX` 锁、仅认领心跳缺失的实例；重写 `param` 为新实例；恢复幂等（md5 命中跳过） | 后端 |
| ☐ | DEV-18 | FR-15 → `oss://` 解析 | `resolve_workspace_path` 纯函数；scheme 校验、`group/user` 归属校验、`check_is_subpath`、存在性校验（仅 `parse_code_cmd`）；非 URI 路径原样透传 | 后端 |
| ☐ | DEV-19 | FR-16 → 工作区挂载 | 追加 `{host_path=mount_path=cluster_path, mount_type=DirectoryOrCreate, read_only=False, name='workspace-path'}`；与既有挂载去重；不在早期调用中做 I/O | 后端 |
| ☐ | DEV-20 | FR-17 → 限额 | 文件数/总字节/page size/并发池上限生效并返回对应错误码；pull 配额预检返回 403 + 明细 | 后端 |
| ☐ | DEV-21 | FR-18 → API-10 配额设置（P1） | 管理员接口可写 `quota` 表 `cloud_storage_quota`；非管理员 403 | 后端 |
| ☐ | DEV-22 | FR-21 → 审计回收（P1） | `run_audit` 实现四个子任务 + 单实例锁 + 开关；`cloud_storage_bucket_usage_size` 有写入点 | 后端 |

### 3.3 单元测试与静态检查

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | UT-01 | 单测覆盖率 | 全部新增/修改模块整体 ≥ 70%，`cloud_storage/service/**` ≥ 85% | 开发 |
| ☐ | UT-02 | 关键纯函数单测 | `normalize_enum`、`parse_json_body`、`resolve_workspace_path`、`local_path_for`、`index` 计算、`local_path/cluster_path` 截断 全部有边界用例 | 开发 |
| ☐ | UT-03 | 静态检查 | lint 通过；无新增 TODO 未登记；`import` 无副作用（在无 `[cloud.storage]` 配置下可 import 全部新增模块） | 开发 |
| ☐ | UT-04 | 幂等与并发单测 | 同 index 并发提交 N 次只产生一份任务（用 fake provider + Redis 打桩验证） | 开发 |

---

## 4. 阶段 3 · 联调（E2E）— 用**真实客户端**验证

> 前置：`enabled=true`，测试用户在灰度白名单内。每条必须**同时**用「旧客户端形态」（枚举串 + `text/plain`）与「规范形态」各跑一遍（除 E2E-08）。

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | E2E-01 | `workspace init` | 生成 `.hfai/workspace.yml`（4 键）；`/ugc/set_sync_status` 写入 `init`；`get_sync_status` 能查到 | 测试 |
| ☐ | E2E-02 | `workspace push`（首次） | 退出码 0；集群侧出现完整目录；PG `push_status=finished`；`.hfai/` 下无残留 zip | 测试 |
| ☐ | E2E-03 | `workspace diff`（推送后立即） | 输出「本地未上传/集群未下载/有差异」三组均为 `None` | 测试 |
| ☐ | E2E-04 | **增量 push** | 不改代码 → `push` 输出「数据已同步，忽略本次操作」且**上传字节为 0**；改 1 个文件 → 仅该文件被传（`--no_zip` 下可精确计数） | 测试 |
| ☐ | E2E-05 | `workspace list` | 表格含 7 列且状态/时间正确；当前工作区加粗 | 测试 |
| ☐ | E2E-06 | 提交任务并在集群看到代码 | `hai python train.py` 触发自动 push；任务 `cd` 到工作区路径；文件内容与本地一致（md5 校验） | 测试 |
| ☐ | E2E-07 | `workspace pull` | 集群新增文件后 `pull` → 本地出现该文件；`filemode` 与集群一致（`stat -c %a`） | 测试 |
| ☐ | E2E-08 | `workspace download <subpath>` | 仅下载指定子路径；其他文件不变 | 测试 |
| ☐ | E2E-09 | `workspace remove -f` / `workspace remove` | `-f` 仅删指定文件（本地不受影响）；无 `-f` 删整区 + 本地 `workspace.yml` 被删 + `list` 不再显示 | 测试 |
| ☐ | E2E-10 | 多用户隔离 | 用户 B 用 `name=A的工作区` 调 6 个接口（含 `delete_files`、两个 status）→ 全部 `FORBIDDEN`/空数据，且 B 的 `get_sync_status` 看不到 A 的记录 | 测试 |
| ☐ | E2E-11 | 中断恢复 | push 执行中 `kill -9` ugc-server → 重启 → 任务自动续跑；`status` 最终为 `finished`；集群文件完整 | 测试/运维 |
| ☐ | E2E-12 | 双 worker 不重复 | `ugc=2` 下重启，日志中恢复仅由**一个** worker 执行；同一 index 无重复传输（对比对象 PUT 次数/指标） | 测试/运维 |
| ☐ | E2E-13 | `--no_zip` 链路 | 逐文件落盘正确，`filemode` 恢复正确，无 zip 残留 | 测试 |
| ☐ | E2E-14 | `--no_diff` / `--force` 语义 | `--no_diff` 跳过集群遍历并全量覆盖；存在差异且无 `--force` 时中止并打印差异 | 测试 |
| ☐ | E2E-15 | `--no_checksum` / `--no_hfignore` | 仅比较 size；`.hfignore` 被忽略（被忽略文件被上传） | 测试 |
| ☐ | E2E-16 | 大文件与分片 | 单个 ≥ 1 GB 文件走分片；传输可中断续传（进度不回退） | 测试 |
| ☐ | E2E-17 | 特殊字符路径 | 目录名含空格、`&`、`#`、中文、emoji 时 push/pull 正确；`local_path` 在 DB 中的截断不导致接口失败（风险 R4） | 测试 |

---

## 5. 阶段 4 · 安全（SEC）

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | SEC-01 | 身份只来自 token（SEC-01） | 请求额外带 `username=victim&group=x&userid=1` 时**被忽略**，仍以 token 用户身份操作；无 token → 401 | 安全/后端 |
| ☐ | SEC-02 | STS 最小权限（SEC-02） | 用签发凭证尝试 `GetObject` 他人前缀 → `AccessDenied`；TTL > 43200 被夹取；凭证为 STS（含 `security_token`），无长期 AK | 安全 |
| ☐ | SEC-03 | 路径穿越（SEC-03） | `file_list` 含 `../`、`/etc/passwd`、`a/../../b` → `PATH_ESCAPE`；`delete_files` 同理；**符号链接**指向工作区外 → `sync_from_cluster` 拒绝该文件并记录 | 安全 |
| ☐ | SEC-04 | 越权 index（SEC-04） | 用户 B 用 A 的 `index` 查 status → 403；B 不能通过伪造 `name` 读到 A 的 `user_sync_status` | 安全 |
| ☐ | SEC-05 | 敏感信息（SEC-05） | 响应/日志/异常栈中搜索长期 AK/SK、`security_token`、`token=` 原文 → 无命中；审计日志保留操作人但不含凭证 | 安全 |
| ☐ | SEC-06 | 资源保护（SEC-06） | 10 001 文件、> 1 TiB、`size=100000` 分别被拒/截断；无超时或 OOM | 安全/后端 |
| ☐ | SEC-07 | 删除保护（SEC-07） | 空 `file_list` 删除整区时产生 INFO 审计日志（含操作人/来源 IP/工作区名） | 安全 |
| ☐ | SEC-08 | 审计留存（SEC-08） | 关键操作（签发 STS、删工作区、超配额拒绝）均有审计记录；轮转策略保证 ≥ 90 天 | 安全/运维 |
| ☐ | SEC-09 | 特性开关不可绕过 | `enabled=false` 时即使持有合法 token 也无法调用任何传输接口 | 后端 |
| ☐ | SEC-10 | 灰度白名单 | 非白名单用户 → `FEATURE_DISABLED`；白名单用户正常 | 后端 |

---

## 6. 阶段 5 · 性能与容量（PERF）

| 状态 | ID | 检查项 | 判定标准（引用 NFR） | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | PERF-01 | 接口时延 | 状态查询 P99 ≤ 100 ms；`get_sync_status` P99 ≤ 200 ms；`get_sts_token` P99 ≤ 1 s；submit P99 ≤ 500 ms（NFR-01） | 测试 |
| ☐ | PERF-02 | 目录遍历 | 10 万文件首次列目录 ≤ 30 s；缓存命中 ≤ 1 s（NFR-02） | 测试 |
| ☐ | PERF-03 | 端到端吞吐 | 10 000 文件 / 10 GB 工作区 push ≤ 10 min（内网 100 MB/s）；带宽利用率 ≥ 70%（NFR-02） | 测试 |
| ☐ | PERF-04 | 并发用户 | 20 用户并发 push/pull 无 5xx；进程池未打爆；内存无持续增长（NFR-03/10） | 测试 |
| ☐ | PERF-05 | 稳定性 | 连续 2 h 混合负载（push/pull/status 轮询）无内存泄漏、无 Redis 连接泄漏、无 fd 泄漏 | 测试 |

---

## 7. 阶段 6 · 可观测性与告警（OBS）

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | OBS-01 | 指标齐备 | 设计 §12.2 的指标全部可抓取；`cloud_storage_bucket_usage_size` 有数据（P1） | 运维 |
| ☐ | OBS-02 | 日志可检索 | 按 `index` 可检索到完整的任务生命周期日志；关键字段（user/name/file_type/bytes/cost/result）齐全 | 运维 |
| ☐ | OBS-03 | 告警规则 | OPS-04 的四条规则已配置并**演练触发**（失败率、积压、用量、DB 失败） | 运维 |
| ☐ | OBS-04 | Gauge 不悬挂（修 F9） | 注入提交期异常后 `cloud_storage_tasks_running` 能回落 | 后端 |

---

## 7.5 阶段 6.5 · 运维与合规（OPS）

| 状态 | ID | 检查项（需求） | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | OPS-01 | 灰度开关（OPS-01） | `enabled` / `enabled_users` / `enabled_groups` 生效；非白名单返回 `FEATURE_DISABLED`；开关变更流程（含重启窗口）写入运维手册 | 运维/后端 |
| ☐ | OPS-02 | 配置热更（OPS-02） | 修改 `[cloud.storage]` → 重启：已完成任务状态不丢（Redis/PG 保留）；重启后 `list` 状态正确 | 运维 |
| ☐ | OPS-03 | 回滚方案（OPS-03） | §11 的 RB-01~RB-04 已演练；回滚不产生脏数据（对象/PG 记录保留） | 运维 |
| ☐ | OPS-04 | 监控告警（OPS-04） | 四条规则已配置且演练触发：任务失败率 > 5%（5 min）、任务积压 > 100、bucket 用量 > 90%、`cloud_storage_db_failure_total` > 0 | 运维 |
| ☐ | OPS-05 | 日志规范（OPS-05） | 同步任务日志含 `index`，可按 index 检索完整生命周期；日志轮转保留 ≥ 7 天 | 运维 |
| ☐ | OPS-06 | 断点文件治理（OPS-06） | `breakpoint_info_path` 可写且按实例隔离；提供残留断点清理脚本并演练（清理不影响进行中任务） | 运维 |
| ☐ | OPS-07 | 变更窗口（OPS-07） | 本期 DDL = 0；若启用 P1 审计表，则在线 DDL 脚本、回滚脚本、锁影响评估齐备 | DBA/后端 |

---

## 8. 阶段 7 · 任务侧集成（TASK）

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | TASK-01 | `oss://` 解析（FR-15） | 提交 `spec.workspace=oss://<group>/<user>/workspaces/<name>` 的任务：pod 内 `pwd` 与 `MARSV2_TASK_WORKSPACE` 均为集群路径 | 后端/测试 |
| ☐ | TASK-02 | 越权与非法 workspace | 引用他人 group/user、`name` 含 `/`、scheme 非 `oss` → 任务创建失败并返回可读错误（不产生「已创建但立即失败」的任务） | 后端 |
| ☐ | TASK-03 | 未同步提示 | 工作区未 push 时提交 → 明确提示 `请先执行 hai-cli workspace push` | 后端 |
| ☐ | TASK-04 | 挂载正确（FR-16） | pod spec 中出现 `workspace-path` 挂载且 `host_path == mount_path == 集群路径`；无重复 mountPath；容器内可写（训练能写 checkpoint） | 后端/运维 |
| ☐ | TASK-05 | 本地路径回归（K-05） | 集群共享盘上的普通路径（非 URI）行为与改动前**完全一致**（对照组任务通过） | 回归测试 |
| ☐ | TASK-06 | 重启任务/挂起恢复 | 任务重启后工作区路径仍正确解析，不因挂载差异导致重启失败 | 测试 |

---

## 9. 阶段 8 · 兼容性与升级（CMP）

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | CMP-01 | 旧客户端全量回归 | 支持的最老客户端完成 E2E-01~E2E-09 全部通过（COMP-01/02） | 测试 |
| ☐ | CMP-02 | Body 三形态 | `text/plain` 外壳、`text/plain` 裸体、`application/json` 三种请求均 `success=1`（COMP-02） | 测试 |
| ☐ | CMP-03 | 独立部署回归 | `SERVER=cloud-storage` 下无前缀路由全量回归通过；`Page[FileInfo]` 响应模型未变（COMP-03） | 后端 |
| ☐ | CMP-04 | 双端语义一致 | 用同一份 `conf/utils.py` 计算 md5 / 打包 zip，两端结果逐字节一致（`md5sum` 对比；zip 内文件名与权限一致）（COMP-04） | 测试 |
| ☐ | CMP-05 | 响应只增不减 | 新增字段不影响老客户端解析（COMP-05） | 测试 |
| ☐ | CMP-06 | 关闭兼容开关演练 | `legacy_param_compat=false` 时旧客户端报 `INVALID_PARAM`、新客户端正常（用于验证升级完成度，不作为上线默认） | 后端 |

---

## 10. 阶段 9 · 发布与灰度（REL）

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | REL-01 | 变更清单与影响面 | 变更文件、配置项、DDL（本期应为 0）、回滚点全部列明；影响面含 ugc-server 其他接口 | 后端 |
| ☐ | REL-02 | 部署顺序 | ① 配置（`enabled=false`）→ ② 代码 → ③ 冒烟（其他接口）→ ④ 打开灰度 → ⑤ 全量。任一步失败即停 | 运维 |
| ☐ | REL-03 | 冒烟集通过 | 测试用例文档中的 **P0 冒烟集** 100% 通过（见用例文档回归矩阵） | 测试 |
| ☐ | REL-04 | 灰度观察窗 | 内部组灰度 ≥ 24 h：无 `INTERNAL_ERROR` 告警、失败率 < 1%、无 `cloud_storage_db_failure_total` 增长 | 运维 |
| ☐ | REL-05 | 全量前提 | 灰度期 E2E 全通过；bucket 用量与预期一致；无用户反馈阻塞问题 | PM/运维 |
| ☐ | REL-06 | 文档同步 | 用户文档（`hai-cli workspace` 用法、限额、FAQ）与运维手册（配置项、排障、清理脚本）已更新 | 文档 |
| ☐ | REL-07 | 通知与公告 | 变更公告含：功能范围、限额、已知限制（如不隐式清理孤儿文件）、求助渠道 | PM |

---

## 10.5 阶段 9.5 · 文档与交付物（DOC / DEP）

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | DOC-01 | 交付物四件套齐备且可追溯（NFR-07/08） | 需求/设计/用例/Checklist 四份文档齐备；交叉引用链接全部可解析；需求 → 用例 → Checklist 追溯无空洞 | 架构 |
| ☐ | DOC-02 | 接口契约与设计一致 | 附录 A 契约快照与实际实现逐字段一致（用自动化脚本比对 OpenAPI 与快照） | 后端 |
| ☐ | DEP-01 | 部署形态可复现（NFR-09） | 形态 A（ugc 内进程）与形态 B（cloud-storage 独立）各有一份可执行部署步骤/配置模板；不含环境专属硬编码 | 运维 |
| ☐ | DEP-02 | 依赖与资源清单 | 新增依赖（若有）、端口、目录、角色（RAM Role）、bucket、配额资源名全部登记；无新增中间件 | 架构/运维 |

---

## 11. 阶段 10 · 回滚演练（RB）

| 状态 | ID | 检查项 | 判定标准 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | RB-01 | 开关回滚 | `enabled=false` + 重启后：所有 `/ugc/*` 返回 `FEATURE_DISABLED`；其他接口正常；耗时 ≤ 5 min | 运维 |
| ☐ | RB-02 | 镜像回滚 | 回滚到上一版本后 ugc-server 正常启动；PG/Redis 无脏数据；已上传对象不影响任务运行（任务读集群路径） | 运维 |
| ☐ | RB-03 | 进行中任务的处置 | 回滚时正在传输的任务：状态可解释（`running` 悬挂或在旧版本中无对应接口）；清理手段已写入运维手册 | 运维/后端 |
| ☐ | RB-04 | 数据留存确认 | 回滚不删除 `user_sync_status`/`user_downloaded_files`/bucket 对象 | DBA |

---

## 12. 阶段 11 · 上线后观察（POST）

| 状态 | ID | 检查项 | 判定标准（观察期 7 天） | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | POST-01 | 关键指标趋势 | 接口成功率 ≥ 99.9%；同步任务失败率 < 5%；`bucket_usage` 增长与用户行为一致 | 运维 |
| ☐ | POST-02 | 任务创建成功率 | 含工作区的任务创建成功率与基线持平（无新增失败原因） | 运维 |
| ☐ | POST-03 | 用户反馈闭环 | 反馈问题分类归档；P0/P1 问题 48 h 内给出结论 | PM |
| ☐ | POST-04 | 遗留项跟踪 | 设计 §16 的 R1~R9、P1 需求（DEV-21/22）、客户端修复项（枚举、`quote()`、breakpoint 目录）建单跟踪 | PM/架构 |

---

## 13. 验收签署（ACC）

| 状态 | ID | 检查项 | 判定标准 | 签署 |
| --- | --- | --- | --- | --- |
| ☐ | ACC-01 | 功能验收 | 需求文档 §10「验收标准」8 条全部满足 | PM |
| ☐ | ACC-02 | 测试验收 | 用例文档 P0 100% 通过；P1 通过率 ≥ 90% 且未通过项有结论 | 测试 |
| ☐ | ACC-03 | 安全验收 | SEC-01~SEC-08 全部通过，无高危遗留 | 安全 |
| ☐ | ACC-04 | 运维验收 | 监控告警/回滚/排障手册齐备；容量与限额已确认 | 运维 |
| ☐ | ACC-05 | 文档验收 | 需求/设计/用例/Checklist 四件套齐备且相互追溯无空洞 | 架构 |

---

## 附录 A · 接口契约快照（GATE-03 冻结内容）

| ID | 方法 | 路径 | 必填查询参数 | Body | 响应关键字段 |
| --- | --- | --- | --- | --- | --- |
| API-01 | POST | `/ugc/get_sts_token` | `token,name,file_type,ttl_seconds` | — | `success, oss.{endpoint,access_key_id,access_key_secret,security_token,bucket}` |
| API-02 | POST | `/ugc/set_sync_status` | `token,file_type,name,direction,status,local_path,cluster_path` | — | `success` |
| API-03 | POST | `/ugc/get_sync_status` | `token,file_type,name` | — | `success, data[].{name,local_path,cluster_path,push_status,last_push,pull_status,last_pull}` |
| API-04 | POST | `/ugc/cloud/cluster_files/list` | `token,name,file_type,no_checksum,no_hfignore,recursive,page,size` | `{"file_list":{"files":[…]}}` | `items[].{path,size,last_modified,md5}, total, page, size, pages` |
| API-05 | POST | `/ugc/sync_to_cluster` | `token,name,file_type,no_zip` | `{"file_list":{"files":[…]}}` | `success, index, dst_path` |
| API-06 | GET | `/ugc/sync_to_cluster/status` | `token,index` | — | `success, status, msg` |
| API-07 | POST | `/ugc/sync_from_cluster` | `token,name,file_type` | `{"file_infos":{"files":[{…}]}}` | `success, index` |
| API-08 | GET | `/ugc/sync_from_cluster/status` | `token,index` | — | `success, status, msg` |
| API-09 | POST | `/ugc/delete_files` | `token,name,file_type` | `{"file_list":{"files":[…]}}` | `success` |

---

## 附录 B · 冒烟验证脚本（发布后 5 分钟自检）

```bash
# 变量
API=http://<ugc-server>:8083            # 或 haproxy 入口
TOKEN=<mars token>
NAME=demo_ws
FT=file_type=FileType.WORKSPACE         # 旧客户端形态（同时验证兼容层）

# 1) 读状态（预期 success=1，无记录时 data 为空数组）
curl -sX POST "$API/ugc/get_sync_status?token=$TOKEN&$FT&name=$NAME" | tee /tmp/ws1.json
python3 -c "import json,sys;d=json.load(open('/tmp/ws1.json'));assert d['success']==1 and 'data' in d, d"

# 2) 写状态（注意：必须带 success 字段；text/plain body 场景见第 5 步）
curl -sX POST "$API/ugc/set_sync_status?token=$TOKEN&$FT&name=$NAME&direction=SyncDirection.PUSH&status=SyncStatus.INIT&local_path=/tmp/$NAME&cluster_path=" | tee /tmp/ws2.json
python3 -c "import json;d=json.load(open('/tmp/ws2.json'));assert d['success']==1, d"

# 3) 再读状态（预期出现 1 条记录）
curl -sX POST "$API/ugc/get_sync_status?token=$TOKEN&$FT&name=$NAME" | python3 -c "import json,sys;d=json.load(sys.stdin);assert d['success']==1 and len(d['data'])>=1, d; print('rows:', len(d['data']))"

# 4) STS（预期含 oss 键与 5 个字段）
curl -sX POST "$API/ugc/get_sts_token?token=$TOKEN&$FT&name=$NAME&ttl_seconds=1800" | python3 -c "import json,sys;d=json.load(sys.stdin);assert d['success']==1 and {'endpoint','access_key_id','access_key_secret','security_token','bucket'} <= set(d['oss']), d; print('sts ok')"

# 5) 列目录（模拟客户端：Content-Type: text/plain + 外壳 body）
curl -sX POST "$API/ugc/cloud/cluster_files/list?token=$TOKEN&$FT&name=$NAME&no_checksum=false&no_hfignore=false&recursive=True&page=1&size=100" \
     -H 'Content-Type: text/plain; charset=utf-8' \
     --data '{"file_list": {"files": ["./"]}}' \
  | python3 -c "import json,sys;d=json.load(sys.stdin);assert set(d)>={'items','total'}, d; print('total:', d['total'])"

# 6) 不存在的 index（预期 400 且响应体仍含 success）
curl -s -o /tmp/ws6.json -w '%{http_code}\n' "$API/ugc/sync_to_cluster/status?token=$TOKEN&index=deadbeef"
python3 -c "import json;d=json.load(open('/tmp/ws6.json'));assert 'success' in d, d"

# 7) 越权自检（预期 401/403 且带 success）
curl -sX POST "$API/ugc/get_sync_status?token=bogus&$FT&name=$NAME" | python3 -c "import json,sys;d=json.load(sys.stdin);assert 'success' in d and d['success']==0, d; print('auth ok')"
```

---

## 附录 C · 排障速查（上线后常用）

| 症状 | 首查 | 处置 |
| --- | --- | --- |
| CLI 报「workspace未初始化」 | 本地 `.hfai/workspace.yml` 是否存在 | 重新 `workspace init` |
| CLI 报「没找到工作区」 | `get_sync_status` 是否返回空 | 检查兼容层与 `file_type` 归一化；必要时手工 `set_sync_status` 写一行 |
| push 卡在「开始同步到集群」 | `status` 接口的 `status`/`msg`；`index` 是否正确返回 | 查 worker 进程与 OSS 权限；`msg` 含错误原因 |
| push 成功但任务读不到代码 | `service.workspace_path` 配置、`ENV-03` 挂载巡检 | 修正配置或补挂载；重跑 push |
| 上传成功但服务端读不到对象 | 客户端 `workspace.yml` 的 `remote` 与服务端推导是否一致 | 重新 `init` 或对齐用户组（设计 §8.1 / R4） |
| 状态接口 400 `NOT_FOUND_INDEX` | 终态 TTL 是否过短、是否跨 pod 查询 | 提高 `status_ttl_finished`；确认请求打到同一集群 |
| 每次 push 都全量重传 | `cluster_files/list` 是否返回空（桩未替换） | 确认 `DEV-07` 已生效；检查 `total` |
| `cloud_storage_db_failure_total` 上升 | PG 连接/表/枚举类型 | 按 `ENV-01` 核对；检查 SQL 中的 `::file_type`/`::sync_status` 转型 |

---

## 补充映射（本表新增项 → 需求/设计）

| Checklist 项 | 对应需求/设计 |
| --- | --- |
| `DEV-01` 领域层抽离 | 设计 ADR-1、§5.1；NFR-07/09 |
| `DEV-03` 双宿主 | 设计 §13.1；COMP-03 |
| `DEV-23` 统一错误码 | 需求 `CON-3`；设计 §4.11、§6.4 |
| `ENV-01`~`ENV-06` | 需求 §9（依赖与假设）；设计 §9.1、§13.1 |
| `CFG-01`~`CFG-05` | FR-19、OPS-01/02/06；设计 §9 |
| `DB-01`~`DB-07` | [DB 审计](workspace-server-db-audit.md) §2/§4/§5/§7；CON-9；FR-02/07/08/17/18；用例 TC-DB-01~TC-DB-12 |
| `UT-01`~`UT-04` | NFR-07；设计 §5.6、§6.2/6.3、§10.1 |
| `E2E-01`~`E2E-17` | 需求 §10 验收标准；FR-01~FR-20；用例文档 §5 |
| `PERF-05` | NFR-04/10 |
| `OBS-04` | 分析报告 F9；设计 §12.2 |
| `TASK-01`~`TASK-06` | FR-15、FR-16；设计 §10 |
| `CMP-01`~`CMP-06` | COMP-01~COMP-06 |
| `REL-01`~`REL-07` | OPS-01/03/05；设计 §13.2 |
| `RB-01`~`RB-04` | OPS-03；设计 §13.3 |
| `POST-01`~`POST-04` | OPS-04；设计 §16 |
| `ACC-01`~`ACC-05` | 需求 §10；NFR-08 |
