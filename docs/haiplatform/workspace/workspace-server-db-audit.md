# 数据库表结构支撑性审计 · `hai-cli workspace`

| 项目 | 内容 |
| --- | --- |
| 审计对象 | `db_schemas/` 中与 workspace 同步相关的表结构，以及 `db/mars_db.py` 的访问层约束 |
| 审计问题 | 现有数据库表结构**能否支撑** workspace 的服务端实现？ |
| 方法 | 静态核对 DDL ↔ 客户端契约 ↔ 设计需求；用 Python 复现 SQLAlchemy 绑定参数正则；枚举标签自动比对；给出可直接执行的 psql 校验语句 |
| 关联文档 | [需求](workspace-server-requirements.md) · [设计](workspace-server-design.md) · [用例](workspace-server-test-cases.md) · [Checklist](workspace-server-checklist.md) |
| 结论口径 | ✅ 直接支撑 · ⚠️ 支撑但有前提 · ❌ 不支撑 |

---

## 0. 结论速览

> **能支撑，且 P0 阶段不需要任何 DDL。**

| 判定 | 内容 |
| --- | --- |
| ✅ **P0 功能闭环可支撑** | `user_sync_status`、`user_downloaded_files` 两张表已存在；`file_type` / `sync_status` 两个 PG 枚举与 Python 侧 `FileType` / `SyncStatus` **标签完全一致**（实测 6/6、9/9）；`quota`、`user`、`user_group`、`storage`、`user_all_groups` 物化视图等关联表齐备；`quota.resource` 是自由字符串，新增 `cloud_storage_quota` **不需要 DDL** |
| ✅ **客户端 7 字段契约 1:1 落位** | `hai workspace list` 需要的 `name/local_path/cluster_path/push_status/last_push/pull_status/last_pull` 全部有对应列，且有 `updated_at` 触发器保证排序稳定 |
| ✅ **幂等与并发有表级保障** | 两张表都有主键：`user_sync_status(user_name,file_type,name)` 直接支撑 `ON CONFLICT ... DO UPDATE`；`user_downloaded_files(file_path,file_md5)` 支撑记账 upsert |
| ⚠️ **3 条实现级硬约束（不遵守则 SQL 直接报错）** | ① 禁止 `%s::type`（绑定参数陷阱，已实测）；② 字面 `%` 必须写 `%%`；③ 参数只能传 tuple、枚举只能传 `.value`。详见 §4 |
| ⚠️ **6 个能力缺口（G1–G6）** | 任务级状态只在 Redis（G1）、无 group/provider 列（G2）、配额语义无法用现有列表达（G3）、push 方向用量 DB 不可见（G4）、缺索引（G5）、无审计表（G6）。**其中 G1/G3 必须在实现里显式定策略**（不改表也能解决），G2/G5/G6 属 P1 增强 |
| ❌ **无迁移机制** | `deploy/dbs/files/init_postgresql.sh:34-52` 的 DDL **只在空库执行**（`task_ng`/`user` 存在即跳过），仓库无 alembic。因此**任何新增 DDL 对既有库都不会自动生效**，必须走人工迁移（§7） |

**一句话结论**：表结构层面 P0 无需改库、可直接开工；真正的风险不在「缺表」，而在**配额语义未定义（G3）**与**DDL 无法自动迁移（§7）**这两件工程决策上。

---

## 1. 审计范围

| 类别 | 文件 | 关注点 |
| --- | --- | --- |
| 主表 | `db_schemas/010.table_user_downloaded_files.sql` | 枚举类型定义 + 记账表 |
| 主表 | `db_schemas/011.table_user_sync_status.sql` | 同步状态表（客户端 `list` 契约） |
| 关联表 | `000.table_user.sql`、`001.table_user_group.sql`、`002.table_quota.sql`、`003.table_storage.sql`、`016.table_external_quota_change_log.sql`、`020.materialized_view_user_all_groups.sql` | 用户/组/配额/挂载/审计/组视图 |
| 访问层 | `db/mars_db.py`（`execute`/`a_execute` 的 patch）、`db/mars_db.py:31,210-228` | 绑定参数与 SQL 文本约束 |
| 装载机制 | `deploy/dbs/files/init_postgresql.sh`、`one/entrypoint.sh:45`、`one/hai-up.sh:288-300` | DDL 何时、如何执行 |
| 代码引用 | 全仓 grep `user_sync_status` / `user_downloaded_files` | 是否有其它读写方 |

**方法学说明**：本次审计**未连接真实数据库**（环境中无 PG 实例），所有结论来自 DDL 文本、代码与本地可复现的正则/枚举比对；§9 附录给出一次性核对 SQL，可在联调环境执行以确认「线上库确实与 `db_schemas` 一致」（这一步已列入 Checklist `ENV-01`）。

---

## 2. 表结构逐项核对

### 2.1 `user_sync_status` ↔ 客户端契约

```sql
create table "user_sync_status" (
    "user_name" varchar(255) not null, "user_role" varchar(255) not null,
    "file_type" file_type not null,    "name" varchar(2047) not null,
    "pull_status" sync_status,         "last_pull" timestamp,
    "push_status" sync_status,         "last_push" timestamp,
    "local_path" varchar(2047) not null, "cluster_path" varchar(2047) not null,
    "updated_at" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp, "deleted_at" timestamp,
    constraint "pri-user_sync_status-user_name-file_type-name"
        primary key ("user_name", "file_type", "name")
);
create index "idx-user_sync_status-user_name" on "user_sync_status" ("user_name");
-- + trigger trigger_update_user_sync_status_updated_at (before update ⇒ updated_at = now())
```

| 客户端期望（`workspace_api.py:199-203` 的表格列） | 表列 | 判定 | 说明 |
| --- | --- | --- | --- |
| `name` | `name` | ✅ | 工作区名；客户端已禁止含 `/`（`workspace_api.py:25-28`） |
| `local_path` | `local_path` | ✅ | 客户端回传的本地绝对路径；`NOT NULL`，服务端内部调用传 `''` 即可 |
| `cluster_path` | `cluster_path` | ✅ | 服务端推导的集群路径 |
| `push_status` | `push_status` (`sync_status`) | ✅ | 客户端直接渲染字符串 |
| `last_push` | `last_push` | ✅ | 建议 `to_char(...,'YYYY-MM-DD HH24:MI:SS')` 输出，客户端原样打印 |
| `pull_status` | `pull_status` (`sync_status`) | ✅ | — |
| `last_pull` | `last_pull` | ✅ | — |
| （排序稳定） | `updated_at` + 触发器 | ✅ | 设计 §4.3 要求 `ORDER BY updated_at DESC`，触发器保证每次写入都刷新 |
| （`remove` 后不再出现） | `deleted_at` | ✅ | 软删后加 `deleted_at is null` 过滤；`init` 重新 upsert 时置回 `null` |
| （按 `file_type` 过滤） | `file_type` (`file_type`) | ✅ | PK 第二列，`name='*'` 时走 PK 前缀 |
| （写前需校验用户） | `user_name` + `user_role` | ✅ | `user_role` 为 `varchar(255)`（**不是** `user_role` 枚举），可安全写入 `'internal'/'external'`（`conf/flags/default.py:2-6` 为普通字符串常量） |

**幂等能力**：PK `(user_name, file_type, name)` 正是设计 §7.2 `ON CONFLICT (...) DO UPDATE` 的冲突目标 ✅；`COALESCE(NULLIF(excluded.local_path,''), old)` 可用于「空值不覆盖旧值」。

### 2.2 `user_downloaded_files` ↔ 配额与记账

| 列 | 类型 | 用途核对 | 判定 |
| --- | --- | --- | --- |
| `user_name` / `user_role` | varchar | 归属与配额统计维度 | ✅ |
| `file_type` | `file_type` | 区分 workspace/env/... | ✅ |
| `file_path` | varchar(2047) | **实际写入的是「集群绝对路径」**（`cloud_storage/api.py:741` 传 `filename=src_files[i]`） | ✅（注意不是相对路径） |
| `file_size` | bigint | 配额求和 | ✅ |
| `file_mtime` | varchar(255) | 来自 `FileInfo.last_modified`（字符串） | ✅（类型不优雅但一致） |
| `file_md5` | varchar(255) | 与 PK 组成冲突键、跳过重复上传 | ✅ |
| `status` | `sync_status` | `running → finished/failed` 记账 | ✅ |
| `deleted_at` | timestamp | 可用于排除已被取代的记录（但当前无代码维护，见 G3） | ⚠️ |
| PK | `("file_path","file_md5")` | upsert 冲突键 | ✅（但见下方「命名不一致」） |

**命名不一致（低危，建议修）**：约束名写作 `pri-user_downloaded_files-file_path-file_mtime`，实际定义却是 `primary key ("file_path","file_md5")`。若实现里按约束名做异常匹配或写文档，会误判。

### 2.3 枚举类型对齐（实测）

| PG 类型（定义于 `010` 文件顶部） | PG 标签 | Python 侧 | 比对结果 |
| --- | --- | --- | --- |
| `file_type` | `dataset, doc, env, pypi, website, workspace` | `conf/utils.py:22-34` `FileType` | ✅ **6/6 完全一致，无差集** |
| `sync_status` | `failed, finished, init, running, stage1_failed, stage1_finished, stage1_running, stage2_failed, stage2_running` | `conf/utils.py:48-64` `SyncStatus` | ✅ **9/9 完全一致，无差集** |
| （Redis 过程态） | — | `cloud_storage/utils.py:85-92` `SyncPhase{init,running,finished,failed}` | ✅ 是 `sync_status` 的子集，写入合法 |

> 复现命令见 §9.2。这意味着设计 §7.3 的状态映射表可以在**不新增枚举值**的前提下落地，`stage1_*/stage2_*` 的四阶段语义与客户端 `SyncStatus` 完全吻合。

### 2.4 关联表

| 表/视图 | 相关列 | 用途 | 判定 |
| --- | --- | --- | --- |
| `user` | `user_name`(uniq)、`token`(uniq)、`role`、`active`、`shared_group` | token→用户；`shared_group` 是工作区集群路径的一段（`get_base_path` 的 `group`） | ✅ |
| `user_group` + `user_all_groups`(物化视图) | `user_groups` 数组 | `get_ugc_user` 与灰度白名单 `enabled_groups` 用 `user.in_any_group` | ✅（视图由 `user`/`user_group` 触发器自动 refresh） |
| `quota` | PK `(user_name, resource)`，`quota bigint`，`expire_time` | 存 `cloud_storage_quota`；`QuotaTable` 是 `AutoSqlTable` 且**自动过滤过期行**（`table_config.py:44-55`） | ✅ **无需 DDL**（`resource` 无枚举/CHECK 约束） |
| `external_quota_change_log` | `editor/external_user/resource/original_quota/quota/expire_time` | 外部用户配额变更留痕，可复用于 `cloud_storage_quota` | ✅ |
| `storage` | `host_path/mount_path/mount_type/read_only/action/owners/conditions` | FR-16 挂载去重（判断工作区路径是否已被既有挂载覆盖） | ✅ |

**📌 一个重要推论（FR-18/FR-17）**：DB 侧不需要任何变更，但**代码侧必须补一个配额访问器**——本仓库 `UserQuota` 只有扁平的 `quota(resource)->int`（`server_model/user_impl/user_quota/implement.py:95-99`），而 `cloud_storage/api.py:467` 用的是 `user.quota.cloud_storage_quota.download`。全仓 grep `cloud_storage_quota` 只有那 1 处调用 + 1 个桩（`api/resource/cloud_storage/default.py:8`），说明这个嵌套访问器原本由私有 `custom.py` 提供。**在开源实现里需要新增 `UserQuotaExtras.cloud_storage_quota`（例如映射 resource `cloud_storage_quota:download`，单位 MB）**，并同步 `api/user/admin` 的设置入口。

---

## 3. 需求 → DB 支撑度矩阵

| 需求 | 依赖的表/存储 | 支撑度 | 说明 |
| --- | --- | --- | --- |
| FR-01 STS 签发 | `user`（shared_group/role）+ 配置 | ✅ | 不写库 |
| FR-02 写同步状态 | `user_sync_status` | ✅ | upsert，见 §2.1 |
| FR-03 查同步状态 | `user_sync_status` | ✅ | `name='*'` 走 PK 前缀 |
| FR-04 列集群文件 | 文件系统 + Redis 缓存 | ✅ | 不涉及 PG |
| FR-05 bucket→集群 | PG 只写状态；任务态在 Redis | ⚠️ G1 | 功能可跑；任务历史不可查 |
| FR-06 查同步进度 | Redis | ⚠️ G1 | DB 无 index→status 映射 |
| FR-07 集群→bucket | `user_downloaded_files` | ⚠️ G3 | 记账可写；**配额语义待定** |
| FR-08 删除工作区 | `user_sync_status.deleted_at` | ✅ | 软删可回滚 |
| FR-09 tagging | 对象存储 | ✅ | DB 不参与（`filemode/expire_at/md5` 在 OSS tagging） |
| FR-10 zip 分发 | 文件系统 | ✅ | — |
| FR-11 断点续传 | 文件系统 + OSS tagging | ✅ | — |
| FR-12 状态机双写 | 两表 + Redis | ✅ | 枚举值已对齐（§2.3） |
| FR-13 崩溃恢复 | Redis `param:{instance}` | ⚠️ G1 | PG 无「未完成任务」概念；恢复完全依赖 Redis 存活 |
| FR-14 参数兼容层 | — | ✅ | 不涉及 DB |
| FR-15 `oss://` 解析 | 配置 + `storage` | ✅ | — |
| FR-16 工作区挂载 | `storage` | ✅ | 用 `mount_path` 去重 |
| FR-17 配额与限额 | `quota` + `user_downloaded_files` | ⚠️ G3/G4 | pull 方向可判定；**push 方向 DB 不可见** |
| FR-18 管理配额接口 | `quota` + `external_quota_change_log` | ✅（需补代码） | 无 DDL，见 §2.4 推论 |
| FR-19 配置自检 | — | ✅ | 不涉及 DB |
| FR-20 进度上报 | Redis | ✅ | — |
| FR-21 审计与回收 | ❌ 无表 | ❌ G6 | 需 P1 新表或走日志系统 |

---

## 4. 三条实现级硬约束（必须写入代码规范）

`db/mars_db.py` **patch 了 SQLAlchemy 的 `Connection.execute`**：

```python
# db/mars_db.py:31
sql_params = sqlparams.SQLParams(in_style='format', out_style='named')

# db/mars_db.py:210-228（节选）
def __execute(self, statement, *multiparams, **params):
    if isinstance(statement, sqlalchemy.sql.elements.TextClause):
        return self._execute(statement, *multiparams, **params)
    sql, params = sql_params.format(statement, multiparams[0]) if len(multiparams) > 0 else (statement, ())
    return self._execute(sqlalchemy.text(sql), params)
```

即：**`%s` → `:pN`（sqlparams）→ `sqlalchemy.text()` 再解析绑定参数**。由此产生三条约束：

| # | 约束 | 反例与后果 | 正确写法 | 证据 |
| --- | --- | --- | --- | --- |
| **C1** | **禁止 `%s::type`**（参数后紧跟 `::`） | `values (..., %s::file_type, ...)` → 变成 `:p1::file_type` → SQLAlchemy 绑定正则 `(?<![:\w\x5c]):(\w+)(?!:)` 因 `(?!:)` 回溯，只能匹配出 **`:p`**，SQL 被改写成 `:p` + `1::file_type` → 报错或语义错乱 | ① 直接传 `%s`，让 PG 按目标列/比较上下文推断类型；② 确需转换写 `CAST(%s AS file_type)`；③ 不要依赖 `%s ::type`（加空格）这种脆弱写法 | 本地实测（§9.2）：对 `':p1::file_type'` 该正则返回 `['p']`，对 `':p1 ::file_type'` 返回 `['p1']` |
| **C2** | **字面 `%` 必须写 `%%`**（当 SQL 带参数时会走 sqlparams 的 format 风格） | `where "name" like 'abc%'` → sqlparams 把 `%` 当占位符 → 报错或参数错位 | `like 'abc%%'`；本项目用到的 `to_char("last_push",'YYYY-MM-DD HH24:MI:SS')` **无 `%`，安全** | `in_style='format'`（`db/mars_db.py:31`）；注意 `sql_params.format()` 仅在 `parameters` 为真时执行（`:221`），**无参数 SQL 不受此约束** |
| **C3** | **参数只能传 tuple；枚举只能传 `.value`** | 传 dict → sqlparams 报错；带 `%s` 却传空 tuple → `text()` 原样下发 → PG 语法错误；传 `SyncStatus.INIT` 这类 `(str,Enum)` 成员会得到 `SyncStatus.INIT` 字面量（与分析报告 F2 同源） | `await MarsDB().a_execute(sql, (a, b, c))`；一律 `status.value` / `file_type.value` | `db/mars_db.py:148`；`conf/utils.py:48-64` 未覆写 `__str__` |

**连带提醒（同一类陷阱）**：`sqlalchemy.text()` 会把「前面不是单词字符的 `:word`」当绑定参数。因此字符串字面量里写 `':download'`（冒号紧跟引号）会被误判；而 `'cloud_storage_quota:download'`（冒号前面是单词字符）是安全的。配额 resource 名建议统一走参数传递，不要拼进 SQL 文本。

---

## 5. 能力缺口清单

| ID | 缺口 | 影响 | 需要 DDL？ | 处置建议 |
| --- | --- | --- | --- | --- |
| **G1** | **没有「同步任务」表**：`index → 状态/进度/参数快照` 全在 Redis（设计 §7.1） | ① Redis 抖动/清空/终态 TTL 到期后，任务历史与幂等依据丢失；② 无法回答「我的同步任务」「昨天失败了多少」；③ 崩溃恢复完全依赖 Redis 存活 | 否（P0 可接受） | P0：Redis 终态 TTL 1800s（设计 ADR-6）+ 恢复心跳（ADR-5）；状态接口对未知 index 返回 `NOT_FOUND_INDEX`（已在契约里）。P1：建 `cloud_storage_sync_log`（§6.1） |
| **G2** | `user_sync_status` **无 `group_name` / `provider` / `remote_path` 列** | 用户 `shared_group` 变更后：DB 里的 `cluster_path` 变陈旧、客户端 `workspace.yml` 里的 OSS 前缀漂移**无法从 DB 检出**（分析报告 F4 / 设计 R4）；跨 provider、跨集群同名工作区无法区分 | 否（P0 靠重新推导） | P0：**所有路径每次由 token 重新推导**，DB 字段仅作展示（已在设计 §8.1）；OSS 侧因前缀不匹配会天然拒绝写入，形成兜底。P1：加列（§6.2） |
| **G3** | **配额语义无法用现有列表达**：`user_downloaded_files` 是 `(file_path,file_md5)` 追加型日志——同一路径改内容会产生**新行**，旧行仍是 `finished`，`sum(file_size)` **只增不减** | 若直接按「全部 finished 行求和」判定配额，用户一旦产出过几版 checkpoint 就会被**永久限流**（请求 403，且无法自愈） | 否（改代码即可） | 必须在实现里**显式定义**（建议组合）：① **判定用量**＝当日累计 `where created_at >= date_trunc('day', now())`，贴合 `cloud_storage/api.py:451` 注释「限制每天上传容量」的原义；② **展示占用**＝按 `file_path` 取最新 `created_at` 去重后求和（或写入新行时把同 path 的旧行置 `deleted_at`，实现 supersede）。二者都无需 DDL，但**需要产品确认口径**（设计 §16 开放问题 2） |
| **G4** | **push 方向（客户端直传 OSS）用量对 DB 不可见** | `user_downloaded_files` 只记录「集群→OSS」；客户端→OSS 的对象只有 OSS tagging 有元数据。因此：单用户在**上传（push）**方向的容量无法由 DB 判定；`BUCKET_USAGE_SIZE` 只能由审计任务 `list_bucket` 得出 | 否（P1 可选） | P0：push 方向不设配额（客户端本地文件大小本身受本地盘约束）；`BUCKET_USAGE_SIZE` 走 §11 审计（list bucket 聚合）。P1：若要按工作区限制 push 容量，需新增按 `{group}/{user}/workspaces/{name}` 前缀的用量表或依赖 OSS 侧配额 |
| **G5** | **缺支撑聚合的索引**：`user_downloaded_files` 只有 `(user_name)` 单列索引；无 `status`/`created_at` 索引；`file_type` 无索引 | 百万行级时「当日用量」「按类型用量」「审计全表聚合」会退化为大量堆扫描 | 是（P1） | 加 `create index ... on "user_downloaded_files"("user_name","status","created_at" desc)`（§6.3） |
| **G6** | **无审计/操作日志表** | SEC-08 要求关键操作（删整区、签发 STS、超配额拒绝）留痕 ≥ 90 天；现有 `external_quota_change_log` 只覆盖外部配额变更 | 是（P1） | P0：先用结构化日志（loguru，设计 §12.3）+ 日志系统留存；P1：建 `cloud_storage_audit_log` 或接入平台统一审计 |

**另附低危「schema smell」（不阻塞，建议登记）**

| # | 现象 | 建议 |
| --- | --- | --- |
| S1 | `user_downloaded_files` 主键约束名与实际定义不一致（名里是 `file_mtime`，定义是 `file_md5`） | 文档/评审中不要引用约束名；P1 顺带 `rename constraint` |
| S2 | `idx-user_sync_status-user_name` 与主键前缀 `(user_name,...)` 重复 | 可保留（无害）或 P1 删除以省写放大 |
| S3 | `create type ... as enum` 无 `IF NOT EXISTS` | 受「DDL 只在空库执行」保护；但**人工迁移脚本严禁重复执行**，必须包在幂等判断里（§7.3） |
| S4 | `user_sync_status` 无 `updated_at` 索引 | 单用户工作区数量级很小（<10³），可接受 |
| S5 | 两张表在开源仓中**只被 DDL 引用，无任何 Python 代码读写** | 与私有实现被裁剪的结论一致；实现时需确认线上库的表结构与数据现状（§9.1） |

---

## 6. DDL 变更建议（P1，含迁移与回滚）

> 下列变更**均非 P0 必需**。若采纳，必须配套 §7 的人工迁移流程。

### 6.1 同步任务表（对应 G1）

```sql
-- db_schemas/035.table_cloud_storage_sync_log.sql
create table "cloud_storage_sync_log" (
    "index"        varchar(64)   not null,          -- sha256(token+name+file_type+*files)
    "user_name"    varchar(255)  not null,
    "user_role"    varchar(255)  not null,
    "file_type"    file_type     not null,
    "name"         varchar(2047) not null,
    "direction"    varchar(16)   not null,          -- push(bucket→集群) / pull(集群→bucket)
    "status"       sync_status   not null,
    "file_count"   integer       not null default 0,
    "total_bytes"  bigint        not null default 0,
    "synced_bytes" bigint        not null default 0,
    "failed_files" integer       not null default 0,
    "error_msg"    text,
    "instance"     varchar(255),                    -- 认领该任务的实例（与 Redis param 对齐）
    "duration_ms"  integer,
    "created_at"   timestamp     not null default current_timestamp,
    "updated_at"   timestamp     not null default current_timestamp,
    "finished_at"  timestamp,
    constraint "pri-cloud_storage_sync_log-index" primary key ("index")
);
create index "idx-cloud_storage_sync_log-user"   on "cloud_storage_sync_log" ("user_name", "created_at" desc);
create index "idx-cloud_storage_sync_log_active" on "cloud_storage_sync_log" ("status") where "status" in ('init','running');
comment on table "cloud_storage_sync_log" is '云存储同步任务（workspace/env 文件同步）执行记录';
```

收益：任务可查/可审计/可做「未完成任务」兜底恢复；Redis 丢失后仍能定位 `running` 悬挂任务。

### 6.2 工作区归属与远端标识（对应 G2）

```sql
alter table "user_sync_status" add column if not exists "group_name"  varchar(255);
alter table "user_sync_status" add column if not exists "provider"    varchar(64);
alter table "user_sync_status" add column if not exists "remote_path" varchar(2047);

-- 回填（幂等，可重复执行）
update "user_sync_status" s
   set "group_name" = u."shared_group"
  from "user" u
 where u."user_name" = s."user_name" and s."group_name" is null;
```

收益：可在 `init` 时校验客户端 `remote` 与服务端推导是否一致并显式报错（消除 F4 的「静默错传」）；`list` 能显示归属组。

### 6.3 聚合索引（对应 G5）

```sql
create index if not exists "idx-user_downloaded_files-user_status_time"
    on "user_downloaded_files" ("user_name", "status", "created_at" desc);
create index if not exists "idx-user_downloaded_files-file_type"
    on "user_downloaded_files" ("file_type") where "deleted_at" is null;
```

### 6.4 回滚脚本（必须与上一个变更一一对应）

```sql
-- rollback for 6.1
drop table if exists "cloud_storage_sync_log";
-- rollback for 6.2
alter table "user_sync_status" drop column if exists "group_name";
alter table "user_sync_status" drop column if exists "provider";
alter table "user_sync_status" drop column if exists "remote_path";
-- rollback for 6.3
drop index if exists "idx-user_downloaded_files-user_status_time";
drop index if exists "idx-user_downloaded_files-file_type";
```

---

## 7. 迁移机制现状与风险（**本次审计最重要的工程结论**）

### 7.1 现状

| 事实 | 证据 |
| --- | --- |
| DDL 由 `init_postgresql.sh` 汇总成一个事务执行 | `deploy/dbs/files/init_postgresql.sh:16-29`：`find db_schemas/ -name '*.sql' \| sort` 后拼进 `/tmp/fuse_sql.sql` 并 `BEGIN/COMMIT` |
| **只在「空库」执行**：只要有 `task_ng` 或 `user` 之一查询失败就重试执行；两者都存在则**整个循环不执行** | `deploy/dbs/files/init_postgresql.sh:34-52`：`while (select id from task_ng 失败) \|\| (select user_name from user 失败); do psql -f fused_sql; done` |
| 容器每次启动都会调用该脚本，但通常不会真正执行 DDL | `one/entrypoint.sh:45` |
| 部署可追加自有 SQL，但同样只在首次初始化生效 | `INIT_SQL=/tmp/init.sql`（`one/entrypoint.sh:45`），内容由 `one/hai-up.sh:288-300` 的 `init.sql.in` 生成 |
| 仓库内**没有** alembic / 迁移框架 / schema 版本表 | `grep -rin "alembic\|migrat" requirements.txt db/ deploy/ one/` 无结果 |
| 两个 workspace 表**没有任何 Python 代码读写**（只有 DDL） | 全仓 `grep -rn "user_sync_status\|user_downloaded_files" --include=*.py` → 无命中 |

### 7.2 风险

1. **`db_schemas/` 新增文件对既有生产库无效**。若按常规提交 `035.table_...sql` 并期望自动生效 → 环境间结构不一致，代码在旧库上报 `relation does not exist`。
2. **线上库可能与 `db_schemas` 漂移**（私有层历史上可能手工加过列/索引）。因此 §6 的 `alter ... add column if not exists` 与「先查后改」是必须的。
3. 第 1 条同样影响**本期 P0**：如果某环境的库建于 `010/011` 文件存在之前，两张表可能缺失 → 需先用 §9.1 的 SQL 确认。

### 7.3 处置（写入 Checklist）

| 步骤 | 内容 |
| --- | --- |
| DB-01 | 用 §9.1 的 SQL 核对线上库：两张表、两个枚举、触发器、索引是否与 `db_schemas` 一致；不一致处登记 |
| DB-02 | P0 上线**不做任何 DDL**；仅在 Checklist 记录「已确认无需变更」 |
| DB-03 | 若采纳 P1 DDL：新增 `deploy/dbs/migrations/<date>-<name>.sql`（**幂等**写法，全部 `if not exists`），与回滚脚本成对提交 |
| DB-04 | 迁移由 DBA 人工执行并留痕（维护一张「已执行迁移」记录表或变更单），因为平台无自动迁移 |
| DB-05 | 迁移演练：在预发库执行 → 验证 → 回滚 → 再执行，确认幂等与回滚可用 |

---

## 8. DB 层验收用例（**已并入**《[功能测试用例](workspace-server-test-cases.md)》§4.13，编号 TC-DB-01~TC-DB-12）

| ID | 优先级 | 前置 | 步骤 | 预期结果 | 覆盖 |
| --- | --- | --- | --- | --- | --- |
| TC-DB-01 | P0 | 已连库 | 执行 §9.1 核对 SQL | 两张表存在；`file_type` 6 值、`sync_status` 9 值与 §2.3 一致；两表 `updated_at` 触发器存在 | ENV-01、DB-01 |
| TC-DB-02 | P0 | 无 `demo` 行 | `set_sync_status(push, init, local, cluster)` 后查行 | 新增 1 行；`push_status='init'`、`last_push` 非空、`local_path/cluster_path` 与入参一致；`updated_at = created_at` | FR-02 |
| TC-DB-03 | P0 | 已有行 | 再以 `local_path=''`、`cluster_path=<新值>` 调 `set_sync_status(pull, stage2_running)` | **同一行**：`pull_status` 更新、`last_pull` 刷新、`push_status` 不变、`local_path` **保持旧值**、`cluster_path` 更新 | FR-02、NFR-05 |
| TC-DB-04 | P0 | 上传 1 个文件 | 查 `user_downloaded_files` | 出现 1 行，`file_path` 为**集群绝对路径**、`file_size` 与文件一致、`status='finished'`、`file_md5` 与对象 tagging 一致 | FR-07 |
| TC-DB-05 | P0 | 同路径改内容后再 pull | 再次查表 | 出现**第 2 行**（md5 不同）；→ 记录 G3 语义：此时两种口径的用量分别为「2×size（累计）」与「1×size（去重）」，实现必须与产品口径一致 | G3、FR-17 |
| TC-DB-06 | P1 | — | 连续 2 次相同参数写状态 | 行数不增；`updated_at` 变更；无唯一键冲突异常 | FR-02 |
| TC-DB-07 | P1 | — | 用 `%s::file_type` 写法执行一次查询 | **必须失败**（复现 C1），证明代码规范生效；改用 `%s` 后通过 | §4 C1 |
| TC-DB-08 | P1 | — | 执行含 `like 'abc%'` 且带参数的 SQL | 必须失败（复现 C2）；写成 `'abc%%'` 后通过 | §4 C2 |
| TC-DB-09 | P1 | 写入 `name` 为 2048 字符 | 调用 `set_sync_status` | 服务端截断至 2047 后成功写入；不出现 `value too long for type character varying(2047)` | 设计 §7.2 |
| TC-DB-10 | P1 | — | 对不存在的用户写状态 | 失败并返回可读错误（`AioUserSelector` 先校验），DB 无脏行 | SEC-01 |
| TC-DB-11 | P1 | 有 `deleted_at` 非空行 | `get_sync_status(name='*')` | 该行不返回 | FR-08 |
| TC-DB-12 | P1 | P1 DDL 已应用 | 执行 §6 迁移脚本 → 回滚脚本 → 再执行 | 三次均成功（幂等）；列/索引状态符合预期 | DB-03~05 |

---

## 9. 附录

### 9.1 一次性核对 SQL（在联调/生产只读执行）

```sql
-- ① 表与列
select table_name, column_name, data_type, is_nullable, character_maximum_length
from information_schema.columns
where table_schema = 'public' and table_name in ('user_sync_status','user_downloaded_files')
order by table_name, ordinal_position;

-- ② 枚举标签（应为 6 / 9 个）
select t.typname, string_agg(e.enumlabel, ',' order by e.enumsortorder) as labels
from pg_type t join pg_enum e on t.oid = e.enumtypid
where t.typname in ('file_type','sync_status') group by 1;

-- ③ 主键与索引
select c.relname as table_name, i.relname as index_name, pg_get_indexdef(x.indexrelid) as def
from pg_index x join pg_class c on c.oid = x.indrelid join pg_class i on i.oid = x.indexrelid
where c.relname in ('user_sync_status','user_downloaded_files');

-- ④ updated_at 触发器
select event_object_table, trigger_name, action_timing, event_manipulation
from information_schema.triggers
where event_object_table in ('user_sync_status','user_downloaded_files');

-- ⑤ 现状体量（评估配额口径与索引必要性）
select user_name, count(*) rows, pg_size_pretty(sum(file_size)) total_bytes,
       count(*) filter (where status = 'finished') finished_rows
from user_downloaded_files where deleted_at is null group by 1 order by rows desc limit 20;

-- ⑥ 是否存在同名工作区跨组的历史脏数据（G2 排查）
select user_name, file_type, name, cluster_path, local_path, push_status, pull_status
from user_sync_status where deleted_at is null order by updated_at desc limit 50;
```

### 9.2 审计用的复现命令

```bash
# ① 绑定参数陷阱（C1）：SQLAlchemy 1.4 的绑定参数正则行为
python3 - <<'PY'
import re
rx = re.compile(r"(?<![:\w\x5c]):(\w+)(?!:)", re.UNICODE)   # sqlalchemy/sql/elements.py
for s in [':p1::file_type', ':p1 ::file_type', 'CAST(:p1 AS file_type)', ':p1::sync_status']:
    print(repr(s), '->', rx.findall(s))
PY
# 实测输出：':p1::file_type' -> ['p']   ':p1 ::file_type' -> ['p1']   'CAST(:p1 AS file_type)' -> ['p1']

# ② 枚举对齐（§2.3）
python3 - <<'PY'
import re, io
pg = io.open('db_schemas/010.table_user_downloaded_files.sql', encoding='utf-8').read()
lab = lambda s: [x.strip().strip("'") for x in s.split(',')]
pg_sync = lab(re.search(r"create type sync_status as enum \(([^)]*)\)", pg).group(1))
pg_ft   = lab(re.search(r"create type file_type as enum \(([^)]*)\)", pg).group(1))
pu = io.open('conf/utils.py', encoding='utf-8').read()
py_sync = re.findall(r"=\s*'([a-z0-9_]+)'", pu.split('class SyncStatus')[1].split('class SyncDirection')[0])
py_ft   = re.findall(r"=\s*'([a-z0-9_]+)'", pu.split('class FileType')[1].split('class FilePrivacy')[0])
print('sync 差集:', set(pg_sync) ^ set(py_sync) or '无')
print('file_type 差集:', set(pg_ft) ^ set(py_ft) or '无')
PY

# ③ 是否已有代码读写这两张表（判断是否孤儿表）
grep -rn "user_sync_status\|user_downloaded_files" --include="*.py" . | grep -v docs || echo "无 Python 引用（仅 DDL）"

# ④ DDL 装载时机
sed -n '34,52p' deploy/dbs/files/init_postgresql.sh
sed -n '45p'    one/entrypoint.sh
```

### 9.3 参考证据索引

| 主题 | 位置 |
| --- | --- |
| 记账表 + 枚举定义 | `db_schemas/010.table_user_downloaded_files.sql:1-3,4-18,32-39` |
| 同步状态表 | `db_schemas/011.table_user_sync_status.sql:1-21,24-31` |
| 用户/组/配额/挂载 | `db_schemas/000.table_user.sql:1-17`、`001`、`002.table_quota.sql:1-8`、`003.table_storage.sql:1-10` |
| 组视图与触发器 | `db_schemas/020.materialized_view_user_all_groups.sql:1-22` |
| DB 访问层 patch | `db/mars_db.py:31,140-182,210-228` |
| DDL 装载 | `deploy/dbs/files/init_postgresql.sh:16-52`、`one/entrypoint.sh:45`、`one/hai-up.sh:288-300` |
| 配额访问器缺失 | `cloud_storage/api.py:452-475`、`server_model/user_impl/user_quota/implement.py:95-99`、`api/resource/cloud_storage/default.py:8-9` |
| 表注册（roaming） | `server_model/user_data/table_config.py:30-56`（两表**未**注册，设计走直连 SQL，读写即时一致） |
| 客户端契约列 | `plugins/haiworkspace/haiworkspace/client/workspace_api.py:199-203` |
