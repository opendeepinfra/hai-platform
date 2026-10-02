# hai-cli images 服务端实现需求说明

> **文档定位**：三件套之二（分析 → **需求** → 设计）。本文只回答「**要做什么、做到什么程度算完成**」，
> 不回答「怎么实现」（见设计文档）。所有需求均以 [hai-cli-images-analysis.md](hai-cli-images-analysis.md)
> 的实测/代码证据为依据，需求 ID 与设计章节、用例 ID、Checklist ID 相互可追溯。
>
> **前置阅读**：[hai-cli-images-analysis.md](hai-cli-images-analysis.md)（§1 结论速览 · §4.6 运行面 · §6 风险 I1–I18）。
>
> **编号约定（务必先读，避免与既有文档混淆）**：
> - **接口号跨特性续编**：`workspace` 用到 API-01~API-12，`env` 用到 API-11（修订）/API-13/API-14。
>   本特性从 **API-15** 起编号，且**刻意与 `api/resource/image/default.py` 的 4 个既有桩同名同序**
>   （`load` → `update_status` → `list` → `delete`），便于评审时一一对应。
> - **需求号本特性独立编号**：`G1–` / `FR-` / `NFR-` / `SEC-` / `OPS-` / `CMP-` / `HC-` / `AC-` / `Q-`，
>   与 `workspace`、`env` 的文档**不共享编号**。
> - **命名空间互不冲突**：`C-3` = 审计（[../hai-cli-client-server-audit.md](../hai-cli-client-server-audit.md) §3.4）的客户端缺陷 ID，
>   本文**沿用不改号**；`I1–I18` = 本特性分析报告的风险 ID；`TC-*` = 用例 ID；`ACC-*` = Checklist 签署项
>   （**与需求的 `AC-*` 是两套，勿混用**）。
>
> **追溯**：需求 → 接口/落点 → 设计章节 → 验收，见 §11。

---

## 1. 背景与目标

### 1.1 现状（摘自分析报告，逐条带证据）

1. **控制面半成品**：客户端 `images list` 可用，但服务端 `user_images` **硬编码 `[]`**（I2）；
   `images load` / `images delete` 在客户端即抛 `AttributeError`（审计 **C-3** / I1），
   服务端**连路由都没有**（实测 404，I3）；`train_image` 表**零行、零写入路径**（I4）。
2. **提交面完整**：任务提交期的自定义镜像校验逻辑齐备，并**反向定义了 5 条硬契约 K1–K5**
   （三段 URL、逐字节拼接、`status='loaded'`、按 `shared_group` 隔离、报错指引用户跑 `images list`）。
3. **运行面结构完整但缺关键脚本（本特性的真正难点）**：launcher 会查 `train_image` 并把 `path` 作为
   `HFAI_IMAGE_WEKA_PATH` 注入，每个计算 pod 起 busybox initContainer 执行
   `/marsv2/scripts/link_hfai_image.sh` —— **该脚本全仓不存在**（I16）；
   且 103 节点上 `/data_local` 不存在、busybox 镜像引用不匹配（I17）。
   **结论：只修控制面，自定义镜像任务仍然跑不起来。**
4. **语义未单点化**：`path`（镜像在 weka 上的位置）与 `load` 入参 `image_tar`（tar 包路径）是两个概念却无处定义（I18）；
   仓库没有 `FileType.IMAGE`/`image_path`，服务端无法校验路径落点（I9）。
5. **服务端的官方排障指引当前必然失效（103 实测）**：用不存在的自定义镜像提交任务，服务端返回
   「用户所在的组 [hfai] 不存在镜像 […] 或镜像仍在加载，**请使用命令 `hfai images list` 检查**」，
   而 `images list` 的 `user_images` **恒为空**（第 1 条）→ 用户按官方指引自查**永远查不出原因**。
   这把 I2 从「列表不显示」升级为「**官方排障路径失效**」，也成为 AC-03 的直接动因。

### 1.2 目标

| ID | 目标 |
| --- | --- |
| G1 | **端到端可用**：一条自定义镜像从「加载」到「被任务成功使用」的完整闭环在 103 上可实测通过（不只「接口返回 200」） |
| G2 | **补齐控制面四处断链**：客户端接口/实现（C-3）、服务端路由、DB 写入、列表读取 |
| G3 | **定义并落地状态机**：`processing → loaded/failed`、`loaded → deleted`，且与客户端子串约定、任务侧精确白名单**三方一致** |
| G4 | **补齐运行面缺失物**：`link_hfai_image.sh` + 挂载种子 + 可配置的基础镜像，并用 103 实测证明 a pod 能真正用上自定义镜像 |
| G5 | **概念单点化**：`image`（`name:tag`）与 `path`（weka 镜像位置）与 `image_tar`（tar 包）三者定义清晰、校验可执行 |
| G6 | **零回归**：`workspace` 9 条、`env` 2 条 `/ugc/*` 路由与 `train_environment` 内建镜像路径行为不变 |

### 1.3 非目标（Out of Scope）

- **不**实现镜像的**跨集群分发/多集群镜像同步**；
- **不**实现镜像**分层去重、增量加载**（tar 整包处理）；
- **不**实现**镜像市场 / 公开镜像共享**（本特性只到 `shared_group` 粒度）；
- **不**改造 `train_environment`（内建镜像）的既有语义与配额模型；
- **不**在本次实现 P2 的空间回收/GC（见 FR-14 与 OPS-05，本期只登记不实现）；
- **不**支持「用户在集群外直接把 tar 上传到集群」（外部用户走 workspace/env 既有上传通道先行上传，见 CMP-06）。

---

## 2. 角色与场景

| 角色 | 场景 |
| --- | --- |
| 组内用户（`is_external=false`） | 把 tar 放到共享盘 → `images load` → `images list` 看到 `processing` → `loaded` → `hfai python x.py -i registry/组/名:tag` 跑任务 |
| 组内其他用户 | `images list` 能看到本组镜像（组内共享）；可 `images delete` 删除本组镜像（与既有 docstring 一致） |
| 外部用户（`is_external=true`） | 先用既有上传通道把 tar 传到共享盘，再走同样的 `images load` |
| 平台运维 | 部署 registry 或初始化节点 `/data_local`；配置 busybox 基础镜像；灰度开关；回滚 |
| 平台服务（内部） | 加载执行方（任务/Job）通过 API-16 回报状态；launcher 读表注入 env |

---

## 3. 需求总览

### 3.1 功能需求（FR）

| ID | 需求 | 优先级 | 落点 |
| --- | --- | --- | --- |
| FR-01 | **修复 C-3**：在 `IUserImage` 增加 `async_load` / `async_delete` 声明，在客户端与**服务端** `UserImage` 各自实现 | P0 | 设计 §6.1 |
| FR-02 | **列表返回真实用户镜像**：`user_images` 由 `TrainImageTable` 按 `user.shared_group` 查询得到，替换硬编码 `[]` | P0 | 设计 §5.2 |
| FR-03 | **API-15 加载登记**：接收 `image_tar` + `image`（可选）+ 共享根校验，写入 `train_image`（`status='processing'`），**按 `image_tar` 幂等 upsert** | P0 | 设计 §4.1 · §5.2 |
| FR-04 | **状态机落地**：定义 `processing/loading/loaded/failed/deleted` 合法值、迁移规则、非法迁移拒绝；三方口径对齐（客户端子串 `deleted`、任务侧精确 `loaded`） | P0 | 设计 §7.3 |
| FR-05 | **API-18 删除**：按 `registry/shared_group/image` 软删（`status='deleted'`）；**必须校验 `shared_group == user.shared_group`** | P0 | 设计 §4.4 · §10 |
| FR-06 | **幂等与重试安全**：FR-03/FR-05 可安全重试（同 `image_tar` 不产生重复行、不重置已完成状态）；客户端变更型调用**不得静默重试** | P0 | 设计 §5.2 · §6.2 |
| FR-07 | **概念单点化**：明确 `image_tar`（tar 包路径）、`image`（`name:tag`，**不含 `/`**）、`path`（**镜像在共享盘上的位置**，喂给 `HFAI_IMAGE_WEKA_PATH`）三者的定义、来源与校验；`image` 缺省时由 tar 文件名派生且**服务端可覆写** | P0 | 设计 §3 |
| FR-08 | **运行面补齐**：新增 `marsv2/scripts/link_hfai_image.sh`（含契约文档与幂等/失败语义）+ 在 `one/hai-up.sh` 的 `storage` 挂载种子里登记 + 基础镜像地址可配置 | P0 | 设计 §5.4 · §9.1 |
| FR-09 | **数据面执行**：定义「谁把 tar 变成可被 link 的镜像」的执行主体与后端；**至少提供一个不依赖内网 registry 的后端**，使 103 可端到端验证 | P0 | 设计 §5.3 · §13 ADR-I2 |
| FR-10 | **API-16 状态回报**：加载执行方以用户 token 回报 `loaded/failed` + `message`；**只有被登记的 `task_id` 可回报**（防伪造） | P0 | 设计 §4.2 |
| FR-11 | **列表输出契约**：`user_images` 每行必含 `registry/shared_group/image/status/image_tar/updated_at` 6 字段；按 `updated_at **DESC**` 返回（修 I7）；出口把 numpy 标量归一化为原生类型（修 I8） | P0 | 设计 §5.2 |
| FR-12 | **客户端失败提示**：`load`/`delete` 失败时打印服务端 `msg`，不再向用户抛裸 `Exception`/`AssertionError` 栈（修 I10） | P1 | 设计 §6.2 |
| FR-13 | **路径单点与校验**：新增镜像共享根配置（`image_path` 或等价单点），服务端用 `check_is_subpath` 校验 `image_tar` 与 `path` 均落在根内 | P0 | 设计 §3 |
| FR-14 | **空间回收（P2，本期只登记）**：`delete` 后可选的 registry tag / 共享盘 tar / 节点镜像清理与审计；本期仅保证**不误导用户**（文案与 `-a` 语义修正） | P2 | 设计 §7.3(P2) |
| FR-15 | **任务侧不变式不得回归**：K1–K5 五条契约（三段 URL、拼接口径、`status='loaded'`、按组隔离、报错指引）在改造后逐条保持 | P0 | 设计 §7.1 |

### 3.2 非功能需求（NFR）

| ID | 需求 |
| --- | --- |
| NFR-01 | **幂等**：同参数重复调用 API-15 恢复到同一状态、同一行，不产生副作用；崩溃后重放安全 |
| NFR-02 | **性能**：`images list` 在单组 200 行量级下 P95 < 1s；不得引入全表扫描以外的逐行查询（禁止 N+1） |
| NFR-03 | **可观测**：加载全流程有结构化日志（`image_tar`/`image`/`task_id`/状态迁移/耗时）与至少 4 个指标（见设计 §9.4） |
| NFR-04 | **可测试性**：核心逻辑（路径校验、状态机、命名派生、组校验）必须可在**无 registry、无 k8s** 的条件下单元测试（对齐 `tests/env/` 的做法） |
| NFR-05 | **兼容**：新增字段一律**追加**，旧客户端忽略即可；旧调用形态（`load <tar>` 单参）继续可用 |
| NFR-06 | **资源**：加载执行体的资源占用可控（CPU/内存上限），不因大 tar 打爆服务器 pod |

### 3.3 安全需求（SEC）

| ID | 需求 |
| --- | --- |
| SEC-01 | **路径越界防护**：`image_tar` 与 `path` 必须 `check_is_subpath(image_root, ...)`；拒绝 `..`、符号链接逃逸、非共享盘路径（含客户端本机绝对路径） |
| SEC-02 | **组隔离**：所有读写以服务端解析的 `user.shared_group` 为准，**绝不采信客户端传入的 group**；删除/回报必须校验目标行 `shared_group` 与用户一致 |
| SEC-03 | **注入防护**：镜像名走**白名单正则**（`[A-Za-z0-9._/-]` + `:tag`），任何进入 shell/任务脚本的字段必须参数化或严格转义；**禁止字符串拼接构造命令** |
| SEC-04 | **鉴权**：新增 `/ugc/*` 路由统一用 `Depends(get_ugc_user)`；API-16 额外校验 `task_id` 归属 |
| SEC-05 | **越权**：不允许**跨组**删除/查看/登记；组内允许删除他人镜像（既有设计，须在文档与帮助文本中显式声明） |
| SEC-06 | **日志脱敏**：日志不得打印 token；`image_tar` 路径按需截断 |
| SEC-07 | **最小权限**：执行加载的 pod 若需访问节点容器运行时，必须**限定在专用命名空间/专用 ServiceAccount**，且**不接受用户可控的命令行** |

### 3.4 运维需求（OPS）

| ID | 需求 |
| --- | --- |
| OPS-01 | **开关与灰度**：`image.enabled` / `enabled_groups` / `enabled_users` 三级灰度（对齐 `cloud.storage` 既有做法），关闭时不抛 500 |
| OPS-02 | **回滚**：一级（关开关）即可让 `load/delete` 失败关闭并提示，不影响 `list` 与内建镜像路径 |
| OPS-03 | **迁移**：新增列/索引必须走 `db_schemas/*.sql` + `init_postgresql.sh` 全量重放机制，且脚本**幂等**（`if not exists`） |
| OPS-04 | **节点前置**：`/data_local` 的存在性必须由部署流程保证（或改由配置/`DirectoryOrCreate` 显式声明），并纳入部署自检 |
| OPS-05 | **空间回收**：P2 提供 `train_image` 的孤儿行/孤儿镜像审计（本期只登记） |

### 3.5 兼容需求（CMP）

| ID | 需求 |
| --- | --- |
| CMP-01 | 旧客户端 `images load <image_tar>`（单参、无 `image`）必须继续可用 |
| CMP-02 | 旧客户端 `images delete <image>` 签名不变 |
| CMP-03 | `train_environment` / `mars_images` 的结构与语义**零改动** |
| CMP-04 | `registry` 列默认值 `registry.high-flyer.cn` 保持；新代码不得依赖该默认值可达 |
| CMP-05 | `user_images` 由 `[]` 变为有内容属于**行为修正**（非破坏），但行内字段名不得改名 |
| CMP-06 | 私有 `custom.py` 覆盖接缝保持有效（`default.py`/`implement.py`/`custom.py` 三层约定不得破坏） |

### 3.6 硬约束（HC）

| ID | 约束 | 原因 |
| --- | --- | --- |
| HC-01 | SQL 三条硬约束：禁止 `%s::type`（写 `CAST(%s AS type)`）、字面 `%` 写 `%%`、参数只能传 tuple 且枚举传 `.value` | `db/mars_db.py` 对 `Connection.execute` 的 patch 会截断绑定参数 |
| HC-02 | 任务侧 URL 拼接口径**逐字节不变**：`registry + '/' + shared_group + '/' + image` | K2，改动即让所有自定义镜像任务校验失败 |
| HC-03 | `'loaded'` 字面量不可改 | K3，任务侧精确匹配 |
| HC-04 | 客户端用**子串** `'deleted' in status` 过滤，服务端状态取值必须包含 `deleted` 子串 | 客户端既有代码 |
| HC-05 | `image` 字段**不含 `/`**（`name:tag`），与 `registry`/`shared_group` 拼成恰好 3 段；**且不得自动补 tag**（`demo` 不得被归一化成 `demo:latest`）—— `image` 必须与用户后续传给 `-i` 的第三段**逐字节一致** | K1/K2；任务校验是逐字节比较，自动补 tag 会导致**永远匹配不上**（设计 §4.1 实现修正 I6b） |
| HC-06 | 任何 DDL 变更必须幂等且通过 `init_postgresql.sh` 重放路径生效 | 仓库无自动迁移框架 |
| HC-07 | 新增能力必须落在 **`ugc-server` 宿主内**（不新增服务/端口） | 与 workspace/env 的分层纪律一致 |
| HC-08 | 运行期脚本必须**随镜像构建进入任务 pod**（放 `marsv2/scripts/` + 登记 `storage` 挂载种子），不得依赖手工 `kubectl cp` | I16 的根因就是「引用了不存在的脚本」 |
| HC-09 | 不得修改 `a_find_user_group_image_urls` 的签名与返回语义 | 任务提交期唯一校验入口 |
| HC-10 | 加载执行体**不得**在服务器 pod 内直接操作节点容器运行时（无 host socket 可挂） | 103 实测：平台 pod 未挂载任何容器运行时 socket |

---

## 4. 接口清单

### 4.1 API-15 `POST /ugc/user/train_image/load`（加载登记）

| 项 | 内容 |
| --- | --- |
| 用途 | 登记一个待加载的镜像 tar，创建 `train_image` 行并触发数据面加载 |
| 路由 | `api/register/implement.py` 的 `ugc` 块内注册（`:67-94` 区块） |
| 实现位置 | 接入层 `api/resource/image/default.py`（替换 `hfai_image_load` 桩，**保持函数名**）→ 领域层新增 `server_model/user_impl/user_image/` 方法 |
| 鉴权 | `Depends(get_ugc_user)` |
| 入参 | `image_tar`（**必填**，共享盘绝对路径）；`image`（**选填**，`name:tag`；缺省由 `image_tar` 的 basename 派生并去掉 `.tar` 后缀）；`shared_group` **不接受**（服务端取 `user.shared_group`） |
| 出参 | `{'success':1,'msg':'...','image':'<registry>/<group>/<name:tag>','image_tar':'...','status':'processing','task_id':<id>}` |
| 失败 | `INVALID_PARAM`（格式/缺参）、`PATH_ESCAPE`（越界）、`IMAGE_TAR_NOT_FOUND`（共享盘不存在）、`FEATURE_DISABLED`、`UNAUTHORIZED`、`IMAGE_NAME_CONFLICT`（同名不同 tar，见 Q-3） |
| 幂等 | **按 `image_tar` upsert**：已存在且 `status in (processing, loading, loaded)` → 直接返回当前状态（不改行）；`failed` → 允许重试（重置为 `processing`） |
| 兼容 | 只传 `image_tar` 的旧形态必须可用（CMP-01） |

> **实现修正 I18（必须遵守）**：入参 `image_tar` 与列 `path` **不是同一个东西**。
> `image_tar` 是**用户提供的 tar 包路径**；`path` 是**加载完成后镜像在共享盘上的位置**，供运行期
> `HFAI_IMAGE_WEKA_PATH` 使用。API-15 在 `processing` 阶段**不得**把 `image_tar` 直接写进 `path`
> （否则运行期 link 会指向一个 tar 文件而非镜像位置）。`path` 由数据面执行成功后在 API-16 中回报。

### 4.2 API-16 `POST /ugc/user/train_image/update_status`（状态回报）

| 项 | 内容 |
| --- | --- |
| 用途 | 加载执行方回报结果（**内部接口**，但走用户 token） |
| 实现位置 | `api/resource/image/default.py`（替换 `hfai_image_update_status` 桩） |
| 鉴权 | `Depends(get_ugc_user)` + **`task_id` 归属校验**（SEC-04） |
| 入参 | `image_tar`（必填，定位行）、`status`（`loaded`/`failed`）、`path`（`loaded` 时**必填**，镜像在共享盘上的位置）、`message`（`failed` 时建议填） |
| 出参 | `{'success':1,'msg':'...','status':'<新状态>'}` |
| 失败 | `INVALID_PARAM`、`FORBIDDEN`（`task_id` 不匹配）、`ILLEGAL_TRANSITION`（如 `deleted → loaded`）、`UNAUTHORIZED` |
| 幂等 | 同 `status` 重复回报无副作用；`loaded` 不可被 `loaded` 以外的回报覆盖（除非管理员重置） |

### 4.3 API-17 `POST /ugc/user/train_image/list`（**修订版**，路由已存在）

| 项 | 内容 |
| --- | --- |
| 现状 | 路由已注册（`api/register/implement.py:71`），`user_images` 硬编码 `[]` |
| 本次改动 | **只改 `user_images` 的数据来源与排序**；`mars_images` 与响应外壳**零改动** |
| 出参（`user_images` 每行） | 必含 `registry` / `shared_group` / `image` / `status` / `image_tar` / `updated_at` 6 字段；可追加 `path` / `task_id` / `created_at` / `message` |
| 排序 | `updated_at **DESC**`（修 I7：客户端取**首个**为基准，DESC 才等于「以最新为准」） |
| 类型 | 出口归一化：`task_id` → `int`，`updated_at`/`created_at` → ISO 字符串（修 I8，避免 `np.int64` 让 FastAPI 编码 500） |
| 过滤 | **服务端返回本组全部状态行**（含 `deleted`），由客户端 `-a/--all` 决定是否隐藏（保持既有分工，CMP-05） |

### 4.4 API-18 `POST /ugc/user/train_image/delete`（删除）

| 项 | 内容 |
| --- | --- |
| 实现位置 | `api/resource/image/default.py`（替换 `hfai_image_delete` 桩） |
| 入参 | `image`（必填，`registry/shared_group/name:tag` **3 段**） |
| 行为 | 解析出 `shared_group` → **校验等于 `user.shared_group`**（SEC-02/SEC-05）→ 把该 `image` 在本组的所有非删除行置 `status='deleted'` |
| 出参 | `{'success':1,'msg':'...','deleted':<行数>}` |
| 失败 | `INVALID_PARAM`（非 3 段）、`FORBIDDEN`（跨组）、`IMAGE_NOT_FOUND`、`FEATURE_DISABLED` |
| 幂等 | 重复删除返回 `deleted: 0` 且 `success:1`（幂等） |
| 命名回收 | **不回收**镜像名（与 docstring 一致）；被删除的名字**不可**被任务使用（因为 `a_find_user_group_image_urls` 用 `status='loaded'` 白名单，天然排除） |

### 4.5 统一约定

| 项 | 约定 |
| --- | --- |
| 鉴权 | `Depends(get_ugc_user)`（`api/depends/implement.py:110-142`），token 走 query string |
| Body | 与 workspace/env 一致：**JSON 放在 `text/plain` body** 或 query，两种都需兼容（旧客户端用 query） |
| 错误体 | `{'success':0,'code':'<CODE>','msg':'<中文可读>'}`；`code` 用本文 §4 各接口列出的取值 |
| 枚举归一化 | 任何进 SQL 的枚举取 `.value`；任何出路 JSON 的 numpy 标量转原生类型 |
| 路径校验 | 统一走 `cloud_storage/utils.py:check_is_subpath`（禁止自造） |
| 日志 | 结构化字段：`user_name` / `shared_group` / `image_tar` / `image` / `task_id` / `status` / `cost_ms` |

---

## 5. 状态与数据

### 5.1 数据表

| 数据 | 位置 | 说明 |
| --- | --- | --- |
| `train_image` | PostgreSQL `public.train_image`（DDL `db_schemas/017.table_train_image.sql`） | 已有表；本次**新增列**（`message`、可选 `user_name`）与**必要索引** |
| 状态写入 | 新增领域层方法（`MarsDB().a_execute`，遵守 HC-01） | 全仓首个 `train_image` 写入路径 |
| 缓存/刷新 | `TrainImageTable`（`AutoTable.AutoBaseTable` → `DBBaseTable`/`RoamingBaseTable`） | 无 parliament 时每次访问重查（天然读到新行）；**有 parliament 时必须显式通知刷新**，否则 `launcher` 侧 `find_one` 读到旧快照（见设计 §7.4） |
| 迁移 | 新增 `db_schemas/035.*.sql`（当前最大编号 `034`） | 幂等（`add column if not exists`），由 `init_postgresql.sh` 全量重放生效（HC-06） |

### 5.2 状态机（详见设计 §7.3）

```
         ┌──────────────┐
   load →│  processing  │──(执行方领取)──►│  loading  │
         └──────────────┘                 └─────┬─────┘
                                                │
                       成功 ◄───────────────────┴───────────────────► 失败
                         │                                            │
                    ┌────▼────┐                                 ┌─────▼─────┐
                    │ loaded  │─── delete ──►  [ deleted ]       │  failed   │─── 重试 ──► processing
                    └─────────┘                                  └───────────┘
```

> **三方口径（HC-03/HC-04 的落地）**：任务侧精确匹配 **`loaded`**；客户端以**子串** `deleted` 过滤；
> 服务端是**唯一**写入方，且必须同时满足前两者。

---

## 6. 验收标准（DoD）

| ID | 验收项 | 判定方式 |
| --- | --- | --- |
| AC-01 | **端到端**：一条自定义镜像从 `load` → `loaded` → **任务成功用其运行并产出预期输出** | 设计 §7 的实测步骤 + §9 用例 E2E-01（103 实机） |
| AC-02 | **C-3 闭环**：`images load` / `images delete` 不再抛 `AttributeError`；文档标记为可用 | `grep` 无 `AttributeError` 复现 + `hai-cli images load --help` + 实机调用 |
| AC-03 | **列表可见**：`images load` 后 `images list` 能看到该行及其真实状态 | 实机对比 API-17 响应与 `psql select * from train_image` |
| AC-04 | **状态机合法**：非法迁移被拒绝；`loaded` 行可被任务使用，`deleted` 行被客户端隐藏（`-a` 可见） | 用例 TC-U/TC-A/TC-C 组 |
| AC-05 | **幂等**：同 `image_tar` 连续 3 次 `load` → 表仍 1 行、状态不倒退；`delete` 两次返回 `deleted:0` | 用例 TC-F 组 |
| AC-06 | **路径安全**：越界路径（`..`、`/etc/passwd`、客户端本机路径）全部被拒且**不产生** DB 行 | 用例 TC-S 组 |
| AC-07 | **组隔离**：A 组用户无法 `delete`/看见 B 组镜像（构造 3 段 URL 跨组尝试） | 用例 TC-S 组 |
| AC-08 | **运行面可用**：`link_hfai_image.sh` 存在、被挂载、被 pod 成功执行（无 `not found`）；`/data_local` 前置被部署自检覆盖 | 用例 TC-T 组 + 部署自检 OPS-04 输出 |
| AC-09 | **无 registry 也能验**：103 在**没有内网 registry** 的条件下完成 AC-01 | E2E-01 环境说明 + 后端选型 ADR-I2 |
| AC-10 | **零回归**：`workspace` / `env` / `train_environment` 既有接口与流程全绿 | 复用 `docs/haiplatform/scripts/` 既有 e2e 脚本（见用例 §7.2） |
| AC-11 | **可观测**：一次 `load` 全流程可在日志中按 `image_tar` 串起来；4 个指标有数据 | 用例 TC-L 组 |
| AC-12 | **迁移可重放**：`init_postgresql.sh` 重复执行不报错、列只加一次 | 重放两次并对比 `\d train_image` |
| AC-13 | **兼容**：旧形态 `load <tar>`（单参）与旧 `list` 字段消费零改动可用 | 用例 TC-O 组 |
| AC-14 | **文档一致**：分析 §6 的 I1–I18 全部在需求/设计/用例/Checklist 中有对应处置或显式「不处置」理由 | 本文 §11 追溯矩阵 + Checklist ACC |

---

## 7. 端到端验证方法（AC-01 的实测步骤）

```
# ① 前置：确认开关、共享根、节点前置
sudo -u fireflyer hai-cli images list                       # 期望：内建镜像 1 行 + 用户镜像（可为空）

# ② 准备一个 tar 并放到共享盘（复用既有上传通道，见 CMP-06）
ls -l /nfs-shared/hai-platform/<image_root>/demo.tar          # 期望：存在

# ③ 加载
sudo -u fireflyer hai-cli images load /nfs-shared/.../demo.tar --image demo:v1
# 期望：success=1，打印 image / status=processing / task_id

# ④ 观察状态收敛（轮询）
sudo -u fireflyer hai-cli images list                        # 期望：该行 status 由 processing → loaded
sudo kubectl -n hai-platform exec hai-platform-0 -- \
  psql -U root -d mars_db -c "select image_tar,image,path,status,task_id from train_image;"

# ⑤ 用该镜像跑任务（AC-01 的关键一步：必须有真实产出，不看 200）
sudo -u fireflyer hai-cli python /tmp/probe.py -- --image <registry>/<group>/demo:v1 -n 1
sudo -u fireflyer hai-cli status <task_id>                   # 期望：pod succeeded
sudo -u fireflyer hai-cli logs <task_id>                     # 期望：probe 输出符合预期（证明用的是自定义镜像）

# ⑥ 删除与不可用性
sudo -u fireflyer hai-cli images delete <registry>/<group>/demo:v1
sudo -u fireflyer hai-cli images list -a                     # 期望：该行 status 含 deleted
# 再用同一镜像提交任务 → 期望：服务端返回「不存在镜像…或镜像仍在加载」
```

**通过判据**：③ 返回 `success=1`；④ 状态收敛到 `loaded` 且 `path` 已回填；**⑤ 任务 `succeeded` 且输出
能被自定义镜像内容区分**（例如镜像内预置一个探针文件/包，`probe.py` import 它）；⑥ 删除后任务提交被拒。

---

## 8. 待确认决策

| ID | 问题 | 建议 | 影响 |
| --- | --- | --- | --- |
| Q-1 | 数据面后端：`registry`（push 内网 registry）还是 `node_local`（按节点 link，运行期已有此机制）？ | **按 §4.6 证据选「node link」为主线**：运行期本来就是 link，registry 只是命名 | 决定 FR-09 实现与 103 可验证性（AC-09） |
| Q-2 | `image_tar` 唯一索引**不含 `shared_group`**：两个组加载同一 tar 会冲突，是否改索引？ | 改为 `(shared_group, image_tar)` 唯一（新迁移文件，幂等） | 影响 I13 与 API-15 的 upsert 冲突语义 |
| Q-3 | 同名不同 tar（`image` 相同）是否允许？客户端已按名字去重 | **允许**（保留历史行），但 `delete` 作用于该名字全部行；提交校验只看 `status='loaded'` | 影响 `images list` 展示（客户端 `last_img_status` 逻辑） |
| Q-4 | 加载由**平台任务**执行（复用 `task_id` 列）还是**专用 k8s Job**？ | 复用平台任务：`task_id` 列与 `update_status` 桩都指向该模型 | 影响 FR-09/FR-10 与工作量（设计 ADR-I3） |
| Q-5 | `/data_local` 从哪来？谁初始化？ | 部署流程显式创建（或把 hostPath 改为 `DirectoryOrCreate` 并写进部署自检 OPS-04） | 影响 AC-08 与 I17① |
| Q-6 | initContainer 基础镜像用哪个？ | 改为**可配置** `manager.image_load_helper_image`，103 用节点已有的 `docker.io/library/busybox:latest` | 影响 I17② |
| Q-7 | 内网 registry 在 103 是否需要部署？ | 若采纳 Q-1 的 node link，**不需要**；仅生产可选 | 影响工作量与 AC-09 |
| Q-8 | 是否新增 `user_name` 列记录归属？ | **建议加**（便于审计与「谁加载的」追溯），但这与「组内共享、组内可删」不冲突 | 影响 §5.1 迁移范围 |

---

## 9. 交付物清单

| 交付物 | 路径 |
| --- | --- |
| 分析报告 | `docs/haiplatform/images/hai-cli-images-analysis.md` |
| 需求说明 | `docs/haiplatform/images/images-server-requirements.md`（本文） |
| 程序设计 | `docs/haiplatform/images/images-server-design.md` |
| 测试用例 | `docs/haiplatform/images/images-server-test-cases.md` |
| Checklist | `docs/haiplatform/images/images-server-checklist.md` |
| 任务列表 | `docs/haiplatform/images/images-server-task-list.md` |
| 运行期脚本 | `marsv2/scripts/link_hfai_image.sh` + `one/hai-up.sh` 挂载种子 |
| 迁移 | `db_schemas/035.table_train_image_alter.sql` |
| 验证脚本 | `docs/haiplatform/scripts/smoke_images.sh` / `e2e_images.sh`（新增） |

---

## 10. 术语

| 术语 | 含义 |
| --- | --- |
| `image_tar` | 用户提供的**镜像 tar 包路径**（共享盘上）；API-15 入参 |
| `image` | **镜像名 `name:tag`**，自身不含 `/`；与 `registry`/`shared_group` 拼成 3 段 URL |
| `path` | **镜像在共享盘上的位置**（DDL 注释：镜像在 weka 上的路径）；运行期作为 `HFAI_IMAGE_WEKA_PATH` |
| 镜像 URL / `image_url` | `registry/shared_group/image`，恰好 3 段；任务校验与客户端展示都用它 |
| `shared_group` | 归属组；由服务端从用户解析，**不可由客户端指定** |
| link（链接） | 运行期把共享盘上的镜像「链接/导入」到节点本地容器运行时的动作（由 `link_hfai_image.sh` 执行） |
| 三段式链路 | 控制面（load/list/delete）→ 提交面（任务校验）→ 运行面（launcher 注入 + pod link） |

---

## 11. 追溯矩阵

| 需求 | 接口 / 落点 | 设计章节 | 验收 |
| --- | --- | --- | --- |
| FR-01 | 客户端 + 服务端 `UserImage`；`IUserImage` | §6.1 | AC-02 / AC-13 |
| FR-02 / FR-11 | API-17 | §5.2 | AC-03 / AC-04 |
| FR-03 / FR-06 / FR-07 | API-15 | §4.1 / §3 / §5.2 | AC-01 / AC-03 / AC-05 |
| FR-04 | 状态机（服务端唯一写入方） | §7.3 | AC-04 / AC-05 |
| FR-05 | API-18 | §4.4 / §10 | AC-04 / AC-07 |
| FR-08 / FR-09 / FR-10 | 运行面脚本 + API-16 | §5.3 / §5.4 / §4.2 | AC-01 / AC-08 / AC-09 |
| FR-12 | 客户端 `image_api.py` | §6.2 | AC-02 |
| FR-13 | `image_path` 单点 + `check_is_subpath` | §3 / §10 | AC-06 |
| FR-14 (P2) | 回收/审计 | §7.3(P2) | 本期不做（登记） |
| FR-15 | 任务侧不变式 | §7.1 | AC-04 / AC-10 |
| NFR-01 | 幂等 upsert | §5.2 | AC-05 |
| NFR-02..06 | 接口与客户端 | §4 / §6.2 / §9.4 | AC-11 / 性能测试 |
| SEC-01..07 | 路径/组/注入/鉴权 | §3 / §4 / §10 | AC-06 / AC-07 |
| OPS-01..05 | 配置/灰度/迁移/节点前置 | §9 / §7.4 | AC-08 / AC-12 |
| CMP-01..06 | 兼容层 | §8 | AC-13 / AC-10 |
| HC-01..10 | 全局硬约束 | §2 / §5 / §7 / §10 | 代码评审 |
| G1..G6 | 全局目标 | §1 / §2 | AC-01..AC-14 |
