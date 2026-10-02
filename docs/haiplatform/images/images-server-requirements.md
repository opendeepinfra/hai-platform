# hai-cli images 服务端实现需求说明

> **本分支说明（`feature/hai-cli-images-rustfs-design`，基线 `feature/hai-cli-env-server-design` @ `33a5b26`）**
>
> - **本次实现的镜像上传主入口 = `hai-cli images push <本地 tar>`**：复用 `workspace`/`env` 既有的 RustFS/S3 流水线
>   （API-01 签发 STS → 客户端直传对象存储 → API-05 stage2 落盘到 `image_path` → API-06 轮询状态），
>   落盘成功后自动登记（API-15）。手工把 tar 放到共享盘再 `images load` 仅作为**兼容/运维旁路**保留。
> - **来源**：控制面（API-15~API-18）与运行面（`marsv2/scripts/link_hfai_image.sh`、`init_manager.py` 注入、`storage` 挂载种子）
>   的设计沿用分支 `feature/hai-cli-images-server-design` 上**已实现并在 103 实测通过**的结论，证据见
>   [images-server-test-report.md](images-server-test-report.md)（被测 tag `f2cb559`）。
> - **状态（2026-10-02 更新）**：**S9（P0 资产并入与入口切换）已完成**，并在 103 上重跑通过
>   （preflight `PASS=30 FAIL=0`、L1 `33 passed`、L2 `PASS=44 FAIL=0`、L3 `PASS=26 FAIL=0`、workspace/env 回归全绿 —— 见
>   [images-server-test-report.md](images-server-test-report.md) §9.1）；**S8（上传通道）已实现并端到端验证通过**
>   （`e2e_images_push.sh` `PASS=33 WARN=1 FAIL=0`：push → 共享盘 md5 一致 → `user_sync_status=finished` →
>   `train_image=loaded` → 任务产出镜像内探针 → 幂等 → 开关一致性/一级回滚可逆 —— 见 §9.2、E2E-09/E2E-10）。
> - **标签约定**：文中 `P0` / `P1` 只用于标注**来源与阶段**（P0 = 控制面 + 运行面，P1 = 上传通道），**不代表可选项**。
> - **资产状态**：P0 资产（控制面/运行面代码、迁移 `035`、`tests/images/test_image_domain.py`、`docs/haiplatform/scripts/`
>   下的 images 脚本）已并入本分支（S9-1）；上传通道新增 `db_schemas/036.file_type_enum_add_image.sql`、
>   `tests/images/test_image_push*.py`、`docs/haiplatform/scripts/e2e_images_push.sh`（S8），**全部已落地**。

> **文档定位**：三件套之二（分析 → **需求** → 设计）。本文只回答「**要做什么、做到什么程度算完成**」，
> 不回答「怎么实现」（见设计文档）。所有需求均以 [hai-cli-images-analysis.md](hai-cli-images-analysis.md)
> 的实测/代码证据为依据，需求 ID 与设计章节、用例 ID、Checklist ID 相互可追溯。
>
> **前置阅读**：[hai-cli-images-analysis.md](hai-cli-images-analysis.md)（§1 结论速览 · §4.6 运行面 · §6 风险 I1–I18）。
>
> **编号约定（务必先读，避免与既有文档混淆）**：
> - **接口号跨特性续编**：`workspace` 用到 API-01~API-12，`env` 用到 API-11（修订）/API-13/API-14。
>   本特性从 **API-15** 起编号，且**刻意与 `api/resource/image/default.py` 的 4 个既有桩同名同序**
>   （`load` → `update_status` → `list` → `delete`），便于评审时一一对应；上传通道的 **API-19** 为
>   本分支新增的预检接口（见 §4.6）。
> - **需求号本特性独立编号**：`G1–` / `FR-` / `NFR-` / `SEC-` / `OPS-` / `CMP-` / `HC-` / `AC-` / `Q-`，
>   与 `workspace`、`env` 的文档**不共享编号**。
> - **命名空间互不冲突**：`C-3` = 审计（[../hai-cli-client-server-audit.md](../hai-cli-client-server-audit.md) §3.4）的客户端缺陷 ID，
>   本文**沿用不改号**；`I1–I18` = 本特性分析报告的风险 ID；`TC-*` = 用例 ID；`ACC-*` = Checklist 签署项
>   （**与需求的 `AC-*` 是两套，勿混用**）。
>
> **追溯**：需求 → 接口/落点 → 设计章节 → 验收，见 §11。
>
> **本次修订（本分支：上传通道升为唯一上传主入口）**：本分支把「`images push` 上传通道」定为**唯一上传主入口**，
> 手工把 tar 放到共享盘 + `images load` 降为**兼容/运维旁路**（CMP-08 保证不回归）。
> **P0（FR-01~FR-15）的设计已在旧分支实现并在 103 实测通过**（见 [images-server-test-report.md](images-server-test-report.md)，
> 被测 tag `f2cb559`）；**本分支的交付 = 并入 P0 资产（[images-server-task-list.md](images-server-task-list.md) §3.10 的
> **S9**，1.0 人日）+ 实现上传通道（设计 §14 的 **S8**，2.0 人日）**，本分支新增合计 **3.0 人日**；
> P0 = **13.0 人日**（旧分支已完成）；特性总计 **16.0 人日**。
> 本次新增/改写：§1.2 的 G7、§1.3、§2、§3.1 的 FR-16~FR-20 口径、§4.6、§5.3、§6 的 AC-15~AC-18、
> §7、§8 的 Q-9~Q-12、§9、§10 术语口径、§11 追溯矩阵；**追溯关系不变**：每条需求仍按
> 「**需求 → 接口/落点 → 设计章节 → 验收**」四段可查（见 §11）。
>
> **引用规则**：本文档集（analysis / requirements / design / test-cases / checklist / task-list / decisions / test-report）
> 是本分支的**同一批交付物**，文中相互引用均为同批目标文件；`docs/haiplatform/scripts/` 下的 images 脚本与
> `tests/images/` **已并入本分支**（S9-1 的 P0 用例 + S8 的上传通道用例），可直接执行。

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
| G7 | **上传闭环（本分支首要目标）**：用户用一条命令把 `docker save` 出的 tar 送进集群（**复用既有 RustFS/S3 通道**），落到 `image_path` 之后自动登记，**直接可被任务使用**，使「准备镜像」不再依赖人工搬运共享盘 |

> **本分支首要目标 = G7**（上传通道）；G1~G6 是 P0 已实测通过的目标，本分支通过 **S9** 把 P0 资产并入本分支
> 并切换入口，保证其**不回归**（AC-10 / AC-18）。

### 1.3 非目标（Out of Scope）

- **不**实现镜像的**跨集群分发/多集群镜像同步**；
- **不**实现镜像**分层去重、增量加载**（tar 整包处理）；
- **不**实现**镜像市场 / 公开镜像共享**（本特性只到 `shared_group` 粒度）；
- **不**改造 `train_environment`（内建镜像）的既有语义与配额模型；
- **不**在本次实现 P2 的空间回收/GC（见 FR-14 与 OPS-05，本期只登记不实现）；
- **不**新造传输层：上传一律复用既有 `cloud_storage`（RustFS/S3）通道与 `haiworkspace` 的 push 实现；
- **不**让计算节点/initContainer 直接从对象存储拉取镜像（节点侧只认共享盘路径 `HFAI_IMAGE_WEKA_PATH`，HC-14）。

> **本特性「支持」用户在集群外直接把 tar 上传到集群**——这是本分支的**唯一上传主入口**，方式是
> 在既有 RustFS/S3 通道上**加 `file_type=image` 分支**（API-01 / API-05 / API-06 全复用，见 §4.6），
> **不新造接口族、不新造传输层**；不再把它列为非目标。

---

## 2. 角色与场景

| 角色 | 场景 |
| --- | --- |
| 组内用户（`is_external=false`） | 一条命令 `hai-cli images push <本地 tar>`（自动落盘 + 自动登记）→ `images list` 看到 `processing` → `loaded` → `hfai python x.py -i registry/组/名:tag` 跑任务（**本分支主路径**） |
| 组内用户（兼容旁路） | 手工把 tar 放到共享盘 → `images load` → `images list` → 跑任务（P0 既有路径，CMP-08 保证不被破坏） |
| 组内其他用户 | `images list` 能看到本组镜像（组内共享）；可 `images delete` 删除本组镜像（与既有 docstring 一致） |
| 外部用户（`is_external=true`） | 与组内用户走**同一条**上传通道（既有 RustFS/S3）：一条命令把 tar 送进集群并自动登记；仅在需要人工干预时才回落到 `images load` |
| 平台运维 | 部署 registry 或初始化节点 `/data_local`；配置 busybox 基础镜像；灰度开关；回滚 |
| 平台服务（内部） | 加载执行方（任务/Job）通过 API-16 回报状态；launcher 读表注入 env |

---

## 3. 需求总览

### 3.1 功能需求（FR）

> **优先级口径（本分支）**：**P0 = 控制面/运行面**（旧分支已实现并在 103 实测通过）；**P1 = 上传通道**
> （本分支实现的**上传主入口**）。`P0` / `P1` 只标注**来源与阶段**，**不代表可选项**。

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
| FR-12 | **客户端失败提示**：`load`/`delete` 失败时打印服务端 `msg`，不再向用户抛裸 `Exception`/`AssertionError` 栈（修 I10） | P0 | 设计 §6.2 |
| FR-13 | **路径单点与校验**：新增镜像共享根配置（`image_path` 或等价单点），服务端用 `check_is_subpath` 校验 `image_tar` 与 `path` 均落在根内 | P0 | 设计 §3 |
| FR-14 | **空间回收（P2，本期只登记）**：`delete` 后可选的 registry tag / 共享盘 tar / 节点镜像清理与审计；本期仅保证**不误导用户**（文案与 `-a` 语义修正） | P2 | 设计 §7.3(P2) |
| FR-15 | **任务侧不变式不得回归**：K1–K5 五条契约（三段 URL、拼接口径、`status='loaded'`、按组隔离、报错指引）在改造后逐条保持 | P0 | 设计 §7.1 |
| FR-16 | **客户端 `images push`**：新增 `images push <本地 tar>`（可选 `--image name:tag`、`--no-load`、`--force`），一条命令完成「签发 STS → 直传对象存储 → 集群 stage2 落盘 → 自动登记」；复用 `haiworkspace` 的 push 实现（provider/分片/断点续传/状态机），**默认禁止 zip 包裹**（落盘必须是 tar 本身） | P1（本分支） | 设计 §6.4 |
| FR-17 | **服务端复用上传接口**：`file_type=image` 时 `/ugc/get_sts_token`（API-01）、`/ugc/sync_to_cluster`（API-05）、`/ugc/sync_to_cluster/status`（API-06）必须可用；`get_base_path` 的 IMAGE 分支**必须同时给出** `cloud_base_path`（S3 key 前缀）与 `cluster_base_path`（共享盘落点，落在 `image_path` 之下） | P1（本分支） | 设计 §4.6 · §5.6 |
| FR-18 | **上传后自动登记**：落盘成功后自动调用 API-15 登记（`image` 缺省由文件名派生）；重复 push 必须**幂等**（复用同一落点、不重复上传、不产生重复行） | P1（本分支） | 设计 §6.4 · §5.2 |
| FR-19 | **数据面开关同源**：`[image].enabled=false`（或灰度未命中）时，**上传通道（签发凭证 / 提交同步）同样必须被拒**并返回 `FEATURE_DISABLED`；只挡控制面 = 一级回滚不成立 | P1（本分支） | 设计 §9.5 |
| FR-20 | **失败可见与可重试**：上传/落盘失败必须打印可诊断原因（`index` / key / 目标路径 / 阶段），支持重试或断点续传；**未落盘成功的 tar 不得被登记为 `loaded`**（禁止「先登记后落盘」） | P1（本分支） | 设计 §4.6 · §7.5 |

### 3.2 非功能需求（NFR）

| ID | 需求 |
| --- | --- |
| NFR-01 | **幂等**：同参数重复调用 API-15 恢复到同一状态、同一行，不产生副作用；崩溃后重放安全 |
| NFR-02 | **性能**：`images list` 在单组 200 行量级下 P95 < 1s；不得引入全表扫描以外的逐行查询（禁止 N+1） |
| NFR-03 | **可观测**：加载全流程有结构化日志（`image_tar`/`image`/`task_id`/状态迁移/耗时）与至少 4 个指标（见设计 §9.4） |
| NFR-04 | **可测试性**：核心逻辑（路径校验、状态机、命名派生、组校验）必须可在**无 registry、无 k8s** 的条件下单元测试（对齐 `tests/env/` 的做法） |
| NFR-05 | **兼容**：新增字段一律**追加**，旧客户端忽略即可；旧调用形态（`load <tar>` 单参）继续可用 |
| NFR-06 | **资源**：加载执行体的资源占用可控（CPU/内存上限），不因大 tar 打爆服务器 pod |
| NFR-07 | **大文件**：上传必须分片且支持断点续传（复用既有 `part_size`/`slice_bytes` 机制），客户端内存占用不随 tar 体积线性增长；≥1 GB tar 在 103 上可完成上传 |
| NFR-08 | **不阻塞**：stage2（S3 → 共享盘）由平台 pod 的**进程池**执行，不得阻塞 ugc-server 事件循环（与 workspace/env 一致） |
| NFR-09 | **可观测**：上传通道必须有结构化日志（`user` / `file_type=image` / `name` / `index` / `key` / `stage` / `bytes` / `cost_ms`）与指标（提交数、字节数、失败 reason、阶段耗时），可据 `index` 串联客户端与服务端 |
| NFR-10 | **可测试性**：上传通道的 key 映射、落点校验、开关一致性、幂等键必须可在**无 S3、无 k8s** 条件下单元测试 |

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
| SEC-08 | **上传作用域最小化**：STS 授权前缀必须**恰好等于** `cloud_base_path`（本用户 + 本类型 + 本名字），跨用户/跨类型的 key 写入必须失败 |
| SEC-09 | **落点约束**：`cluster_base_path` 必须 `check_is_subpath(image_path, ...)`；`name` 不得含 `/` 或 `..`；上传的相对路径不得逃逸出落点（复用 stage2 既有 `check_is_subpath`） |
| SEC-10 | **凭据不落盘/不进任务**：对象存储凭据只发给发起上传的客户端进程，**不得**写入共享盘、`train_image`、任务 env 或 initContainer |

### 3.4 运维需求（OPS）

| ID | 需求 |
| --- | --- |
| OPS-01 | **开关与灰度**：`image.enabled` / `enabled_groups` / `enabled_users` 三级灰度（对齐 `cloud.storage` 既有做法），关闭时不抛 500 |
| OPS-02 | **回滚**：一级（关开关）即可让 `load/delete` 失败关闭并提示，不影响 `list` 与内建镜像路径 |
| OPS-03 | **迁移**：新增列/索引必须走 `db_schemas/*.sql` + `init_postgresql.sh` 全量重放机制，且脚本**幂等**（`if not exists`） |
| OPS-04 | **节点前置**：`/data_local` 的存在性必须由部署流程保证（或改由配置/`DirectoryOrCreate` 显式声明），并纳入部署自检 |
| OPS-05 | **空间回收**：P2 提供 `train_image` 的孤儿行/孤儿镜像审计（本期只登记） |
| OPS-06 | **上传开关**：上传可独立关闭（建议 `[image].upload_enabled`）或与 `enabled` 同源；关闭时客户端得到明确提示，不得出现「传上去了但登记不了」的中间态 |
| OPS-07 | **容量与上限**：单 tar 大小上限可配置（建议 `[image].max_tar_bytes`），超限**快速失败**并在客户端提示；共享盘容量属运维监控项 |
| OPS-08 | **枚举迁移**：`user_sync_status.file_type` 是 PostgreSQL **enum**（`workspace/dataset/env/doc/pypi/website`），上传状态落库前必须新增幂等迁移 `db_schemas/036`：`alter type file_type add value if not exists 'image'`；**未迁移前上传状态落库必然失败** |
| OPS-09 | **排障链路**：一次上传必须能按 `index` / `name` 串起「客户端日志 → ugc-server 日志 → 共享盘落点 → `user_sync_status` 行 → `train_image` 行」 |

### 3.5 兼容需求（CMP）

| ID | 需求 |
| --- | --- |
| CMP-01 | 旧客户端 `images load <image_tar>`（单参、无 `image`）必须继续可用 |
| CMP-02 | 旧客户端 `images delete <image>` 签名不变 |
| CMP-03 | `train_environment` / `mars_images` 的结构与语义**零改动** |
| CMP-04 | `registry` 列默认值 `registry.high-flyer.cn` 保持；新代码不得依赖该默认值可达 |
| CMP-05 | `user_images` 由 `[]` 变为有内容属于**行为修正**（非破坏），但行内字段名不得改名 |
| CMP-06 | 私有 `custom.py` 覆盖接缝保持有效（`default.py`/`implement.py`/`custom.py` 三层约定不得破坏） |
| CMP-07 | 既有 `workspace` / `env` 的 push/pull 行为与 `file_type` 语义**零改动**（上传通道是**加分支**，不是改语义） |
| CMP-08 | 旧客户端（没有 `images push`）继续可用：手工把 tar 放到共享盘 + `images load` 的既有路径不得被破坏（兼容/运维旁路） |
| CMP-09 | 上传状态复用既有 `user_sync_status` 表（主键 `(user_name, file_type, name)` 不变），**不新建表** |

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
| HC-11 | `file_type=image` 必须**同时**在 `get_base_path`（路径派生）、`get_bucket_name`（bucket）、`submit_to_cluster`（白名单）三处放行；缺任意一处上传都只是「半通」 | 三处各自独立判断，缺一处会得到 400/PATH_ESCAPE/权限错误等**互不相同的失败面** |
| HC-12 | 上传通道必须调用 `check_image_enabled`（与控制面同一个开关函数），**禁止**只挡控制面 | env 特性 **N4** 的教训：只挡控制面时一级回滚不成立 |
| HC-13 | 上传落点必须落在 `image_path` 之下（与 API-15 的 `check_is_subpath` 同源） | 否则会出现「上传成功但 `load` 报 PATH_ESCAPE」的割裂体验 |
| HC-14 | 节点侧（initContainer / 任务容器）**不得**持有对象存储凭据，仍只读 `HFAI_IMAGE_WEKA_PATH` | `init_manager` 只注入 `HFAI_IMAGE*`；SEC-10 的落地约束 |

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
| 兼容 | 只传 `image_tar` 的旧形态必须可用（CMP-01）；**本分支 `images push` 落盘成功后自动调用本接口**（FR-18） |

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

### 4.6 上传通道（本分支主入口）：复用 workspace/env 的既有接口 + API-19 预检

> **这是本分支的主入口链路**：`hai-cli images push` 的全部服务端能力来自下面的**复用接口 + 预检**，
> 不新增传输层、不新增上传接口族。
>
> **设计立场**：**不新造传输层、不新造上传接口族**。上传 = 「API-01 签发凭证 → 客户端直传对象存储 →
> API-05 提交 stage2 → API-06 轮询状态」，与 `workspace`/`env` 完全同一条流水线，只是 `file_type='image'`。

| 接口 | 方法与路径 | 与既有接口的关系 | image 专属语义 |
| --- | --- | --- | --- |
| **API-01** | `POST /ugc/get_sts_token` | **复用**（workspace API-01） | `file_type=image`；`name` 为镜像条目名（单段，不含 `/`）；返回 STS 的授权前缀 = `cloud_base_path`（Q-10 推荐 `{group}/shared/images/{user}/{name}`） |
| **API-05** | `POST /ugc/sync_to_cluster` | **复用**（workspace API-05） | `file_type=image`、`no_zip=true`、`files=[相对路径]`；落盘 `cluster_base_path/<相对路径>`，且 `cluster_base_path` 必须在 `image_path` 之下（Q-9 推荐 `{image_path}/{name}`） |
| **API-06** | `GET /ugc/sync_to_cluster/status` | **复用**（workspace API-06） | 以 `index` 轮询；终态 `FINISHED` / `STAGE2_FAILED` 必须带可读 `msg` |
| **API-19** | `POST /ugc/user/train_image/push_precheck` | **新增**（对标 env 的 API-11；Q-11 建议冻结为「新增」） | 入参 `image`（可选，缺省由文件名派生）；返回 `name` / `cluster_path` / `cloud_path` / `index` / `exists`（共享盘是否已有）/ `registered`（是否已有 `loaded` 行）；用于**落点协商 + 幂等 + 跳过重复上传** |

| 情形 | 期望 |
| --- | --- |
| `enabled=false` 或灰度未命中 | API-01 / API-05 一律 `FEATURE_DISABLED`（HTTP 200，`success:0`），**不产生任何共享盘写入**（FR-19 / HC-12） |
| `name` 含 `/`、`..`，或落点逃逸 | `INVALID_PARAM` / `PATH_ESCAPE`，且不写盘（SEC-09） |
| 单 tar 超过上限 | `PAYLOAD_TOO_LARGE` 或专用 `IMAGE_TAR_TOO_LARGE`，快速失败（OPS-07） |
| 同一 tar 重复 push | 复用同一 `index` 与落点：已 FINISHED 则跳过上传（幂等），返回「数据已同步」语义（FR-18） |
| 上传中断后重试 | 断点续传或 `force` 重提；不得留下半成品 `train_image` 行（FR-20） |
| 落盘成功但登记失败 | 客户端**必须**显式报错并提示「可重试 `load`」；不得静默当作成功（FR-20） |

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

### 5.3 上传状态与数据（P1：上传通道）

| 数据 | 位置 | 说明 |
| --- | --- | --- |
| 上传状态 | PostgreSQL `user_sync_status`（DDL `db_schemas/011.table_user_sync_status.sql`），`file_type='image'` | **复用**既有表与状态机（`init → stage1_running → stage1_finished → stage2_running → finished / stage2_failed`）；主键 `(user_name, file_type, name)` 不变（CMP-09） |
| 枚举扩展 | `db_schemas/036.file_type_enum_add_image.sql`（新增） | `alter type file_type add value if not exists 'image'`；**幂等**、由 `init_postgresql.sh` 重放（OPS-08 / HC-06） |
| 落点映射 | `cloud_base_path` / `cluster_base_path` | 由 `get_base_path(..., FileType.IMAGE)` 单点派生；`cluster` 必须在 `image_path` 之下，`cloud` 决定 STS 授权前缀（SEC-08 / HC-13） |
| 幂等键 | `index = hashkey(token, name, file_type, *files)` | 既有实现；重试/重复请求靠它命中（FR-18） |

> **迁移前置（OPS-08）**：`user_sync_status.file_type` 的 `image` 取值依赖幂等迁移
> `db_schemas/036.file_type_enum_add_image.sql`；**未迁移前上传状态落库必然失败**（PostgreSQL enum 会拒绝
> `file_type='image'`，上传会停在 stage1 之后拿不到状态行）。因此该迁移是上传通道的**前置条件**，
> 必须与 S8 一并交付并纳入 AC-12 的重放验证。

> **两条状态机必须解耦**：`user_sync_status`（**字节有没有搬完**）与 `train_image.status`（**镜像能不能被任务用**）
> 是两套状态。**只有共享盘落盘成功之后**才允许写 `train_image.status='loaded'`；反之，
> `delete`（`train_image`）**不删**共享盘 tar 与对象存储对象（P2 才做回收，FR-14）。

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
| AC-12 | **迁移可重放**：`init_postgresql.sh` 重复执行不报错、列只加一次（含 036 的 enum 扩展） | 重放两次并对比 `\d train_image` / enum 取值 |
| AC-13 | **兼容**：旧形态 `load <tar>`（单参）与旧 `list` 字段消费零改动可用 | 用例 TC-O 组 |
| AC-14 | **文档一致**：分析 §6 的 I1–I18 全部在需求/设计/用例/Checklist 中有对应处置或显式「不处置」理由 | 本文 §11 追溯矩阵 + Checklist ACC |
| AC-15 ✅ **已实测** | **上传闭环（本分支发布门禁）**：本地（共享盘之外）的 tar → `images push` → 共享盘出现同一文件（**md5 一致**）→ `train_image` 有 `loaded` 行 → 用该镜像跑任务 `succeeded` 且输出可区分 | E2E-09 已执行：`e2e_images_push.sh` **PASS=33 WARN=1 FAIL=0**（test-report §9.2）；主判据三条全部有原文输出 |
| AC-16 ✅ **已实测** | **开关一致性**：`[image].enabled=false` 时 API-01/API-05 均 `FEATURE_DISABLED`、共享盘与对象存储**零新增写入**；恢复后同参数可用 | E2E-10 已执行（含 `upload_enabled` 独立开关）；见 test-report §9.2 |
| AC-17 🟡 **部分实测** | **幂等与续传**：同 tar 重复 push 不重复上传（`index` 命中）、不产生重复 `train_image` 行；中断后重试/续传可成功 | 幂等部分 ✅（TC-UP-06，实测「跳过上传 + mtime 不变 + 仍 1 行」）；**「中断后重试/续传」未做故障注入**（FI-09，见 test-report §9.3） |
| AC-18 ✅ **已实测** | **上传通道零回归**：`workspace` / `env` 的 push/pull 行为不变（`smoke_ugc 8/8`、`e2e_workspace 19/19`、`smoke_env 20/20`、`e2e_env 16/16`） | 同一套脚本在并入后的基线上全绿（test-report §9.1）；另实测手工放盘路径可还原（§9.2） |

> **本分支发布门禁 = AC-15**（上传闭环）。AC-15 的**主判据**是三条实测证据缺一不可：
> ① **共享盘文件 md5 与本地 tar 一致**；② `train_image` 存在该镜像的 **`loaded` 行**（`path` 已回填）；
> ③ **用该镜像提交任务 `succeeded` 且输出可区分**（镜像内预置探针，日志可验）。**「接口 200」不算通过。**
> AC-16 / AC-17 / AC-18 与 AC-15 同批签署（Checklist 阶段 17，GATE-09）。

---

## 7. 端到端验证方法

> **两条路径的分工（本分支）**：**路径 ①（主路径，本分支）** 是 `images push` 上传闭环，对应
> **AC-15（本分支发布门禁）**；**路径 ②（兼容旁路）** 是手工放盘 + `images load`，即 P0 已实测通过的既有序列，
> 本分支只需保证**不回归**（CMP-08）。

### 7.1 路径 ①（主路径，本分支）：本地 tar → `images push` → md5 核对 → `images list` → 提交任务

> 用例：**E2E-09（上传闭环）** / **E2E-10（开关一致性）**；脚本：
> `docs/haiplatform/scripts/e2e_images_push.sh` —— 已随 S8 落地，实测 `PASS=33 WARN=1 FAIL=0`（见 §9 与 [images-server-test-report.md](images-server-test-report.md) §9.2）。

```
# ① 前置：确认开关、共享根、节点前置
sudo -u fireflyer hai-cli images list                       # 期望：内建镜像 1 行 + 用户镜像（可为空）

# ② 在共享盘之外准备 tar（本地目录必须不在共享盘内，否则出现「数据已同步」假象）
mkdir -p /tmp/hai-image-push && cd /tmp/hai-image-push
docker save <本地镜像> -o demo.tar
md5sum demo.tar                                             # 记下本地 md5，供 ④ 逐字节核对

# ③ 一条命令上传 + 自动登记（本分支主入口；默认 push 成功后自动 load）
sudo -u fireflyer hai-cli images push /tmp/hai-image-push/demo.tar --image demo:v1
# 期望：打印 index / cloud key / cluster 落点与 stage1→stage2 进度，最后 success=1

# ④ md5 核对 + 列表
md5sum /nfs-shared/hai-platform/<image_path>/demo/demo.tar   # 期望：与 ② 的 md5 完全一致
sudo -u fireflyer hai-cli images list                        # 期望：该行 processing → loaded，path 已回填
sudo kubectl -n hai-platform exec hai-platform-0 -- \
  psql -U root -d mars_db -c "select image_tar,image,path,status,task_id from train_image;"

# ⑤ 用该镜像跑任务（AC-15 的关键一步：必须有真实产出，不看 200）
sudo -u fireflyer hai-cli python /tmp/probe.py -- --image <registry>/<group>/demo:v1 -n 1
sudo -u fireflyer hai-cli status <task_id>                   # 期望：pod succeeded
sudo -u fireflyer hai-cli logs <task_id>                     # 期望：probe 输出符合预期（证明用的是自定义镜像）

# ⑥ 幂等：同 tar 再 push 一次 → 期望 index 命中、不重复上传、不产生重复 train_image 行
sudo -u fireflyer hai-cli images push /tmp/hai-image-push/demo.tar --image demo:v1
```

**通过判据（AC-15）**：③ 落盘成功且 `index` 轮询到 `FINISHED`；④ 共享盘文件 **md5 与本地一致**
且 `train_image` 有 `loaded` 行；⑤ 任务 `succeeded` 且输出**能被自定义镜像内容区分**
（例如镜像内预置一个探针文件/包，`probe.py` import 它）；⑥ 重复 push 幂等。
若落盘成功但登记失败，客户端必须**显式报错**并提示可重试 `load`（FR-20），不得静默当作成功。

### 7.2 路径 ②（兼容/运维旁路）：手工放 tar 到 `image_path` + `images load`（AC-01）

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

**通过判据（AC-01）**：③ 返回 `success=1`；④ 状态收敛到 `loaded` 且 `path` 已回填；**⑤ 任务 `succeeded` 且输出
能被自定义镜像内容区分**（例如镜像内预置一个探针文件/包，`probe.py` import 它）；⑥ 删除后任务提交被拒。

---

## 8. 待确认决策

| ID | 问题 | 建议（推荐值） | 影响 | 状态 |
| --- | --- | --- | --- | --- |
| Q-1 | 数据面后端：`registry`（push 内网 registry）还是 `node_local`（按节点 link，运行期已有此机制）？ | **按 §4.6 证据选「node link」为主线**：运行期本来就是 link，registry 只是命名 | 决定 FR-09 实现与 103 可验证性（AC-09） | 已在旧分支冻结（见 [images-server-decisions.md](images-server-decisions.md) §2） |
| Q-2 | `image_tar` 唯一索引**不含 `shared_group`**：两个组加载同一 tar 会冲突，是否改索引？ | 改为 `(shared_group, image_tar)` 唯一（新迁移文件，幂等） | 影响 I13 与 API-15 的 upsert 冲突语义 | 已在旧分支冻结（decisions §2） |
| Q-3 | 同名不同 tar（`image` 相同）是否允许？客户端已按名字去重 | **允许**（保留历史行），但 `delete` 作用于该名字全部行；提交校验只看 `status='loaded'` | 影响 `images list` 展示（客户端 `last_img_status` 逻辑） | 已在旧分支冻结（decisions §2） |
| Q-4 | 加载由**平台任务**执行（复用 `task_id` 列）还是**专用 k8s Job**？ | 复用平台任务：`task_id` 列与 `update_status` 桩都指向该模型 | 影响 FR-09/FR-10 与工作量（设计 ADR-I3） | 已在旧分支冻结（decisions §2） |
| Q-5 | `/data_local` 从哪来？谁初始化？ | 部署流程显式创建（或把 hostPath 改为 `DirectoryOrCreate` 并写进部署自检 OPS-04） | 影响 AC-08 与 I17① | 已在旧分支冻结（decisions §2） |
| Q-6 | initContainer 基础镜像用哪个？ | 改为**可配置** `manager.image_load_helper_image`，103 用节点已有的 `docker.io/library/busybox:latest` | 影响 I17② | 已在旧分支冻结（decisions §2） |
| Q-7 | 内网 registry 在 103 是否需要部署？ | 若采纳 Q-1 的 node link，**不需要**；仅生产可选 | 影响工作量与 AC-09 | 已在旧分支冻结（decisions §2） |
| Q-8 | 是否新增 `user_name` 列记录归属？ | **建议加**（便于审计与「谁加载的」追溯），但这与「组内共享、组内可删」不冲突 | 影响 §5.1 迁移范围 | 已在旧分支冻结（decisions §2） |
| Q-9 | 上传对象是**单文件**（`{image_path}/<name>.tar`）还是**目录 + tar**（`{image_path}/<name>/<file>.tar`）？ | **推荐「目录 + tar」**：`cluster = {image_path}/{name}` —— `get_base_path` 的 IMAGE 分支现为 `{image_root}/{name}`（目录语义），且目录允许同名多版本/审计；单文件方案要改分支语义 | 影响 FR-17/FR-18、落点与 `load` 入参形态 | **已冻结并实现**（提交 `fc773e5`） |
| Q-10 | S3 key 布局（`cloud_base_path`）取什么？ | **推荐 `{group}/shared/images/{user}/{name}`**，与 env 的 `{group}/shared/hfai_envs/...` 对齐；bucket 沿用 `private_bucket`（`get_bucket_name` 已天然支持） | 影响 STS 授权前缀与对象布局（SEC-08） | **已冻结并实现**（提交 `fc773e5`） |
| Q-11 | 是否新增 API-19 预检接口？ | **推荐新增**：否则客户端只能「先传再登记」，重复上传只能靠 stage2 的 `index` 兜底，也无法在上传前告知「已在集群且已 `loaded`」 | 影响 FR-18 的体验与 S8 工作量（新增 S8-5 预检任务，估时见 [images-server-task-list.md](images-server-task-list.md) §3.9） | **已冻结并实现**（提交 `fc773e5`） |
| Q-12 | push 成功后是否**自动 `load`**？ | **推荐自动**（提供 `--no-load`）：与用户心智一致；错误面需区分「上传失败」与「登记失败」 | 影响 §4.6 错误面与 E2E 步骤 | **已冻结并实现**（提交 `fc773e5`） |

> **冻结口径**：**Q-1~Q-8 已在旧分支冻结**（[images-server-decisions.md](images-server-decisions.md) §2，本分支不再重议）；
> **Q-9~Q-12 为「建议冻结（推荐项）」**，**本文档集按推荐项展开**（§4.6 / §5.3 / §6 / §7 均按推荐值书写），
> 正式冻结在 **S0 / GATE-09** 复核；若复核改判，需同步回写 §4.6、§5.3、§7 与任务列表 S8-1/S8-4/S8-5。

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
| 实施决策记录 | `docs/haiplatform/images/images-server-decisions.md` |
| 运行期脚本（P0，随 S9 并入） | `marsv2/scripts/link_hfai_image.sh` + `one/hai-up.sh` 挂载种子 |
| 迁移（P0，随 S9 并入） | `db_schemas/035.table_train_image_alter.sql` |
| 迁移（本分支） | `db_schemas/036.file_type_enum_add_image.sql`（`file_type` 枚举加 `image`，幂等） |
| 客户端上传（本分支） | `images push`（复用 `plugins/haiworkspace` 的 push 实现；`--image` / `--no-load` / `--force`） |
| 验证脚本（本分支） | `docs/haiplatform/scripts/e2e_images_push.sh`（上传闭环 + 开关一致性 + 负例）——**已落地并实测** |
| 验证脚本（P0，随 S9 并入） | `docs/haiplatform/scripts/smoke_images.sh` / `e2e_images.sh` |

> **资产状态与工作量口径**：上表中 `docs/haiplatform/scripts/` 下的 images 脚本与 `tests/images/`
> **已并入本分支**（S9-1 并入 P0 资产，S8 落地上传通道资产）；实测结果见 [images-server-test-report.md](images-server-test-report.md) §9.1/§9.2。
> 本分支新增工作量合计 **3.0 人日** = **S8（上传通道，2.0）** + **S9（P0 资产并入与入口切换，1.0）**；
> **P0 = 13.0 人日**（旧分支已完成并 103 实测通过）；**特性总计 = 16.0 人日**。

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
| `cloud_base_path` | 对象存储（RustFS/S3）上的 **key 前缀**（推荐 `{group}/shared/images/{user}/{name}`）；也是 STS 的授权前缀（SEC-08） |
| `cluster_base_path` | 共享盘上的**落点目录**（推荐 `{image_path}/{name}`）；必须在 `image_path` 之下（HC-13） |
| `index` | 一次同步的**幂等键**：`hashkey(token, name, file_type, *files)`；重试/重复请求靠它命中 |
| stage1 / stage2 | stage1 = 客户端 → 对象存储；stage2 = 对象存储 → 集群共享盘（`sync_to_cluster`） |

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
| G1..G6 | 全局目标（P0，经 S9 并入并保不回归） | §1 / §2 | AC-01..AC-14 |
| **—— 上传通道（本分支主入口，P1）——** | | | |
| G7 ✅ **已达成** | 上传闭环（**本分支首要目标**）：一条命令把 tar 送进集群并直接可被任务使用 | §1 / §6.4 / §14(S8) | AC-15（已实测） |
| FR-16 | 客户端 `images push`（`--image` / `--no-load` / `--force`，恒 `no_zip`） | §6.4 / §14(S8) | AC-15 / AC-17 |
| FR-17 | API-01/05/06 的 `file_type=image` + `cloud_base_path`/`cluster_base_path` 单点派生 | §3.5 / §4.6 / §5.6 | AC-15 |
| FR-18 | 落盘成功后自动登记（API-15）+ 重复 push 幂等 | §6.4 / §5.2 | AC-15 / AC-17 |
| FR-19 | 数据面开关同源（`check_image_enabled`） | §9.5 / §4.6 | AC-16 |
| FR-20 | 失败可见与可重试（禁止「先登记后落盘」） | §4.6 / §7.5 | AC-17 / AC-18 |
| NFR-07..10 | 大文件 / 不阻塞 / 可观测 / 可测试 | §4.6 / §5.6 / §9.4 | AC-17 / 压测 |
| SEC-08..10 | 授权前缀 / 落点约束 / 凭据不进任务 | §4.6 / §5.6 / §10 | AC-16 / 安全用例 |
| OPS-06..09 | 上传开关 / 容量上限 / 枚举迁移 036 / 排障链路 | §3.5 / §9.1 / §9.5 / §7.5 | AC-16 / AC-17 / AC-18 |
| CMP-07..09 | workspace/env 零改动 / 旧客户端可用 / 不新建表 | §8 | AC-18 |
| HC-11..14 | 三处白名单 / 开关同源 / 落点同源 / 节点无凭据 | §4.6 / §5.6 / §7.5 | 代码评审 |
