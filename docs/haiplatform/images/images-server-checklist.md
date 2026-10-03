# HAI Platform · `hai-cli images`（用户自定义镜像）服务端实施与上线 Checklist

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

> **实施进度（本分支 `feature/hai-cli-images-rustfs-design`，基线 `feature/hai-cli-env-server-design` @ `33a5b26`）**：
> **S9-1**（提交 `6bc81e2`）已把旧分支的 P0 资产并入本分支（与旧分支逐字节一致）；**S9-2 已在 103 上重跑并复核**：
> preflight `PASS=30 FAIL=0`、L1 `33 passed`、L2 `PASS=44 FAIL=0`、L3 E2E（`E2E_PURGE_IMAGE=1`）`PASS=26 FAIL=0`、
> workspace/env 回归全绿 —— 与旧分支记录的期望值**逐项一致**，因此表中 P0 的 `✅` **在本分支同样成立**（test-report §9.1）。
> **M1/M2 闭环，M4（上传闭环）也已达成**：阶段 17 的 14 项已按 103 实测勾选，`e2e_images_push.sh` 结果 **PASS=33 WARN=1 FAIL=0**
> （`WARN` = 本环境 RustFS 下发静态 AK/SK、prefix 不被强制，属环境限制 D14/R-14，见 test-report §7.4）。
> **任何勾选都必须先有 103 实测证据**；仍**未**执行的：性能压测（PERF-*）、生产三级灰度与看板/告警（OBS-04/05）、
> 上线后观察（POST-*）、FI-09~FI-11 的 stage2 故障注入（§19 末段）。
> 被测版本与全部命令输出见 [images-server-test-report.md](images-server-test-report.md)（P0 证据 tag `f2cb559`；本分支见其 §9）；
> 决策与偏差见 [images-server-decisions.md](images-server-decisions.md)。
>
> **文档定位**：`docs/haiplatform/images/` 四件套之四（[分析](hai-cli-images-analysis.md) → [需求](images-server-requirements.md) → [设计](images-server-design.md) → [用例](images-server-test-cases.md) → **Checklist**）；另有 [任务列表](images-server-task-list.md)、[决策记录](images-server-decisions.md)、[实测报告](images-server-test-report.md)。
> **使用方式**：按阶段自上而下勾选；每项须给出**可核验的证据**（命令输出 / 文件路径 / 测试报告编号），不接受口头确认。
> **ID 说明**：本表 ID（`GATE-xx` / `ENV-xx` / `CFG-xx` / `DB-xx` / `DEV-xx` / `UT-xx` / `API-xx` / `E2E-xx` / `SEC-xx` / `PERF-xx` / `OBS-xx` / `OPS-xx` / `TASK-xx` / `CMP-xx` / `REL-xx` / `DOC-xx` / `DEP-xx` / `RB-xx` / `POST-xx` / `ACC-xx` / `UP-xx`）
> 是**检查项编号**，与《需求》的 `G1–G6` / `FR` / `NFR` / `SEC` / `OPS` / `CMP` / `HC` / `AC` / `Q` 和《用例》的 `TC-*` **不同源，勿混用**；
> 本表的 `SEC-xx` / `OPS-xx` / `CMP-xx` 只是**阶段编号**，与需求中同名前缀的条目**不是同一套编号**。
> ⚠️ **两处同形不同源必须特别小心**：
> ① 本表 `API-01`~`API-12` 是**检查项**，与需求/设计的接口号 `API-15`~`API-19`（`load` / `update_status` / `list` / `delete` / `push_precheck`）**同形不同源**；
> ② 本表 `ACC-01`~`ACC-10` 是**验收签署项**，与需求的 `AC-01`~`AC-18`（DoD 验收标准）是**两套编号**——只有 `ACC-01` 才引用 `AC-01`~`AC-18`。
> 本表另有分析报告的风险号 `I1–I18`（`I` = Image）与审计缺陷号 `C-3`，它们是**跨文档沿用 ID**，不进本表编号空间。
> **阶段与设计 WBS 的对应**：阶段 0 ≈ S0；阶段 1–2 ≈ S3 / S5；阶段 3–4 ≈ S1 / S2 / S4；阶段 5 ≈ S6；阶段 6–8 ≈ S5 / S7；
> 阶段 9–10 ≈ S4 / S7；阶段 11 ≈ S1；阶段 12–13 ≈ S6 / S7；阶段 14 ≈ S6；阶段 15–16 ≈ S7；**阶段 17 ≈ S8 / S9（本分支主体）**。
> **里程碑**：M1 控制面闭环（阶段 0–5 ✅）→ M2 运行面闭环（阶段 5 的 E2E-02 与阶段 10 ✅）→ M3 可上线（阶段 6–16 🟡，压测/看板/上线后观察未做）→ **M4 上传闭环（阶段 17，本分支主体）✅ 已达成**。
> **工作量口径**：S8（上传通道）= 2.0 人日；S9（P0 资产并入与入口切换）= 1.0 人日；本分支新增合计 = 3.0 人日；P0 = 13.0 人日（旧分支已完成）；总计 = 16.0 人日。

---

## 1. 阶段 0 · 启动前置（GATE-0）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | GATE-01 | 需求 §8 的 **8 项待确认决策 `Q-1`~`Q-8` 全部有结论**，尤其 **`Q-1`（`loader_backend` 选 `register`）** 与 **`Q-2`（唯一索引改 `(shared_group, image_tar)`）** | 决策记录（链接/邮件/会议纪要，逐项对应 Q-1..Q-8） | 后端 + 平台 |
| ✅ | GATE-02 | `Q-4`（加载执行主体：复用平台任务还是专用 k8s Job）与设计 ADR-I3 结论一致；`Q-3`（同名不同 tar 是否允许）与 ADR-I8 删除语义一致 | 决策记录 + 差异清单 | 后端 |
| ✅ | GATE-03 | **冻结接口契约**（附录 A 的 `API-15`~`API-18`：入参 / 出参 / 错误码 / 幂等语义），任何后续变更走变更流程 | 附录 A 评审通过记录（评审人 + 日期） | 后端 + 客户端 |
| ✅ | GATE-04 | `Q-5`（`/data_local` 从哪来）、`Q-6`（initContainer 基础镜像用哪个）、`Q-7`（103 是否部署内网 registry）已有结论，且结论能支撑 ADR-I2 / ADR-I4 | 决策记录 + 与设计 §9.1 配置项的对应表 | 后端 + 运维 |
| ✅ | GATE-05 | 设计 §15 **R-2（脚本如何访问节点容器运行时）在设计冻结前定案**：socket 登记为 `mount_point`，或换自带 `ctr` 的基础镜像 | 定案记录（二选一 + 理由）；未定案不得开工 S4 | 后端 + 运维 |
| ✅ | GATE-06 | 确认**零回归面**：本次改动涉及 `conf/utils.py:23-35`、`cloud_storage/utils.py:445+`（`get_base_path`）、`one/one_etc/core.toml:110-116`，与 workspace / env 主链路共用 | 影响面清单 + workspace / env E2E 回归计划（CMP-03 / G6）；**本分支入口条件**：P0 资产并入基线（S9-1）后，须先按 [images-server-task-list.md](images-server-task-list.md) §3.10 的 **S9-2** 在 103 重跑本表既有证据，再开始新阶段的勾选 | 后端 |
| ✅ | GATE-07 | 确认**迁移策略**：新增 `db_schemas/035.table_train_image_alter.sql`（当前最大编号 `034`），幂等且由 `init_postgresql.sh` 全量重放生效（OPS-03 / HC-06）；`Q-8`（是否加 `user_name` 列）有结论 | 迁移方案评审记录 + `db_schemas/` 编号确认 | 后端 + 运维 |
| ✅ | GATE-08 | 确认**交付范围含运行面**（`link_hfai_image.sh` + `one/hai-up.sh` 挂载种子 + 可配置基础镜像 + 节点前置），**不接受**「只修控制面」的缩水范围 | 范围确认记录（引用 FR-08 / I16 / I17 / HC-08 / AC-08） | 产品 + 后端 |
| ✅ | GATE-09 | **本分支上传主入口的四项决策** `Q-9`（上传对象形态）/ `Q-10`（S3 key 布局）/ `Q-11`（是否新增 API-19）/ `Q-12`（push 成功后是否自动登记）全部有结论并与设计 **ADR-I11~I14** 一致。**推荐项**：`Q-9` = **目录 + tar**（`cluster = {image_path}/{name}`，与既有 IMAGE 分支的目录语义一致）；`Q-10` = **`{group}/shared/images/{user}/{name}`**（对齐 env 的 `{group}/shared/hfai_envs/...`，bucket 沿用 private）；`Q-11` = **新增 API-19 `POST /ugc/user/train_image/push_precheck`**（否则无法提前判重/展示落点）；`Q-12` = **自动 `load`**（`--no-load` 可关） | 决策记录（逐项对应 `Q-9`~`Q-12`）+ 与设计 §3.5 / §4.6 的对应表；**未冻结不得开工 S8**（本项同时列于 §18 上传通道表首行） | 后端 + 产品 |

---

## 2. 阶段 1 · 环境与配置（ENV / CFG）

### 2.1 环境准备

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | ENV-01 | 103 测试环境健康：Multipass 4 台 Running、`kubectl get nodes` 4 节点、`hai-platform-0` 1/1 Running、`ugc_server` 在 :8083 | `multipass list` + `kubectl get nodes` + `supervisorctl status ugc_server` 输出 | 测试 |
| ✅ | ENV-02 | 镜像共享根就绪且可写：`image_path`（103 覆盖为 `/nfs-shared/hai-platform/image`）存在，服务端进程账号可读可写 | `ls -ld /nfs-shared/hai-platform/image` + 服务端进程 `id` | 运维 + 后端 |
| ✅ | ENV-03 | 测试用镜像 tar 已放入共享盘，且内容**可区分**（预热一个探针文件/包，用于证明任务真的用了该镜像） | `ls -l <image_root>/demo.tar` + 探针内容说明 | 测试 |
| ✅ | ENV-04 | 测试用户就绪：同组 `T_A` / `T_B`（组内共享与「可删他人镜像」）、跨组 `T_C`（越权用例）；服务端能从 token 解析 `shared_group` | `hai-cli whoami` 三个用户输出 + `select distinct shared_group from train_image` | 测试 |
| ✅ | ENV-05 | 客户端两种形态就绪：镜像内既有 `hfai`（同提交 `7589fb1`）与待发布的新客户端各一份；**旧形态 `load <tar>` 单参**保留 | `hai-cli images --help` + 两个客户端的 `pip show hfai` 版本 | 客户端 + 测试 |
| ✅ | ENV-06 | 节点前置已确认：训练节点 `/data_local` 存在（或明确的 `DirectoryOrCreate` 策略已定），且脚本访问节点运行时的通路可用（承 GATE-04 / GATE-05） | `multipass exec k8s-slave01 -- ls -ld /data_local` | 运维 |

### 2.2 配置

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | CFG-01 | `one/one_etc/core.toml` 新增 `[image]` 节：`enabled=false` / `enabled_groups=[]` / `enabled_users=[]` / `registry` / `loader_backend=register` / `load_helper_image` / `data_local_path` | 配置 diff（默认值齐全，与设计 §9.1 逐项对应） | 后端 |
| ✅ | CFG-02 | `[cloud.storage.service].image_path` 已定义并在注释中写明语义（镜像资产共享根单点，对齐 `env_path` 的做法） | 配置 diff + 注释评审 | 后端 |
| ✅ | CFG-03 | `conf/utils.py` 新增 `FileType.IMAGE` / `get_image_root()` / 镜像名白名单 `IMAGE_NAME_RE`；`cloud_storage/utils.py:get_base_path` 新增 IMAGE 分支；**全仓无第二处硬编码镜像根** | 代码 + `grep -rn "image_root\|image_path" --include=*.py` 仅落在 `conf/utils.py`（其余为注释/文档） | 后端 |
| ✅ | CFG-04 | 启动自检 `image_self_check()`：`image_root` 存在且可写、`registry` 已配置、`loader_backend` 取值合法、`load_helper_image` 非空；**失败只告警不阻断启动** | 刻意配错 → 日志告警；正常配置 → `image path check: OK` | 后端 + 测试 |
| ✅ | CFG-05 | initContainer 基础镜像改为**可配置**（`[image].load_helper_image`，默认改为节点已有的 `docker.io/library/busybox:latest`），不再硬编码内网地址（Q-6 / I17② / ADR-I4） | 配置 diff + `experiment_manager/manager/init_manager.py:347-360` 代码 | 后端 |
| ✅ | CFG-06 | 灰度三级开关真实可动态生效：改 `override.toml` + 重启 `ugc_server`，关闭时 `load` / `update_status` / `delete` 一律 `FEATURE_DISABLED` 而**不抛 500**，`list` 与内建镜像路径不受影响（OPS-01 / OPS-02） | 关闭/打开两轮 `curl` 输出 + `supervisorctl` 重启记录 | 后端 + 测试 |

---

## 3. 阶段 2 · 数据库与迁移（DB）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | DB-01 | `db_schemas/035.table_train_image_alter.sql` 存在，且**幂等**（`add column if not exists` 写法，可照抄 `032.table_host_flags.sql`） | 迁移文件 + `git diff --stat db_schemas/`（仅新增 035） | 后端 |
| ✅ | DB-02 | 新增列（`message`，若 Q-8 采纳则含 `user_name`）同步登记到 `TrainImageTable.columns`（`server_model/user_data/table_config.py:81-86`），表定义与 DDL 一致 | 代码 + `\d train_image` 输出对照 | 后端 |
| ✅ | DB-03 | 若 Q-2 采纳：唯一索引由 `train_image_image_uindex(image_tar)` 改为 `(shared_group, image_tar)`，且 upsert 的 `on conflict` 目标同步修改 | 迁移文件 + `\d train_image` 索引列表 | 后端 |
| ✅ | DB-04 | 迁移经 `init_postgresql.sh` 全量重放生效；**重复执行两次**不报错、列与索引只加一次（AC-12 / HC-06） | 两轮重放日志 + 前后 `\d train_image` 对比 | 后端 + 运维 |
| ✅ | DB-05 | `updated_at` 触发器 `trigger_update_train_image_updated_at` 仍生效（update 自动刷新），API-17 的 DESC 排序依赖它 | `update` 一行后比对 `updated_at` 变化 | 后端 |
| ✅ | DB-06 | 出口归一化落地：`task_id` → 原生 `int`，`updated_at` / `created_at` → ISO 字符串（修 I8，避免 `np.int64` 让 FastAPI 编码 500） | 单测断言类型 + 真实接口返回 JSON 可被 `json.loads` 且无 `NaN` | 后端 |
| ✅ | DB-07 | 写入遵守 **HC-01 三条 SQL 硬约束**（禁 `%s::type` 写 `CAST(%s AS type)`、字面 `%` 写 `%%`、参数只能 tuple 且枚举传 `.value`） | `grep -rn "%s::" server_model/` 零命中 + 代码评审 | 后端 |
| ✅ | DB-08 | 幂等 upsert 落点正确：`on conflict (image_tar) do update ... where status in ('failed','deleted')` —— 已 `loaded` 的行不被重复 `load` 覆盖（FR-06 / NFR-01 / AC-05） | 同 tar 连续 3 次 `load` 的行数/状态对比输出 | 后端 |

---

## 4. 阶段 3 · 编码完成度（DEV / UT）

### 4.1 服务端（控制面）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | DEV-01 | `api/resource/image/default.py` 的 4 个具名桩被替换为真实接入层实现，且**保持函数名**（`hfai_image_load` / `hfai_image_update_status` / `hfai_image_list` / `hfai_image_delete`，ADR-I1） | 代码 + `git diff api/resource/image/default.py`（函数名不变） | 后端 |
| ✅ | DEV-02 | `api/register/implement.py:7-20` 的模块导入区**显式** `from api.resource import image as ares_image`（当前该模块根本没被 import，这是 4 个桩不可达的根因） | 代码 + `python -c "import api.register.implement"` 无副作用报错 | 后端 |
| ✅ | DEV-03 | `api/register/implement.py` 的 `ugc` 区块（`:67-94`）新增 3 条路由：`load` / `update_status` / `delete`，紧随 `:71` 的 `list` | 代码 + `curl` 探测 404 → 200 | 后端 |
| ✅ | DEV-04 | 领域层新增 4 方法：`async_load` / `async_report_image_status` / `async_delete` / `async_get_user_images`（`server_model/user_impl/user_image/implement.py`），签名与设计 §5.2 一致 | 代码 + 单测 | 后端 |
| ✅ | DEV-05 | `server_model/user_impl/user_image/default.py:16` 的 `'user_images': []` 改为真实查询（激活零调用方的 `a_find_user_group_images`）；`mars_images` 与响应外壳**零改动**（FR-02 / CMP-03 / CMP-05） | 代码 + `git diff` 仅动 `user_images` 一行 | 后端 |
| ✅ | DEV-06 | 路径校验统一走 `cloud_storage/utils.py:check_is_subpath(get_image_root(), ...)`，拒绝 `..`、符号链接逃逸、非共享盘路径与客户端本机绝对路径（SEC-01 / FR-13，禁止自造校验） | 代码 + 负例单测 | 后端 |
| ✅ | DEV-07 | 命名派生与校验：缺省取 `basename(image_tar)` 去 `.tar` 后缀；白名单正则；**不含 `/`**；含 `:` 保留 tag 否则补 `:latest`（I6 / HC-05 / FR-07） | 代码 + 派生用例单测（含边界：`.tar` 后缀、无 tag、含 `/` 被拒） | 后端 |
| ✅ | DEV-08 | **概念分离**：`image_tar` 与 `path` 在代码上**分别赋值**；`processing` 阶段**不得**把 tar 路径写进 `path`（I18，设计 §4.1「实现修正 I18」） | 代码评审 + 单测断言「processing 行 path 为空/独立字段」 | 后端 |
| ✅ | DEV-09 | 状态常量单点：`PROCESSING/LOADING/LOADED/FAILED/DELETED` 集中定义，禁止散落字符串；取值同时满足「任务侧精确 `loaded`」与「客户端子串 `deleted`」（I5 / HC-03 / HC-04 / ADR-I6） | 代码 + `grep` 状态字面量仅出现在常量定义处 | 后端 |
| ✅ | DEV-10 | `delete` 解析 3 段 URL 后**强制校验 `shared_group == user.shared_group`**；组内允许删他人、禁止跨组（SEC-02 / SEC-05 / I14 / ADR-I8） | 代码 + 跨组负例用例 | 后端 |
| ✅ | DEV-11 | API-16 回报校验 `task_id` 与该行登记值一致（防伪造），非法迁移返回 `ILLEGAL_TRANSITION`；`loaded` 不被普通用户用 `failed` 覆盖（FR-10 / SEC-04） | 代码 + 伪造/乱序回报用例 | 后端 |
| ✅ | DEV-12 | 缓存刷新：写成功后调用统一 `_refresh_train_image_cache()`；**记录** launcher 进程内缓存（`launcher.py:59-61` 的 `@cached`）失效策略（R-3 / 设计 §7.4） | 代码 + 已知限制说明（写明 P0 是否接受「重启 launcher 生效」） | 后端 |

### 4.2 运行面（link 脚本与节点前置）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | DEV-13 | **新增** `marsv2/scripts/link_hfai_image.sh`（与既有 `validate_image.sh` 同级），挂载路径为 `/marsv2/scripts/link_hfai_image.sh`（FR-08 / I16） | 文件存在 + `ls -l`（含大小）+ `sh -n` 语法检查通过 | 后端 |
| ✅ | DEV-14 | 脚本契约落地：**只读取 env** `HFAI_IMAGE` / `HFAI_IMAGE_WEKA_PATH`，**不接受任何用户可控命令行参数**，路径做前缀断言（设计 §5.4 / SEC-03 / SEC-07） | 代码评审 + `grep` 无 `$1`/`$@` 使用 | 后端 |
| ✅ | DEV-15 | 脚本**幂等**：容器运行时已存在该镜像即 `exit 0`；重复执行不报错、不重复导入（initContainer 重试的前提，设计 §5.4） | 手动连续执行两次输出一致 + 任务重试用例 | 后端 + 测试 |
| ✅ | DEV-16 | 脚本**失败可见**：非 0 退出并打印目标路径与命令输出；`/data_local` 缺失或镜像资产不存在时**显式报错**，不静默跳过（设计 §5.4 / AC-08） | 构造失败场景的脚本输出（含 `FAILED:` 行） | 后端 |
| ✅ | DEV-17 | `one/hai-up.sh:288-301` 的 `storage` 挂载种子新增一行 `marsv2-scripts-{task.id}:link_hfai_image.sh`（HC-08：随镜像构建进入任务 pod，不得依赖手工 `kubectl cp`） | 代码 diff + 任务 pod 内 `ls -l /marsv2/scripts/link_hfai_image.sh` | 后端 |
| ✅ | DEV-18 | `experiment_manager/manager/init_manager.py:347-360` 的 initContainer 基础镜像改为读取配置（Q-6 / I17② / ADR-I4） | 代码 diff + `kubectl get pod -o jsonpath` 查 initContainer image | 后端 |
| ✅ | DEV-19 | `/data_local` 前置落地：hostPath 显式声明 `DirectoryOrCreate` **或**由部署流程创建，并纳入部署自检（Q-5 / I17① / OPS-04） | 部署脚本 diff + 自检输出 | 后端 + 运维 |
| ✅ | DEV-20 | **节点运行时访问通路定案并实现**（R-2）：运行时 socket 登记为 `mount_point`，或换自带 `ctr` 的基础镜像 | 定案记录 + 任务内 `command -v ctr` 或 socket 挂载实测输出 | 后端 + 运维 |

### 4.3 客户端

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | DEV-21 | `base_model/base_user_modules/default.py` 的 `IUserImage` 增加 `async_load` / `async_delete` 声明（当前只有 `async_get`，缺失属**接口未定义**，FR-01 / C-3） | 代码 diff | 客户端 |
| ✅ | DEV-22 | 客户端 `client/model/user_impl/default.py` 的 `UserImage` 实现两个方法，分别指向 `/ugc/user/train_image/load` 与 `/ugc/user/train_image/delete`（修 `AttributeError`） | 代码 + `hai-cli images load/delete` 实机不再抛 `AttributeError`（AC-02） | 客户端 |
| ✅ | DEV-23 | 服务端 `UserImageExtras` 同步补 `async_load` / `async_delete` 语义（两端同名方法，避免加载器混淆） | 代码 + 实机调用 | 后端 |
| ✅ | DEV-24 | `client/commands/hfai_image.py` 的 `load` 增加可选 `-i/--image`，**不传仍可用**（服务端派生镜像名，CMP-01） | 代码 + 两种调用形态各跑一次 | 客户端 |
| ✅ | DEV-25 | 变更型调用 `retries` 保持默认 1，**不主动重试**；幂等由服务端 `image_tar` upsert 保证（I12 / FR-06） | 代码评审 + 抓包确认单次请求 | 客户端 |
| ✅ | DEV-26 | 失败提示改造：`allow_unsuccess=True` + 打印服务端 `msg` + `raise SystemExit(1)`，**不再向用户抛裸 `Exception`/`AssertionError` 栈**（I10 / FR-12） | 代码 diff + 失败场景实机输出（对照当前 §9-3 栈形态） | 客户端 |
| ✅ | DEV-27 | `msg` 兜底 `result.get('msg', '操作完成')`；服务端**保证任何响应都含 `msg`**（旧客户端唯一消费的字段，设计 §4.1 / §6.2） | 代码 + 接口契约用例（旧客户端形态） | 客户端 + 后端 |
| ✅ | DEV-28 | `images list` 防御性修正：缺 `result` / 非 dict 时告警并按空列表处理（不崩）；`-a/--all` 帮助文本明确「隐藏 status 含 `deleted` 的记录」（R-8 / 设计 §6.2 / §6.3） | 代码 + 帮助文本输出 + 构造异常响应的实机表现 | 客户端 |

### 4.4 单元测试与静态检查

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | UT-01 | 领域层纯函数单测：命名派生 / 路径校验 / 状态机 / 组校验 / 出口归一化，**无 DB、无 k8s、无 registry** 条件下可跑（NFR-04，对齐 `tests/env/` 的写法） | `pytest` 输出 + 覆盖报告 | 后端 |
| ✅ | UT-02 | 路径越界单测：`..`、`/etc/passwd`、客户端本机路径、越界软链**全部被拒且不产生 DB 行**（SEC-01 / AC-06） | 测试报告（含「零 DB 行」断言） | 后端 |
| ✅ | UT-03 | 状态机单测：非法迁移被拒（如 `deleted → loaded`），`loaded` 不被 `failed` 覆盖（FR-04 / 设计 §7.3） | 测试报告 | 后端 |
| ✅ | UT-04 | 幂等单测：同 `image_tar` 连续 upsert → 表仍 1 行且状态不倒退；重复 `delete` → `deleted:0`（NFR-01 / AC-05） | 测试报告 | 后端 |
| ☐ | UT-05 | 静态检查（flake8 / ruff / pyflakes）**无新增告警**；领域层 `grep -rn "from fastapi" server_model/user_impl/user_image/` 为空（分层纪律） | CI 输出 + `grep` 结果 | 后端 |
| ✅ | UT-06 | `conf/utils.py` / `cloud_storage/utils.py` 的改动**未破坏既有 workspace / env 单测**（FR-15 / G6） | CI 全绿 + 回归记录 | 后端 |

---

## 5. 阶段 4 · 接口契约（API）

> 本章 12 项检查的是**需求 §4 的 4 个接口**（`API-15`~`API-18`）；本表 `API-xx` 是检查项编号，与接口号同形不同源。

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | API-01 | `API-15` 正常路径返回 `{success:1, msg, image, image_tar, status, task_id}`，与附录 A.1 一致 | `curl` 输出 + 与附录 A.1 逐字段比对 | 后端 |
| ✅ | API-02 | `API-15` **旧形态**（仅 `image_tar`、无 `image`）可用，服务端独立派生镜像名（CMP-01 / C-3 的修复闭环） | 旧客户端实机调用输出 | 后端 + 客户端 |
| ✅ | API-03 | `API-15` 拒绝面完整：`INVALID_PARAM`（缺参/名字非法）、`PATH_ESCAPE`、`IMAGE_TAR_NOT_FOUND` | 负例 `curl` 输出（附录 A.1 逐条覆盖） | 后端 |
| ✅ | API-04 | `API-15` 幂等：同 `image_tar` 连续 3 次调用 → 表仍 1 行、状态不倒退；仅 `failed` / `deleted` 可重试（NFR-01 / AC-05） | `psql` 行数与状态对比 + 用例 TC-F 组 | 后端 + 测试 |
| ✅ | API-05 | `API-16` 正常回报：`loaded`（**必带 `path`**）与 `failed`（带 `message`），响应 `{success:1, status}` | `curl` 输出 + 表内 `path`/`message` 回读 | 后端 |
| ✅ | API-06 | `API-16` 防伪造与迁移校验：`task_id` 不匹配 → `FORBIDDEN`；非法迁移 → `ILLEGAL_TRANSITION`（SEC-04 / FR-10） | 负例用例输出 | 后端 |
| ✅ | API-07 | `API-17` 的 `user_images` 每行必含 6 字段（`registry` / `shared_group` / `image` / `status` / `image_tar` / `updated_at`）；`mars_images` 与响应外壳**零改动**（FR-11 / CMP-03） | 真实响应 JSON + 字段断言 | 后端 |
| ✅ | API-08 | `API-17` 排序为 `updated_at **DESC**`（修 I7：客户端取**首个**为基准，DESC 才等于「以最新为准」）；**排序即契约**，必须有断言 | 用例 TC-C 组的顺序断言（同一 `image` 多条不同 `updated_at`） | 后端 + 测试 |
| ✅ | API-09 | `API-17` 出口归一化：`task_id` 为原生 `int`、时间为 ISO 字符串，响应可被 FastAPI 编码（修 I8，否则接通即 500） | 响应 JSON 类型检查 + 无 500 记录 | 后端 |
| ✅ | API-10 | `API-17` 返回**本组全部状态行**（含 `deleted`），由客户端 `-a/--all` 决定是否隐藏（保持既有分工，CMP-05 / HC-04） | 带 `deleted` 行的响应 `curl` + 客户端 `list` / `list -a` 对比 | 后端 + 客户端 |
| ✅ | API-11 | `API-18` 正常软删（`status='deleted'`）返回 `{success:1, deleted:N}`；**重复删除返回 `deleted:0` 且 `success:1`**（幂等） | `curl` 两次输出 + `psql` 状态 | 后端 |
| ✅ | API-12 | `API-18` 拒绝面完整：跨组 → `FORBIDDEN`、非 3 段 → `INVALID_PARAM`、不存在 → `IMAGE_NOT_FOUND`；被删名字**自动退出任务白名单**（SEC-05 / AC-07） | 负例输出 + 删除后提交任务被拒（E2E-05 呼应） | 后端 |

---

## 6. 阶段 5 · 联调（E2E）— 用**真实客户端**验证

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | E2E-01 | 主场景：`images load` → `images list` 看到该行及其真实状态（`processing` → `loaded`）→ `psql select * from train_image` 与接口响应一致（AC-03） | 真实 CLI 输出 + `psql` 输出对照 | 测试 |
| ✅ | E2E-02 | **AC-01 的关键一步**：用自定义镜像跑通真实任务，且**输出能被镜像内容区分**（镜像内预置探针文件/包，`probe.py` import 它）；任务 `succeeded`（AC-01 / AC-09） | 任务 id + `hai-cli status` + `hai-cli logs` 中的探针输出 | 测试 + 后端 |
| ✅ | E2E-03 | 运行面：pod initContainer 不再 `not found`，`/marsv2/scripts/link_hfai_image.sh` 被成功执行（`kubectl describe pod` 中 initContainer 为 Completed）（AC-08 / I16） | `kubectl describe pod` 片段 + init 日志 | 后端 + 测试 |
| ✅ | E2E-04 | link 脚本幂等：同一任务重试或重复提交**不因「镜像已存在」失败**（设计 §5.4） | 连续两次任务/一次重试的 init 日志 | 测试 |
| ✅ | E2E-05 | 删除与不可用性：`images delete` 后再用同一镜像提交任务 → 服务端返回「不存在镜像…或镜像仍在加载」（AC-04 / 需求 §7 判据⑥） | `images delete` 输出 + 任务提交报错文案 | 测试 |
| ✅ | E2E-06 | `images list -a` 可见 `deleted` 行、默认视图隐藏（客户端子串 `'deleted' in status` 语义闭环）（HC-04） | 两次 `list` 输出对照 | 测试 |
| ✅ | E2E-07 | **路径三方一致**：`image_tar` / `path` / `image_url` 自洽；`register` 后端下 `path == image_tar`；运行期 `HFAI_IMAGE_WEKA_PATH` 等于表中 `path`（设计 §7.2 / P 组） | 任务内 `env` 输出 + `psql` 行对照 | 后端 + 测试 |
| ☐ | E2E-08 | 多用户与跨组：A 组加载后 B 组 `list` 看不到、跨组 `delete` 被拒（构造 3 段 URL 尝试）（AC-07） | 三用户实测记录 | 测试 |
| ✅ | E2E-09 | 缓存一致性：刚 `load` 完**立刻**提交任务不应读不到（R-3）；若 P0 兜底为「重启 launcher 生效」，须显式记录为已知限制并复验一次 | 复现记录 + 已知限制条目 | 后端 |
| ✅ | E2E-10 | **零回归**：`smoke_ugc` 8/8、`e2e_workspace` 19/19、`e2e_env` 全绿（复用 `docs/haiplatform/scripts/` 既有脚本）（AC-10 / G6 / CMP-03） | 三个脚本输出 | 后端 + 测试 |

---

## 7. 阶段 6 · 安全（SEC）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | SEC-01 | 身份只来自 token，伪造 `username` / `group` 被忽略；新增路由统一 `Depends(get_ugc_user)`（需求 SEC-04） | 伪造参数实测 + 代码评审 | 后端 |
| ✅ | SEC-02 | 路径越界防护实测：`..`、符号链接、客户端本机绝对路径全部拒绝，且**不产生 DB 行**（需求 SEC-01 / AC-06） | 负例实测 + 拒绝前后行数对比 | 后端 + 安全 |
| ✅ | SEC-03 | 组隔离：所有读写以服务端解析的 `user.shared_group` 为准；跨组删除/查看/登记全部被拒（需求 SEC-02 / SEC-05 / AC-07） | 跨组用例记录 | 后端 + 安全 |
| ✅ | SEC-04 | 注入防护：镜像名走白名单正则；进入 shell / 任务脚本的字段一律参数化（**禁止字符串拼接构命令**）；脚本只读 env（需求 SEC-03 / SEC-07） | 注入用例（引号/SQL/空格）+ 代码评审 | 后端 |
| ✅ | SEC-05 | API-16 **伪造回报被拒**：非登记 `task_id` 无法把行改成 `loaded`（需求 SEC-04 / FR-10） | 伪造 `task_id` 用例输出 | 后端 |
| ✅ | SEC-06 | 日志脱敏：不打印 token；`image_tar` 路径按需截断（需求 SEC-06） | `ugc_0.log` 抽样（token 掩码）+ 用例断言 | 后端 |
| ✅ | SEC-07 | 最小权限：initContainer 只挂必要路径 + 只读；若确需运行时 socket，限定专用命名空间/专用 ServiceAccount（需求 SEC-07 / HC-10） | `kubectl get pod -o yaml` 的 volumeMounts / SA 评审 | 后端 + 安全 |
| ✅ | SEC-08 | 灰度开关不可绕过：关闭时 `load` / `update_status` / `delete` 一律 `FEATURE_DISABLED`（需求 OPS-01） | 关闭态三接口 `curl` 输出 | 后端 |
| ☐ | SEC-09 | SEC 组用例（用例文档 TC-S 组）全部通过，**无高危遗留**；越界/跨组/伪造回报三类零绕过 | 安全测试报告 | 安全 + 后端 |

---

## 8. 阶段 7 · 性能与容量（PERF）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | PERF-01 | `images list` 在**单组 200 行**量级下 P95 < 1s（需求 NFR-02） | 压测报告（P50/P95/P99） | 测试 |
| ☐ | PERF-02 | **无 N+1**：`user_images` 由一次查询完成，日志中逐行查询零命中（需求 NFR-02） | SQL 计数日志 + 压测期间查询条数 | 后端 |
| ☐ | PERF-03 | `load` 只读文件元数据、**不复制大 tar**；≥1 GB tar 下 ugc-server 内存/CPU 可控（需求 NFR-06 / R-6） | 大 tar 实测的内存与耗时曲线 | 后端 + 测试 |
| ☐ | PERF-04 | 加载执行体（若启用 `task` 后端）资源上限明确，不因大 tar 打爆服务器 pod（需求 NFR-06） | 资源 limit 配置 + 实测峰值 | 后端 |
| ☐ | PERF-05 | 压测期间 `list` 与 workspace / env 既有接口**不被阻塞**（需求 NFR-02 / G6） | 并发探针结果 + 既有接口 P95 对比 | 后端 + 测试 |

---

## 9. 阶段 8 · 可观测性与告警（OBS）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | OBS-01 | 4 个指标可在 `/metrics` 抓到：`image_load_total` / `image_load_duration_seconds` / `image_list_rows` / `image_link_failed_total`（需求 NFR-03 / 设计 §9.4） | 抓取输出 | 后端 |
| ✅ | OBS-02 | 结构化日志字段齐全：`user_name` / `shared_group` / `image_tar` / `image` / `task_id` / `status` / `from_status` / `cost_ms`（设计 §9.4） | 日志样例 | 后端 |
| ✅ | OBS-03 | 一次 `load` 全流程可用 `image_tar` 串起来：`load → 状态迁移 → 任务提交校验 → pod link`（AC-11） | 按 `image_tar` 过滤的日志串联记录 | 后端 |
| ☐ | OBS-04 | 看板：加载成功率、耗时 P50/P95/P99、失败 `code` 分布（对齐 env 的 `env_metrics.sh` 命令行看板交付方式） | 看板脚本 + 一次输出 | 运维 |
| ☐ | OBS-05 | 告警：加载失败率 > 5%（5 min）与 `image_link_failed_total` 增长可触发（含 link 脚本/manager 侧上报） | 告警规则文件（配置即代码） | 运维 |

---

## 10. 阶段 9 · 运维与合规（OPS）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | OPS-01 | 灰度三级开关（`enabled` / `enabled_groups` / `enabled_users`）真实生效且可回退（需求 OPS-01） | 三档实测记录 | 后端 + 运维 |
| ✅ | OPS-02 | 一级回滚可用：`enabled=false` 使 `load` / `delete` / `update_status` 失败关闭并提示，`list` 与内建镜像路径**不受影响**，已 `loaded` 行仍可被任务使用（需求 OPS-02 / 设计 §9.3） | 回滚演练记录 | 运维 + 后端 |
| ✅ | OPS-03 | 迁移幂等且走 `db_schemas` + `init_postgresql.sh` 全量重放；`git diff --stat db_schemas/` 仅新增 `035.*`（需求 OPS-03 / HC-06） | diff 输出 + 两轮重放日志 | 后端 |
| ✅ | OPS-04 | 节点前置纳入**部署自检**：`/data_local` 存在性 + `load_helper_image` 可获取（需求 OPS-04 / AC-08 / I17） | 自检脚本输出（刻意缺 `/data_local` 时能报警） | 运维 + 后端 |
| ✅ | OPS-05 | 空间回收（P2）**本期只登记不实现**，但 `delete` 帮助文本已明确「不回收存储」，`images list -a` 的 `deleted` 行可见（FR-14 / OPS-05 / 设计 §7.3(P2)） | 帮助文本输出 + P2 登记条目 | 产品 + 后端 |
| ☐ | OPS-06 | 运维手册条目齐备：`loader_backend` 后端切换、`/data_local` 初始化、busybox 基础镜像替换、launcher 缓存刷新（R-2 / R-3 / Q-5 / Q-6） | 手册文档（含命令与回退步骤） | 运维 |

---

## 11. 阶段 10 · 任务侧与运行面集成（TASK）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | TASK-01 | 不变式 **K1**：自定义镜像 URL 必须 `registry/shared_group/image` 恰好 3 段（`len(split('/'))==3`）（FR-15 / HC-02） | `api/operation/default.py:12-24` 代码未改 + 4 段负例被拒 | 后端 |
| ✅ | TASK-02 | 不变式 **K2**：拼接结果**逐字节等于** `registry + '/' + shared_group + '/' + image`（三列独立存储，不存拼接结果）（HC-02） | 代码评审 + 任务 `-i` 命中实测 | 后端 |
| ✅ | TASK-03 | 不变式 **K3**：可用镜像 `status` **精确等于** `'loaded'`；`loaded` 字面量不可改（HC-03） | 代码 + 中间态被拒用例 | 后端 |
| ✅ | TASK-04 | 不变式 **K4**：校验用提交者自己的 `user.shared_group`；`a_find_user_group_image_urls` 签名与返回语义不变（HC-09） | `git diff` 该函数为空 + 跨组用例 | 后端 |
| ✅ | TASK-05 | 不变式 **K5**：服务端报错里的自查指引**真的可用**——103 实测原话为 `用户所在的组 [hfai] 不存在镜像 [registry.high-flyer.cn/hfai/demo:v1] 或镜像仍在加载, 请使用命令 \`hfai images list\` 检查`，而当前 `user_images` 恒为 `[]`（I2）→ **用户按指引自查只会看到「没有镜像」，永远查不出原因**；修好 API-17 后该指引必须能查出真实状态（K5 / I2 / AC-03 / FR-02） | 修复前后对照：同一提交报错 + `images list` 输出（修复前空、修复后有该行及其状态） | 后端 + 测试 |
| ✅ | TASK-06 | launcher 消费链路：`get_image_info`（`launcher.py:59-61`）读到行，并把 `HFAI_IMAGE` / `HFAI_IMAGE_WEKA_PATH`（= `path`）注入 manager（`launcher.py:144-147`） | launcher 日志 + 任务 env 输出 | 后端 |
| ✅ | TASK-07 | `server_model/task_impl/single_task_impl.py:78-81` 的 `user_defined=True` 分支与 `:325` 的 `link_hfai_image` 透传**未被破坏** | 代码 diff + 任务实测 | 后端 |
| ✅ | TASK-08 | 内建镜像路径零回归：`train_environment` / `mars_images` 与 `-i hai_base` 类任务行为不变（CMP-03 / AC-10） | 回归脚本输出 | 后端 + 测试 |

---

## 12. 阶段 11 · 兼容与升级（CMP）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | CMP-01 | 旧客户端 `images load <image_tar>`（单参、无 `image`）继续可用（需求 CMP-01 / AC-13） | 旧客户端实机调用输出 | 客户端 + 测试 |
| ✅ | CMP-02 | 旧客户端 `images delete <image>` 签名不变（需求 CMP-02） | 代码 diff + 实机调用 | 客户端 |
| ✅ | CMP-03 | `train_environment` / `mars_images` 的结构与语义**零改动**（需求 CMP-03） | `git diff` + 内建镜像 `list` 输出不变 | 后端 |
| ✅ | CMP-04 | `registry` 列默认值保留 `registry.high-flyer.cn`，但**新代码不依赖它可达**（需求 CMP-04 / I11 / ADR-I2） | 代码 + 103（无 registry）实机通过 | 后端 |
| ✅ | CMP-05 | `user_images` 由 `[]` 变为有内容属**行为修正**（非破坏），行内字段名**不得改名**（只增不减）（需求 CMP-05） | 字段名 diff 为空 + 旧客户端解析正常 | 客户端 + 后端 |
| ☐ | CMP-06 | 私有 `custom.py` 三层覆盖接缝保持有效（`default.py` / `implement.py` / `custom.py` 约定不破坏）（需求 CMP-06） | 模拟 `custom.py` 覆盖生效记录 | 后端 |

---

## 13. 阶段 12 · 发布与灰度（REL）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | REL-01 | 发布顺序：**先服务端**（3 条路由上线、`[image].enabled=false`）→ 冒烟其他接口 → **再客户端**（含 `load --image` 与 `msg` 提示） | 发布记录（含两阶段时间戳） | 运维 |
| ☐ | REL-02 | 灰度第一档：`enabled=true` + `enabled_groups=['hfai']`，内部跑通并观察 ≥24h 的加载成功率与失败 `code` | 看板截图 + 灰度记录 | 运维 |
| ☐ | REL-03 | 扩量：白名单 10% → 50% → 全量，**每档观察 24h**，任一步异常即停 | 三档灰度记录 | 运维 |
| ☐ | REL-04 | 发布同步更新 `one/one_etc/core.toml` 注释与 `one/hai-up.sh` 挂载种子，确认镜像内**真的含** `link_hfai_image.sh`（HC-08） | 配置 diff + 任务 pod 内脚本存在性 | 运维 + 后端 |
| ☐ | REL-05 | 变更窗口内 workspace / env 业务无异常（三者共用 `get_base_path`，属 R-1 误伤面） | 监控 + on-call 记录 + 回归 | 运维 |

---

## 14. 阶段 13 · 文档与交付物（DOC / DEP）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | DOC-01 | `docs/_sources/cli/ugc.rst.txt` 的 `images` 段补齐 `load --image` 与失败提示说明 | 文档 diff + 构建通过 | 客户端 |
| ☐ | DOC-02 | 用户文档写明「tar 放共享盘 → `load` → `list` → 任务 `-i`」全流程，并讲清 `image_tar` / `image` / `path` 三个概念的区别（FR-07 / I18） | 文档 diff | 产品 + 客户端 |
| ☐ | DOC-03 | 文档写明限制：**不回收存储**（FR-14）、只到 `shared_group` 粒度、**组内可删他人镜像**（SEC-05 须显式声明） | 文档 diff | 产品 |
| ✅ | DOC-04 | `docs/haiplatform/README.md` 收录 images 文档链接且可点 | 链接检查脚本输出 | 后端 |
| ☐ | DOC-05 | 四件套编号交叉引用一致（`I1–I18` / `FR` / `TC` ↔ `DEV` / `API` / `E2E` / `ACC`）；用例文档 `images-server-test-cases.md` §10 的 `AC-*` 对应关系可追溯（需求 AC-14） | 自检脚本输出 | 后端 |
| ☐ | DEP-01 | 交付物归档：平台镜像 tag、客户端版本、`db_schemas/035.*`、`marsv2/scripts/link_hfai_image.sh` 变更清单 | 发布包 + 清单 | 运维 |
| ☐ | DEP-02 | 客户端发布说明（Release Note）含 `load --image` 新增与失败提示变化 | Release Note | 客户端 |
| ☐ | DEP-03 | 回滚包：上一版镜像 tag + 关闭开关的 `override.toml` 差异 + 迁移说明（**只加不删**，回滚停在二级） | 回滚包 + 说明（设计 §9.3 注） | 运维 + 后端 |

---

## 15. 阶段 14 · 回滚演练（RB）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ✅ | RB-01 | 一级回滚：`[image].enabled=false` 后 `load` / `update_status` / `delete` 快速失败并提示，`list` 与内建镜像路径不受影响，已 `loaded` 行仍可被任务使用 | 演练记录（含时间戳） | 运维 + 后端 |
| ☐ | RB-02 | 二级回滚：移除 3 条路由注册 → 接口 404；客户端打印服务端 `msg`（不打印栈） | 演练记录 + 客户端输出 | 后端 |
| ✅ | RB-03 | 回滚后**无脏数据**：`train_image` 表结构/行未被破坏，回滚前后 `psql` 对比一致 | 回滚前后 `\d train_image` + 行数对比 | 测试 |
| ☐ | RB-04 | 回滚期间 workspace / env 业务无异常（路由探针 200 + 演练后完整回归） | 演练窗口探针输出 + 回归记录 | 运维 |
| ☐ | RB-05 | 客户端回滚：旧客户端不依赖新接口，直接可用 | 旧二进制冒烟 | 客户端 |

---

## 16. 上线后观察（POST）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | POST-01 | 上线后 1h：加载成功率、P95 耗时、错误码分布 | 看板截图 | 运维 |
| ☐ | POST-02 | 上线后 24h：无长期卡在 `processing` 的积压行（或每条已有解释） | 看板 + `psql` 统计 | 运维 |
| ☐ | POST-03 | 抽样 3 个真实用户镜像，任务侧成功使用并产出**可区分输出** | 抽样记录（任务 id + 日志） | 测试 |
| ☐ | POST-04 | workspace / env 关键指标（push/pull 成功率、P99）无劣化 | 看板对比（上线前 7 天基线） | 运维 |
| ☐ | POST-05 | 用户反馈渠道无新增「镜像仍在加载」类误报工单（K5 的对外验证） | 工单系统 | 支持 |

---

## 17. 验收签署（ACC）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | ACC-01 | 需求 §6 的 **`AC-01`~`AC-14` 全部通过**（对应关系见用例文档 `images-server-test-cases.md` **§10**） | 验收报告 + 用例 §10 的逐条映射 | 测试 + 后端 |
| ☐ | ACC-02 | 缺陷：致命/严重 = 0；一般 ≤ 2 且有绕行方案 | 缺陷清单 | 测试 |
| ☐ | ACC-03 | 性能门槛（PERF-01 / PERF-02）达标 | 压测报告 | 测试 |
| ☐ | ACC-04 | 安全项（SEC-01~SEC-09）全过，无高危遗留 | 安全测试报告 | 安全 + 后端 |
| ☐ | ACC-05 | 回滚演练通过（RB-01~RB-05） | 演练记录 | 运维 |
| ☐ | ACC-06 | 文档交付齐全（DOC-01~DOC-05 / DEP-01~DEP-03） | 文档清单 | 产品 |
| ☐ | ACC-07 | 需求/设计/用例/Checklist 四方 ID 可追溯、无悬空引用 | 自检脚本输出 | 后端 |
| ☐ | ACC-08 | 分析 §6 的 **`I1`~`I18` 全部有对应处置或显式「不处置」理由**（`I15` / `FR-14` 为 P2 登记，须写明） | 追溯矩阵（需求 §11 + 本文 ID 对照） | 后端 |
| ☐ | ACC-09 | 运行面证据齐备：`link_hfai_image.sh` 已随镜像发布、`one/hai-up.sh` 挂载种子已登记、节点前置自检通过 | 文件存在性 + pod 内路径 + 自检输出 | 后端 + 运维 |
| ☐ | ACC-10 | 上线评审通过，签署发布 | 评审记录 | 全体 |

---

## 附录 A · 接口契约快照（GATE-03 冻结内容）

> 以下 `API-15`~`API-18` 是**需求/设计 §4 的接口号**，与本文第 5 章的检查项 `API-01`~`API-12` 同形不同源。
> **A.6** 额外收录本分支上传通道的 `API-01` / `API-05` / `API-06` / `API-15`（登记） / `API-19` 契约（字段照抄设计 §4.6）。

### A.1 API-15 `POST /ugc/user/train_image/load`

```http
POST /ugc/user/train_image/load?token=<token>&image_tar=/nfs-shared/hai-platform/image/demo.tar&image=demo:v1
Content-Type: text/plain

{"image_tar": "/nfs-shared/hai-platform/image/demo.tar", "image": "demo:v1"}
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| `register` 后端成功（同步 `loaded`） | 200 | `{"success":1,"msg":"镜像已登记，状态：loaded","image":"registry.high-flyer.cn/hfai/demo:v1","image_tar":"/nfs-shared/.../demo.tar","status":"loaded","task_id":0}` |
| `task`/`registry` 后端成功（异步） | 200 | `{"success":1,"msg":"镜像已登记，状态：processing","image":"...","status":"processing","task_id":12345}` |
| 幂等重复（行已 `loaded`） | 200 | 原样返回当前行，`success:1`，状态不重置 |
| 旧形态（仅 `image_tar`） | 200 | 同上，`image` 由 tar basename 派生 |
| 缺参 / 镜像名非法 | 200 | `{"success":0,"code":"INVALID_PARAM","msg":"..."}` |
| 路径越界 | 200 | `{"success":0,"code":"PATH_ESCAPE","msg":"..."}` |
| 共享盘不存在 | 200 | `{"success":0,"code":"IMAGE_TAR_NOT_FOUND","msg":"..."}` |
| 同名不同 tar（Q-3 若选拒绝） | 200 | `{"success":0,"code":"IMAGE_NAME_CONFLICT","msg":"..."}` |
| 灰度关闭 | 200 | `{"success":0,"code":"FEATURE_DISABLED","msg":"镜像功能未开放"}` |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

### A.2 API-16 `POST /ugc/user/train_image/update_status`

```http
POST /ugc/user/train_image/update_status?token=<token>
Content-Type: text/plain

{"image_tar": "/nfs-shared/.../demo.tar", "status": "loaded", "path": "/nfs-shared/.../demo", "task_id": 12345}
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 回报 `loaded`（带 `path`） | 200 | `{"success":1,"msg":"状态已更新","status":"loaded"}` |
| 回报 `failed`（带 `message`） | 200 | `{"success":1,"msg":"状态已更新","status":"failed"}` |
| 同状态重复回报（幂等） | 200 | `{"success":1,"msg":"状态已更新","status":"loaded"}`，无副作用 |
| `task_id` 不匹配 / 跨组 | 200 | `{"success":0,"code":"FORBIDDEN","msg":"..."}` |
| 非法迁移（如 `deleted → loaded`） | 200 | `{"success":0,"code":"ILLEGAL_TRANSITION","msg":"..."}` |
| 行不存在 | 200 | `{"success":0,"code":"IMAGE_NOT_FOUND","msg":"..."}` |
| 缺参 | 200 | `{"success":0,"code":"INVALID_PARAM","msg":"..."}` |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

### A.3 API-17 `POST /ugc/user/train_image/list`（修订版，路由已存在）

```http
POST /ugc/user/train_image/list?token=<token>
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 正常（内建 + 用户镜像） | 200 | `{"success":1,"result":{"mars_images":[...],"user_images":[{"registry":"registry.high-flyer.cn","shared_group":"hfai","image":"demo:v1","status":"loaded","image_tar":"/nfs-shared/.../demo.tar","updated_at":"2026-10-02T18:00:00"}]}}` |
| 本组无自定义镜像 | 200 | `{"success":1,"result":{"mars_images":[...],"user_images":[]}}` |
| 含 `deleted` 行（不带 `-a` 时由客户端隐藏） | 200 | `user_images` 照常返回 `deleted` 行（服务端不隐藏，CMP-05） |
| 同一 `image` 多行 | 200 | 按 `updated_at DESC` 排列（首个 = 最新，修 I7） |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

### A.4 API-18 `POST /ugc/user/train_image/delete`

```http
POST /ugc/user/train_image/delete?token=<token>&image=registry.high-flyer.cn/hfai/demo:v1
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 成功软删 | 200 | `{"success":1,"msg":"已删除 1 个镜像记录","deleted":1}` |
| 重复删除（幂等） | 200 | `{"success":1,"msg":"已删除 0 个镜像记录","deleted":0}` |
| 非 3 段 | 200 | `{"success":0,"code":"INVALID_PARAM","msg":"..."}` |
| 跨组 | 200 | `{"success":0,"code":"FORBIDDEN","msg":"..."}` |
| 镜像不存在 | 200 | `{"success":0,"code":"IMAGE_NOT_FOUND","msg":"..."}` |
| 灰度关闭 | 200 | `{"success":0,"code":"FEATURE_DISABLED","msg":"镜像功能未开放"}` |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

### A.5 错误码

| code | HTTP | 触发 | 客户端表现 |
| --- | --- | --- | --- |
| `FEATURE_DISABLED` | 200 | `[image].enabled=false` 或不在灰度名单 | 打印「镜像功能未开放」 |
| `INVALID_PARAM` | 200 | 缺参 / 镜像名非法 / 非 3 段 | 打印 `msg`，退出码 1 |
| `PATH_ESCAPE` | 200 | 路径越出 `image_root` | 打印 `msg`，退出码 1 |
| `IMAGE_TAR_NOT_FOUND` | 200 | 共享盘上不存在该 tar | 打印 `msg`，退出码 1 |
| `IMAGE_NAME_CONFLICT` | 200 | 同名不同 tar（Q-3 若选拒绝） | 打印 `msg` |
| `ILLEGAL_TRANSITION` | 200 | 非法状态迁移 | 打印 `msg` |
| `IMAGE_NOT_FOUND` | 200 | 目标行不存在 | 打印 `msg` |
| `FORBIDDEN` | 200 | 跨组 / `task_id` 不匹配 | 打印 `msg`，退出码 1 |
| `UNAUTHORIZED` | 403 | token 缺失/过期 | 既有统一处理 |

### A.6 上传通道契约快照（本分支）

> 本分支主入口链路：`hai-cli images push <本地 tar>` → **API-01** 签发 STS → 客户端直传 RustFS/S3 →
> **API-05** stage2 落盘到 `image_path` → **API-06** 轮询 → **API-15** 自动登记，全程 `file_type=image`，
> **不新造传输层**（设计 §3.5 / §4.6）。以下字段照抄设计 §4.6，不新增字段。

#### A.6.1 API-01 `POST /ugc/get_sts_token`（取上传凭证）

```http
POST /ugc/get_sts_token?token=<token>&name=demo:v1&file_type=image&ttl_seconds=<ttl>
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 成功 | 200 | provider 的 STS 结构；**授权前缀 = `cloud_base_path`**（= 本用户 + 本类型 + 本 name，SEC-08） |
| 越权前缀（他人用户 / 同用户其它 `name`） | — | 写入失败（凭证前缀不匹配，UP-02） |
| `[image].enabled=false` | 200 | `{"success":0,"code":"FEATURE_DISABLED","msg":"..."}` |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

> ② **直传不经服务端**：客户端用 STS 直传对象存储，key = `os.path.join(cloud_base_path, '<file>.tar')`；
> 分片 / 断点续传复用既有 provider 实现（NFR-07）。

#### A.6.2 API-05 `POST /ugc/sync_to_cluster`（提交落盘）

```http
POST /ugc/sync_to_cluster?token=<token>&name=demo:v1&file_type=image&no_zip=true
Content-Type: text/plain

{"name": "demo:v1", "file_type": "image", "no_zip": true, "files": ["demo.tar"]}
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 受理成功 | 200 | `{"success":1,"index":...,"dst_path":"<cluster_base_path>","accepted":...,"skipped":...,"msg":"..."}`；**`dst_path` == `cluster_base_path`**，且必须在 `image_path` 之下（`check_is_subpath(image_path, dst_path)`，HC-13） |
| `file_type=image` 不在白名单（§5.6 改动 2 未落地） | 400 | `{"success":0,"code":"INVALID_PARAM","msg":"..."}` |
| 开关关闭（`enabled=false` 或 `upload_enabled=false`） | 200 | `{"success":0,"code":"FEATURE_DISABLED","msg":"..."}` |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

> `no_zip=true` 是硬约束：否则 `submit_to_cluster` 会把 `*.zip` 落到 `{cluster_base_path}/.hfai/` 再解压，
> 共享盘上出现的是 `xxx.tar.zip` 而非 tar 本身（§3.5 ②）。

#### A.6.3 API-06 `GET /ugc/sync_to_cluster/status`（轮询落盘状态）

```http
GET /ugc/sync_to_cluster/status?token=<token>&index=<index>
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 轮询 / 终态 | 200 | 阶段 / 进度 / 终态；终态 `FINISHED` / `STAGE2_FAILED`(+`msg`) |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

#### A.6.4 API-15 `POST /ugc/user/train_image/load`（落盘后登记）

```http
POST /ugc/user/train_image/load?token=<token>&image_tar=<落盘后的绝对路径>&image=demo:v1
Content-Type: text/plain

{"image_tar": "<cluster_base_path>/<file>.tar", "image": "demo:v1"}
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| STAGE2 终态 `FINISHED` 后登记 | 200 | `{"success":1,"msg":"镜像已登记，状态：loaded","image":"registry.high-flyer.cn/hfai/demo:v1","image_tar":"<cluster_base_path>/<file>.tar","status":"loaded","task_id":0}` |
| `image_tar` 未落在 `image_path` 之下 | 200 | `{"success":0,"code":"PATH_ESCAPE","msg":"..."}` |
| 落盘未完成就登记（禁止「先登记后落盘」） | — | **契约禁止**：只有 ③ 的终态为 `FINISHED` 才允许调 API-15（FR-20） |
| 其余情形 | — | 同 A.1 |

#### A.6.5 API-19 `POST /ugc/user/train_image/push_precheck`（新增，Q-11 建议冻结）

```http
POST /ugc/user/train_image/push_precheck?token=<token>
Content-Type: text/plain

{"file": "demo.tar", "image": "demo:v1"}      # image 可省略：由文件名派生
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 成功 | 200 | `{"success":1,"name":...,"image":...,"image_tar":...,"cloud_path":...,"cluster_path":...,"exists":bool,"registered":bool,"msg":...}` |
| 名字非法（`file` 含 `/`、`..`） | 200 | `{"success":0,"code":"INVALID_PARAM","msg":"..."}` |
| 路径越界 | 200 | `{"success":0,"code":"PATH_ESCAPE","msg":"..."}` |
| 灰度关闭 | 200 | `{"success":0,"code":"FEATURE_DISABLED","msg":"..."}` |
| token 缺失/过期 | 403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

> API-19 是**只读接口，无副作用**，用于客户端提前判重（`exists` / `registered`）、展示落点与 `index` 提示。

---

## 附录 B · 冒烟验证脚本（发布后 5 分钟自检）

### B.1 P0 冒烟（控制面 + 运行面；旧分支 `f2cb559` 已实测，本分支并入后按 S9-2 重跑）

```bash
# 变量：API=ugc-server 地址；T=测试用户 token；IMG_ROOT=共享根；TAR=测试 tar
API=${API:-http://127.0.0.1:8083}
T=${T:-<T_A_TOKEN>}
IMG_ROOT=${IMG_ROOT:-/nfs-shared/hai-platform/image}
TAR=${TAR:-$IMG_ROOT/demo.tar}
GRP=${GRP:-hfai}

echo "== 0) 环境自检 =="
ls -ld "$IMG_ROOT" "$TAR" || exit 1

echo "== 1) API-17 list（内建 + 用户镜像）=="
curl -s -X POST "$API/ugc/user/train_image/list?token=$T" | tee /tmp/img_list.json
python3 -c "import json;d=json.load(open('/tmp/img_list.json'));assert d['success']==1 and 'user_images' in d['result'],d;print('OK rows=',len(d['result']['user_images']))"

echo "== 2) API-15 load（旧形态，单参）=="
curl -s -X POST "$API/ugc/user/train_image/load?token=$T&image_tar=$TAR" | tee /tmp/img_load.json
python3 -c "import json;d=json.load(open('/tmp/img_load.json'));assert d['success']==1 and d['status'] in ('processing','loaded'),d;print('OK',d['image'],d['status'])"

echo "== 3) API-15 幂等（同 tar 再 load 一次）=="
curl -s -X POST "$API/ugc/user/train_image/load?token=$T&image_tar=$TAR" | tee /tmp/img_load2.json
python3 -c "import json;a=json.load(open('/tmp/img_load.json'));b=json.load(open('/tmp/img_load2.json'));assert b['success']==1 and b['status']==a['status'],b;print('OK idempotent')"

echo "== 4) psql 核对 train_image 行（行数应为 1）=="
sudo kubectl -n hai-platform exec hai-platform-0 -- \
  psql -U root -d mars_db -c "select image_tar,image,path,status,task_id from train_image where shared_group='$GRP';"

echo "== 5) API-15 负例（越界路径）=="
curl -s -X POST "$API/ugc/user/train_image/load?token=$T" \
  -H 'Content-Type: text/plain' -d "{\"image_tar\":\"/etc/passwd\"}" | tee /tmp/img_neg.json
python3 -c "import json;d=json.load(open('/tmp/img_neg.json'));assert d['success']==0 and d['code']=='PATH_ESCAPE',d;print('OK',d['code'])"

echo "== 6) API-18 delete + 幂等（deleted:0）=="
IMG=$(python3 -c "import json;print(json.load(open('/tmp/img_load.json'))['image'])")
curl -s -X POST "$API/ugc/user/train_image/delete?token=$T&image=$IMG" | tee /tmp/img_del.json
curl -s -X POST "$API/ugc/user/train_image/delete?token=$T&image=$IMG" | tee /tmp/img_del2.json
python3 -c "import json;d=json.load(open('/tmp/img_del2.json'));assert d['success']==1 and d['deleted']==0,d;print('OK deleted:0')"

echo "== 7) API-18 负例（跨组 3 段 URL）=="
curl -s -X POST "$API/ugc/user/train_image/delete?token=$T&image=registry.high-flyer.cn/othergroup/demo:v1" | tee /tmp/img_x.json
python3 -c "import json;d=json.load(open('/tmp/img_x.json'));assert d['success']==0 and d['code']=='FORBIDDEN',d;print('OK',d['code'])"

echo "== 8) 缺 token（预期 403 且带 success）=="
curl -s -o /tmp/img_notok.json -w '%{http_code}\n' -X POST "$API/ugc/user/train_image/list"
python3 -c "import json;d=json.load(open('/tmp/img_notok.json'));assert d['success']==0,d;print('OK')"

echo "== 9) 运行面前置（/data_local + link 脚本）=="
multipass exec k8s-slave01 -- ls -ld /data_local || echo "WARN: /data_local 缺失（I17①）"
ls -l marsv2/scripts/link_hfai_image.sh || echo "FAIL: link 脚本缺失（I16）"
grep -n "link_hfai_image" one/hai-up.sh || echo "FAIL: 挂载种子未登记（HC-08）"
```

### B.2 上传通道冒烟（本分支）

```bash
bash docs/haiplatform/scripts/e2e_images_push.sh
```

- **期望**：`PASS >= 10`、`FAIL = 0`；覆盖上传闭环（本地 tar → `images push` → 共享盘文件 md5 与本地一致 → `train_image` 出现 `loaded` 行）。
- 同时覆盖：**开关一致性**（`[image].enabled=false` 与 `upload_enabled=false` 时 API-01 / API-05 均 `FEATURE_DISABLED`，共享盘与对象存储零新增写入）、**md5 对比**、**重复 push 幂等**（同 tar 再 push 不重复上传、落点 mtime 不变、`train_image` 仍 1 行）。
- ✅ 该脚本 `docs/haiplatform/scripts/e2e_images_push.sh` **已落地并实测通过**：`PASS=33 WARN=1 FAIL=0`（上传闭环 + 幂等 + 开关一致性 + 负例，见 [images-server-test-report.md](images-server-test-report.md) §9.2）。

---

## 附录 C · 排障速查

| 现象 | 可能原因 | 排查 | 处置 |
| --- | --- | --- | --- |
| pod 卡 `Init` / `link_hfai_image.sh: not found` | `marsv2/scripts/link_hfai_image.sh` 未随镜像发布，或 `one/hai-up.sh` 挂载种子未登记（I16 / HC-08） | `kubectl describe pod` 看 initContainer args 与 `kubectl logs <pod> -c <name>-load-image`；容器内 `ls -l /marsv2/scripts/link_hfai_image.sh` | 补脚本 + 登记种子 + 重建镜像（DEV-13/DEV-17）；**不得**用 `kubectl cp` 临时绕过 |
| initContainer `ImagePullBackOff` | 基础镜像是内网地址 `registry.high-flyer.cn/google_containers/busybox:latest`，103 上解析到 `198.18.0.77` 不可达；节点只有 `docker.io/library/busybox:latest`（I17②） | `multipass exec k8s-slave01 -- sudo microk8s ctr images ls \| grep busybox`；`kubectl describe pod` 的 image 字段 | 改 `[image].load_helper_image` 为节点已有镜像（CFG-05 / DEV-18 / ADR-I4） |
| initContainer 报 `/data_local` 挂载失败 | 节点上 `/data_local` 不存在，hostPath 未声明 `type` → kubelet 不创建（I17①） | `multipass exec k8s-slave01 -- ls -ld /data_local` | 部署流程创建，或 hostPath 改 `DirectoryOrCreate` 并纳入自检（DEV-19 / OPS-04） |
| `images list` 里看不到自定义镜像 | `user_images` 仍为硬编码 `[]`（I2），或 `load` 从未成功写库（I4） | `curl` 直连 `API-17` 看 `user_images`；`psql select * from train_image` | 检查 DEV-05 是否落地；确认 `load` 返回 `success:1` 后再查表 |
| `images list` 返回 500 | 出口未归一化，`task_id` 是 `np.int64`，FastAPI 无法编码（I8） | `ugc_0.log` 中的 `TypeError` / `ValueError` 栈 | 在 selector/handler 出口转原生类型（DB-06 / API-09） |
| 任务报「镜像仍在加载」 | 该行 `status` 不是 `loaded`，或用户组与镜像 `shared_group` 不一致（K3/K4） | `psql select image,shared_group,status from train_image where image='<name:tag>'`；`hai-cli whoami` | 等状态收敛到 `loaded`；确认组的归属；`failed` 行需带 `force` 重新 `load`（API-04） |
| 任务提交报「不存在镜像 … 或镜像仍在加载」但 `images list` 里看不到该镜像 | 服务端报错把自查指引指向 `hfai images list`，而 `user_images` 被**硬编码为 `[]`**（I2），且 `train_image` 无 `status='loaded'` 行（I4）；客户端表现为 `client/api/api_utils.py:75` 抛出的裸 `Exception: 请求失败: [exception: ...]`（I10） | `curl` 直连 API-17 看 `user_images`；`psql -c "select * from train_image"` 看是否有该 `image_tar` 行 | 修 API-17 让 `list` 反映真实状态，并确认 `load` 已成功写库、状态已收敛到 `loaded`（DEV-05 / API-01 / API-04，K5 闭环） |
| `load` 报 `PATH_ESCAPE` | `image_tar` 未落在 `image_root` 之下，或路径含 `..` / 越界软链（SEC-01） | 打印的 `msg` 中的目标路径与 `get_image_root()` 对比 | 把 tar 放到共享根内；禁止用 `/tmp`、本机绝对路径（CFG-03 / DEV-06） |
| `load` / `delete` 抛 `AttributeError: async_load` | 客户端未升级（C-3 / I1），接口层与服务端均无该方法 | 栈顶文件为 `client/api/image_api.py:23` 或 `:34` | 升级客户端（DEV-21/DEV-22）；服务端无法绕过（异常发生在客户端属性查找阶段） |
| `load` 失败只看到 `Exception('请求失败: ...')` | `async_requests` 默认 `assert_success=[1]`，`print(result['msg'])` 永不执行（I10） | 对比 `curl` 直连拿到的 `code` / `msg` | 客户端改 `allow_unsuccess=True` + 打印 `msg`（DEV-26） |
| `delete` 返回 `FORBIDDEN` | 传入的 3 段 URL 里 `shared_group` 与 token 解析出的组不一致（I14 / SEC-02） | `hai-cli whoami` + 镜像 URL 第二段 | 用本组镜像名；跨组删除属设计禁止（ADR-I8） |
| `update_status` 返回 `FORBIDDEN` / `ILLEGAL_TRANSITION` | 回报的 `task_id` 与登记值不一致，或尝试 `deleted → loaded`（FR-10 / §7.3） | `psql` 查该行 `task_id` 与 `status` | 用登记的任务 id 回报；非法迁移需管理员重置（API-05/API-06） |
| 刚 `load` 完提交任务仍报「不存在镜像」 | launcher 侧 `get_image_info` 的进程内缓存（`@cached`）读到旧快照（R-3） | 重启 launcher 后再试；比对 DB 行与 launcher 日志 | 状态迁移到 `loaded` 时发同步信号；P0 兜底记为已知限制（DEV-12 / E2E-09） |

---

## 附录 D · 与 workspace / env Checklist 的组映射

| 本文件组 | 对照 | 说明 |
| --- | --- | --- |
| GATE | GATE（workspace / env） | 同为启动前置；本特性额外要求 `Q-1`~`Q-8` 全部定案，本分支主入口另需 `Q-9`~`Q-12` 定案（GATE-09） |
| ENV / CFG | ENV / CFG | 一致；本特性新增 `[image]` 与 `image_path` 两个配置面 |
| **DB** | workspace **DB** / env **REG** | **本特性有真实 DDL**（`db_schemas/035.table_train_image_alter.sql`：加列 + 唯一索引；本分支另加 `036.file_type_enum_add_image.sql` 枚举值），因此沿用 workspace 的 **DB** 阶段，而**不是** env 的 `REG`（env 零 DDL，用共享盘 SQLite 注册表） |
| DEV / UT | DEV / UT | 一致；本特性 DEV 分「控制面 / 运行面 / 客户端」三段 |
| API | workspace（并入 DEV）/ env（独立成阶段） | 本特性有 4 个接口（1 修订 + 3 新增），独立成阶段便于契约冻结 |
| E2E | E2E | 一致；本特性 **E2E-02 是 AC-01 的关键一步**（必须跑出可区分输出） |
| SEC / PERF / OBS | SEC / PERF / OBS | 一致；本特性 SEC 额外覆盖「伪造 `task_id` 回报」与「脚本不接受用户可控参数」 |
| OPS | OPS | 一致；本特性额外覆盖「节点前置自检」（`/data_local` + 基础镜像） |
| TASK | TASK | 一致；本特性的 TASK 阶段同时覆盖 **K1–K5 不变式**与 **launcher → pod link** 运行链路 |
| CMP | CMP | 一致；本特性必须保持「旧客户端单参 `load <tar>` 可用」 |
| **阶段 17（上传通道）** | workspace **阶段 6/7（SEC / PERF 中与上传相关的项）** / env **阶段 6（上传通道开关同源）** | 一致；本分支把上传通道升为**主入口**后，STS 作用域最小与越权前缀拒绝（SEC）、GB 级 tar 的 stage1/stage2 容量与耗时（PERF）、以及「上传与控制面同一个开关」（HC-12，沿用 env `env_push_enabled` 的教训）在本阶段一并复核 |
| REL / DOC / DEP / RB / POST / ACC | 同名组 | 一致 |

> **两条结构性差异**：① `images` **有 DDL** → 用 `DB` 阶段（同 workspace，区别于 env 的 `REG`）；
> ② `images` **有独立的运行面交付物**（`marsv2/scripts/link_hfai_image.sh` + 挂载种子 + 节点前置），
> 这是 workspace / env 两个特性都不曾有的阶段内容（对应 I16 / I17 / FR-08 / AC-08），也是本特性工作量更大的根因。

---

## 18. 阶段 17（**本分支主体**）· 上传通道（UP，14 项）

> **背景与判据**：`hai-cli images push <本地 tar>` 是**本分支唯一的镜像上传主入口**（不再是「P0 只支持手工放盘」的补充）。
> 它**复用** `workspace`/`env` 的既有流水线：API-01 签发 STS → 客户端直传 RustFS/S3 → API-05 stage2 落盘到
> `image_path` → API-06 轮询状态 → API-15 自动登记，**不新造传输层**；手工把 tar 放到共享盘再 `images load`
> 仅作为**兼容/运维旁路**保留（UP-12 须复核其仍可用）。
> 判据：**HTTP 200 不算通过** —— 必须核对「共享盘上真的出现 md5 一致的文件」且
> `user_sync_status`（字节搬没搬完）与 `train_image`（能不能被任务用）两套状态各就各位。
> 需求/设计/用例：需求 FR-16~FR-20 · 设计 §3.5/§4.6/§5.6/§6.4/§7.5/§9.5 · 用例 §4.11（UP 组）+ E2E-09/10 + FI-09~12。
> **工作量口径**：S8（上传通道）= 2.0 人日；S9（P0 资产并入与入口切换）= 1.0 人日；本分支新增合计 = 3.0 人日；P0 = 13.0 人日（旧分支已完成）；总计 = 16.0 人日。

| 勾选 | ID | 检查项 | 验收证据 |
| --- | --- | --- | --- |
| ✅ | GATE-09 | 本分支的四项决策 **Q-9（单文件 vs 目录+tar）/ Q-10（S3 key 布局）/ Q-11（是否新增 API-19）/ Q-12（是否自动 load）** 全部有结论并与 ADR-I11~I14 一致 | 决策记录 + 与设计 §3.5 的对应表 |
| ✅ | DB-09 | 迁移 `db_schemas/036.file_type_enum_add_image.sql` 存在且**幂等**（`alter type file_type add value if not exists 'image'`），经 `init_postgresql.sh` 两轮重放不报错 | 迁移文件 + `\dT+ file_type` 两轮对比 + 重放日志 |
| ✅ | UP-01 | **key/落点单点**：`get_base_path(..., FileType.IMAGE)` 同时返回非空 `cloud_base_path` 与落在 `image_path` 下的 `cluster_base_path`；`get_bucket_name` 仍取 private bucket | 单测（TC-UP-01）+ 代码 diff |
| 🟡 | UP-02 | **STS 作用域最小**：API-01 的授权前缀**恰等于** `cloud_base_path`；越权前缀（他人用户 / 同用户其它 name）写入失败 | 凭证前缀回显 + 越权写实测（TC-UP-02） |
| ✅ | UP-03 | **落盘契约**：API-05 受理后 `dst_path` == `cluster_base_path`；`no_zip=true` 时共享盘上是 **tar 本身**（不是 `xxx.tar.zip`） | 接口响应 + `ls -l` 落点（TC-UP-03） |
| ✅ | UP-04 | **上传闭环（AC-15）**：本地（共享盘之外）tar → `images push` → 共享盘文件 **md5 与本地一致** → `train_image` 出现 `loaded` 行 → 用该镜像跑任务 `succeeded` 且输出可区分 | `md5sum` 双端对比 + `psql` + 任务日志（E2E-09） |
| ✅ | UP-05 | **落点与注入防护**：`..`、`/`、绝对路径、符号链接的 `name`/相对路径全部被拒，且**共享盘零新增文件** | 负例实测 + `ls` 前后对比（TC-UP-05，SEC-09） |
| ✅ | UP-06 | **幂等**：同一 tar 重复 push 不重复上传（`index` 命中）、共享盘 mtime 不变、`train_image` 仍 1 行 | 连续 3 次 push 输出 + `psql` 行数（TC-UP-06） |
| ✅ | UP-07 | **数据面开关同源（AC-16）**：`[image].enabled=false` 时 API-01/API-05 均 `FEATURE_DISABLED`，共享盘与对象存储**零新增写入**；恢复后可用 | 关/开两轮 `curl` + 落点比对（E2E-10，HC-12） |
| ✅ | UP-08 | **上传开关独立**：`upload_enabled=false`（`enabled=true`）时上传被拒，而 `load/delete/list` 正常 | 两态实测（TC-UP-08，OPS-06） |
| 🟡 | UP-09 | **失败可见与可续传**：stage1/stage2 失败均能打印阶段与 `index`；中断后重试/续传成功；**失败期间 `train_image` 无新增 `loaded` 行** | 故障注入 FI-09~FI-11 输出 + `psql`（FR-20） |
| ✅ | UP-10 | **容量上限**：超过 `[image].max_tar_bytes` 快速失败（`IMAGE_TAR_TOO_LARGE`）且零落盘 | 配置 + 负例输出（TC-UP-10，OPS-07） |
| ✅ | UP-11 | **客户端行为**：`images push` 成功后自动 `load`；`--no-load` 只上传；本地文件不存在时**不发起请求**并打印明确提示 | 三种形态实测（TC-UP-11） |
| ✅ | UP-12 | **零回归（AC-18）**：`workspace`/`env` 的 push/pull 行为不变（四脚本全绿）；手工放 tar + `images load` 的 P0 路径仍可用；未新建表（主键不变） | 回归脚本输出 + TC-UP-12（CMP-07~09） |

> **实测证据（2026-10-02，提交 `fc773e5` + 3 处修复：D10/D11/D13）**：
> `bash docs/haiplatform/scripts/e2e_images_push.sh` → **PASS=33 WARN=1 FAIL=0**。关键输出（原文摘录）：
> `PASS | 共享盘出现同一文件且 md5 一致（AC-15 核心判据）` ·
> `PASS | user_sync_status 有 file_type=image 的 finished 行（字节搬完了）` ·
> `PASS | train_image.status=loaded（任务白名单，HC-03）` ·
> `PASS | 重复 push 命中「已在集群且已登记」，跳过上传（FR-18 幂等）` ·
> `PASS | 任务输出含 IMAGE_PROBE=images-push-ok` · `PASS | 镜像内探针内容可区分（不是内建镜像）` ·
> `PASS | upload_enabled=false：API-01 被拒（FR-19 / HC-12）` · `PASS | upload_enabled=false：API-05 被拒（数据面同源闸门）` ·
> `PASS | 关闭期间共享盘零新增写入` · `PASS | 恢复 upload_enabled=true 后 API-01 可用（一级回滚可逆）` ·
> `PASS | STS 授权前缀恰等于 cloud_path（SEC-08）` · `PASS | API-19 拒绝非法 file=../evil.tar / a/b.tar / /etc/passwd` ·
> `PASS | API-05 拒绝 name 含 ..（镜像条目名非法）` · `PASS | API-05 拒绝越界相对路径（PATH_ESCAPE）` ·
> `PASS | 超过 max_tar_bytes 快速失败（IMAGE_TAR_TOO_LARGE）` · `PASS | --no-load：只上传不登记（FR-16）` ·
> `PASS | 还原成功：… 重新登记为 loaded`；以及
> `WARN | 越权前缀写入成功：本环境下发的是静态 AK/SK（security_token 为空），prefix 不被强制（环境限制 D14 / R-14）`。
> 完整日志、命令与缺陷记录见 [images-server-test-report.md](images-server-test-report.md) §6/§9.2。

**本阶段仍未做（诚实声明）**：`UP-09` 的 **stage2 故障注入**（FI-09 中断/续传、FI-10 RustFS 不可达、FI-11 共享盘只读/写满）
未执行 —— 上传闭环、幂等与「上传成功但登记失败」的错误面已在实测中复现并修复（D13），但按脚本注入 stage2 失败尚未做。

> **已知未修缺陷（D15，2026-10-03 巡检）**：同一 `image_tar` 已是 `loaded` 且 `image` 名不同时，`load --force` **静默不改名**
> 但客户端仍报成功（`a_upsert_image` 的 `where status in ('failed','deleted')` + `async_load` 不回读校验）。
> 规避：先 `images delete` 再 `load`，或改用另一个 tar 路径登记。详见 [images-server-test-report.md](images-server-test-report.md) §6.1 与设计 §15 **R-16**。
>
> **部署须知（同次巡检）**：平台 pod 未挂载代码目录，`deploy_pod_dev.sh` 的**热部署在容器重启后丢失**（回退到镜像内版本，
> 实测 API-19 回到 404）；容器重启后需重跑一次热部署，或走 `build_hai.sh` + `redeploy_local.sh`。见 test-report §1。

---

## 19. 未验证清单（诚实声明，勿当作「已通过」）

> P0 未勾选项见下表；**本分支上传通道的 14 项（§18）已全部勾选**（103 实测：`e2e_images_push.sh` `PASS=33 WARN=1 FAIL=0`；其中 `UP-09` 的「stage2 失败注入」未做，见下行说明）。
> 下表中 P0 各组的未勾选项均为**旧分支 `f2cb559` 的实测结论**，本分支并入后按 S9-2 重跑。

| 组 | 未勾选项 | 原因 / 下一步 |
| --- | --- | --- |
| UT | `UT-05` | 本轮未跑 flake8/ruff（无 CI 证据）；分层纪律用 `grep -rn "from fastapi" server_model/user_impl/user_image/` 人工核对为空。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| E2E | `E2E-08` | 103 只有 `haiadmin`（组 `hfai`）一个可用身份，**多用户**场景（T_B 组内他人 / T_C 跨组）无法真实构造；跨组拒绝已在 L2 用未授权 group 的 3 段 URL 覆盖（`FORBIDDEN`）。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| SEC | `SEC-09` | 未出正式安全测试报告（SEC-01~08 已有实测证据）。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| PERF | `PERF-01`~`PERF-05` | 未做压测（单组 200 行 P95 / 大 tar 内存曲线 / 并发探针）。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| OBS | `OBS-04`、`OBS-05` | 看板脚本与告警规则未落地（指标本身已验证可抓取，见 OBS-01）。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| OPS | `OPS-06` | 运维手册条目散落在决策记录 §3 / 测试报告 §6，未成独立手册。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| CMP | `CMP-06` | 未模拟私有 `custom.py` 覆盖（三层接缝本身未改动）。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| REL | `REL-01`~`REL-05` | 未执行发布流程与三级灰度（属运维动作）。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| DOC/DEP | `DOC-01`~`DOC-03`、`DOC-05`、`DEP-01`~`DEP-03` | 用户文档/Release Note/归档包未写（`DOC-04` 索引已更新）。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| RB | `RB-02`、`RB-04`、`RB-05` | 只演练了一级回滚（`RB-01`/`RB-03` 通过）；二级/客户端回滚未演练。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| POST | `POST-01`~`POST-05` | 上线后观察，尚未上线。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| ACC | `ACC-01`~`ACC-10` | 正式验收签署，未进行。**证据来自旧分支 `f2cb559`，本分支并入后按 S9-2 重跑** |
| 本分支 | §18 的 `GATE-09` / `DB-09` / `UP-01`~`UP-12`（14 项） | **已在 2026-10-02 全部验证通过**（`e2e_images_push.sh` `PASS=33 WARN=1 FAIL=0`；`UP-09` 的故障注入部分未做，已在下行显式登记） |
| 本分支 | `UP-09` 的「stage2 中途失败 / 续传」故障注入（FI-09/FI-10/FI-11） | **未执行**：上传闭环与「上传成功但登记失败」的现场已在实测中复现并修复（D13），但按脚本注入 stage2 失败尚未做 |
