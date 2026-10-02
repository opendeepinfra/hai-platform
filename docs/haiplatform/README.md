# docs/haiplatform · HAI Platform 服务端分析 / 设计文档索引

本目录收录三个 `hai-cli` 子特性的**逆向分析**与**服务端实现设计**文档：

- `hai-cli workspace` —— 工作区同步（客户端已完整、服务端已实现并上线验证）；
- `hai-cli env` —— 虚拟环境（`haienv`，本地闭环完整、跨端上传链路本次补齐）；
- `hai-cli images` —— 用户自定义镜像（`hfai images`，**三段式链路：控制面半成品 / 提交面完整 / 运行面缺一个脚本**）。

```
docs/haiplatform/
├── README.md          本索引
├── workspace/         workspace 特性文档（分析 / 需求 / 设计 / 用例 / Checklist / DB 审计 / 任务清单 / 数据流 / 测试环境）
├── env/               env 特性文档（分析 / 需求 / 设计 / 用例 / Checklist）
├── images/            images 特性文档（分析 / 需求 / 设计 / 用例 / Checklist / 任务列表）
└── scripts/           部署与验证脚本（见 §3.5）
```

> 目录内的所有相对链接都指向同级子目录（`workspace/` 或 `env/`）。

## 0. 测试环境（真实环境）

| 文档 | 内容 | 适用读者 |
| --- | --- | --- |
| [test-environment.md](workspace/test-environment.md) | **192.168.100.103 真实测试环境**：拓扑与节点规格、浏览器 / `hai-cli` / kubectl·FreeLens 三种访问方式、Terraform 部署流程、π 计算冒烟验证、已固化修复（源码层 / 编排层 / 节点本地）、7 项已知脆弱点、故障恢复 Runbook、DB schema 变更须知 | 全体（部署、联调、排障） |

> 与 [workspace-server-test-cases.md](workspace/workspace-server-test-cases.md) §2 的区别：**本文档描述真实已部署环境**；用例集 §2 是供编写用例时假设的抽象设定（单机 `localfs` / 真实 OSS 两条路径）。联调请以本文档为准。

## 1. 现状分析

| 文档 | 内容 | 适用读者 |
| --- | --- | --- |
| [hai-cli-workspace-analysis.md](workspace/hai-cli-workspace-analysis.md) | 对现有仓库的逆向分析：客户端插件、服务端半开源现状、9 个 `/ugc/*` 接口契约、10 项风险（F1–F10）、证据索引 | 全体 |
| [hai-cli-images-analysis.md](images/hai-cli-images-analysis.md) | 对 `hfai images`（用户自定义镜像）的逆向分析：**三段式链路**（控制面 / 提交面 / 运行面）实测盘点、`images load/delete` 的 `AttributeError`（沿用审计 C-3）、`user_images` 硬编码空、`train_image` 表零写入、**运行面 `link_hfai_image.sh` 缺失**、18 项风险（I1–I18）、S1–S10 场景矩阵、103 实测原始输出 | 全体 |

## 2. 服务端设计交付物（4 件套）

按「需求 → 设计 → 用例 → 检查」顺序阅读；每份文档的需求 ID / 用例 ID / Checklist ID 相互可追溯。

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [workspace-server-requirements.md](workspace/workspace-server-requirements.md) | 服务端实现需求：21 条功能需求、12 个接口、10 条非功能、8 条安全、7 条运维、6 条兼容、10 条硬约束、验收标准、追溯矩阵 | §3 需求总览 · §3.1 硬约束 · §10 DoD · §11 追溯矩阵 |
| [workspace-server-design.md](workspace/workspace-server-design.md) | 程序设计：总体架构、模块与文件清单、接口契约、领域层、兼容层、状态与数据、限额配置、任务侧 `oss://` 与挂载、审计、安全、部署灰度回滚、时序图、12 条 ADR | §4 接口契约 · §5 领域层 · §6 兼容层 · §7 状态与数据 · §10 任务侧 · §15 ADR · §17 WBS |
| [workspace-server-test-cases.md](workspace/workspace-server-test-cases.md) | 功能测试用例：分层策略、环境与数据准备、A–L + DB 共 13 组用例（182 + 12 条）、端到端场景、故障注入、性能/安全/兼容测试、回归矩阵、缺陷分级 | §1 策略 · §4 用例 · §5 E2E 场景 · §10 回归矩阵 |
| [workspace-server-checklist.md](workspace/workspace-server-checklist.md) | 实施与上线 Checklist：GATE/ENV/CFG/DB/DEV/UT/E2E/SEC/PERF/OBS/OPS/TASK/CMP/DOC+DEP/REL/RB/POST/ACC（组数表述见任务列表 §7 #3）、接口契约快照、冒烟脚本、排障速查 | §3 编码完成度 · §4 联调 · §10 发布灰度 · 附录 A/B/C |
| [workspace-server-db-audit.md](workspace/workspace-server-db-audit.md) | 数据库支撑性审计：表结构 ↔ 客户端契约逐字段核对、枚举实测对齐、**3 条 DB 访问层硬约束**、6 个能力缺口（G1–G6）、P1 DDL 与迁移/回滚、TC-DB 用例 | §0 结论 · §2 表核对 · §4 硬约束 · §5 缺口 · §7 迁移机制 |

## 2.5 `hai-cli env` 特性（分析 / 需求 / 设计 / 用例 / Checklist）

按「分析 → 需求 → 设计 → 用例 → 检查」顺序阅读；需求 ID（FR/NFR/SEC/OPS/CMP/HC）、验收 ID（AC-01~AC-12）与用例 ID（TC-*）、检查项 ID（GATE/ENV/…/ACC）在追溯矩阵中可回溯。

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [hai-cli-env-analysis.md](env/hai-cli-env-analysis.md) | **逆向分析**：`haienv` 的三种调用形态、客户端本地闭环（命令/数据模型/激活/任务衔接）、上传链路 5 处断点（E1–E4 / E13）、服务端运行时消费与上传入口桩、端到端场景矩阵（S1–S7）、**13 项风险 E1–E13**、证据索引 | §1 结论速览 · §3 客户端 · §4 服务端 · §5 场景矩阵 · §6 风险 |
| [env-server-requirements.md](env/env-server-requirements.md) | **需求说明**：目标/非目标、12 条功能需求、6 条非功能、6 条安全、5 条运维、5 条兼容、8 条硬约束、2 个 P0 接口（API-11 修订 / API-13）+ 1 个 P2、统一约定、12 条验收（AC-01~AC-12）、待确认决策、追溯矩阵 | §3 需求总览 · §4 接口清单 · §6 DoD · §8 待确认 · §11 追溯矩阵 |
| [env-server-design.md](env/env-server-design.md) | **程序设计**：路径约定单点化与对齐（`env_root`）、两接口契约与领域层签名、注册表写入（复用镜像内 `haienv` 包）、写权限探测、客户端 `env push` 子命令与 `push_venv` 重写、任务侧改动、端到端时序图、灰度/回滚/可观测、安全与兼容、7 条 ADR（E1–E7）、WBS 与里程碑（≈6.5 人日） | §2 架构 · §3 路径约定 · §4 接口契约 · §5 服务端 · §6 客户端 · §13 ADR · §14 WBS |
| [env-server-test-cases.md](env/env-server-test-cases.md) | **功能测试用例**：分层模型（L1–L4）、`localfs`/真实 S3 两套环境与 env fixture 构造、**11 组 106 条用例（U/A/P/C/REG/S/F/O/T/L/I）**、8 个端到端场景、8 条故障注入矩阵、优先级与回归矩阵（含 workspace 回归）、缺陷分级、需求↔用例↔验收追溯表 | §1 策略 · §2 环境与 fixture · §3 总览 · §4 详细用例 · §5 E2E · §6 故障注入 · §7 回归矩阵 · §9 追溯 |
| [env-server-checklist.md](env/env-server-checklist.md) | **实施与上线 Checklist**：16 个阶段（阶段 0–15：GATE/ENV/CFG/**REG**（替代 workspace 的 DB 阶段，本特性零 DDL）/DEV/UT/API/E2E/SEC/PERF/OBS/OPS/TASK/CMP/REL/DOC+DEP/RB/POST/ACC）、接口契约快照（附录 A）、冒烟脚本（附录 B）、排障速查（附录 C）、与 workspace 组的映射（附录 D） | §1 GATE · §2 ENV/CFG/REG · §3–4 DEV/API · §5 E2E · §12 REL · §14 RB · §16 ACC · 附录 A/B/C/D |
| [env-server-test-report.md](env/env-server-test-report.md) | **实现与 103 实测记录**：落地文件清单与关键实现决策、L1 单元（40 条 / `env_registry` 行覆盖率 100%）、L2 接口冒烟（19/19）、客户端单测（9/9）、E2E（fixture → `env push` → 任务 `source haienv` → 探针 import）、workspace 回归、实现期新发现并修复的 4 个缺陷、AC-01~AC-12 对照 | §1 落地清单 · §2 环境 · §3 L1 · §4 L2 · §5 E2E · §6 缺陷 · §7 验收对照 |

联调脚本（`env_fixture.py` / `build_cli_local.sh` / `deploy_pod_dev.sh` / `mount_env_root.sh` /
`patch_env_override.py` / `smoke_env.sh` / `e2e_env.sh`）见 [scripts/README.md](scripts/README.md) §5。

## 2.6 `hai-cli images` 特性（分析 / 需求 / 设计 / 用例 / Checklist）

按「分析 → 需求 → 设计 → 用例 → 检查」顺序阅读；需求 ID（FR/NFR/SEC/OPS/CMP/HC）、验收 ID（AC-01~AC-14）与用例 ID（TC-*）、检查项 ID（GATE/ENV/CFG/DB/…/ACC）在追溯矩阵中可回溯。

> **与前两个特性最大的不同**：`images` 多了一条**运行期链路**（`launcher.py:144-147` 查表注入 `HFAI_IMAGE_WEKA_PATH`
> → 计算 pod 的 `link_hfai_image.sh` initContainer），而这条链路**缺一个脚本**（`marsv2/scripts/link_hfai_image.sh`
> 全仓不存在）。因此本特性的 DoD 不是「接口返回 200」，而是「**自定义镜像能真正跑起一个任务并产出可区分输出**」（AC-01）。

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [hai-cli-images-analysis.md](images/hai-cli-images-analysis.md) | **逆向分析**：三段式链路盘点（控制面半成品 / 提交面完整 / 运行面缺脚本）、`images load/delete` 抛 `AttributeError`（审计 C-3）、`user_images` 硬编码 `[]`（I2）、4 个未注册具名桩（I3）、`train_image` 零行零写入（I4）、**运行面 `link_hfai_image.sh` 缺失（I16）+ 103 节点前置实测（I17）**、任务侧 5 条硬契约 K1–K5、S1–S10 场景矩阵、**18 项风险 I1–I18**、103 实测原始输出 | §1 结论速览 · §3 客户端 · §4 服务端 · §4.6 运行面 · §5 场景矩阵 · §6 风险 · §9 实测 |
| [images-server-requirements.md](images/images-server-requirements.md) | **需求说明**：6 个目标、15 条功能需求、6 条非功能、7 条安全、5 条运维、6 条兼容、**10 条硬约束**、4 个接口（**API-15 `load` / API-16 `update_status` / API-17 `list` 修订 / API-18 `delete`，刻意与 4 个既有桩同名同序**）、状态机、**14 条验收（AC-01~AC-14）**、8 个待确认决策、追溯矩阵 | §3 需求总览 · §4 接口清单 · §5 状态与数据 · §6 DoD · §8 待确认 · §11 追溯矩阵 |
| [images-server-design.md](images/images-server-design.md) | **程序设计**：概念单点（`image_tar` vs `image` vs `path`）、4 接口契约与错误码、领域层签名与幂等 upsert、三种 loader 后端（`register` P0 默认 / `task` / `registry`）、**运行期 `link_hfai_image.sh` 契约与参考实现**、客户端补 `async_load/async_delete`、缓存一致性（launcher 进程内缓存）、灰度回滚可观测、端到端时序、**10 条 ADR（ADR-I1~I10）**、WBS ≈ **13 人日** | §3 概念与路径单点 · §4 接口契约 · §5 服务端 · §5.4 link 脚本 · §6 客户端 · §7 运行面 · §13 ADR · §14 WBS |
| [images-server-test-cases.md](images/images-server-test-cases.md) | **功能测试用例**：分层模型（L1–L4）、103（**无内网 registry**）与可选 registry 两套环境、**10 组用例（U/A/P/C/DB/S/F/O/T/L）**、8 个端到端场景（**E2E-01 = 控制面→运行面全链路，必须产出可区分输出**）、8 条故障注入、优先级与回归矩阵（含 workspace/env 回归）、需求↔用例↔验收追溯 | §1 策略 · §2 环境 · §3 总览 · §4 详细用例 · §5 E2E · §6 故障注入 · §7 回归矩阵 |
| [images-server-checklist.md](images/images-server-checklist.md) | **实施与上线 Checklist**：阶段 0–14（GATE/ENV/CFG/**DB**/DEV/UT/API/E2E/SEC/PERF/OBS/OPS/TASK/CMP/REL/DOC+DEP/RB/POST/ACC —— 本特性**有 DDL**，故用 workspace 的 `DB` 阶段而非 env 的 `REG`）、接口契约快照（附录 A）、冒烟脚本（附录 B）、排障速查（附录 C）、与 workspace/env 组的映射（附录 D） | §1 GATE · §3 DB · §4 DEV · §5 API · §6 E2E · §11 TASK · §17 ACC · 附录 A/B/C/D |
| [images-server-task-list.md](images/images-server-task-list.md) | **执行视图**：现状实测核对（含 I1–I18）、需求/约束汇总、S0–S7 **文件级**任务（含 `marsv2/scripts/link_hfai_image.sh`、迁移 `035`、节点前置 `/data_local`）、103 两套环境测试任务、待确认决策、**文档间不一致裁决**（审计 §5 缺口矩阵遗漏 images）、里程碑与关键路径 | §1 现状核对 · §3 任务列表 · §4 测试任务 · §5 待确认 · §6 不一致裁决 |
| [images-server-decisions.md](images/images-server-decisions.md) | **S0 决策记录**：Q-1~Q-8 冻结与 ADR 对应、R-2 定案（运行时通路只挂 initContainer：socket / ctr / 宿主 glibc 到 `/host-lib` + 显式 loader）、4 项文档不一致裁决（含「**不自动补 `:latest`**，以设计 I6b 为准」）、GATE 勾选对照 | §1 基线 · §2 决策 · §3 R-2 · §4 裁决 · §5 GATE |
| [images-server-test-report.md](images/images-server-test-report.md) | **实现与 103 实测记录**：落地文件清单、L1 `33 passed`、L2 `PASS=44 FAIL=0`、L3 E2E `PASS=26 FAIL=0`（强制重导入版）、迁移两轮重放、workspace/env 回归零回归、`/metrics` 与一级回滚演练、**9 个实现期缺陷（含新风险 I19：长 init 被 unschedulable 看门狗打断）**、AC 对照与未验证清单 | §0 结论 · §1 环境 · §3 L1 · §4 L2 · §5 DB · §6 缺陷 · §7 L3+回归+回滚 · §8 复现 |

> **与 [hai-cli-client-server-audit.md](hai-cli-client-server-audit.md) 的关系**：审计记录了客户端侧 **C-3**
> （`images load/delete` 抛 `AttributeError`），但**全文没有出现 `user_images`**、**没有任何 images 的 `S-x`**、
> §5 缺口矩阵也**没有 images 行**，且**完全未覆盖运行面**（`link_hfai_image` / `HFAI_IMAGE_WEKA_PATH` / launcher 查表注入）。
> `images` 文档集是这些结论的补充与修订，裁决项见任务列表 §6。

## 3. 实施视图（需求梳理 + 任务列表）

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [workspace-server-task-list.md](workspace/workspace-server-task-list.md) | **执行视图**：本仓库现状实测核对（11 项缺口）、五份文档的需求/接口/约束汇总、P0 分阶段任务列表（S0–S12 + P1-1~4，含文件清单、验收、依赖、估时）、103 环境测试任务（含 `localfs`/真实 OSS 两套环境对照）、待确认决策、文档间不一致裁决项 | §1 现状核对 · §2 需求梳理 · §3 任务列表 · §4 测试任务 · §5 待确认决策 · §7 文档不一致 |
| [workspace-dataflow.md](workspace/workspace-dataflow.md) | **数据流程图**：本地工作区 ↔ RustFS（S3）↔ K8s 共享盘 的 push/pull 时序图与 ASCII 图、任务侧 `s3://` 工作区挂载流程、路径/key 映射、tagging/zip/进度/状态机等关键机制、本环境实际取值速查、复现命令 | §0 一页简图 · §2 push · §3 pull · §4 任务侧 · §5 关键机制 · §6 取值速查 |

## 3.5 运维与编排资产

| 位置 | 内容 |
| --- | --- |
| [scripts/](scripts/) | 部署与验证脚本：镜像离线构建（`build_hai.sh` + `patch_dockerfile.py`）、无 registry 凭据时的部署旁路（`redeploy_local.sh`）、RustFS 部署（`deploy_rustfs.sh`）、`[cloud.storage]` 配置生成（`config_cloud_storage.sh`）、接口冒烟（`smoke_ugc.sh`）、7 子命令 E2E（`e2e_workspace.sh`）、S3 语义验证（`rustfs_check.py`）、**`hai-cli images` 现状基线探测（`probe_images.sh`）**；含用法与排障速查（§5 env / §6 images） |
| [../../deploy/terraform/](../../deploy/terraform/) | 测试环境编排快照（Multipass VM → MicroK8s → Hai Platform 三层），从 `hai-install` 复制、剔除本地 state 与凭据 |

## 4. 一页速览（结论）

1. **客户端已完整，服务端是断的**：客户端 9 个 `/ugc/*` 调用中，本仓库只注册 1 个且返回空列表；设计补齐全部 9 个 + 3 个 P1 接口。
2. **必须兼容客户端实际形态**：枚举串（`file_type=FileType.WORKSPACE`）、`text/plain` 的 JSON Body、`{"file_list":{...}}` 外壳——三者都由服务端兼容层吸收（分析报告 F2/F3，设计 ADR-2/ADR-3）。
3. **复用而非重写**：`cloud_storage/` 既有 1 900 行领域逻辑（STS、分页、进程池、断点续传、tagging）抽为 `service/` 层，`ugc-server` 与 `cloud-storage` 两个宿主共用。
4. **零 DDL 上线**：复用 `user_sync_status` / `user_downloaded_files`，只补 SQL 方法。
5. **生产的三个必答题**：多 worker 恢复互斥（设计 ADR-5）、终态 TTL ≥ 客户端超时（ADR-6）、任务侧 `oss://` 解析与挂载（FR-15/16）。
6. **数据库：P0 零 DDL 即可支撑**，但有三条访问层硬约束（禁止 `%s::type`、字面 `%` 要写 `%%`、参数只能 tuple + 枚举传 `.value`），且**仓库无自动迁移框架**（`init_postgresql.sh` 只在空库执行 DDL），任何新增表/列必须走人工迁移（DB 审计 §4/§7）。
7. **`hai-cli env`（haienv）：本地闭环完整，跨端是半成品**——客户端有 4 个本地子命令但**没有 `push`**（`push_venv` 是死代码且枚举必然失败、子进程命令还拼错可执行文件）；服务端**只有任务运行时消费**（`HAIENV_PATH` + `source haienv`），上传端点 `API-11` 只有桩且未注册路由。补齐只需 **3 件事**：入口（`env push` + `.value` + 可执行文件解析）、`env_root` 路径对齐、注册表写入（复用镜像内 `haienv` 包写 `venv.db`），复用既有 `cloud_storage` 传输通道，**不新增 Postgres 表**；规模 ≈6.5 人日（env 分析 §1、设计 §1/§14）。文档已齐：分析 + 需求/设计/用例/检查四件套（§2.5）。
8. **`hai-cli images`（用户自定义镜像）：三段式链路，三段完成度依次递减**——**① 控制面半成品**：`images list` 能用但服务端 `user_images` **硬编码 `[]`**，`images load/delete` 在客户端即抛 `AttributeError`（审计 **C-3**），服务端 `/ugc/user/train_image/{load,delete}` 实测 **404**，`train_image` 表**零行、零写入路径**；**② 提交面完整**：`registry/group/image:tag` 三段校验 + `status='loaded'` 白名单齐备（并反向定义了 5 条硬契约 K1–K5）；**③ 运行面结构完整但缺关键脚本**：`launcher` 会查表把 `path` 作为 `HFAI_IMAGE_WEKA_PATH` 注入，每个计算 pod 起 busybox initContainer 执行 `/marsv2/scripts/link_hfai_image.sh`，而**该脚本全仓不存在**（`marsv2/scripts/` 无此文件、`one/hai-up.sh` 挂载种子里也没有），且 103 节点上 `/data_local` 不存在、busybox 镜像引用不匹配（分析 §4.6 / §9-8）。
9. **`images` 的收口只需 4 件事，但 DoD 必须是「跑通一个任务」**——① 填充 `api/resource/image/default.py` 里**已存在的 4 个具名桩**并注册 3 条路由；② `user_images` 接上**已存在但零调用方**的 `TrainImageSelector.a_find_user_group_images`（并按 `updated_at DESC` 返回）；③ 补 `marsv2/scripts/link_hfai_image.sh` + 挂载种子 + 节点前置可配置；④ 客户端补 `async_load`/`async_delete` 并把失败提示从裸异常栈改为打印 `msg`。数据面 P0 选 **`register` 后端**（只登记、真正 import 推迟到 pod 启动时由 link 完成），因此 **103 在没有内网 registry 的条件下也能端到端验证**（AC-09）。**只修控制面不够**：`status='loaded'` 造出来后任务仍会卡在缺失的 link 脚本上（分析 §4.6 R3、风险 I16）。规模 ≈13 人日（设计 §14）。文档已齐：分析 + 需求/设计/用例/检查 + 任务列表（§2.6）。

## 5. 阅读路径建议

- **架构/评审**：分析报告 §1 结论 → 需求 §3 → 设计 §1/§15。
- **后端实现**：任务列表 §3 → 设计 §3 文件清单 → §4 接口契约 → §5 领域层 → §6 兼容层 → §7 数据 → Checklist §3。
- **测试**：任务列表 §4 → 需求 §11 追溯矩阵 → 用例 §1/§4 → Checklist §4/§5。
- **运维/发布**：设计 §9/§13 → Checklist §2/§10/§11/§12 + 附录 B/C。
- **`hai-cli env` 特性**：env 分析 §1 结论速览 → §3/§4 现状 → §6 风险（E1–E13）→ 需求 §3/§4 → 设计 §3 路径约定 → §4 接口契约 → §5 服务端 → §6 客户端 → §13 ADR → §14 WBS → 用例 §4/§5 → Checklist §1–§5、§16 + 附录 B/C。
- **`hai-cli images` 特性**：images 分析 §1 结论速览 → §3 客户端 / §4 服务端 → **§4.6 运行面（务必先读）** → §5 场景矩阵 → §6 风险（I1–I18）→ 需求 §3/§4/§5 → 设计 §3 概念单点 → §4 接口契约 → §5 服务端（含 §5.4 link 脚本）→ §6 客户端 → §7 运行面 → §13 ADR → §14 WBS → 用例 §4/§5（E2E-01 是 AC-01 的唯一判定依据）→ Checklist §1–§6、§11、§17 + 附录 A/C → 任务列表 §1 现状核对 / §3 任务列表 / §6 不一致裁决。

## 6. 全局横切审计

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [hai-cli-client-server-audit.md](hai-cli-client-server-audit.md) | **客户端 / 服务端实现现状审计**（基线 `d372319`，初版 `b866c10`，文首有变更说明）：把 `hai-cli` 全部子命令与服务端全部路由（**85 条**，operating 35 / query 33 / ugc **15** / monitor 2）放在一张表上做「调用 ↔ 注册」差分；`default.py`/`implement.py`/`custom.py` 三层约定与「桩的三种含义」判别规则；**6 条客户端调用缺服务端路由**、32 个 `not implemented` 桩的可达性分类、C-1~C-11 客户端缺陷与 S-1~S-11 服务端缺陷（含 C-7/C-8 闭环状态表）、按 P0–P3 排序的不完整清单、收口顺序建议、复现命令 | §0 结论速览 · §2.3 三层约定 · §3 客户端清单 · §4 服务端清单 · §5 缺口矩阵 · §7 env 判定 · §8 不完整清单 · §9 收口顺序 |

> 该文档是**横切视角**：`workspace/` 与 `env/` 两套文档是单特性深挖，本文做全局面盘点与交叉验证，结论与二者一致。审计基线 `b866c10`。

## 7. 进度快照（当前实际状态）

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [PROGRESS.md](PROGRESS.md) | **三特性开发进度快照**（跨特性进度视图）：分支与基线、三条工作流进度总表（代码 / 实机验证 / Checklist 勾选率 / 单元测试 / 里程碑）、每特性已完成与未完成清单、`images` 的 S0–S7 状态与**工作量燃尽**（≈10.5 / 13 人日）、运行面（`link_hfai_image.sh`）在真实任务 pod 上的调试经过、**文档与代码不一致清单**、剩余工作与建议顺序、按时间追加的增量记录 | §0 结论速览 · §2 进度总表 · §5 `images` · §6 不一致清单 · §7 剩余工作 · §8 增量记录 |

> **阅读建议**：先读本文 §0/§2 建立全局印象，再按需下钻到对应特性的分析 / 需求 / 设计 / 用例 / Checklist。
> 本文只回答「现在做到哪了」，**不重复**各特性文档的设计与需求条文（避免两处维护）。
> 维护方式：每次实质推进**只追加** §8 增量记录，并同步更新 §2 总表与对应特性章节的「状态 / 剩余」列。
