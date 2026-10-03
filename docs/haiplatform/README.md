# docs/haiplatform · HAI Platform 服务端分析 / 设计文档索引

本目录收录三个 `hai-cli` 子特性的**逆向分析**与**服务端实现设计**文档：

- `hai-cli workspace` —— 工作区同步（客户端已完整、服务端已实现并上线验证）；
- `hai-cli env` —— 虚拟环境（`haienv`，本地闭环完整、跨端上传链路本次补齐）；
- `hai-cli images` —— 用户自定义镜像（`hfai images`，三段式链路：控制面半成品 / 提交面完整 / 运行面缺一个脚本；**本次把上传入口做成 `images push` → RustFS → 共享盘**）。

```
docs/haiplatform/
├── README.md          本索引
├── workspace/         workspace 特性文档（分析 / 需求 / 设计 / 用例 / Checklist / DB 审计 / 任务清单 / 数据流 / 测试环境）
├── env/               env 特性文档（分析 / 需求 / 设计 / 用例 / Checklist）
├── images/            images 特性文档（分析 / 需求 / 设计 / 用例 / Checklist / 任务列表 / 决策记录 / 实测报告）
└── scripts/           部署与验证脚本（见 §3.5）
```

> 目录内的所有相对链接都指向同级子目录（`workspace/`、`env/` 或 `images/`）。

## 0. 测试环境（真实环境）

| 文档 | 内容 | 适用读者 |
| --- | --- | --- |
| [test-environment.md](workspace/test-environment.md) | **192.168.100.103 真实测试环境**：拓扑与节点规格、浏览器 / `hai-cli` / kubectl·FreeLens 三种访问方式、Terraform 部署流程、π 计算冒烟验证、已固化修复（源码层 / 编排层 / 节点本地）、7 项已知脆弱点、故障恢复 Runbook、DB schema 变更须知 | 全体（部署、联调、排障） |

> 与 [workspace-server-test-cases.md](workspace/workspace-server-test-cases.md) §2 的区别：**本文档描述真实已部署环境**；用例集 §2 是供编写用例时假设的抽象设定（单机 `localfs` / 真实 OSS 两条路径）。联调请以本文档为准。

## 1. 现状分析

| 文档 | 内容 | 适用读者 |
| --- | --- | --- |
| [hai-cli-workspace-analysis.md](workspace/hai-cli-workspace-analysis.md) | 对现有仓库的逆向分析：客户端插件、服务端半开源现状、9 个 `/ugc/*` 接口契约、10 项风险（F1–F10）、证据索引 | 全体 |
| [hai-cli-images-analysis.md](images/hai-cli-images-analysis.md) | 对 `hfai images`（用户自定义镜像）的逆向分析：**三段式链路**（控制面 / 提交面 / 运行面）实测盘点、`images load/delete` 的 `AttributeError`（沿用审计 C-3）、`user_images` 硬编码空、`train_image` 表零写入、**运行面 `link_hfai_image.sh` 缺失**、**上传入口缺失（§4.7）**、风险清单（I1–I19）、场景矩阵、103 实测原始输出 | 全体 |

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

按「分析 → 需求 → 设计 → 用例 → 检查」顺序阅读；需求 ID（FR/NFR/SEC/OPS/CMP/HC）、验收 ID（AC-01~AC-18）与用例 ID（TC-* / E2E-* / FI-*）、检查项 ID（GATE/ENV/CFG/DB/DEV/UT/API/E2E/SEC/PERF/OBS/OPS/TASK/CMP/DOC+DEP/REL/RB/POST/ACC）在追溯矩阵中可回溯。

> **本分支（`feature/hai-cli-images-rustfs-design`，基线 `feature/hai-cli-env-server-design` @ `33a5b26`）的定位**：
> **镜像上传主入口 = `hai-cli images push <本地 tar>`** —— 复用 `workspace`/`env` 既有的 RustFS/S3 流水线
> （API-01 签发 STS → 客户端直传 → API-05 stage2 落盘到 `image_path` → API-06 轮询），落盘后自动登记（API-15）；
> 手工把 tar 放到共享盘再 `images load` 只作为**兼容/运维旁路**保留。
>
> **来源与状态**：控制面（API-15~API-18）与运行面的设计**沿用旧分支** `feature/hai-cli-images-server-design`
> 上已实现并在 103 实测通过的结论（被测 tag `f2cb559`）；本分支基线 `33a5b26` 上 images 的**代码/迁移/脚本/测试都不存在**，
> **S9（并入 P0 资产）与 S8（上传通道）均已完成**，并在 103 上验证通过（`e2e_images_push.sh` `PASS=33 WARN=1 FAIL=0`：上传闭环 + md5 一致 + 幂等 + 开关一致性 + 负例，见 [images-server-test-report.md](images/images-server-test-report.md) §9.1/§9.2）。
> 文中 `P0` / `P1` 只标注来源与阶段（P0 = 控制面 + 运行面，P1 = 上传通道），**不代表可选项**。

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [hai-cli-images-analysis.md](images/hai-cli-images-analysis.md) | **逆向分析**：三段式链路盘点（控制面半成品 / 提交面完整 / 运行面缺脚本 + **上传入口缺失**）、`images load/delete` 抛 `AttributeError`（审计 C-3）、`user_images` 硬编码 `[]`（I2）、4 个未注册具名桩（I3）、`train_image` 零行零写入（I4）、**运行面 `link_hfai_image.sh` 缺失（I16）+ 103 节点前置实测（I17）**、任务侧 5 条硬契约 K1–K5、场景矩阵、风险 I1–I19、103 实测原始输出 | §1 结论速览 · §3 客户端 · §4 服务端 · §4.6 运行面 · **§4.7 上传入口** · §5 场景矩阵 · §6 风险 · §9 实测 |
| [images-server-requirements.md](images/images-server-requirements.md) | **需求说明**：7 个目标（G7 = 上传闭环为本分支首要目标）、20 条功能需求（FR-16~FR-20 = 上传通道）、非功能/安全/运维/兼容/**硬约束 HC-11~HC-14**、4 个控制面接口（**API-15 `load` / API-16 `update_status` / API-17 `list` 修订 / API-18 `delete`**）+ 上传通道复用 API-01/05/06 + 新增 **API-19**、状态机、**18 条验收（AC-01~AC-18，AC-15 = 上传闭环发布门禁）**、决策 Q-1~Q-12、追溯矩阵 | §3 需求总览 · §4 接口清单 · §4.6 上传通道 · §5 状态与数据 · §6 DoD · §8 待确认 · §11 追溯矩阵 |
| [images-server-design.md](images/images-server-design.md) | **程序设计**：概念单点（`image_tar` vs `image` vs `path` + `cloud_base_path` / `cluster_base_path`）、4 接口契约与错误码、领域层签名与幂等 upsert、三种 loader 后端（`register` 默认 / `task` / `registry`）、**运行期 `link_hfai_image.sh` 契约与参考实现**、**上传通道：复用 API-01/05/06 + 新增 API-19、服务端四处改动、客户端 `images push`、与运行面解耦**、缓存一致性、灰度回滚可观测、端到端时序、**14 条 ADR（ADR-I1~I14）**、WBS（P0 ≈13 人日已完成 / 本分支新增 S8 2.0 + S9 1.0） | §3 概念与路径单点 · §3.5 上传三路径 · §4 接口契约 · §4.6 上传通道 · §5 服务端 · §5.4 link 脚本 · §5.6 上传四处改动 · §6.4 `images push` · §7.5 与运行面衔接 · §9.5 开关与回滚 · §13 ADR · §14 WBS |
| [images-server-test-cases.md](images/images-server-test-cases.md) | **功能测试用例**：分层模型（L1–L4）、103（**无内网 registry**）与可选 registry 两套环境、**11 组 104 条用例（U/A/P/C/DB/S/F/O/T/L + UP）**、10 个端到端场景（**E2E-01 = 控制面→运行面全链路；E2E-09 = 上传闭环，本分支第一发布门禁**）、12 条故障注入（FI-09~FI-12 = 上传通道）、优先级与回归矩阵（含 workspace/env 回归）、需求↔用例↔验收追溯 | §1 策略 · §2 环境与 fixture · §3 总览 · §4 详细用例（§4.11 UP 组） · §5 E2E · §6 故障注入 · §7 回归矩阵 |
| [images-server-checklist.md](images/images-server-checklist.md) | **实施与上线 Checklist**：阶段 0–17（GATE/ENV/CFG/**DB**/DEV/UT/API/E2E/SEC/PERF/OBS/OPS/TASK/CMP/REL/DOC+DEP/RB/POST/ACC + **阶段 17 上传通道（本分支主体）**）、接口契约快照（附录 A，含 **A.6 上传通道**）、冒烟脚本（附录 B，含 B.2 上传冒烟）、排障速查（附录 C）、与 workspace/env 组的映射（附录 D）、未验证清单 | §1 GATE · §3 DB · §4 DEV · §5 API · §6 E2E · §11 TASK · **§18 阶段 17** · §17 ACC · 附录 A/B/C/D |
| [images-server-task-list.md](images/images-server-task-list.md) | **执行视图**：现状实测核对（21 项，含**第 21 项「没有面向 images 的上传通道」**）、需求/约束汇总、S0–S7 文件级任务（旧分支已完成）、**S8 上传通道（本分支主体）+ S9 P0 资产并入与入口切换**、103 两套环境测试任务（T-11/T-12/T-13/T-14）、待确认决策 Q-1~Q-12、文档间不一致裁决、里程碑（M0~M4）与关键路径 | §1 现状核对 · §3 任务列表（§3.9 S8 / §3.10 S9） · §4 测试任务 · §5 待确认 · §6 不一致裁决 · §7 里程碑 |
| [images-server-decisions.md](images/images-server-decisions.md) | **S0 决策记录**：基线 `33a5b26` 的 11 文件 md5 核对（与旧分支逐字一致）与**缺失物清单**、Q-1~Q-8 冻结（本分支不复议）、**Q-9~Q-12 上传主入口四项决策（建议冻结）**、R-2 定案（运行时通路只挂 initContainer：socket / ctr / 宿主 glibc 到 `/host-lib` + 显式 loader）、6 项文档间裁决（含「**不自动补 `:latest`**」「本分支口径优先」）、GATE-01~GATE-09 勾选对照 | §1 基线 · §2 Q-1~Q-8 · §3 Q-9~Q-12 · §4 R-2 · §5 裁决 · §6 GATE |
| [images-server-test-report.md](images/images-server-test-report.md) | **实现与 103 实测记录**：P0 证据（旧分支 tag `f2cb559`）——落地文件清单、L1 `33 passed`、L2 `PASS=44 FAIL=0`、L3 E2E `PASS=26 FAIL=0`（强制重导入版）、迁移两轮重放、workspace/env 回归零回归、`/metrics` 与一级回滚演练、9 个实现期缺陷（含新风险 I19）；**本分支 §9.1** = S9-2 在新基线重跑（preflight `30/0`、L1 `33 passed`、L2 `44/0`、L3 `26/0`、回归全绿），**§9.2** = 上传通道端到端 `e2e_images_push.sh` **`PASS=33 WARN=1 FAIL=0`** 与实现期缺陷 **D10/D11/D13**（+ 环境限制 **D14**），**§6.1** = 2026-10-03 巡检发现的**未修缺陷 D15**（`load --force` 同 tar 换 image 名时静默 no-op / 设计 R-16）与「热部署不抗容器重启」部署须知，**§9.3** = 未做项（FI-09~FI-11 故障注入、AC-17 续传腿、PERF、D15） | §0 结论 · §1 环境 · §3 L1 · §4 L2 · §5 DB · §6 缺陷 · §7 L3+回归+回滚 · **§7.4 上传通道已实现** · **§9 本分支实测（S9-2/S8-6）** |

> **上传通道（本分支主入口）一句话**：`images push` **复用** `workspace`/`env` 的 RustFS/S3 流水线，
> 只加 `file_type=image` 分支 + 强制 `no_zip=true` + 落点必须在 `image_path` 之下；服务端四处改动
> （`get_base_path` IMAGE 分支、`sync_to_cluster` 白名单与 `check_image_enabled`、迁移 `036`、可选 API-19），
> 客户端一条命令，节点侧一行不改（HC-14）。需求 FR-16~FR-20 · 设计 §3.5/§4.6/§5.6/§6.4/§7.5/§9.5 · ADR-I11~I14 ·
> 用例 §4.11 + E2E-09/10 + FI-09~12 · Checklist 阶段 17（14 项已勾选，103 实测 `PASS=33 WARN=1 FAIL=0`）。

> **与 [hai-cli-client-server-audit.md](hai-cli-client-server-audit.md) 的关系**：审计（**第三版，基线 `f995cbe`**）已把
> images 的 **C-3 标为已闭环**、ugc 路由 **15 → 19**、`not implemented` 桩 **32 → 28**，并把 s3/rustfs 的
> 「静态 AK/SK、prefix 不强制」（**R-14**）作为待收口的安全项登记；但审计的**视角是横切差分**，对
> **运行面**（`link_hfai_image.sh` / `HFAI_IMAGE_WEKA_PATH` / launcher 查表注入 / initContainer 导入）与
> **上传通道**（`images push` → RustFS → 共享盘 → 登记）仍以 `images/` 文档集为准。
> 两者的差异与裁决见 [images/images-server-task-list.md](images/images-server-task-list.md) §6。

## 3. 实施视图（需求梳理 + 任务列表）

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [workspace-server-task-list.md](workspace/workspace-server-task-list.md) | **执行视图**：本仓库现状实测核对（11 项缺口）、五份文档的需求/接口/约束汇总、P0 分阶段任务列表（S0–S12 + P1-1~4，含文件清单、验收、依赖、估时）、103 环境测试任务（含 `localfs`/真实 OSS 两套环境对照）、待确认决策、文档间不一致裁决项 | §1 现状核对 · §2 需求梳理 · §3 任务列表 · §4 测试任务 · §5 待确认决策 · §7 文档不一致 |
| [workspace-dataflow.md](workspace/workspace-dataflow.md) | **数据流程图**：本地工作区 ↔ RustFS（S3）↔ K8s 共享盘 的 push/pull 时序图与 ASCII 图、任务侧 `s3://` 工作区挂载流程、路径/key 映射、tagging/zip/进度/状态机等关键机制、本环境实际取值速查、复现命令 | §0 一页简图 · §2 push · §3 pull · §4 任务侧 · §5 关键机制 · §6 取值速查 |

## 3.5 运维与编排资产

| 位置 | 内容 |
| --- | --- |
| [scripts/](scripts/) | 部署与验证脚本：镜像离线构建（`build_hai.sh` + `patch_dockerfile.py`）、无 registry 凭据时的部署旁路（`redeploy_local.sh`）、RustFS 部署（`deploy_rustfs.sh`）、`[cloud.storage]` 配置生成（`config_cloud_storage.sh`）、接口冒烟（`smoke_ugc.sh`）、7 子命令 E2E（`e2e_workspace.sh`）、S3 语义验证（`rustfs_check.py`）；含用法与排障速查。**images 相关脚本**（`probe_images.sh`、`image_fixture.sh`、`patch_image_override.py`、`check_images_preflight.sh`、`smoke_images.sh`、`e2e_images.sh`、`e2e_images_push.sh`）已随 S9-1/S8 落地（上传通道脚本实测 `PASS=33 WARN=1 FAIL=0`） |
| [../../deploy/terraform/](../../deploy/terraform/) | 测试环境编排快照（Multipass VM → MicroK8s → Hai Platform 三层），从 `hai-install` 复制、剔除本地 state 与凭据 |

## 4. 一页速览（结论）

1. **客户端已完整，服务端是断的**：客户端 9 个 `/ugc/*` 调用中，本仓库只注册 1 个且返回空列表；设计补齐全部 9 个 + 3 个 P1 接口。
2. **必须兼容客户端实际形态**：枚举串（`file_type=FileType.WORKSPACE`）、`text/plain` 的 JSON Body、`{"file_list":{...}}` 外壳——三者都由服务端兼容层吸收（分析报告 F2/F3，设计 ADR-2/ADR-3）。
3. **复用而非重写**：`cloud_storage/` 既有 1 900 行领域逻辑（STS、分页、进程池、断点续传、tagging）抽为 `service/` 层，`ugc-server` 与 `cloud-storage` 两个宿主共用。
4. **零 DDL 上线**：复用 `user_sync_status` / `user_downloaded_files`，只补 SQL 方法。
5. **生产的三个必答题**：多 worker 恢复互斥（设计 ADR-5）、终态 TTL ≥ 客户端超时（ADR-6）、任务侧 `oss://` 解析与挂载（FR-15/16）。
6. **数据库：P0 零 DDL 即可支撑**，但有三条访问层硬约束（禁止 `%s::type`、字面 `%` 要写 `%%`、参数只能 tuple + 枚举传 `.value`），且**仓库无自动迁移框架**（`init_postgresql.sh` 只在空库执行 DDL），任何新增表/列必须走人工迁移（DB 审计 §4/§7）。
7. **`hai-cli env`（haienv）：本地闭环完整，跨端是半成品**——客户端有 4 个本地子命令但**没有 `push`**（`push_venv` 是死代码且枚举必然失败、子进程命令还拼错可执行文件）；服务端**只有任务运行时消费**（`HAIENV_PATH` + `source haienv`），上传端点 `API-11` 只有桩且未注册路由。补齐只需 **3 件事**：入口（`env push` + `.value` + 可执行文件解析）、`env_root` 路径对齐、注册表写入（复用镜像内 `haienv` 包写 `venv.db`），复用既有 `cloud_storage` 传输通道，**不新增 Postgres 表**；规模 ≈6.5 人日（env 分析 §1、设计 §1/§14）。文档已齐：分析 + 需求/设计/用例/检查四件套（§2.5）。
8. **`hai-cli images`（用户自定义镜像）：三段式链路，三段完成度依次递减**——**① 控制面半成品**：`images list` 能用但服务端 `user_images` **硬编码 `[]`**，`images load/delete` 在客户端即抛 `AttributeError`（审计 **C-3**），服务端 `/ugc/user/train_image/{load,delete}` 实测 **404**，`train_image` 表**零行、零写入路径**；**② 提交面完整**：`registry/group/image:tag` 三段校验 + `status='loaded'` 白名单齐备（并反向定义了 5 条硬契约 K1–K5）；**③ 运行面结构完整但缺关键脚本**：`launcher` 会查表把 `path` 作为 `HFAI_IMAGE_WEKA_PATH` 注入，每个计算 pod 起 busybox initContainer 执行 `/marsv2/scripts/link_hfai_image.sh`，而**该脚本全仓不存在**（`marsv2/scripts/` 无此文件、`one/hai-up.sh` 挂载种子里也没有），且 103 节点上 `/data_local` 不存在、busybox 镜像引用不匹配。**④ 上传入口同样缺失**：客户端只有 `list/load/delete`，`load` 只吃共享盘上已有的 tar，用户此前只能自己想办法搬运（分析 §4.7、任务列表 §1 第 21 项）。
9. **`images` 的收口 + 上传主入口（本分支）**——① **旧分支已完成并 103 实测通过**：填充 4 个具名桩并注册 3 条路由、`user_images` 接上 `TrainImageSelector.a_find_user_group_images`（`updated_at DESC`）、补 `marsv2/scripts/link_hfai_image.sh` + 挂载种子 + 节点前置可配置、客户端补 `async_load/async_delete`；数据面 P0 选 **`register` 后端**，因此 **103 在没有内网 registry 的条件下也能端到端验证**（AC-09）；证据：L1 `33 passed` / L2 `PASS=44 FAIL=0` / L3 `PASS=26 FAIL=0` / workspace+env 回归全绿。② **本分支把上传做成主入口**：`images push <本地 tar>` → RustFS/S3（API-01/05/06）→ `image_path` → 自动 `load`，共 4 处服务端改动 + 迁移 `036` + 客户端子命令（可选 API-19 预检），**节点侧一行不改**（HC-14）。③ **本分支工作量**：S9 并入 P0 资产并在并入基线上重跑 P0 证据（1.0）+ S8 上传通道（2.0）= **新增 3.0 人日**；P0 13.0 人日已在旧分支完成，合计 **16.0 人日**。文档已齐：分析 + 需求/设计/用例/检查 + 任务列表 + 决策记录 + 实测报告（§2.6）。

## 5. 阅读路径建议

- **架构/评审**：分析报告 §1 结论 → 需求 §3 → 设计 §1/§15。
- **后端实现**：任务列表 §3 → 设计 §3 文件清单 → §4 接口契约 → §5 领域层 → §6 兼容层 → §7 数据 → Checklist §3。
- **测试**：任务列表 §4 → 需求 §11 追溯矩阵 → 用例 §1/§4 → Checklist §4/§5。
- **运维/发布**：设计 §9/§13 → Checklist §2/§10/§11/§12 + 附录 B/C。
- **`hai-cli env` 特性**：env 分析 §1 结论速览 → §3/§4 现状 → §6 风险（E1–E13）→ 需求 §3/§4 → 设计 §3 路径约定 → §4 接口契约 → §5 服务端 → §6 客户端 → §13 ADR → §14 WBS → 用例 §4/§5 → Checklist §1–§5、§16 + 附录 B/C。
- **`hai-cli images` 特性**：images 分析 §1 结论速览 → §3 客户端 / §4 服务端 → **§4.6 运行面（务必先读）** → **§4.7 上传入口** → §5 场景矩阵 → §6 风险 → 需求 §3/§4/§5 → 设计 §3 概念单点 / §3.5 上传三路径 → §4 接口契约（§4.6 上传通道）→ §5 服务端（§5.4 link 脚本 / §5.6 上传四处改动）→ §6 客户端（§6.4 `images push`）→ §7 运行面（§7.5 衔接）→ §13 ADR → §14 WBS → 用例 §4/§5（E2E-01 = 兼容旁路判定，**E2E-09 = 上传闭环判定**）→ Checklist §1–§6、§11、§17、**§18 + 附录 A.6/B.2** → 任务列表 §1 现状核对 / §3.9–§3.10 / §6 不一致裁决 → 决策记录 §1 缺失物清单 / §3 Q-9~Q-12。

## 6. 全局横切审计

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [hai-cli-client-server-audit.md](hai-cli-client-server-audit.md) | **客户端 / 服务端实现现状审计**（**第三版基线 `f995cbe`**；第二版 `d372319`、初版 `b866c10`，文首有变更说明）：把 `hai-cli` 全部子命令与服务端全部路由（**89 条**，operating 35 / query 33 / ugc **19**（含 **train_image 4 条**）/ monitor 2）放在一张表上做「调用 ↔ 注册」差分；`default.py`/`implement.py`/`custom.py` 三层约定与「桩的三种含义」判别规则；**6 条客户端调用缺服务端路由**（不变；images 的 3 条已闭环）、**28 个** `not implemented` 桩的可达性分类、C-1~C-11 客户端缺陷与 S-1~S-11 服务端缺陷（含 **C-3**/C-7/C-8 闭环状态表）、**R-14（s3 前缀不强制）**、按 P0–P3 排序的不完整清单、收口顺序建议、复现命令 | §0 结论速览 · §2.3 三层约定 · §3 客户端清单 · §4 服务端清单 · §5 缺口矩阵 · §7 env 判定 · §8 不完整清单 · §9 收口顺序 |

> 该文档是**横切视角**：`workspace/`、`env/` 与 `images/` 三套文档是单特性深挖，本文做全局面盘点与交叉验证。**当前基线 `f995cbe`**（第三版）：images 的控制面 + 运行面 + 上传通道已实现并 103 实测通过，C-3 闭环、ugc 路由 15 → 19、桩 32 → 28；运行面与上传通道的深挖仍以 `images/` 文档集为准（差异与裁决见 [images/images-server-task-list.md](images/images-server-task-list.md) §6）。
