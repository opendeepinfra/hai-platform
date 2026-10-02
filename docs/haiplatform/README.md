# docs/haiplatform · HAI Platform 服务端分析 / 设计文档索引

本目录收录两个 `hai-cli` 子特性的**逆向分析**与**服务端实现设计**文档：

- `hai-cli workspace` —— 工作区同步（客户端已完整、服务端已实现并上线验证）；
- `hai-cli env` —— 虚拟环境（`haienv`，本地闭环完整、跨端上传链路本次补齐）。

```
docs/haiplatform/
├── README.md          本索引
├── workspace/         workspace 特性文档（分析 / 需求 / 设计 / 用例 / Checklist / DB 审计 / 任务清单 / 数据流 / 测试环境）
├── env/               env 特性文档（分析 / 需求 / 设计 / 用例 / Checklist）
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

## 3. 实施视图（需求梳理 + 任务列表）

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [workspace-server-task-list.md](workspace/workspace-server-task-list.md) | **执行视图**：本仓库现状实测核对（11 项缺口）、五份文档的需求/接口/约束汇总、P0 分阶段任务列表（S0–S12 + P1-1~4，含文件清单、验收、依赖、估时）、103 环境测试任务（含 `localfs`/真实 OSS 两套环境对照）、待确认决策、文档间不一致裁决项 | §1 现状核对 · §2 需求梳理 · §3 任务列表 · §4 测试任务 · §5 待确认决策 · §7 文档不一致 |
| [workspace-dataflow.md](workspace/workspace-dataflow.md) | **数据流程图**：本地工作区 ↔ RustFS（S3）↔ K8s 共享盘 的 push/pull 时序图与 ASCII 图、任务侧 `s3://` 工作区挂载流程、路径/key 映射、tagging/zip/进度/状态机等关键机制、本环境实际取值速查、复现命令 | §0 一页简图 · §2 push · §3 pull · §4 任务侧 · §5 关键机制 · §6 取值速查 |

## 3.5 运维与编排资产

| 位置 | 内容 |
| --- | --- |
| [scripts/](scripts/) | 部署与验证脚本：镜像离线构建（`build_hai.sh` + `patch_dockerfile.py`）、无 registry 凭据时的部署旁路（`redeploy_local.sh`）、RustFS 部署（`deploy_rustfs.sh`）、`[cloud.storage]` 配置生成（`config_cloud_storage.sh`）、接口冒烟（`smoke_ugc.sh`）、7 子命令 E2E（`e2e_workspace.sh`）、S3 语义验证（`rustfs_check.py`）；含用法与排障速查 |
| [../../deploy/terraform/](../../deploy/terraform/) | 测试环境编排快照（Multipass VM → MicroK8s → Hai Platform 三层），从 `hai-install` 复制、剔除本地 state 与凭据 |

## 4. 一页速览（结论）

1. **客户端已完整，服务端是断的**：客户端 9 个 `/ugc/*` 调用中，本仓库只注册 1 个且返回空列表；设计补齐全部 9 个 + 3 个 P1 接口。
2. **必须兼容客户端实际形态**：枚举串（`file_type=FileType.WORKSPACE`）、`text/plain` 的 JSON Body、`{"file_list":{...}}` 外壳——三者都由服务端兼容层吸收（分析报告 F2/F3，设计 ADR-2/ADR-3）。
3. **复用而非重写**：`cloud_storage/` 既有 1 900 行领域逻辑（STS、分页、进程池、断点续传、tagging）抽为 `service/` 层，`ugc-server` 与 `cloud-storage` 两个宿主共用。
4. **零 DDL 上线**：复用 `user_sync_status` / `user_downloaded_files`，只补 SQL 方法。
5. **生产的三个必答题**：多 worker 恢复互斥（设计 ADR-5）、终态 TTL ≥ 客户端超时（ADR-6）、任务侧 `oss://` 解析与挂载（FR-15/16）。
6. **数据库：P0 零 DDL 即可支撑**，但有三条访问层硬约束（禁止 `%s::type`、字面 `%` 要写 `%%`、参数只能 tuple + 枚举传 `.value`），且**仓库无自动迁移框架**（`init_postgresql.sh` 只在空库执行 DDL），任何新增表/列必须走人工迁移（DB 审计 §4/§7）。
7. **`hai-cli env`（haienv）：本地闭环完整，跨端是半成品**——客户端有 4 个本地子命令但**没有 `push`**（`push_venv` 是死代码且枚举必然失败、子进程命令还拼错可执行文件）；服务端**只有任务运行时消费**（`HAIENV_PATH` + `source haienv`），上传端点 `API-11` 只有桩且未注册路由。补齐只需 **3 件事**：入口（`env push` + `.value` + 可执行文件解析）、`env_root` 路径对齐、注册表写入（复用镜像内 `haienv` 包写 `venv.db`），复用既有 `cloud_storage` 传输通道，**不新增 Postgres 表**；规模 ≈6.5 人日（env 分析 §1、设计 §1/§14）。文档已齐：分析 + 需求/设计/用例/检查四件套（§2.5）。

## 5. 阅读路径建议

- **架构/评审**：分析报告 §1 结论 → 需求 §3 → 设计 §1/§15。
- **后端实现**：任务列表 §3 → 设计 §3 文件清单 → §4 接口契约 → §5 领域层 → §6 兼容层 → §7 数据 → Checklist §3。
- **测试**：任务列表 §4 → 需求 §11 追溯矩阵 → 用例 §1/§4 → Checklist §4/§5。
- **运维/发布**：设计 §9/§13 → Checklist §2/§10/§11/§12 + 附录 B/C。
- **`hai-cli env` 特性**：env 分析 §1 结论速览 → §3/§4 现状 → §6 风险（E1–E13）→ 需求 §3/§4 → 设计 §3 路径约定 → §4 接口契约 → §5 服务端 → §6 客户端 → §13 ADR → §14 WBS → 用例 §4/§5 → Checklist §1–§5、§16 + 附录 B/C。

## 6. 全局横切审计

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [hai-cli-client-server-audit.md](hai-cli-client-server-audit.md) | **客户端 / 服务端实现现状审计**：把 `hai-cli` 全部子命令与服务端全部路由（83 条，operating 35 / query 33 / ugc 13 / monitor 2）放在一张表上做「调用 ↔ 注册」差分；`default.py`/`implement.py`/`custom.py` 三层约定与「桩的三种含义」判别规则；**7 条客户端调用缺服务端路由**、33 个 `not implemented` 桩的可达性分类、C-1~C-11 客户端缺陷与 S-1~S-11 服务端缺陷、按 P0–P3 排序的不完整清单、收口顺序建议、复现命令 | §0 结论速览 · §2.3 三层约定 · §3 客户端清单 · §4 服务端清单 · §5 缺口矩阵 · §8 不完整清单 · §9 收口顺序 |

> 该文档是**横切视角**：`workspace/` 与 `env/` 两套文档是单特性深挖，本文做全局面盘点与交叉验证，结论与二者一致。审计基线 `b866c10`。
