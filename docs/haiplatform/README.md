# docs/haiplatform · HAI Platform 服务端分析 / 设计文档索引

本目录收录 `hai-cli workspace` 工作区同步特性的**逆向分析**与**服务端实现设计**文档。

## 0. 测试环境（真实环境）

| 文档 | 内容 | 适用读者 |
| --- | --- | --- |
| [test-environment.md](test-environment.md) | **192.168.100.103 真实测试环境**：拓扑与节点规格、浏览器 / `hai-cli` / kubectl·FreeLens 三种访问方式、Terraform 部署流程、π 计算冒烟验证、已固化修复（源码层 / 编排层 / 节点本地）、7 项已知脆弱点、故障恢复 Runbook、DB schema 变更须知 | 全体（部署、联调、排障） |

> 与 [workspace-server-test-cases.md](workspace-server-test-cases.md) §2 的区别：**本文档描述真实已部署环境**；用例集 §2 是供编写用例时假设的抽象设定（单机 `localfs` / 真实 OSS 两条路径）。联调请以本文档为准。

## 1. 现状分析

| 文档 | 内容 | 适用读者 |
| --- | --- | --- |
| [hai-cli-workspace-analysis.md](hai-cli-workspace-analysis.md) | 对现有仓库的逆向分析：客户端插件、服务端半开源现状、9 个 `/ugc/*` 接口契约、10 项风险（F1–F10）、证据索引 | 全体 |

## 2. 服务端设计交付物（4 件套）

按「需求 → 设计 → 用例 → 检查」顺序阅读；每份文档的需求 ID / 用例 ID / Checklist ID 相互可追溯。

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [workspace-server-requirements.md](workspace-server-requirements.md) | 服务端实现需求：21 条功能需求、12 个接口、10 条非功能、8 条安全、7 条运维、6 条兼容、10 条硬约束、验收标准、追溯矩阵 | §3 需求总览 · §3.1 硬约束 · §10 DoD · §11 追溯矩阵 |
| [workspace-server-design.md](workspace-server-design.md) | 程序设计：总体架构、模块与文件清单、接口契约、领域层、兼容层、状态与数据、限额配置、任务侧 `oss://` 与挂载、审计、安全、部署灰度回滚、时序图、12 条 ADR | §4 接口契约 · §5 领域层 · §6 兼容层 · §7 状态与数据 · §10 任务侧 · §15 ADR · §17 WBS |
| [workspace-server-test-cases.md](workspace-server-test-cases.md) | 功能测试用例：分层策略、环境与数据准备、A–L + DB 共 13 组用例（182 + 12 条）、端到端场景、故障注入、性能/安全/兼容测试、回归矩阵、缺陷分级 | §1 策略 · §4 用例 · §5 E2E 场景 · §10 回归矩阵 |
| [workspace-server-checklist.md](workspace-server-checklist.md) | 实施与上线 Checklist：GATE/ENV/CFG/DB/DEV/UT/E2E/SEC/PERF/OBS/OPS/TASK/CMP/DOC+DEP/REL/RB/POST/ACC（组数表述见任务列表 §7 #3）、接口契约快照、冒烟脚本、排障速查 | §3 编码完成度 · §4 联调 · §10 发布灰度 · 附录 A/B/C |
| [workspace-server-db-audit.md](workspace-server-db-audit.md) | 数据库支撑性审计：表结构 ↔ 客户端契约逐字段核对、枚举实测对齐、**3 条 DB 访问层硬约束**、6 个能力缺口（G1–G6）、P1 DDL 与迁移/回滚、TC-DB 用例 | §0 结论 · §2 表核对 · §4 硬约束 · §5 缺口 · §7 迁移机制 |

## 3. 实施视图（需求梳理 + 任务列表）

| 文档 | 内容 | 关键章节 |
| --- | --- | --- |
| [workspace-server-task-list.md](workspace-server-task-list.md) | **执行视图**：本仓库现状实测核对（11 项缺口）、五份文档的需求/接口/约束汇总、P0 分阶段任务列表（S0–S12 + P1-1~4，含文件清单、验收、依赖、估时）、103 环境测试任务（含 `localfs`/真实 OSS 两套环境对照）、待确认决策、文档间不一致裁决项 | §1 现状核对 · §2 需求梳理 · §3 任务列表 · §4 测试任务 · §5 待确认决策 · §7 文档不一致 |

## 4. 一页速览（结论）

1. **客户端已完整，服务端是断的**：客户端 9 个 `/ugc/*` 调用中，本仓库只注册 1 个且返回空列表；设计补齐全部 9 个 + 3 个 P1 接口。
2. **必须兼容客户端实际形态**：枚举串（`file_type=FileType.WORKSPACE`）、`text/plain` 的 JSON Body、`{"file_list":{...}}` 外壳——三者都由服务端兼容层吸收（分析报告 F2/F3，设计 ADR-2/ADR-3）。
3. **复用而非重写**：`cloud_storage/` 既有 1 900 行领域逻辑（STS、分页、进程池、断点续传、tagging）抽为 `service/` 层，`ugc-server` 与 `cloud-storage` 两个宿主共用。
4. **零 DDL 上线**：复用 `user_sync_status` / `user_downloaded_files`，只补 SQL 方法。
5. **生产的三个必答题**：多 worker 恢复互斥（设计 ADR-5）、终态 TTL ≥ 客户端超时（ADR-6）、任务侧 `oss://` 解析与挂载（FR-15/16）。
6. **数据库：P0 零 DDL 即可支撑**，但有三条访问层硬约束（禁止 `%s::type`、字面 `%` 要写 `%%`、参数只能 tuple + 枚举传 `.value`），且**仓库无自动迁移框架**（`init_postgresql.sh` 只在空库执行 DDL），任何新增表/列必须走人工迁移（DB 审计 §4/§7）。

## 5. 阅读路径建议

- **架构/评审**：分析报告 §1 结论 → 需求 §3 → 设计 §1/§15。
- **后端实现**：任务列表 §3 → 设计 §3 文件清单 → §4 接口契约 → §5 领域层 → §6 兼容层 → §7 数据 → Checklist §3。
- **测试**：任务列表 §4 → 需求 §11 追溯矩阵 → 用例 §1/§4 → Checklist §4/§5。
- **运维/发布**：设计 §9/§13 → Checklist §2/§10/§11/§12 + 附录 B/C。
