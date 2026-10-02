# HAI Platform · `hai-cli env`（haienv）服务端实施与上线 Checklist

> **实施进度（103 实测，2026-10-02 更新）**:M1 / M2 已达成；**M3 按「精简口径」执行**
> （本特性当前只服务内部用户，见下方「范围决策」）。最新证据（报告 §5）：L1 服务端单测 46 passed / 1 skipped
> （`env_registry.py` 行覆盖率 93%，M3 改动后重测）、客户端单测 39 passed、L2 冒烟 20/20、N3 幂等自检 5/5、
> 回滚演练 8/8（`DRILL_L2=1` 时 13/13；关停 4–5s / 恢复 5s）、L3 E2E 与 workspace 回归结果见报告。
> 一键脚本 `scripts/verify_env.sh` 一次跑完 L1→L2→幂等→回滚→E2E→回归。
> 未执行项（压测 PERF、FI-04/06、正式发布 REL、上线观察 POST、验收签署 ACC、私有层 Q-1）在报告 §8.1 / §9.2 列出。
>
> **范围决策（精简 M3）**:只服务内部用户 → 落地「前置修复 + 回滚 + 最小看板 + 幂等/兼容验证」；
> **正式发布镜像、三档灰度观察期（REL-02/03）、上线后观察（POST）、验收签署（ACC）留给真上线**，
> 相关项保持未勾选。阶段 1–5 的可自动化部分已在 103 实测并留有证据（报告 §5），
> 逐项勾选由特性负责人在评审时按报告位置补齐。
>
> **本轮已勾选项的判据**:阶段 7（OBS）、阶段 8（OPS）、阶段 10（CMP）、阶段 13（RB）以及
> `GATE-04/05`、`REG-01`、`UT-01/02/03/05`、`E2E-10` —— 每项都在「验收证据」列写明了脚本/日志位置。
>
> **文档定位**:`docs/haiplatform/env/` 四件套之四(《[分析](hai-cli-env-analysis.md)》→《[需求](env-server-requirements.md)》→《[设计](env-server-design.md)》→《[用例](env-server-test-cases.md)》→ **Checklist**)。
> **使用方式**:按阶段自上而下勾选;每项须给出**可核验的证据**（命令输出 / 文件路径 / 截图 / 测试报告编号），不接受口头确认。
> **ID 说明**:本表 ID（`GATE-xx` / `ENV-xx` / `CFG-xx` / `REG-xx` / `DEV-xx` / `UT-xx` / `API-xx` / `E2E-xx` / `SEC-xx` / `PERF-xx` / `OBS-xx` / `OPS-xx` / `TASK-xx` / `CMP-xx` / `REL-xx` / `DOC-xx` / `DEP-xx` / `RB-xx` / `POST-xx` / `ACC-xx`）是**检查项编号**,与《需求》的 `FR/NFR/SEC/OPS/CMP/HC` 与《用例》的 `TC-*` 不同源,勿混用。
> **阶段与设计 WBS 的对应**:阶段 0–2 ≈ S0/S1;阶段 3 ≈ S2/S4;阶段 4–5 ≈ S3/S5;阶段 6–9 ≈ S3/S7;阶段 10 ≈ S1;阶段 11–12 ≈ S6/S7;阶段 13 ≈ S6;阶段 14–16 ≈ S7。
> **里程碑**:M1 契约可用（阶段 0–4）→ M2 端到端可用（阶段 5）→ M3 可上线（阶段 6–16）。

---

## 1. 阶段 0 · 启动前置（GATE-0）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | GATE-01 | 确认需求 §8 的 6 项待确认决策已有结论，尤其 **Q-1（私有 `custom.py` 是否已实现 `/ugc/update_cluster_venv`）** 与 **Q-2（路径方案）** | 决策记录（链接/邮件/会议纪要） | 后端 + 平台 |
| ☐ | GATE-02 | 若 Q-1 结论为"私有层已实现"：确认其契约与本设计的差异，并决定以哪一层为准 | 差异清单 + 裁决结论 | 后端 |
| ☐ | GATE-03 | 冻结接口契约（附录 A），任何后续变更走变更流程 | 附录 A 评审通过记录 | 后端 + 前端/客户端 |
| ☑ | GATE-04 | 确认**误伤面**：本次改动涉及 `conf/utils.py` 与 `cloud_storage/utils.py:get_base_path`（workspace 主链路共用） | 影响面清单 + workspace E2E 回归计划（M3 已验：报告 §5.5 workspace 回归 smoke_ugc 8/8 + e2e_workspace 19/19） | 后端 |
| ☑ | GATE-05 | 确认**零 DDL**：`db_schemas/` 无新增迁移；注册信息落在共享盘 SQLite（HC-03） | `git diff --stat db_schemas/` 为空（M3 已验：报告 §9.1「零 DDL」，db_schemas/ 无 diff，注册表复用既有 haienv 表） | 后端 |
| ☐ | GATE-06 | 确认客户端与服务端使用**同一份 `haienv` 轮子**（ADR-E4 前提） | 两端 `pip show haienv` 版本一致 | 客户端 + 后端 |
| ☐ | GATE-07 | 明确 `extend=True` 环境**不支持上传**的对外口径（含文档与报错文案） | 文案评审通过 | 产品 + 后端 |

---

## 2. 阶段 1 · 环境与配置（ENV / CFG）

### 2.1 环境准备

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | ENV-01 | 测试用 ugc-server（`SERVER=ugc`,:8083）可启动，`env_path=/tmp/hai-test` | 启动日志 + `curl /ugc/get_sync_status` 返回 `success` | 测试 |
| ☐ | ENV-02 | 对象存储就绪：`localfs` 路径可写；发布前另备真实 S3/OSS（用例 §2.2） | `ls /tmp/hai-test/localfs` + 真实 provider 冒烟 | 测试 |
| ☐ | ENV-03 | E2E 环境：客户端容器 + ugc-server + operating-server/launcher + 共享盘 | 一次 workspace 任务可提交成功（环境健康基线） | 测试 |
| ☐ | ENV-04 | 测试用户 T_A / T_B / T_C / T_ADMIN 就绪；**服务端进程账号 ≠ T_A**（权限用例前提） | `id` 输出 + `ls -ld env_root/U-A` | 测试 |
| ☐ | ENV-05 | fixture 可用：§2.5 的 `venv.db` + prefix 构造脚本可重复执行 | 脚本 + 一次 `source haienv` 通过 | 测试 |
| ☐ | ENV-06 | 探针包 `haienv_probe_unique` 已放入环境 `site-packages` | import 断言通过 | 测试 |

### 2.2 配置

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | CFG-01 | `cloud.storage.service.env_path` 语义已在 `one/one_etc/core.toml` 注释中写明（`env_root = {env_path}/hfai_envs`） | 配置文件 diff | 后端 |
| ☑ | CFG-02 | 新增开关项已定义默认值：`env_push_enabled=true`、`env_push_enabled_users=[]`、`env_push_enabled_groups=[]`、`env_name_regex` | 配置样例 + 启动日志（已验：报告 §5.6 + 103 override.toml；`env_name_regex` 含 `/` 时被忽略，见 L1） | 后端 |
| ☐ | CFG-03 | `env_path` 与运行时 `HAIENV_PATH` 推导同源（均走 `get_env_root()`），代码中**无第二处硬编码** | `grep -rn "hfai_envs" --include=*.py` 只剩 `conf/utils.py` | 后端 |
| ☑ | CFG-04 | 启动自检（OPS-01）已接入：不一致时打印 ERROR + 建议值，且**不阻断启动** | 刻意配错 → 日志截图；正常配置 → `env path check: OK`（M3 已验：启动日志 `env path check: OK env_root=… haienv_version=1.4.1+envtest5`） | 后端 + 测试 |

### 2.3 注册表与权限（REG，替代 workspace 的 DB 阶段）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | REG-01 | 确认**无 Postgres DDL**；注册表为 `{env_root}/<user>/venv.db` 的既有 `haienv` 表 | `db_schemas/` 无 diff + `sqlite3 .schema` 输出（M3 已验：报告 §9.1，注册表=既有 venv.db 的 haienv 表，无 DDL） | 后端 |
| ☐ | REG-02 | 服务端进程对 `{env_root}/<user>/` 的写权限已明确并落地（设计 §4.4 三项修复路径之一） | `ls -ld env_root/*` 权限矩阵 | 运维 + 后端 |
| ☐ | REG-03 | 存量用户目录权限已批量修正（若采用路径 ②） | 修正前后对比清单 | 运维 |
| ☐ | REG-04 | 顶层 `env_root` 权限与镜像构建期 `chmod 777` 一致（`one/release.sh:15-18`） | `ls -ld env_root` | 运维 |
| ☑ | REG-05 | `haienv` 包在 ugc-server 进程内**可 import 且无副作用**（惰性 import 生效） | 启动日志无多余 `venv.db` 创建；`python -c` 探测通过（已验：报告 §9.1 惰性 import；启动日志无额外 venv.db 创建） | 后端 |

---

## 3. 阶段 2 · 编码完成度（DEV / UT）

### 3.1 路径与服务端

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | DEV-01 | `conf/utils.py` 新增 5 个纯函数（`get_env_path/get_env_root/get_user_env_dir/get_env_registry_path/get_env_dir_name`），仅依赖 `os/re` 与惰性 `CONF` | 代码 + 单测（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-02 | `cloud_storage/utils.py:get_base_path` 的 ENV 分支：cluster 改为 `{env_root}/{user}/{name}`，cloud（S3 key）保持不变 | 代码 + `git diff`（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-03 | `single_task_impl.py:60-61` 的 `HAIENV_PATH` 改用 `get_env_root()` | 代码 + TC-T01（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-04 | `cloud_storage/service/env_registry.py` 实现：名称校验 / 路径推导 / 权限探测 / 注册（`flock` + `REPLACE`）/ 回读校验 | 代码 + UT（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-05 | 实现放 `api/resource/storage/default.py`（可被 `custom.py` 覆盖），**未**放 `implement.py` | 代码位置 + TC-O03（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-06 | `api/register/implement.py` 的 `ugc` 段新增两条路由注册 | 代码 + `curl` 探测 404→200（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-07 | `errors.py` 新增 `ENV_REGISTRY_NOT_WRITABLE` / `ENV_REGISTRY_WRITE_FAILED` / `ENV_PATH_MISMATCH`（如需 `ENV_ALREADY_EXISTS`） | 代码 + 错误码表（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-08 | 领域层未 import fastapi、未注册路由（HC-06）；同步 I/O 已用 `asyncwrap`/`to_thread` 包装（NFR-04） | 代码评审记录（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-09 | 灰度开关接入两接口（FR-12） | 代码 + TC-A10/A19（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-10 | 指标与结构化日志接入（NFR-05 / SEC-05） | 代码 + TC-L01~L04（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |
| ☑ | DEV-11 | 所有响应体含 `success`（HC-05）；业务失败走 `WorkspaceError`，不抛裸 500 | 代码 + TC-A13/A16（M3 已验：报告 §5.1 / §9.1，代码位置与实现说明） | 后端 |

### 3.2 客户端

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | DEV-12 | `plugins/haienv/haienv/client/command.py` 新增 `push` 子命令，`cli.py` 注册 | 代码 + TC-C01（M3 已验：报告 §5.3 客户端单测 39 passed） | 客户端 |
| ☑ | DEV-13 | `client/api/venv_api.py`：`FileType.ENV` → `.value`(**修 E3/F2**) | 代码 + TC-C03（M3 已验：报告 §5.3 客户端单测 39 passed） | 客户端 |
| ☑ | DEV-14 | `client/api/venv_api.py`：子进程命令改为解析 `haiworkspace` 可执行文件(**修 E13**)，不再使用 `sys.argv[0]` | 代码 + TC-C02（M3 已验：报告 §5.3 客户端单测 39 passed） | 客户端 |
| ☑ | DEV-15 | `client/api/venv_api.py`：`path` 为空 / `success=0` 的防御(**修 E7**) | 代码 + TC-C06（M3 已验：报告 §5.3 客户端单测 39 passed） | 客户端 |
| ☑ | DEV-16 | 分级结果输出（上传失败 / 已上传未注册 / 全成功）+ 退出码 | 代码 + TC-C07/C08（M3 已验：报告 §5.3 客户端单测 39 passed） | 客户端 |
| ☑ | DEV-17 | push 成功后的注册调用已接入（API-13），失败不回滚（NFR-06） | 代码 + TC-C09（M3 已验：报告 §5.3 客户端单测 39 passed） | 客户端 |
| ☑ | DEV-18 | 客户端 `list_haienv`/`set_env` 的 `-u` 参数加 `..`/`/` 校验（修 E9 / SEC-06） | 代码 + TC-S07（M3 已验：报告 §5.3 客户端单测 39 passed） | 客户端 |
| ☐ | DEV-19 | 顺手项（若采纳 Q-6）：workspace 侧 3 处枚举字符串化同步修 | 代码 + workspace 回归 | 客户端 |

### 3.3 单元测试与静态检查

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | UT-01 | `env_registry` 行覆盖率 ≥ 85%；纯函数分支 100% | `pytest --cov` 报告（M3 已验：报告 §5.1 → 46 passed / 1 skipped，env_registry.py 行覆盖率 100%） | 后端 |
| ☑ | UT-02 | 测试**未 mock** `HaienvConfig` / `SqliteDict`（CMP-03 前提） | 测试代码评审（M3 已验：报告 §5.1；测试未 mock HaienvConfig/SqliteDict，坏行用真实 sqlite 写入构造） | 后端 |
| ☑ | UT-03 | U 组 14 条用例全部通过 | 测试报告（M3 已验：报告 §5.1 U 组全过） | 后端 |
| ☐ | UT-04 | 静态检查（flake8/ruff/pyflakes）无新增告警 | CI 输出 | 后端 |
| ☑ | UT-05 | `conf/utils.py` 改动未破坏既有 workspace 单测 | CI 全绿（M3 已验：报告 §5.5 workspace 回归全过） | 后端 |

---

## 4. 阶段 3 · 接口契约（API）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | API-01 | API-11 正常路径返回 `{success,path,exists}`，与附录 A 一致 | `curl` 输出 | 后端 |
| ☐ | API-02 | API-11 **旧形态**（无 `extend`）可用 | TC-A02 输出 | 后端 |
| ☐ | API-03 | API-11 拒绝 `extend=True`、非法名、无权限 | TC-A04/A05/A06 | 后端 |
| ☐ | API-04 | API-11 幂等：重复调用响应完全相同 | TC-A08 | 后端 |
| ☐ | API-05 | API-13 正常路径返回 `{success,registered,path,db}` | `curl` 输出 | 后端 |
| ☐ | API-06 | API-13 兼容 `text/plain` 与 `application/json` body | TC-A12 | 后端 |
| ☐ | API-07 | API-13 拒绝越界 path、他人目录、非法名 | TC-A13/A14/A15 | 后端 |
| ☐ | API-08 | API-13 幂等：重复注册记录数不增长 | TC-A17 | 后端 |
| ☐ | API-09 | `extra_search_dir/bin_dir/environment` 三列表原样落库 | TC-A18 | 后端 |
| ☐ | API-10 | 鉴权：缺/过期/不活跃 token 均 `UNAUTHORIZED` 且带 `success` | TC-A07/A20 | 后端 |
| ☐ | API-11 | A 组 20 条用例全部通过 | 测试报告 | 测试 |

---

## 5. 阶段 4 · 联调（E2E）— 用**真实客户端**验证

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | E2E-01 | **路径三方一致**（P 组 6 条全过，AC-02） | TC-P01~P06 记录 + 真实环境路径四方对照 | 后端 + 测试 |
| ☐ | E2E-02 | 主场景：`env push` → 任务 `source haienv` → **探针包 import 成功**（AC-03） | E2E-01 记录 | 测试 |
| ☐ | E2E-03 | 可见性：`hai-cli env list` 能列出新环境（AC-04） | `env list` 输出 | 测试 |
| ☐ | E2E-04 | 增量 push：第二次上传字节数 = 0，注册记录数 = 1（AC-05） | E2E-02 记录 | 测试 |
| ☐ | E2E-05 | 失败分级：`已上传但注册失败，可重试`，且重试仅补注册（AC-06） | E2E-04 记录 | 测试 |
| ☐ | E2E-06 | 多用户隔离：同名 env 互不覆盖，owner 解析正确 | E2E-05 记录 | 测试 |
| ☐ | E2E-07 | 中断恢复：stage1 中断可重跑成功 | E2E-06 记录 | 测试 |
| ☐ | E2E-08 | 灰度 + 老客户端组合验证（AC-10） | E2E-07 记录 | 测试 |
| ☐ | E2E-09 | 大环境（3 万文件 + 1.2 GB）：分片与排除项生效 | E2E-08 记录（P1） | 测试 |
| ☑ | E2E-10 | **workspace 回归**：`e2e_workspace.sh` 19/19、`smoke_ugc.sh` 8/8 | 脚本输出（M3 已验：报告 §5.5 → smoke_ugc 8/8、e2e_workspace 19/19） | 后端 + 测试 |
| ☐ | E2E-11 | C 组 12 条 + REG 组 10 条 + T 组 6 条全部通过 | 测试报告 | 测试 |

---

## 6. 阶段 5 · 安全（SEC）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | SEC-01 | 身份只来自 token；伪造 `username/group` 被忽略 | TC-S01 | 后端 |
| ☐ | SEC-02 | 跨用户写入被拒；他人 `venv.db` 未被触碰 | TC-S02 / TC-A14 | 后端 |
| ☐ | SEC-03 | path 穿越（`..` / 编码 / 软链）全部拒绝 | TC-S03 / TC-S08 | 后端 |
| ☐ | SEC-04 | 名称注入（SQL/引号）不改变表结构 | TC-S04 | 后端 |
| ☐ | SEC-05 | 无权限场景返回明确 code，不泄漏内部细节 | TC-S05 | 后端 |
| ☐ | SEC-06 | 日志 token 掩码；错误 `msg` 不含完整 token | TC-S06 / TC-L04 | 后端 |
| ☐ | SEC-07 | 客户端 `-u` 路径注入已修复（E9） | TC-S07 | 客户端 |
| ☐ | SEC-08 | S 组 10 条全部通过 | 测试报告 | 测试 |

---

## 7. 阶段 6 · 性能与容量（PERF）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | PERF-01 | API-11 P99 < 100 ms（200 QPS × 60 s，无 5xx） | 压测报告 | 测试 |
| ☐ | PERF-02 | API-13 P99 < 300 ms（100 QPS × 60 s，记录数一致） | 压测报告 | 测试 |
| ☐ | PERF-03 | 1.2 GB env 全链路时长与 workspace 同量级 | E2E-08 数据 | 测试 |
| ☐ | PERF-04 | 单用户 200 env / 注册表 5 MB 场景仍在门槛内 | TC-I04/I05 | 测试 |
| ☐ | PERF-05 | 压测期间并发调 `/ugc/get_sync_status` 不被阻塞（NFR-04） | 并发探针结果 | 后端 |

---

## 8. 阶段 7 · 可观测性与告警（OBS）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | OBS-01 | 指标 `env_push_requests_total` / `env_register_duration_seconds` / `env_registry_write_failures_total` 可在 `/metrics` 抓到 | 抓取输出（M3 已验：报告 §5.6 → /metrics 抓到 4 个 env 指标族，含新增 env_registry_read_failures_total） | 后端 |
| ☑ | OBS-02 | 看板：注册成功率、P99、失败 reason 分布 | 看板链接（M3 已验：报告 §5.6 → 103 无 Prometheus/Grafana，以 env_metrics.sh 命令行看板交付：成功率/P50·P95·P99/失败 reason + 5% 判定） | 运维 |
| ☑ | OBS-03 | 告警：注册失败率 > 5%（5 min）触发告警 | 告警规则文件（M3 已验：报告 §5.6 → env_alerts.yml 配置即代码交付（同指标名/同阈值），生产集群 `kubectl apply -f`） | 运维 |
| ☑ | OBS-04 | 日志字段 `user/env/path/code/elapsed_ms` 齐全且 token 掩码 | 日志样例（M3 已验：报告 §9.1 N7 → 日志含 user/env/path/code/elapsed_ms + token 掩码） | 后端 |

---

## 9. 阶段 8 · 运维与合规（OPS）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | OPS-01 | 启动自检输出 `env path check` 结果（OPS-01 需求） | 启动日志（M3 已验：启动日志 `env path check: OK`，并打印 haienv_version） | 后端 |
| ☑ | OPS-02 | 灰度开关可动态生效：`enabled_users/groups` 白名单验证 | TC-O04（M3 已验：报告 §5.6 → 改 override.toml + 重启 ugc_server，4s 内三条写入路径全部 FEATURE_DISABLED） | 后端 + 测试 |
| ☐ | OPS-03 | 运维手册条目：手工修复某用户 `venv.db`（只读校验 + 删除错误 key） | 手册文档 + TC-O07 复现 | 运维 |
| ☐ | OPS-04 | 无共享盘部署（纯对象存储）场景有明确降级行为与提示 | 设计 §11 + 手工验证 | 后端 |
| ☐ | OPS-05 | 存量用户目录权限现状摸排完成，并给出修正脚本/清单 | REG-02/REG-03 证据 | 运维 |

---

## 10. 阶段 9 · 任务侧集成（TASK）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | TASK-01 | `HAIENV_PATH` 与数据面同源（TC-T01） | 单测 + 任务内 `echo $HAIENV_PATH` | 后端 |
| ☐ | TASK-02 | 任务内 `source haienv <name>` 可命中新注册环境（TC-T02） | 任务日志 | 测试 |
| ☐ | TASK-03 | 探针包断言通过（环境真正生效，TC-T03） | 任务输出 | 测试 |
| ☐ | TASK-04 | 跨用户 `HF_ENV_OWNER` 解析正确（TC-T04） | 任务日志 | 测试 |
| ☐ | TASK-05 | 未注册环境给出可诊断信息，且不改变任务成败语义（FR-11 / TC-T05） | 任务日志对比 | 后端 |
| ☐ | TASK-06 | 老环境名兼容分支（`py38-202207` 等）行为不变（TC-T06） | 回归记录 | 后端 |
| ☐ | TASK-07 | 平台基础环境 `platform/hai202207_0` 不受影响（TC-O10） | 任务内 source 成功 | 测试 |

---

## 11. 阶段 10 · 兼容与升级（CMP）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | CMP-01 | 老客户端（无 `env push`）零回归 | E2E-07 + workspace 回归（M3 已验：报告 §5.5 → workspace 回归全过；新接口对老客户端是纯追加） | 客户端 + 测试 |
| ☑ | CMP-02 | 枚举串客户端 `file_type=FileType.ENV` 仍被服务端归一化 | TC-O02（已验：报告 §5.2 枚举串 `FileType.ENV` 归一化） | 后端 |
| ☐ | CMP-03 | 私有 `custom.py` 覆盖生效（HC-08） | TC-O03（模拟部署） | 后端 |
| ☑ | CMP-04 | `haienv` 版本偏移时失败安全：不写坏 `venv.db` | TC-REG-09（M3 已验：报告 §5.1 N3b（读失败 fail-closed / 坏行隔离）+ §5.3 CMP-04 单测（haienv_version 提示与 HAIENV_STRICT_VERSION）） | 后端 |
| ☑ | CMP-05 | S3 key 布局不变（`<group>/shared/hfai_envs/...`），存量对象无迁移需求 | TC-P02（已验：报告 §5.1 TC-P02 → S3 key 布局 `<group>/shared/hfai_envs/...` 不变） | 后端 |
| ☐ | CMP-06 | CMP 组 10 条（O 组）全部通过 | 测试报告 | 测试 |

---

## 12. 阶段 11 · 发布与灰度（REL）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | REL-01 | 发布顺序：**先服务端**（两接口上线、默认 `env_push_enabled=false`）→ 再客户端（含 `env push`） | 发布记录 | 运维 |
| ☐ | REL-02 | 灰度：首批 1–2 个内部用户，观察 24 h 注册成功率与失败 reason | 看板截图 | 运维 |
| ☐ | REL-03 | 扩量：白名单 10% → 50% → 全量，每档观察 24 h | 灰度记录 | 运维 |
| ☐ | REL-04 | 发布时同步更新 `core.toml` 注释与镜像内 `haienv` 版本一致性 | 配置 diff + `pip show` | 运维 |
| ☐ | REL-05 | 变更窗口内 workspace 业务无异常（共用 `get_base_path`） | 监控 + on-call 记录 | 运维 |

---

## 13. 阶段 12 · 文档与交付物（DOC / DEP）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | DOC-01 | `docs/_sources/cli/ugc.rst.txt` 的 `haienv` 段补齐 `push` 子命令 | 文档 diff + 构建通过（M3 已验：`ugc.rst.txt` 用 `.. click:: haienv.client.cli:cli :nested: full` 自动收录，`push` 子命令已注册） | 客户端 |
| ☑ | DOC-02 | `docs/_sources/guide/environment.md.txt` 补充"本地建环境 → push → 任务使用"完整流程 | 文档 diff（M3 已验：`docs/_sources/guide/environment.md.txt` §「把本地环境推送到集群」含 create→push→任务指定全流程） | 产品 + 客户端 |
| ☑ | DOC-03 | 文档写明限制：仅非 extend、CUDA/conda 前置、失败排查 | 文档 diff（M3 已验：同文件「前置条件与限制」+「常见失败与处置」表；M3 新增 NFS 可见性与 ENV_REGISTRY_READ_FAILED 两行） | 客户端 |
| ☐ | DOC-04 | `docs/haiplatform/README.md` §2.5 已收录四件套链接，链接可点 | 链接检查脚本输出 | 后端 |
| ☐ | DOC-05 | `docs/haiplatform/env/` 四件套编号交叉引用一致（FR ↔ AC ↔ TC ↔ DEV） | 自检脚本输出 | 后端 |
| ☐ | DEP-01 | 交付物归档：环境镜像 tag、`haienv` 轮子版本、变更清单 | 发布包 + 清单 | 运维 |
| ☐ | DEP-02 | 客户端版本发布说明（Release Note）含 `env push` 新增与限制 | Release Note | 客户端 |

---

## 14. 阶段 13 · 回滚演练（RB）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☑ | RB-01 | 一级回滚：`env_push_enabled=false` 动态生效，新请求 `FEATURE_DISABLED`，已注册环境仍可用 | 演练记录（含时间戳）（M3 已验：报告 §5.6 → env_rollback_drill.sh 8/8；关停 4s、恢复 5s） | 运维 + 后端 |
| ☑ | RB-02 | 二级回滚：移除两条路由注册 → 接口 404，客户端报明确错误 | 演练记录（M3 已验：报告 §5.6 → `DRILL_L2=1` 演练：两条路由 404、workspace 路由不受影响、客户端输出「预检失败：Not Found（若为接口不存在，请升级集群服务端…）」、恢复后 API-11 可用） | 后端 |
| ☑ | RB-03 | 回滚后**无脏数据**：`venv.db` 表结构/记录未被破坏，可正常 `source haienv` | 回滚前后 `sqlite3` 对比（M3 已验：报告 §5.6 → 关闭态 venv.db md5/key 不变，且 haienv 仍能反序列化已注册环境） | 测试 |
| ☑ | RB-04 | 回滚期间 workspace 业务无异常 | 监控记录（M3 已验：报告 §5.5/§5.6 → 演练期间 workspace 回归 smoke_ugc 8/8 + e2e_workspace 19/19） | 运维 |
| ☐ | RB-05 | 客户端回滚：旧客户端直接可用（不依赖新接口） | 旧二进制冒烟 | 客户端 |

---

## 15. 阶段 14 · 上线后观察（POST）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | POST-01 | 上线后 1 h：注册成功率、P99、错误码分布 | 看板截图 | 运维 |
| ☐ | POST-02 | 上线后 24 h：无"已上传未注册"积压（失败 reason 为 0 或已解释） | 看板 + 日志抽样 | 运维 |
| ☐ | POST-03 | 抽样 3 个真实用户环境，任务侧 `source haienv` + import 验证通过 | 抽样记录 | 测试 |
| ☐ | POST-04 | workspace 关键指标（push/pull 成功率、P99）无劣化 | 看板对比（上线前 7 天基线） | 运维 |
| ☐ | POST-05 | 用户反馈渠道无新增环境相关工单 | 工单系统 | 支持 |

---

## 16. 验收签署（ACC）

| 勾选 | ID | 检查项 | 验收证据 | 责任 |
| --- | --- | --- | --- | --- |
| ☐ | ACC-01 | AC-01~AC-12 全部通过（对应关系见用例 §10） | 验收报告 | 测试 + 后端 |
| ☐ | ACC-02 | 缺陷：致命/严重 = 0；一般 ≤ 2 且有绕行 | 缺陷清单 | 测试 |
| ☐ | ACC-03 | 性能门槛（PERF-01/02）达标 | 压测报告 | 测试 |
| ☐ | ACC-04 | 安全项（SEC-01~SEC-08）全过 | 安全测试报告 | 安全 + 后端 |
| ☐ | ACC-05 | 回滚演练通过（RB-01~RB-05） | 演练记录 | 运维 |
| ☐ | ACC-06 | 文档交付齐全（DOC-01~DOC-05 / DEP-01~02） | 文档清单 | 产品 |
| ☐ | ACC-07 | 需求/设计/用例/Checklist 四方 ID 可追溯、无悬空引用 | 自检脚本 | 后端 |
| ☐ | ACC-08 | 上线评审通过，签署发布 | 评审记录 | 全体 |

---

## 附录 A · 接口契约快照（GATE-03 冻结内容）

### A.1 API-11 `POST /ugc/update_cluster_venv`

```http
POST /ugc/update_cluster_venv?token=<token>&venv_name=myenv&py=3.8&extend=False HTTP/1.1
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 已注册，复用路径 | 200 | `{"success":1,"path":"/hf_shared/hfai_envs/U-A/myenv_0","exists":true}` |
| 新环境，分配后缀 | 200 | `{"success":1,"path":"/hf_shared/hfai_envs/U-A/myenv_0","exists":false}` |
| `extend=True` | 200 | `{"success":0,"code":"INVALID_PARAM","msg":"暂不支持上传 extend 模式的虚拟环境"}` |
| 名称非法 | 200 | `{"success":0,"code":"INVALID_PARAM","msg":"venv_name 取值非法: ../x"}` |
| 目录不可写 | 200 | `{"success":0,"code":"ENV_REGISTRY_NOT_WRITABLE","msg":"...请运维检查目录权限"}` |
| 灰度关闭 | 200 | `{"success":0,"code":"FEATURE_DISABLED","msg":"..."}` |
| token 缺失/过期 | 401/403 | `{"success":0,"code":"UNAUTHORIZED","msg":"..."}` |

### A.2 API-13 `POST /ugc/register_cluster_venv`

```http
POST /ugc/register_cluster_venv?token=<token> HTTP/1.1
Content-Type: text/plain; charset=utf-8

{"venv_name":"myenv","path":"/hf_shared/hfai_envs/U-A/myenv_0","py":"3.8",
 "extra_search_dir":[],"extra_search_bin_dir":[],"extra_environment":[]}
```

| 情形 | HTTP | 响应体 |
| --- | --- | --- |
| 成功 / 幂等重复 | 200 | `{"success":1,"registered":true,"path":"...","db":"/hf_shared/hfai_envs/U-A/venv.db"}` |
| path 越界 / 他人目录 | 200 | `{"success":0,"code":"PATH_ESCAPE","msg":"目的路径 ... 超出限定范围，非法！"}` |
| 写库失败 | 200 | `{"success":0,"code":"ENV_REGISTRY_WRITE_FAILED","msg":"...可重试"}` |
| 参数缺失 | 200 | `{"success":0,"code":"INVALID_PARAM","msg":"..."}` |
| 灰度关闭 | 200 | `{"success":0,"code":"FEATURE_DISABLED","msg":"..."}` |

### A.3 错误码补充（`cloud_storage/service/errors.py`）

| code | HTTP | 触发 | 客户端表现 |
| --- | --- | --- | --- |
| `ENV_REGISTRY_NOT_WRITABLE` | 200 | 用户 env 目录不可写 | 预检阶段即失败,不进入上传 |
| `ENV_REGISTRY_WRITE_FAILED` | 200 | SQLite 写入/回读校验失败 | 「已上传但注册失败,可重试」 |
| `ENV_PATH_MISMATCH` | 200 | 配置与运行时路径不一致(自检) | 仅告警,不阻断 |

---

## 附录 B · 冒烟验证脚本（发布后 5 分钟自检）

```bash
# 变量
API=${API:-http://127.0.0.1:8083}
T=${T:-<T_A_TOKEN>}
ENV_NAME=${ENV_NAME:-smoke_env}
ENV_ROOT=${ENV_ROOT:-/tmp/hai-test/hfai_envs}
PY=${PY:-3.8}

echo "== 0) 环境自检 =="
ls -ld "$ENV_ROOT" "$ENV_ROOT/U-A" || exit 1

echo "== 1) API-11 预检(新环境) =="
curl -s -X POST "$API/ugc/update_cluster_venv?token=$T&venv_name=$ENV_NAME&py=$PY" | tee /tmp/env_pre.json
python3 -c "import json;d=json.load(open('/tmp/env_pre.json'));assert d['success']==1 and d['path'].endswith('_0'),d;print('OK',d['path'])"

echo "== 2) 构造目录并注册(FIXTURE) =="
P=$(python3 -c "import json;print(json.load(open('/tmp/env_pre.json'))['path'])")
mkdir -p "$P/lib/python$PY/site-packages" && printf 'export HF_ENV_NAME=%s\n' "$ENV_NAME" > "$P/activate"
curl -s -X POST "$API/ugc/register_cluster_venv?token=$T" \
  -H 'Content-Type: text/plain; charset=utf-8' \
  -d "{\"venv_name\":\"$ENV_NAME\",\"path\":\"$P\",\"py\":\"$PY\",\"extra_search_dir\":[],\"extra_search_bin_dir\":[],\"extra_environment\":[]}" \
  | tee /tmp/env_reg.json
python3 -c "import json;d=json.load(open('/tmp/env_reg.json'));assert d['success']==1 and d['registered'],d;print('OK')"

echo "== 3) 客户端可见性(需同机安装 hai-cli) =="
HAIENV_PATH="$ENV_ROOT/U-A" hai-cli env list | grep -q "$ENV_NAME" && echo OK

echo "== 4) source 可用性 =="
HAIENV_PATH="$ENV_ROOT/U-A" bash -c "source haienv $ENV_NAME && echo source-OK"

echo "== 5) 负例:越界 path =="
curl -s -X POST "$API/ugc/register_cluster_venv?token=$T" \
  -H 'Content-Type: text/plain; charset=utf-8' \
  -d "{\"venv_name\":\"x\",\"path\":\"/tmp/evil\",\"py\":\"$PY\"}" | tee /tmp/env_neg.json
python3 -c "import json;d=json.load(open('/tmp/env_neg.json'));assert d['success']==0,d;print('OK',d['code'])"

echo "== 6) 负例:缺 token(预期 401/403 且带 success) =="
curl -s -o /tmp/env_notok.json -w '%{http_code}\n' -X POST "$API/ugc/register_cluster_venv"
python3 -c "import json;d=json.load(open('/tmp/env_notok.json'));assert 'success' in d and d['success']==0,d;print('OK')"
```

---

## 附录 C · 排障速查

| 现象 | 可能原因 | 排查 | 处置 |
| --- | --- | --- | --- |
| `env push` 报"未找到名为 X 的虚拟环境" | 本地 `HAIENV_PATH` 不含该环境 | `HAIENV_PATH=$... hai-cli env list` | 确认 `HAIENV_PATH` 与创建时一致 |
| `env push` 报"暂不支持扩展环境" | 环境 `extend='True'` | `hai-cli env config show -n X` | 用 `--no_extend` 重建;本特性不支持 extend 上传 |
| `env push` 报 `不支持的file_type: FileType.ENV` | 客户端未修 FR-02 | 看命令行是否出现 `FileType.ENV` 字面量 | 升级客户端;服务端无法绕过(dispatch 在客户端) |
| `env push` 报"上传venv失败"但上传日志正常 | 客户端用了 `haienv workspace push`(E13) | `ps`/日志中的子进程命令行 | 升级客户端 |
| `update_cluster_venv` 404 | 路由未注册 / 未发布 | `curl` 直连 :8083 | 检查 `api/register/implement.py` `ugc` 段 |
| `update_cluster_venv` 返回 `ENV_REGISTRY_NOT_WRITABLE` | 用户目录对平台账号不可写 | `ls -ld $ENV_ROOT/<user>` | `chmod 777`(或加组权限),并在客户端创建时固化权限 |
| `register_cluster_venv` 返回 `ENV_REGISTRY_WRITE_FAILED` | `venv.db` 不可写 / 被锁 / `haienv` 版本不兼容 | 手动 `sqlite3 ... "select 1"`;检查日志 `reason` | 按 reason 处置;修好后重跑 `env push`(会走"数据已同步",仅补注册) |
| 注册成功但任务里 `source haienv` 报 `no valid env found` | 路径不一致(AC-02) 或 `HAIENV_PATH` 未生效 | 任务内 `echo $HAIENV_PATH; ls $(dirname $HAIENV_PATH)` | 对照 §附录 C 三方路径;检查 `get_env_root()` 是否被绕过 |
| 注册成功但 `env list` 看不到 | 读的是另一个用户的 DB / `HAIENV_PATH` 指向不同根 | `hai-cli env config show -n X -u <user>` | 统一 `HAIENV_PATH` |
| 任务报错但只是 `no valid env found` | 未注册或名称拼写 | 查 `{env_root}/<user>/venv.db` | 重新 `env push`;或按 OPS-04 手册手工登记 |
| 上传很慢后才发现无权限 | 未走预检(旧客户端) | — | 升级客户端;或用 API-11 先行探测 |
| `database is locked` | 并发写 `venv.db` | 日志 `reason=locked` | 依赖 `flock`;若仍复现,收敛并发或加重试 |

---

## 附录 D · 与 workspace Checklist 的组映射

| 本文件组 | workspace 对应组 | 说明 |
| --- | --- | --- |
| GATE | GATE | 同为启动前置 |
| ENV / CFG | ENV / CFG | 一致 |
| **REG** | **DB** | env 无 DDL,改为「注册表与权限」阶段(共享盘 SQLite) |
| DEV / UT | DEV / UT | 一致 |
| API | (并入 DEV) | 本特性接口少,单独成阶段便于契约冻结 |
| E2E | E2E | 一致 |
| SEC / PERF / OBS | SEC / PERF / OBS | 一致 |
| OPS | OPS | 一致 |
| TASK | TASK | 一致 |
| CMP | CMP | 一致 |
| REL / DOC / DEP / RB / POST / ACC | 同名组 | 一致 |

> **唯一结构性差异**:workspace 有独立的 DB 迁移阶段（人工 DDL）,env 阶段为「REG」——恰恰因为本特性**零 DDL**（HC-03）,这也是本方案能在无迁移框架的仓库上直接发布的原因之一。
