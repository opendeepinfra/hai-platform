# HAI Platform · `hai-cli` 三特性开发进度快照

> **文档定位**：本文件是**跨特性的进度视图**，回答「现在做到哪了、证据在哪、还剩多少」。
> 其余文档各司其职：分析答「是什么」、需求答「做什么」、设计答「怎么做」、用例答「怎么测」、
> Checklist 答「怎么验收」、任务列表答「谁在哪个文件上做」；**只有本文件回答「当前实际状态」**。
>
> **快照基线**：`feature/hai-cli-images-server-design` @ **`0227599`**（2026-10-02 19:44），
> 采集时间 **2026-10-02 19:49**（Asia/Shanghai）。三份 Checklist 的勾选数由脚本逐行统计得出，
> 与各文档自述数字一致（`images` 0/156 · `env` 74/133 · `workspace` 0/130）。
>
> **维护方式**：每次实质推进后**只追加**「§8 增量记录」，并同步更新 §2 总表与对应特性章节的
> 「状态 / 剩余」两列；**不要**重写历史结论。判断某条是否完成，一律以**可复核证据**为准
> （命令输出 / 文件路径 / 测试报告编号），不接受口头声明——这条纪律与三份 Checklist 的文首约定一致。

---

## 0. 结论速览

1. **代码进度领先文档与验收进度**。三条特性的 P0 代码基本都已落地，但 Checklist 勾选率分别是
   `workspace` **0%**、`env` **56%**、`images` **0%**——缺的是**证据**，不是代码。
2. **`workspace` / `env` 已实机跑通**：`workspace` 8/8 冒烟 + 19/19 E2E（2026-10-01）；
   `env` L1 46 passed / L2 20/20 / L3 16/16 / 回滚 8/8（2026-10-02）。
3. **`images` 是当前主战场**，且**正在真实任务 pod 上调试运行面**：L1 33 passed、L2 `PASS=42 FAIL=0`，
   但 **L3（AC-01）仍未通过**——2026-10-02 在 103 上以 task 25/26 实测时 initContainer 退出码 1
   （helper 镜像 glibc 被覆盖），修复已提交（`0227599` 19:44）但**该修复尚未有复测记录**。
4. **三条分支均未合入 `main`**（`main` 仍停在 2025-06-02），`images` 分支领先 `origin/main` **38** 个提交，
   已包含 `workspace` / `env` 的全部工作。
5. **唯一的结构性障碍**：`conf/proj_conf/custom.py` 是部署私有文件、不在仓库内，导致本仓库
   **无法在本地/CI 运行任何测试**（import 期即失败），只能进镜像验证——这也是文档与代码脱节能长期潜伏的根因。

---

## 1. 分支与基线

| 分支 | 最新提交 | 日期 | 说明 |
| --- | --- | --- | --- |
| `main` / `origin/main` | `1a90f87` | 2025-06-02 | 近一年未动；三条特性均**未合入** |
| `feature/hai-cli-workspace-server-design` | `537202d` | 2026-10-02 | 领先 `main` 16 个提交 |
| `feature/hai-cli-env-server-design` | `33a5b26` | 2026-10-02 | 领先 27 个提交（含 workspace） |
| **`feature/hai-cli-images-server-design`**（当前） | **`0227599`** | 2026-10-02 | 领先 **38** 个提交（含前两者全部工作） |

三条分支是链式演进，**当前分支即最新综合态**；`backup/i18n-*-pre` 为各自开工前的还原点。

---

## 2. 三条工作流进度总表

| 特性 | 代码实现 | 实机验证 | Checklist | 单元测试 | 里程碑 |
| --- | --- | --- | --- | --- | --- |
| **workspace**（`ugc`） | P0 完成，P1 未做 | ✅ 8/8 冒烟 + 19/19 E2E（10-01，镜像 `e03c42c`） | **0/130** | **无** | M1/M2 实质达成；M3 未达 |
| **env**（`haienv`） | P0 完成（FR-01~12） | ✅ L1 46+1skip · L2 20/20 · L3 16/16 · 回滚 8/8（10-02，`envtest5`） | **74/133**（56%） | ✅ 有 | M1/M2 达成；M3 走「精简口径」 |
| **images**（`hfai images`） | **S0–S5 完成** | 🟡 **L1 33 passed · L2 42/42**（10-02 热部署）；**L3 未通过** | **0/156** | 15 个函数 / 33 例（已绿） | **M1 接近达成；M2 未达** |

---

## 3. `workspace`（工作区同步）

**已完成**
- 9 个 `/ugc/*` 接口全部注册并实现（API-01~09）；兼容层吸收枚举串 / `text/plain` JSON / `{"file_list":{...}}` 三种客户端形态。
- 领域逻辑由 `cloud_storage/` 既有 1 900 行抽取为 `cloud_storage/service/`，`ugc-server` 与 `cloud-storage` 两宿主共用。
- P0 零 DDL：复用 `user_sync_status` / `user_downloaded_files`。
- 依赖注入提供 `[cloud.storage]` 配置面（`one/one_etc/core.toml:97-140`）。

**实机证据**（2026-10-01，103 + RustFS）
- `docs/haiplatform/scripts/smoke_ugc.sh`：断言点 8 处 → **8/8 PASS**。
- `docs/haiplatform/scripts/e2e_workspace.sh`：断言点 19 处 → **19/19 PASS**（镜像 `2ad75bf` 与 `e03c42c` 各一轮）。
- 见 [workspace-server-task-list.md](workspace/workspace-server-task-list.md) §8。

**未完成**
- **P1 全部未实现**：FR-17 配额（pull 侧退化为 `_quota_limit_mb()` 硬回退 102400 MB）、FR-18 管理端配额 API（仅桩）、FR-21 审计/回收（`cloud_storage/audit/default.py` 是 no-op，`cloud_storage_bucket_usage_size` 指标无写入点）；API-10 / API-12 无代码无路由。
- **零单元测试**：`tests/` 下只有 `env/` 与 `images/`，`UT-01~04` 与 NFR-07 无证据。
- PERF / SEC / OBS / REL / RB / POST / ACC **全部未执行**。

**已知偏差**
- 设计 ADR-8 与 ENV-05 要求 `localfs` provider，实际 `cloud_storage/service/context.py` 明确抛「未内置」；M2 是在 `s3`/RustFS 上达成的，**与设计口径不符**。
- 灰度开关未覆盖全部接口：`check_feature_enabled` 只在 3 处调用，`get_sts_token` / `set_sync_status` / `get_sync_status` / `cluster_files/list` 绕过（审计 S-7）。
- 无全局 `Exception` 处理器，未捕获异常返回裸 500（无 `success` 字段），违反 CON-3 / DEV-23（审计 S-1）。
- 崩溃恢复已实现但审计 S-2 记录 5 处正确性缺陷，E2E-11/12 从未执行。

---

## 4. `env`（`haienv` 虚拟环境）

**已完成**：FR-01~FR-12 全部有代码落点（`env push` 子命令、`.value` 序列化、API-11/API-13、`env_root` 路径单点、fail-closed、灰度开关、任务侧诊断）。

**实机证据**（2026-10-02，103，镜像 `envtest5`）
| 层 | 结果 |
| --- | --- |
| L1 服务端单测 | 46 passed / 1 skipped，`env_registry.py` 行覆盖率 **93%** |
| L1 客户端单测 | 39 passed（含 `create` 前置用例） |
| L2 接口冒烟 | `smoke_env.sh` **PASS=20 FAIL=0** |
| L3 端到端 | `e2e_env.sh` **PASS=16 FAIL=0**（push → S3 key → 集群落地注册 → 二次 push 幂等 → 任务 `source haienv` + 探针 import） |
| N3 幂等自检 | **5/5** |
| 回滚演练 | **8/8**（`DRILL_L2=1` 时 13/13；关停 4–5s / 恢复 5s） |
| workspace 回归 | 8/8 + 19/19 |
| 一键 `verify_env.sh` | **PASS=6 FAIL=0** |

详见 [env-server-test-report.md](env/env-server-test-report.md)。

**未完成**：PERF-01~05（无压测）、REL/POST/ACC（留给真上线）、OPS-03 runbook、OPS-04 无共享盘降级、CMP-03 私有 `custom.py` 扩展点（该文件不存在）、SEC-03 的 URL 编码/软链边界、FI-04/06 故障注入。

**待裁决**：N1/N2——客户端把用户 env 目录 `chmod 777`，且 `venv.db` 是 `pickle`；103 内部以「同组互信」为前提，上生产前必须改 770 + 平台组属主。

---

## 5. `images`（用户自定义镜像，当前主战场）

### 5.1 阶段状态

| 阶段 | 估时 | 状态 | 关键证据 |
| --- | --- | --- | --- |
| S0 决策冻结 | 0.5 | ✅ | [images-server-decisions.md](images/images-server-decisions.md)：Q-1~Q-8 + R-2 定案 |
| S1 服务端控制面 | 3.0 | ✅ | `api/resource/image/default.py` 4 桩实现；`api/register/implement.py:75-77` 3 条路由；selector DESC + 归一化 + 幂等 upsert |
| S2 客户端 | 1.5 | ✅ | `async_load/async_delete` 两端齐备；`-i/--image`、`--force`；失败改打服务端 `msg` |
| S3 数据面 + 迁移 | 1.0 | ✅ | `db_schemas/035.table_train_image_alter.sql`（幂等加列 + 唯一键改 `(shared_group,image_tar)`）；`register` 后端同步置 `loaded` |
| S4 运行面 | 2.0 | 🟡 **代码完成，正在实机调试** | `marsv2/scripts/link_hfai_image.sh` 已新增并登记挂载种子；但 **AC-01 未通过** |
| S5 状态机/幂等/缓存 | 1.0 | ✅ | 状态常量单点；`on conflict ... where status in ('failed','deleted')`；写后刷缓存 |
| S6 测试 | 2.5 | 🟡 | L1 **33 passed**、L2 **PASS=42 FAIL=0**；**L3 未通过、回归未做** |
| S7 灰度/回滚/可观测/文档 | 1.5 | 🔴 | 灰度开关（领域层 `check_image_enabled`）与 5 个指标已埋点；**无看板脚本、无告警规则、无回滚演练、`ugc.rst.txt` 未提及 images** |

### 5.2 工作量燃尽

| | 人日 |
| --- | --- |
| 总工作量（设计 §14） | **13.0** |
| 已完成（S0–S3、S5，S4 代码，S6 的 L1+L2） | **≈10.5（81%）** |
| **剩余** | **≈2.5**（S6-3 E2E 0.75 + S6-4 回归 0.25 + S7 1.5） |
| 关键路径 S0→S1→S4→S6→S7 | 9.5 中已完成 8.0 |

### 5.3 运行面调试经过（S4 / I16 / I17）

`AC-01` 的判据是「自定义镜像**真正跑起一个任务并产出可区分输出**」，不是接口 200。该链路已在 103 上真实迭代：

1. `f2cb559`（18:40）——宿主动态链接的 `ctr` 缺 loader，实测 `SIGFPE`（exit 136）→ 补 glibc 目录与 loader 挂载。
2. **`0227599`（19:44）——真实任务 pod（task 25/26）实测失败**：把宿主 glibc 挂到 `/lib/x86_64-linux-gnu`
   会覆盖 helper 镜像（busybox:latest，Debian trixie / glibc 2.41）自身的依赖，initContainer 退出码 1，
   日志仅 `/bin/sh: /lib/x86_64-linux-gnu/libc.so.6: version 'GLIBC_2.38' not found`。
   修正：宿主 lib 改挂 **`/host-lib`**，link 脚本用 `/host-lib/ld-linux-x86-64.so.2 --library-path /host-lib`
   显式运行 `ctr`；`runtime_loader_file` 键被移除。决策记录 §3 已补三档实测对照。
3. `12a54ff`（19:41）——preflight 误查 `/marsv2/scripts/`（那是**任务期**挂载点，平台 pod 内不存在），
   改查镜像内仓库路径并升级为 FAIL。

> ⚠️ **当前最关键未知数**：`0227599` 的修正**尚无复测记录**。S4 是否真正闭环，取决于下一次任务 pod 实测。

### 5.4 下一步（唯一能判定 AC-01 的路径）

```bash
# ① 重建平台镜像（把 link 脚本 / 挂载种子 / init_manager 改动带进任务侧）
bash docs/haiplatform/scripts/check_images_preflight.sh      # 部署前置自检，任一项 FAIL 先修
bash docs/haiplatform/scripts/build_hai.sh <tag>
bash docs/haiplatform/scripts/redeploy_local.sh <tag>

# ② 造夹具 + 配置
bash docs/haiplatform/scripts/image_fixture.sh               # 生成带可区分探针的 demo:v1 tar
sudo python3 docs/haiplatform/scripts/patch_image_override.py

# ③ 三层验证
bash docs/haiplatform/scripts/smoke_images.sh http://10.205.52.200   # L2，期望 FAIL=0
bash docs/haiplatform/scripts/e2e_images.sh                          # L3 = AC-01，判据是可区分输出
```

判据：任务 `succeeded` 且日志出现镜像内探针内容；`kubectl describe pod` 中 initContainer Completed；
删除镜像后提交任务被拒且 `images list` 能解释原因（K5 闭环）。

---

## 6. 文档与代码不一致清单（待修）

| # | 问题 | 位置 | 影响 |
| --- | --- | --- | --- |
| 1 | **测试报告缺失但被引用** | [scripts/README.md](scripts/README.md) §6 两次指向 `images-server-test-report.md`，该文件不存在 | 悬空链接；L1/L2 结果只存在于 commit message，不可追溯 |
| 2 | **「尚未开工」与事实相反** | [images-server-checklist.md:3](images/images-server-checklist.md#L3)（称全部 ☐、共 156 项）、[images-server-task-list.md:16](images/images-server-task-list.md#L16)（称所有任务待办） | 会误导后续接手者以为要从零开始 |
| 3 | **Checklist 未随闭环更新** | `workspace` **0/130** 全未勾，而其 [task-list §8](workspace/workspace-server-task-list.md) 已声明 8/8 + 19/19 跑通 | 治理证据缺失，无法据此发布 |
| 4 | **env 报告数字自相矛盾** | [env-server-test-report.md](env/env-server-test-report.md) §1 写「40 passed / 覆盖率 100%」，§5 写「46 passed / 93%」；客户端测试数在四处出现 10 / 12 / 37 / 39 四种取值 | 验收时无法举证 |
| 5 | **用例期望值过时** | [images-server-test-cases.md:396](images/images-server-test-cases.md#L396) 写 `e2e_images.sh` 期望 `PASS=8`、`smoke_images.sh` 期望 `≥12`；脚本实际分别有 22 与 41 处断言点（实测 42） | 判据与脚本脱节 |
| 6 | **workspace 现状章节过时** | [workspace-server-task-list.md](workspace/workspace-server-task-list.md) §1 仍称「无 `[cloud.storage]`」「9 个函数全为桩」「`cloud_storage/service/` 不存在」；[test-environment.md](workspace/test-environment.md) 仍称运行镜像为 `7589fb1` | 与实际代码/部署矛盾 |

---

## 7. 剩余工作与建议顺序

**按「先固化证据、再补功能」排序**

1. **复测 `images` 运行面** → 跑通 L3（AC-01）。这是唯一未闭环的 P0 主链路。
2. **写 `images-server-test-report.md`** → 固化 L1 33 / L2 42 / L3 结果，消除悬空链接。
3. **刷新三份 Checklist 与两处「尚未开工」** → 按实测结果逐条勾选并填证据列。
4. **补 `images` 的 S7**（1.5 人日）→ 看板脚本 + 告警规则 + 回滚演练，直接复用 `env` 的
   [env_metrics.sh](scripts/env_metrics.sh) / [env_alerts.yml](scripts/env_alerts.yml) 范式。
5. **修 §6 表中 4/5/6 三处数字与文字** → 低风险、纯文档。
6. **补 `workspace` 单测与 P1**（FR-17/18/21、API-10/12）→ 工作量最大，可延后。
7. **降低验证门槛** → 提供可入库的 `custom.py` 示例 + CI 骨架，否则问题 1/3/4 会反复出现。
8. **规划合并** → 按 workspace → env → images 顺序走 PR，避免 38 个提交一次性合入。

---

## 8. 增量记录

### 2026-10-02 19:49（本快照）

- **新增提交**：`eee0af7`（19:25）、`455097c`（19:26）、`12a54ff`（19:41）、`0227599`（19:44）。
- **契约修正**：`load` 响应的 `image` 改为完整三段 URL（对齐 Checklist 附录 A.1），裸名另以 `image_name`
  返回（字段只增不改，CMP-05）；新增 `test_u08b` 锁定该契约。
- **首次 `images` 实机记录**：L1 `33 passed`、L2 `smoke_images.sh` **PASS=42 FAIL=0**、启动自检 `image path check: OK`。
- **运行面真实任务 pod 实测**：task 25/26 initContainer 退出码 1（glibc 2.38 缺失），已定位并修正为
  「宿主 glibc 挂 `/host-lib` + 显式 loader 运行 ctr」；**该修正待复测**。
- **部署链路修复**：`deploy_pod_dev.sh` 的 `PATHS` 补 `image_metrics.py`（漏掉会让 `ugc_server` 导入期崩溃）；
  preflight 改查镜像内仓库路径并将缺失升级为 FAIL。
- **尚未变化**：三份 Checklist 勾选数（0/156 · 74/133 · 0/130）与 `images-server-test-report.md` 的缺失状态。
