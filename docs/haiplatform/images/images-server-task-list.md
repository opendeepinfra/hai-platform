# HAI Platform · `hai-cli images` 服务端实施任务列表

> **文档定位**：**实施视图**。分析（[hai-cli-images-analysis.md](hai-cli-images-analysis.md)）负责「是什么」，
> 需求（[images-server-requirements.md](images-server-requirements.md)）负责「做什么、做到什么程度」，
> 设计（[images-server-design.md](images-server-design.md)）负责「怎么做」，Checklist（[images-server-checklist.md](images-server-checklist.md)）负责「怎么验收」，
> 本文负责「**谁在哪个文件上、按什么顺序、花多久把它做出来**」。
>
> **前置阅读**：分析 **§1/§4.6/§5/§6**（结论速览、运行面、S1–S10 场景矩阵、风险 I1–I18）；
> 需求 **§3/§4/§8**（FR/NFR/SEC/OPS/CMP/HC 与 4 个接口、Q-1..Q-8）；设计 **§3/§5/§13/§14**（路径单点、模块与 loader 后端、ADR-I1..I10、WBS+里程碑）。
>
> **基线**：`fireflyer@192.168.100.103:~/hai-platform` @ `e03c42c`（工作树含未提交改动；分析 §1/§4.6 涉及的 6 个镜像相关文件
> 已用 `md5sum` 与本地逐字节核对一致）；客户端运行时为镜像内 `hfai`（同提交 `7589fb1`）。103 实测用户为 **`haiadmin`，`shared_group = hfai`**（`hai-cli whoami`）。
>
> **与设计 WBS 的关系**：本文把设计 §14 的 **S0–S7 展开到文件级**，估时口径与设计一致
> （**总计 ≈ 13 人日**，P0 关键路径 S0→S1→S4→S6→S7 ≈ 9.5 人日）；里程碑沿用 M1/M2/M3。
> **全文状态**：本特性**尚未开工**，所有任务均为待办，不接受「已实现」的口头声明。

---

## 1. 现状核对（本仓库实测）

> 以下结论均在 `e03c42c` 基线上实际 grep/read/`psql`/`curl` 得到，不是文档转述；`file:line` 为可复核证据。

| # | 缺口 | 现状（实测证据，`file:line`） | 风险 / 需求 |
| --- | --- | --- | --- |
| 1 | 客户端 `async_load` 缺失 | `client/api/image_api.py:23` 调用 `user.image.async_load(tar)`，而 `client/model/user_impl/default.py:5-8` 只实现 `async_get`，接口层 `base_model/base_user_modules/default.py:20-22` 也**没有声明** → `AttributeError: 'UserImage' object has no attribute 'async_load'`（103 §9-3 实测栈顶） | **I1** / C-3 / FR-01 |
| 2 | 客户端 `async_delete` 缺失 | `client/api/image_api.py:34` 调用 `async_delete`，同类缺口 → `AttributeError: ... no attribute 'async_delete'. Did you mean: 'async_get'?`（103 §9-4） | **I1** / C-3 / FR-01 |
| 3 | 服务端 `user_images` 硬编码空 | `server_model/user_impl/user_image/default.py:16` 直接返回 `'user_images': []`；本该是数据源的 `server_model/selector/train_image_selector.py:43-47`（`a_find_user_group_images`）**全仓零调用方** | **I2** / FR-02 |
| 4 | 4 个具名桩未注册、模块未被 import | `api/resource/image/default.py:3-16` 的 `hfai_image_load/update_status/list/delete` 全为 `'not implemented'`；`api/register/implement.py:71` 只注册了 `list`，`load`/`delete` 实测 **HTTP 404** `{"success":0,"msg":"Not Found"}`（103 §9-2）；`api/resource/image` 在注册模块中**从未被 import** | **I3** / FR-03 / FR-05 / ADR-I1 |
| 5 | `train_image` 零写入路径 | 全仓检索 `TrainImageTable` 的写调用为空；`psql -c "select * from train_image"` → **0 rows**（103 §9-5/§9-6）→ `status` 永远到不了 `'loaded'` | **I4** / FR-03 / FR-09 |
| 6 | 状态机未定义 | `status` 无枚举、无合法值集合、无迁移规则；DDL 默认 `'processing'`（`db_schemas/017.table_train_image.sql:12`）。客户端按**子串** `'deleted' in status` 过滤（`client/commands/hfai_image.py:67`），任务侧按**精确** `status='loaded'` 白名单（`api/operation/default.py:21`）——**两头口径不一致** | **I5** / FR-04 / HC-03 / HC-04 |
| 7 | 镜像名（`name:tag`）来源未定义 | `client/commands/hfai_image.py:79-90` 的 `load` **只发 tar 路径**；而任务侧 `api/operation/default.py:17-21` 要求 `image` **不含 `/`** 且与 `registry`/`shared_group` 拼成恰好 3 段 | **I6** / FR-03 / FR-07 |
| 8 | 去重方向自相矛盾 | 客户端 `hfai_image.py:60-66` 取「**首次见到**」为基准（注释却写「以最新的为准」）；数据源 `train_image_selector.py:46` 用 `.sort_values('updated_at')`（**升序**）→ 实际基准是**最旧**那条 | **I7** / FR-11 / ADR-I5 |
| 9 | numpy 序列化导致 500 | `a_find_user_group_images` 用 `df.to_dict('records')` 返回 `np.int64` 的 `task_id`；FastAPI `jsonable_encoder` 无法编码 → 一旦按 FR-02 接通即 500（对照 `server_model/user_impl/user_image/implement.py:17` 已有的 `int(...)` 规避） | **I8** / FR-11 |
| 10 | 无 `FileType.IMAGE` / `image_path` | `conf/utils.py:23-35` 只有 `DATASET/WORKSPACE/ENV/DOC/PYPI/WEBSITE`；`cloud_storage/utils.py:445+` 的 `get_base_path` 无镜像分支；`one/one_etc/core.toml:110-116` 无 `image_path` → 「tar 在共享盘哪个根下」**无单点定义**，服务端无法做落点校验 | **I9** / FR-13 / SEC-01 |
| 11 | 失败面是裸异常 | 103 实测：任务提交被拒时用户看到的是 `client/api/api_utils.py:75` 抛出的 **`Exception: 请求失败: [exception: ...]`**；`image_api.py:24-25` 的 `print(result['msg'])` 因默认 `assert_success=[1]`（断言在 `api_utils.py:100-104`）**永不执行** | **I10** / FR-12 |
| 12 | 103 无内网 registry | `kubectl get svc -A \| grep -iE "registry\|5000"` 为空、无 registry 工作负载；DDL 默认 `registry.high-flyer.cn` **不可达**（103 §9-7）→ 「推送进 registry」的数据面在当前环境不可验证 | **I11** / Q-1 / Q-7 / ADR-I2 |
| 13 | 无幂等键设计 | `image_api.py:23` 的变更型调用默认 `retries=1`（对照 `:12` 的 `retries=3`），服务端无幂等键 → 一旦补重试即重复建行/重复建任务 | **I12** / FR-06 / NFR-01 |
| 14 | 唯一索引不含组 | `db_schemas/017.table_train_image.sql:24-25` 的 `train_image_image_uindex` **只含 `image_tar`** → 两个组加载同一 tar 必然唯一键冲突；软删是否让出唯一键亦未定义 | **I13** / Q-2 / R-4 |
| 15 | 跨组越权面 | `client/commands/hfai_image.py:92-99` 的 `delete` 入参是镜像名，DDL **无 `user_name` 列**；服务端若不校验 URL 第二段与 `user.shared_group` 相等，即可跨组删除/登记 | **I14** / SEC-02 / ADR-I8 |
| 16 | 空间回收缺失（P2） | `delete` 的 docstring 写「以释放空间」，但 registry tag / 共享盘 tar / 节点镜像缓存均不处理，`-a` 只隐藏行不释放空间 | I15 / FR-14 / OPS-05 |
| 17 | **服务端自查指引指向空数据源** | 103 实测（命令见 §4.2）：用**没有 `status='loaded'` 行**的自定义镜像提交任务，服务端返回 `用户所在的组 [hfai] 不存在镜像 [registry.high-flyer.cn/hfai/demo:v1] 或镜像仍在加载, 请使用命令 \`hfai images list\` 检查`；而 `images list` 的 `user_images` 恒为空（第 3 行）→ **服务端让用户自查的那条命令，查不出任何原因**（跨特性可观测性断链） | **I2 + I4 + K5** / FR-02 / AC-03 |
| 18 | 运行期脚本缺失 | `grep -rn link_hfai_image .` → **3 处引用 / 0 处定义**；`experiment_manager/manager/init_manager.py:358` 以 `args=['/marsv2/scripts/link_hfai_image.sh']` 引用；`marsv2/scripts/`（11 个文件）无此脚本；`one/hai-up.sh:292-300` 的 `storage` 挂载种子也**没有它** → initContainer `not found`、**pod 卡 Init** | **I16** / FR-08 / HC-08 |
| 19 | 节点前置不满足 | 103 实测：`/data_local` **不存在**（hostPath 未声明 `type` → kubelet 不创建）；节点上只有 `docker.io/library/busybox:latest`，而 `init_manager.py:351` 要 `registry.high-flyer.cn/google_containers/busybox:latest`（解析到 `198.18.0.77`，不可达）→ `ImagePullBackOff` | **I17** / FR-08 / ADR-I4 |
| 20 | `path` 语义未单点化 | `launcher.py:147` 把 `path` 作为 `HFAI_IMAGE_WEKA_PATH` 注入并喂给 link 脚本（= **镜像在共享盘的位置**），而 `images load` 的入参是 **tar 包路径**；DDL 注释「镜像在 weka 上的路径」未被文档化，两者极易写混 | **I18** / FR-07 |

> **一句话**：这不是「补 2 个接口」，而是「**补控制面四处断链 + 补一个运行期脚本 + 修节点前置**」；
> 第 17 行进一步说明：**控制面不修，服务端现有的错误提示本身就在误导用户**。

---

## 2. 需求与约束汇总

> 只做**摘要与指针**，完整条文见需求文档对应章节，避免两处维护。

### 2.1 功能需求（FR，需求 §3.1）

| ID | 摘要 | 优先级 | 设计落点 |
| --- | --- | --- | --- |
| FR-01 | 修 C-3：`IUserImage` 增加 `async_load` / `async_delete`，客户端与服务端各自实现 | P0 | §6.1 |
| FR-02 | `user_images` 由 `TrainImageTable` 按 `user.shared_group` 真实查询，替换硬编码 `[]` | P0 | §5.2 |
| FR-03 | API-15 加载登记：`image_tar` + 可选 `image` + 共享根校验 + 按 `image_tar` 幂等 upsert | P0 | §4.1 / §5.2 |
| FR-04 | 状态机落地：`processing/loading/loaded/failed/deleted` + 迁移规则 + 三方口径对齐 | P0 | §7.3 |
| FR-05 | API-18 删除：按 3 段 URL 软删，**必须校验 `shared_group == user.shared_group`** | P0 | §4.4 / §10 |
| FR-06 | 幂等与重试安全：同 `image_tar` 不产生重复行、不重置已完成状态；变更型调用不静默重试 | P0 | §5.2 / §6.2 |
| FR-07 | 概念单点：`image_tar`（tar 包）/ `image`（`name:tag`，不含 `/`）/ `path`（镜像位置）三者定义与校验 | P0 | §3 |
| FR-08 | 运行面补齐：`link_hfai_image.sh` + `one/hai-up.sh` 挂载种子 + 基础镜像可配置 | P0 | §5.4 / §9.1 |
| FR-09 | 数据面执行：定义加载后端，**至少一个不依赖内网 registry** | P0 | §5.3 / ADR-I2 |
| FR-10 | API-16 状态回报：只有被登记的 `task_id` 可回报（防伪造） | P0 | §4.2 |
| FR-11 | 列表输出契约：6 字段必备、`updated_at DESC`、出口归一化 numpy 标量 | P0 | §5.2 |
| FR-12 | 客户端失败提示打印服务端 `msg`，不再抛裸异常栈 | P1 | §6.2 |
| FR-13 | 路径单点（`image_path`）+ `check_is_subpath` 校验 | P0 | §3 |
| FR-14 | 空间回收（P2，本期只登记，仅保证文案不误导） | P2 | §7.3(P2) |
| FR-15 | 任务侧 K1–K5 不变式不得回归 | P0 | §7.1 |

### 2.2 非功能 / 安全 / 运维 / 兼容 / 硬约束（摘要）

| 组 | 关键条目 | 说明 |
| --- | --- | --- |
| NFR | `NFR-01` 幂等 · `NFR-02` list P95 < 1s 且无 N+1 · `NFR-03` 日志 + 4 指标 · `NFR-04` 无 registry/无 k8s 可单测 · `NFR-05` 只追加字段 · `NFR-06` 大 tar 不打爆 pod | 落点：设计 §4 / §5.2 / §9.4；对齐 `tests/env/` 的 L1 写法 |
| SEC | `SEC-01` 路径越界 · `SEC-02` 组隔离（绝不采信客户端 group） · `SEC-03` 注入防护 · `SEC-04` 鉴权 + `task_id` 归属 · `SEC-05` 禁跨组、组内可删他人 · `SEC-06` 日志脱敏 · `SEC-07` 最小权限 | 落点：设计 §3 / §4 / §10 |
| OPS | `OPS-01` 三级灰度 · `OPS-02` 一级回滚 · `OPS-03` 迁移幂等走 `db_schemas` · `OPS-04` 节点前置自检 · `OPS-05` 空间回收 P2 登记 | 落点：设计 §9 / §7.4 |
| CMP | `CMP-01` 旧单参 `load` 可用 · `CMP-02` `delete` 签名不变 · `CMP-03` `train_environment` 零改动 · `CMP-04` registry 默认值保留但不可依赖其可达 · `CMP-05` 字段只增不改名 · `CMP-06` `custom.py` 三层覆盖有效 | 落点：设计 §8 |
| HC | `HC-01` 三条 SQL 硬约束 · `HC-02` K2 拼接逐字节不变 · `HC-03` `'loaded'` 字面量不可改 · `HC-04` 服务端状态须含 `deleted` 子串 · `HC-05` `image` 不含 `/` · `HC-06` DDL 幂等重放 · `HC-07` 落在 `ugc-server` 宿主 · `HC-08` 脚本随镜像进入 pod · `HC-09` `a_find_user_group_image_urls` 签名不变 · `HC-10` 不得在服务器 pod 操作节点运行时 | 违反即返工；`HC-01` / `HC-08` 是实测最容易踩的两条 |

### 2.3 接口清单（需求 §4）

| ID | 方法 | 路径 | 性质 | 幂等 | 客户端调用点 |
| --- | --- | --- | --- | --- | --- |
| API-15 | POST | `/ugc/user/train_image/load` | **新增**（替换 `hfai_image_load` 桩） | 按 `image_tar` upsert | `client/api/image_api.py:23`（修复后） |
| API-16 | POST | `/ugc/user/train_image/update_status` | **新增**（替换桩，内部走用户 token） | 同状态无副作用 | 无（加载执行方） |
| API-17 | POST | `/ugc/user/train_image/list` | **修订**（路由已存在，只改 `user_images` 来源与排序） | 是 | `client/api/image_api.py:12` |
| API-18 | POST | `/ugc/user/train_image/delete` | **新增**（替换桩） | 重复删除 `deleted:0` | `client/api/image_api.py:34`（修复后） |

**统一约定**：鉴权 `Depends(get_ugc_user)`；body 兼容 query 与 `text/plain` JSON；成功 `{'success':1,...}`，
失败 `{'success':0,'code':...,'msg':...}`；枚举入 SQL 取 `.value`，出路 JSON 归一化 numpy 标量（需求 §4.5）。

### 2.4 状态机与三方口径（设计 §7.3）

| 当前 | 事件 | 目标 | 允许 |
| --- | --- | --- | --- |
| （无行） | `load`（`register` 后端） | `loaded` | ✅ 同步 |
| （无行） | `load`（`task`/`registry` 后端） | `processing` | ✅ 异步 |
| `processing` / `loading` | 回报成功 / 失败 | `loaded`（须带 `path`）/ `failed`（带 `message`） | ✅ |
| `failed` | 重新 `load` | `processing` / `loaded` | ✅ |
| `loaded` | 重新 `load` | 保持 `loaded`，返回现状 | ❌ 覆盖 |
| `loaded` | `delete` | `deleted` | ✅ |
| `deleted` | `load`（不带 `--force`）/ 回报 `loaded` | —— | ❌ |
| `deleted` | 回报 `loaded` | —— | ❌ `ILLEGAL_TRANSITION` |

> 三方口径：任务侧**精确** `'loaded'`（HC-03）、客户端**子串** `'deleted'`（HC-04）、服务端是**唯一**写入方（ADR-I6）。

---

## 3. 任务列表（P0 分阶段）

> 阶段划分与设计 §14 的 S0–S7 一致；每阶段给出**文件级**任务、交付物与验收口径。估时单位为**人日**。

### 3.1 S0 · 启动前置（0.5）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S0-1 决策冻结 | 需求 §8（`Q-1`~`Q-8`）、设计 §13（ADR-I1..I10） | 8 项决策记录 + 与 ADR 的对应表 | 8 项全有结论（GATE-01/02/04/07） | — | 0.3 |
| S0-2 接口契约冻结 | [images-server-checklist.md](images-server-checklist.md) 附录 A（API-15~API-18） | 冻结版契约 + 评审记录 | GATE-03 通过；后续变更走变更流程 | S0-1 | 0.15 |
| S0-3 基线核对 | `client/commands/hfai_image.py`、`client/api/image_api.py`、`server_model/user_impl/user_image/{default,implement}.py`、`server_model/selector/train_image_selector.py`、`server_model/user_data/table_config.py` | 6 文件 `md5sum` 记录 + 103/本地一致性结论 | 与 `e03c42c` 逐字节一致（分析 §9-9 的方法复用） | — | 0.05 |

> **R-2 是 S0 的硬出口**：`link_hfai_image.sh` 如何访问节点运行时（socket 登记为 `mount_point`，或换自带 `ctr` 的基础镜像）
> **必须在 S0 定案**，否则 S4 无法开工。

### 3.2 S1 · 服务端控制面（3.0）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S1-1 路径单点与配置 | `conf/utils.py`、`cloud_storage/utils.py`、`one/one_etc/core.toml` | `FileType.IMAGE` / `get_image_root()` / `IMAGE_NAME_RE` / `get_base_path` IMAGE 分支 / `[image]` 节 / `image_path` | CFG-01~04 通过（DEV-06/07 的前置） | S0 | 0.5 |
| S1-2 领域层四方法 | `server_model/user_impl/user_image/implement.py`、`server_model/user_impl/user_image/default.py` | `async_load` / `async_report_image_status` / `async_delete` / `async_get_user_images`；`user_images` 改为真实查询 | DEV-04/05/08/09；单测可无 DB 覆盖 | S1-1 | 1.0 |
| S1-3 接入层填桩 | `api/resource/image/default.py` | 4 个具名函数真实实现（**保持函数名**，ADR-I1） | DEV-01；错误码映射到 §4.5 表 | S1-2 | 0.5 |
| S1-4 路由注册与导入 | `api/register/implement.py` | 显式 `from api.resource import image as ares_image` + 3 条路由 | DEV-02/03；`curl` 404→200 | S1-3 | 0.25 |
| S1-5 selector 改造 | `server_model/selector/train_image_selector.py` | `a_find_user_group_images` 改 DESC + 出口归一化；新增 `a_delete_by_group_image` | API-08/09；DB-06 | S1-1 | 0.5 |
| S1-6 表配置同步 | `server_model/user_data/table_config.py` | `TrainImageTable.columns` 追加 `message`（+ 可选 `user_name`） | DB-02；与 DDL 一致 | S1-1 | 0.25 |

### 3.3 S2 · 客户端（1.5）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S2-1 接口层补声明 | `base_model/base_user_modules/default.py` | `IUserImage.async_load` / `async_delete` 声明 | DEV-21；`grep` 三方法齐备 | S0 | 0.25 |
| S2-2 客户端实现 | `client/model/user_impl/default.py` | 两个方法指向 `train_image/{load,delete}` | DEV-22；实机不再 `AttributeError`（AC-02） | S2-1 | 0.5 |
| S2-3 命令层 | `client/commands/hfai_image.py` | `load` 增加 `-i/--image`；`-a/--all` 帮助文本与防御性 `.get()` | DEV-24/28；CMP-01 旧形态仍可用 | S2-2 | 0.25 |
| S2-4 失败提示 | `client/api/image_api.py` | `allow_unsuccess=True` + 打印 `msg` + `SystemExit(1)`；`msg` 兜底；`retries` 保持 1 | DEV-25/26/27；失败场景输出对照（修 I10） | S2-2 | 0.5 |

### 3.4 S3 · 数据面 `register` 后端与迁移（1.0）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S3-1 迁移脚本 | `db_schemas/035.table_train_image_alter.sql` | 幂等加列（`message`，可选 `user_name`）+ 唯一索引改 `(shared_group, image_tar)`（Q-2） | DB-01/03/04；两轮重放不报错（AC-12） | S1 | 0.4 |
| S3-2 `register` 后端 | `server_model/user_impl/user_image/implement.py`、`conf/utils.py` | `loader_backend` 分支：`register` 同步置 `loaded`；`task`/`registry` 走 `processing` + 建任务 | DEV-08/09；103 无 registry 可端到端（AC-09） | S1-2 | 0.3 |
| S3-3 组校验与软删 | `server_model/selector/train_image_selector.py`、`server_model/user_impl/user_image/implement.py` | 3 段解析 + `shared_group` 强制比对 + 软删语句 | DEV-10；API-11/12 | S1-5 | 0.3 |

### 3.5 S4 · 运行面（2.0，**本特性真正的增量**）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S4-1 link 脚本 | **新增** `marsv2/scripts/link_hfai_image.sh` | 幂等脚本（只读 env、已存在即 `exit 0`、失败可见） | DEV-13~16；`sh -n` 通过；连续两次执行不报错 | S0（含 R-2 定案） | 1.0 |
| S4-2 挂载种子登记 | `one/hai-up.sh` | `storage` 种子新增 `marsv2-scripts-{task.id}:link_hfai_image.sh`（HC-08） | DEV-17；任务 pod 内 `ls -l /marsv2/scripts/link_hfai_image.sh` | S4-1 | 0.25 |
| S4-3 基础镜像可配置 | `experiment_manager/manager/init_manager.py`、`one/one_etc/core.toml` | `[image].load_helper_image`（默认 `docker.io/library/busybox:latest`） | DEV-18；`kubectl get pod` 中 initContainer image 正确（Q-6/I17②） | S0 | 0.25 |
| S4-4 节点前置与自检 | 部署脚本、`one/one_etc/core.toml` | `/data_local` 创建或 `DirectoryOrCreate` + 部署自检项 | DEV-19；OPS-04；刻意缺失时自检报警（Q-5/I17①） | S0 | 0.5 |

### 3.6 S5 · 状态机 / 幂等 / 缓存（1.0）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S5-1 状态常量与迁移校验 | `server_model/user_impl/user_image/implement.py`、`api/resource/image/default.py` | 集中常量 + 迁移白名单 + 错误码 `ILLEGAL_TRANSITION`/`FORBIDDEN` | UT-03；API-06；HC-03/HC-04 双向满足 | S1-2 | 0.4 |
| S5-2 幂等 upsert 收紧 | `server_model/selector/train_image_selector.py`、`server_model/user_impl/user_image/implement.py` | `on conflict ... where status in ('failed','deleted')` + 并发 `load` 同 tar 只留一行 | DB-08；UT-04；API-04（AC-05） | S3-1 | 0.3 |
| S5-3 缓存刷新 | `server_model/user_data/table_config.py`、`launcher.py`（读侧确认） | `_refresh_train_image_cache()` + launcher `@cached` 失效策略（或明确的 P0 兜底） | DEV-12；E2E-09；R-3 记录为已知限制 | S1-2 | 0.3 |

### 3.7 S6 · 测试（2.5）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S6-1 L1 单测 | `tests/images/`（新增，对齐 `tests/env/test_env_registry.py` 写法） | 命名派生/路径/状态机/组校验/归一化用例 | UT-01~04；无 DB、无 k8s、无 registry 可跑（NFR-04） | S1–S5 | 1.0 |
| S6-2 L2 接口测试 | `docs/haiplatform/scripts/smoke_images.sh`（新增，对齐 `smoke_ugc.sh`） | 4 接口成功/失败/幂等/错误码 | API-01~12；CI 可重复 | S1–S5 | 0.5 |
| S6-3 **L3 E2E（AC-01）** | `docs/haiplatform/scripts/e2e_images.sh`（新增） | load → list → 任务成功产出 → delete → 任务被拒 | E2E-01~09；**必须跑出可区分输出** | S4 + S6-2 | 0.75 |
| S6-4 回归 | 复用 `docs/haiplatform/scripts/smoke_ugc.sh` / `e2e_workspace.sh` / `e2e_env.sh` | 三脚本输出 | E2E-10；AC-10（零回归） | S6-2 | 0.25 |

### 3.8 S7 · 灰度 / 回滚 / 可观测 / 文档（1.5）

| 任务 | 文件清单 | 交付物 | 验收 | 依赖 | 估时 |
| --- | --- | --- | --- | --- | --- |
| S7-1 灰度与开关 | `one/one_etc/core.toml`、`override.toml`（103） | 三级灰度 + `FEATURE_DISABLED` 快速失败 | CFG-06（Checklist）；OPS-01（需求）；SEC-08（Checklist，**与需求的 `SEC-xx` 不同源**）；REL-02/03（Checklist） | S1–S6 | 0.4 |
| S7-2 回滚演练 | 与 Checklist 附录 B 配套的演练脚本 | 一级/二级回滚演练记录 | RB-01~05；OPS-02（`list` 与内建镜像不受影响） | S7-1 | 0.3 |
| S7-3 可观测 | 指标埋点、日志字段、看板与告警规则文件 | 4 指标 + 结构化日志 + 看板脚本 + 告警规则 | OBS-01~05；AC-11 | S1–S5 | 0.4 |
| S7-4 文档与交付物 | `docs/_sources/cli/ugc.rst.txt`、`docs/_sources/guide/*`、`docs/haiplatform/README.md`、Release Note、交付清单 | 文档 diff + 归档包 | DOC-01~05；DEP-01~03；AC-14 | S6 | 0.4 |

**估时合计**：0.5 + 3.0 + 1.5 + 1.0 + 2.0 + 1.0 + 2.5 + 1.5 = **13.0 人日**（与设计 §14 一致）。

---

## 4. 测试任务（103 实机）

### 4.1 两条环境路径

| 路径 | `loader_backend` | 是否需要内网 registry | 定位 | 是否必过 |
| --- | --- | --- | --- | --- |
| **路径 1（主线）** | `register` | **不需要**（只登记 + 校验，真正 import 推迟到 pod 启动的 link） | 103 **无任何 registry**（§1 第 12 行），这是唯一能端到端验证的后端（ADR-I2） | ✅ **必须通过** |
| 路径 2（可选） | `registry`（生产）/ `task`（P1） | 需要（预导入后 push） | 仅发布前可选验证；103 不可达，属 release-only | ❌ 不阻塞 P0 |

> **判定纪律**：路径 1 未跑通不得进入 M3；路径 2 允许「环境不具备」而显式记为未执行，**不得**用 mock 冒充。

### 4.2 可复现命令序列（路径 1）

```bash
# ① 前置：开关、共享根、节点前置
sudo -u fireflyer hai-cli whoami                                  # 期望：haiadmin / shared_group=hfai
ls -l /nfs-shared/hai-platform/image/demo.tar                     # 期望：探针 tar 存在
multipass exec k8s-slave01 -- ls -ld /data_local                  # 期望：存在（否则 I17①）

# ② 控制面：加载 → 列表 → 库内核对
sudo -u fireflyer hai-cli images load /nfs-shared/hai-platform/image/demo.tar --image demo:v1
sudo -u fireflyer hai-cli images list                             # 期望：该行 status=processing → loaded
sudo kubectl -n hai-platform exec hai-platform-0 -- \
  psql -U root -d mars_db -c "select image_tar,image,path,status,task_id from train_image;"

# ③ 运行面：用自定义镜像跑真实任务（AC-01 关键一步，必须有可区分输出）
sudo -u fireflyer hai-cli python /tmp/probe_img.py -- \
  --image registry.high-flyer.cn/hfai/demo:v1 -n 1
sudo -u fireflyer hai-cli status <task_id>                        # 期望：succeeded
sudo -u fireflyer hai-cli logs <task_id>                          # 期望：探针输出符合预期
sudo kubectl -n hai-platform describe pod <pod> | grep -A5 load-image   # 期望：initContainer Completed

# ④ 负例基线（修复前实测，用于对照）
#   无 status='loaded' 行时提交任务，服务端原话：
#   用户所在的组 [hfai] 不存在镜像 [registry.high-flyer.cn/hfai/demo:v1] 或镜像仍在加载,
#   请使用命令 `hfai images list` 检查
#   而客户端把业务失败抛成裸异常：Exception: 请求失败: [exception: ...]（api_utils.py:75）
#   修复后必须同时满足：⑤ 报错文案不变 + `images list` 能查出该行真实状态（K5 闭环）

# ⑤ 删除与不可用性
sudo -u fireflyer hai-cli images delete registry.high-flyer.cn/hfai/demo:v1
sudo -u fireflyer hai-cli images list -a                          # 期望：status 含 deleted
sudo -u fireflyer hai-cli python /tmp/probe_img.py -- \
  --image registry.high-flyer.cn/hfai/demo:v1 -n 1                # 期望：被拒（且提示可自查）
```

**通过判据**：② 返回 `success=1` 且状态收敛到 `loaded`、`path` 已回填；
③ 任务 `succeeded` 且输出**能被镜像内容区分**（不看 HTTP 200）；⑤ 删除后提交被拒，且此时 `images list` 能解释原因。

### 4.3 测试任务表

| ID | 任务 | 判定 | 依赖 |
| --- | --- | --- | --- |
| T-01 | 103 环境健康（4 节点 + `hai-platform-0` 1/1 + `ugc_server` :8083） | 全部通过 | — |
| T-02 | 镜像构建与部署（含 `link_hfai_image.sh`；本环境 registry push 无凭据时走 `docker save` + `ctr images import`） | 平台 pod 1/1、内建镜像任务冒烟通过 | S4-2 |
| T-03 | 配置就绪（`[image]` + `image_path` + `load_helper_image`） | CFG-01~04；`enabled=false` 时 `FEATURE_DISABLED` | S1-1 |
| T-04 | DB 核对（迁移 + 索引 + 触发器） | DB-01~05；两轮重放幂等 | S3-1 |
| T-05 | 测试数据（探针 tar、跨组用户 `T_A/T_B/T_C`、灰度白名单） | 可重复构造（脚本化） | T-03 |
| T-06 | L1 单测（无 DB/无 k8s/无 registry） | UT-01~06 全过 | S6-1 |
| T-07 | L2 接口冒烟（附录 B 脚本 9 步） | API-01~12 全过 | T-02/T-03 |
| T-08 | **L3 E2E（路径 1）** | E2E-01~09；AC-01 有可区分输出 | T-07 + S4 |
| T-09 | 回归 | E2E-10；`smoke_ugc` 8/8、`e2e_workspace` 19/19、`e2e_env` 全绿 | T-08 |
| T-10 | 路径 2（可选，release-only） | 有 registry 时 registry 后端可用；无则显式记为未执行 | T-08 |

---

## 5. 待确认决策

| ID | 问题 | 建议 | 阻塞的阶段 |
| --- | --- | --- | --- |
| Q-1 | 数据面后端：`registry`（push 内网 registry）还是 `register`/node link？ | **选 `register` 主线**（运行期本就是 link，registry 只是命名；103 无 registry，见 §1 第 12 行 / ADR-I2） | **S0**（阻塞 S3/S4） |
| Q-2 | `image_tar` 唯一索引不含 `shared_group`，两个组加载同一 tar 会冲突 | 改为 `(shared_group, image_tar)` 唯一（新迁移文件，幂等），`on conflict` 目标同步改 | **S0**（阻塞 S3-1） |
| Q-3 | 同名不同 tar（`image` 相同）是否允许 | **允许**（保留历史行），`delete` 作用于该名字全部行；提交校验只看 `status='loaded'` | S0（影响 API-15/18 语义） |
| Q-4 | 加载由**平台任务**执行还是**专用 k8s Job** | 复用平台任务（`task_id` 列与 `update_status` 桩共同指向该模型，ADR-I3） | **S0**（阻塞 S3-2） |
| Q-5 | `/data_local` 从哪来、谁初始化 | 部署流程显式创建，或 hostPath 改 `DirectoryOrCreate` 并纳入部署自检（OPS-04） | **S0**（阻塞 S4-4） |
| Q-6 | initContainer 基础镜像用哪个 | 改为可配置 `[image].load_helper_image`，103 用节点已有的 `docker.io/library/busybox:latest` | **S0**（阻塞 S4-3） |
| Q-7 | 103 是否需要部署内网 registry | 若采纳 Q-1 的 node link，**不需要**；仅生产可选 | **S0**（决定是否走路径 2） |
| Q-8 | 是否新增 `user_name` 列记录归属 | **建议加**（便于审计与「谁加载的」追溯），与「组内共享、组内可删」不冲突 | S0（影响 S1-6/S3-1 迁移范围） |

> 设计 §15 明确：**Q-1 / Q-2 / Q-4 / Q-5 / Q-6 / Q-7 必须在 S0 定案**，否则 S1/S4 无法开工；
> Q-3 / Q-8 可稍后但必须在 S1 结束前定稿（影响接口语义与迁移范围）。

---

## 6. 文档间不一致裁决项

| # | 不一致 | 双方原文/现状 | 裁决 |
| --- | --- | --- | --- |
| 1 | **审计 §5 缺口矩阵不含 images** | 全局横切审计 [hai-cli-client-server-audit.md](../hai-cli-client-server-audit.md) §5 的缺口矩阵为 **6 行**（并对原第 7 行 `/ugc/update_cluster_venv` 划线标注「已闭环」），**没有任何 images 条目**；审计全文也没有 `images` 的 `S-x` | 裁决：**`images load/delete` 应补为新的第 7 行缺口**（客户端已会调用、服务端无路由，一旦修好 C-3 立刻退化为此形态，见分析 §7）。本文按此口径实施，审计 §5 的修订由文档责任人另行落笔（**本任务不改审计文件**） |
| 2 | **审计完全未覆盖运行面** | 审计没有 `link_hfai_image` / `HFAI_IMAGE*` / launcher 查表注入 env 的任何段落 | 裁决：以分析 **§4.6** 为准，把运行面交付物（`link_hfai_image.sh` + 挂载种子 + 节点前置）纳入本特性 DoD（FR-08 / AC-08 / HC-08），**不得**按「审计未提」而缩水范围 |
| 3 | `assert_success` 行号口径不一致 | 分析 §8 写 `client/api/api_utils.py:63-136`（`assert_success` 在 `:100-104`）；设计 §16 写 `:48-119`（`assert_success` 在 `:106-108`） | 裁决：以**实测代码**为准（S0-3 基线核对时确认并回写）；本任务列表统一引用「`api_utils.py` 的 `assert_success`」语义，不锁定行号 |
| 4 | **`API-xx` 同形不同源** | Checklist 的 `API-01..API-12` 是**检查项**；需求/设计的 `API-15..API-18` 是**接口号** | 裁决：跨文档引用**必须带文档前缀**（如「Checklist API-07」vs「需求 API-17」）；同理 Checklist 的 `ACC-*` 与需求的 `AC-*` 是两套编号 |
| 5 | `SEC/OPS/CMP` 同名前缀两套编号 | Checklist 阶段的 `SEC-xx`/`OPS-xx`/`CMP-xx` 是检查项编号，与需求需求条目同名前缀 | 裁决：同上，引用带前缀；本文 §2.2 一律写「需求 SEC-01」形态 |
| 6 | `images list` 服务端过滤 vs 客户端隐藏 | 需求 API-17 要求服务端返回**本组全部状态行（含 `deleted`）**，由客户端 `-a/--all` 决定隐藏（CMP-05）；而分析 §3.3 记录客户端用**子串** `'deleted' in status` 过滤 | 裁决：保持既有分工（服务端不隐藏、客户端子串过滤），服务端状态词表必须含 `deleted` 子串（HC-04）；此条同时是 Checklist API-10 / E2E-06 的判据 |

> **另有两处「不是冲突但要显式声明」**：① 分析 §3.3 注的 `mars_images` 缺 `cuda` 导致显示 `unknown`，
> 属 `train_environment.config` 数据填充问题，**不属本特性缺陷**，本文与 Checklist 均不据此立检查项；
> ② `I15`（空间回收）与 `FR-14` 为 **P2，本期只登记不实现**，在 Checklist OPS-05 / ACC-08 中显式写明「不处置理由」。

---

## 7. 里程碑与关键路径

### 7.1 里程碑（沿用设计 §14）

| 里程碑 | 判定 | 对应阶段 |
| --- | --- | --- |
| **M1 控制面闭环** | `images list` 能看到真实行；`load` / `delete` 不再抛 `AttributeError`；AC-02/AC-03/AC-05/AC-06/AC-07 通过 | S0–S3 + S6-1/S6-2 |
| **M2 运行面闭环** | 用自定义镜像跑通一个真实任务并产出**可区分输出**；link 脚本无 `not found`；**AC-01/AC-08/AC-09** 通过 | S4 + S5 + S6-3 |
| **M3 可上线** | 灰度开关可控、一级回滚可用、指标有数据、workspace/env 回归全绿（AC-10/AC-11/AC-12/AC-13） | S7 + S6-4 |

### 7.2 关键路径与估时

```
S0(0.5) ─► S1(3.0) ─► S4(2.0) ─► S6(2.5) ─► S7(1.5)   ≈ 9.5 人日
              │                     ▲
              ├─► S2(1.5) ──────────┤（客户端可与 S1 并行，S6 一并验收）
              ├─► S3(1.0) ──────────┤
              └─► S5(1.0) ──────────┘（S5 依赖 S1/S3）
```

| 项 | 值 |
| --- | --- |
| 关键路径 | **S0 → S1 → S4 → S6 → S7**（≈ **9.5 人日**） |
| 总工作量 | ≈ **13.0 人日**（S0 0.5 + S1 3.0 + S2 1.5 + S3 1.0 + S4 2.0 + S5 1.0 + S6 2.5 + S7 1.5） |
| 可并行 | S2（客户端）与 S1/S3 无耦合；S5 的部分单测可与 S3 同步编写；S4 的 S4-3/S4-4 与 S4-1 可并行 |
| 不可并行（硬依赖） | **S4-1 必须在 R-2 定案后才能动工**；S6-3（AC-01）必须等 S4-1/S4-2 完成，否则任务必卡 Init（I16） |
| 相对参照 | `env` 特性 ≈ 6.5 人日；本特性更大的原因是**多了一条运行期链路**（`images` 必须让 pod 真正用上镜像，而 `env` 只需补上传入口） |

> **工作量锚点提醒**：本特性的价值锚点是 **AC-01（跑通一个真实任务）**，不是「接口返回 200」。
> 分析与设计均已证明：只修控制面**不足以**让自定义镜像跑起来（I16/I17），因此 S4 的 2.0 人日**不可裁剪**到 S6 的测试预算里。
