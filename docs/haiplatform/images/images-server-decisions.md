# HAI Platform · `hai-cli images` 实施决策记录（S0 出口）

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

> **文档定位**：任务列表 [images-server-task-list.md](images-server-task-list.md) §3.1（S0）与 §3.10（S9）的交付物，
> 对应 Checklist [images-server-checklist.md](images-server-checklist.md) 的 **GATE-01~GATE-09**。
> 本文只记录**决策与裁决**，不重复需求/设计条文；每条决策都给出落点文件与验收口径。
>
> **基线**：`feature/hai-cli-images-rustfs-design` @ `33a5b26`（从 `feature/hai-cli-env-server-design` 切出，
> 受跟踪文件与该提交逐字节一致；工作树仅多一个未跟踪的 `docs/haiplatform/PROGRESS.md`）。
> 本文写于上传通道实施之前，实施过程中的偏差一律回写本文件（§7）。

---

## 1. S0-3 基线核对（11 个镜像相关文件的 md5）

| 文件 | `33a5b26` 的 md5 |
| --- | --- |
| `client/commands/hfai_image.py` | `023a17b5c9242c8edae1c53a80cee7dc` |
| `client/api/image_api.py` | `5916befd78c2581d96576243deae23e0` |
| `server_model/user_impl/user_image/default.py` | `764ce156e0874b705a031516d2a16371` |
| `server_model/user_impl/user_image/implement.py` | `c15507c0477e84959e510905007638ef` |
| `server_model/selector/train_image_selector.py` | `5339fe70ba4d1284418f94e429faab61` |
| `server_model/user_data/table_config.py` | `bc5c146a06b7579d9cc4ad2c862ba800` |
| `api/resource/image/default.py` | `94dbec86f4312ff126d59f00420b3bc5` |
| `api/register/implement.py` | `50ad81727abb7e92959b3f41bb1bea44` |
| `launcher.py` | `edf5eeed0a79a801cf461a10c212138a` |
| `experiment_manager/manager/init_manager.py` | `a164189cc7ad4714de20a24c7c811d20` |
| `one/hai-up.sh` | `2c93efacbd8050961ebcad96165e6fb7` |

复核命令：`git show 33a5b26:<path> | md5sum`。

> **结论**：这 11 个值与旧分支 `feature/hai-cli-images-server-design` 的 S0 核对值（基线 `b85c139`）
> **逐个相同** —— 说明控制面/运行面设计所依赖的既有文件在本分支基线上**逐字节未变**，
> 因此旧分支已经实现并实测通过的结论可以整体沿用（§2、§4），本分支只需补上传通道（§3）与资产并入（§1.1）。

### 1.1 本分支基线「缺失物」清单（S9-1 的输入）

| 目标物 | `33a5b26` 上的状态 | 来源 |
| --- | --- | --- |
| `marsv2/scripts/link_hfai_image.sh` | **不存在** | 旧分支新增（运行面唯一缺失物，I16） |
| `db_schemas/035.table_train_image_alter.sql` | **不存在** | 旧分支新增（Q-2/Q-8） |
| `db_schemas/036.file_type_enum_add_image.sql` | **不存在** | 本分支新增（上传通道，OPS-08） |
| `tests/images/`（L1 单测） | **不存在** | 旧分支新增 |
| `image_metrics.py`（4 指标） | **不存在** | 旧分支新增 |
| `docs/haiplatform/scripts/` 的 images 脚本 | **不存在** | 旧分支新增 + 本分支新增 `e2e_images_push.sh` |
| `conf/utils.py` 的 `FileType.IMAGE` | **不存在**（`conf/utils.py:23-35` 只有 dataset/workspace/env/doc/pypi/website） | 旧分支新增 + 本分支直接用 |
| `cloud_storage/utils.py::get_base_path` 的 IMAGE 分支 | **不存在**（函数在 `:445`，只有 WORKSPACE/ENV/DATASET/DOC/PYPI/WEBSITE 分支） | 本分支新增（S8-1） |
| `sync_to_cluster` 的 IMAGE 白名单 | **不存在**（`cloud_storage/service/sync_to_cluster.py:55-56` 仅放 `(WORKSPACE, ENV)`） | 本分支新增（S8-3） |
| PG `file_type` 枚举含 `image` | **不存在**（`db_schemas/010.table_user_downloaded_files.sql:8`） | 本分支新增（S8-2） |
| 客户端 `images push` | **不存在**（`client/commands/hfai_image.py` 只有 `list:33` / `load:79` / `delete:92`） | 本分支新增（S8-4） |

> **一句话**：本分支的基线是「没有任何 images 代码」的干净基线；上传主入口的四项前置改动（key/落点、白名单、枚举、客户端入口）
> 与 P0 的资产并入都是**本次必须做完的事**，不是可选项。

---

## 2. Q-1 ~ Q-8 决策冻结（GATE-01/02/04/07）—— 本分支**不复议**

| ID | 决策 | 与 ADR 的对应 | 落点文件 | 验收 |
| --- | --- | --- | --- | --- |
| **Q-1** | 数据面主线用 **`register` 后端**：`load` 只校验+登记，状态同步置 `loaded`；真正 import 推迟到 pod 启动时由 link 脚本完成 | ADR-I2 | `[image].loader_backend='register'`；`server_model/user_impl/user_image/implement.py` | AC-09（103 无 registry 可端到端） |
| **Q-2** | 唯一索引由 `(image_tar)` 改为 **`(shared_group, image_tar)`**，新迁移 `035` 幂等重建；`on conflict` 目标同步改 | ADR-I8 / R-4 | `db_schemas/035.table_train_image_alter.sql`；`train_image_selector.a_upsert_image` | DB-03 / TC-DB-02 |
| **Q-3** | **允许同名不同 tar**：不同 `image_tar` 各自成行，保留历史；`delete` 作用于该 `image` 的全部行；已被删除的行重新 `load` 需显式 `--force` | ADR-I8 | `train_image_selector.a_delete_by_group_image`；`implement.async_load(force=...)` | API-11 / API-12 / TC-A14 |
| **Q-4** | 加载执行主体复用**平台任务**（`task_id` 列 + `update_status` 桩的既有模型）；P0 只实现 `register`，`task`/`registry` 执行体留后续 | ADR-I3 | `implement.async_load` 的后端分支 | 记录为已知限制（§5.3） |
| **Q-5** | `/data_local` 由部署创建，**同时**把 hostPath 改为 `DirectoryOrCreate` 兜底，并纳入部署自检 | ADR-I4 | `experiment_manager/manager/init_manager.py`（initContainer 卷） | DEV-19 / OPS-04 |
| **Q-6** | initContainer 基础镜像改为 `[image].load_helper_image`，103 用节点已有的 `docker.io/library/busybox:latest` | ADR-I4 | `init_manager.py`；`one/one_etc/core.toml` | CFG-05 / DEV-18 |
| **Q-7** | 103 **不部署内网 registry**；只验证「registry 不可达 + `register` 后端仍成功」 | ADR-I2 | `docs/haiplatform/scripts/e2e_images.sh` §0 | AC-09；路径 2 记为「未验证」 |
| **Q-8** | **新增 `user_name` 列**记录「谁加载的」；权限仍是「组内共享、组内可删他人、禁止跨组」 | ADR-I8 | `db_schemas/035.*`；`table_config.TrainImageTable.columns` | DB-02 / SEC-05 |

> 这 8 条在旧分支已**实现并 103 实测通过**（L1 `33 passed` / L2 `PASS=44 FAIL=0` / L3 `PASS=26 FAIL=0`，
> 证据见 [images-server-test-report.md](images-server-test-report.md)）。本分支把它们作为**前提**沿用：
> 若文档条文与旧实现冲突，以旧实现为准（§5.5）。

---

## 3. Q-9 ~ Q-12：上传主入口四项决策（**已冻结并实现**，GATE-09 ✅）

| ID | 推荐决策 | 备选 | 影响 | 正式冻结 |
| --- | --- | --- | --- | --- |
| **Q-9** | 落点取「**目录 + tar**」：`cluster_base_path = {image_path}/{name}`，落盘文件 `{image_path}/{name}/<file>.tar` | 单文件 `{image_path}/<name>.tar` | 目录语义与 `get_base_path` 既有的 `{root}/{name}` 形态一致；同名多版本/审计更自然；`load` 入参是落盘后的文件路径 | S0 / GATE-09 |
| **Q-10** | S3 key 前缀 `cloud_base_path = {group}/shared/images/{user}/{name}`，bucket 沿用 `private_bucket`（`get_bucket_name` 对 IMAGE 天然走私有分支） | `{group}/shared/images/{user}`（单文件） | 决定 STS 授权前缀（SEC-08）与对象布局，需与 env 的 `{group}/shared/hfai_envs/...` 风格对齐 | S0 / GATE-09 |
| **Q-11** | **新增 API-19** `POST /ugc/user/train_image/push_precheck` | 不新增（客户端只能「先传再登记」） | 客户端可提前判重、展示落点、`registered=true` 时直接提示可提交任务；工作量见任务列表 §3.9 的 S8-5 | S0 / GATE-09 |
| **Q-12** | push 成功后**自动 `load`**（提供 `--no-load`） | 只上传，登记由用户另发命令 | 与用户心智一致；但客户端**必须**区分「上传失败」与「上传成功、登记失败」两种错误面（FR-20） | S0 / GATE-09 |

> **展开口径**：需求/设计/用例/Checklist 中的上传通道条文**一律按上表推荐项展开**
> （设计 §3.5 落点与 key、§5.6 改动 1、§4.6 API-19、§6.4 客户端 `images push`）。
>
> **落地情况（2026-10-02）**：四项决策**已按推荐值冻结并实现**（提交 `fc773e5`，其后 3 处修复）：
> `Q-9` 落点 = `{image_path}/{name}`（`cloud_storage/utils.py` 的 IMAGE 分支）·
> `Q-10` key = `{group}/shared/images/{user}/{name}`（同处，并成为 API-01 的 STS 授权前缀）·
> `Q-11` API-19 已上线（`api/resource/image/default.py` + `api/register/implement.py`）·
> `Q-12` push 成功后自动 `load`（`client/api/image_api.py`，`--no-load` 可关）。
> 端到端实测：`e2e_images_push.sh` → **PASS=33 WARN=1 FAIL=0**（[images-server-test-report.md](images-server-test-report.md) §9.2）。

---

## 4. R-2 定案（GATE-05）：link 脚本如何访问节点容器运行时

**结论：把运行时通路做成 `[image]` 下的四个配置项，并且只挂进 initContainer（不挂主容器）。**

| 配置键 | 语义 | 103 取值 |
| --- | --- | --- |
| `containerd_socket` | 节点 containerd socket（以 `Socket` 类型 hostPath 挂到 initContainer 的 `/run/containerd/containerd.sock`） | `/var/snap/microk8s/common/run/containerd.sock` |
| `runtime_bin_dir` | 提供 `ctr` 的宿主目录（只读挂到 `/host-bin`，脚本优先用 PATH 里的 `ctr`，否则用 `/host-bin/ctr`） | `/snap/microk8s/current/bin` |
| `runtime_lib_dir` | 宿主 glibc 目录（只读挂到 **`/host-lib`**）—— 宿主 `ctr` 是**动态链接**的，helper 镜像自带的 glibc 版本不匹配；link 脚本用 `/host-lib/ld-linux-x86-64.so.2 --library-path /host-lib` 显式运行 ctr | `/usr/lib/x86_64-linux-gnu` |
| `image_mount_root` | 镜像 tar 所在共享根，**按同一路径**挂进 initContainer（`HFAI_IMAGE_WEKA_PATH` 就在其下） | `/nfs-shared/hai-platform/workspace/image`（见 §5.2） |

**实测依据（103 三档对照，先 `microk8s.ctr run` 等价复现，再在真实任务 pod 上验收）**：
① 只挂 `ctr` + socket → `error while loading shared libraries: libdl.so.2`；
② 把宿主 lib 目录挂到 `/lib/x86_64-linux-gnu` → **initContainer 直接失败**：
   `/bin/sh: /lib/x86_64-linux-gnu/libc.so.6: version 'GLIBC_2.38' not found (required by /bin/sh)`
   —— helper 镜像（busybox:latest，Debian trixie / glibc 2.41）自己的 `/bin/sh` 依赖被覆盖的 `libc`；
③ 改为把宿主 lib 目录挂到**另一个路径** `/host-lib`，并用宿主 loader 显式运行 ctr
   （`/host-lib/ld-linux-x86-64.so.2 --library-path /host-lib /host-bin/ctr …`）→ `ctr images ls` 正常、
   helper 镜像自身不受影响。这就是最终 R-2 方案（也是 `runtime_loader_file` 被移除的原因）。

**为什么不用「把 socket 登记为 mount_point（storage 行）」**：storage 行是**任务级**挂载，
会同时把节点运行时 socket 挂进**主容器**（违反 SEC-07 最小权限，也扩大攻击面）。
改成 initContainer 专属卷后：主容器零新增挂载；socket/ctr/镜像根三项留空即回到旧行为（向后兼容）。

**基础镜像选择**：节点上 `docker.io/library/busybox:latest` 已存在（三节点实测），
busybox + 宿主 `ctr` 二进制即可完成 `ctr -n k8s.io images import`，无需拉取任何镜像（I17② 闭环）。

**与上传通道的关系（本分支）**：R-2 只解决「tar 已经在共享盘上之后怎么进节点 containerd」；
上传通道解决「tar 怎么到共享盘」。两者解耦：上传通道落地后 **R-2 相关实现一行不改**（设计 §7.5 / ADR-I14）。

---

## 5. 文档间不一致的裁决

### 5.1 `image` 是否自动补 `:latest` —— **以设计 §4.1「实现修正 I6b」为准：不补**

- 冲突双方：设计 §4.1「实现修正 I6b」（**不得**自动补 tag，理由：任务侧 K2 是逐字节比较）
  vs Checklist **DEV-07** / 用例 **TC-U02 / TC-A02**（"无 tag 补 `:latest`"）。
- 裁决：**不补 tag**。`image` 原样保存（tar basename 去 `.tar` 后缀，或用户 `--image` 显式给定），
  并在 `images list` 中以 `registry/shared_group/image` 形式展示——这正是用户应当**原样**传给
  `-i` 的第三段。补 `:latest` 会让 `-i registry/<group>/demo`（不带 tag）永远匹配不上。
- 影响：TC-U02/TC-A02 的期望值改为「派生 `demo`」；单元测试 `test_u02_derive_image_name` 显式断言该口径。
- 后续：需求/用例文档的这三处文字待文档责任人修订（与任务列表 §6 的既有做法一致）。

### 5.2 103 的 `image_path` 位置 —— 用 `{workspace}/image`，并记录部署前提

- 实测：平台 StatefulSet 只把 `/nfs-shared/hai-platform/{workspace,log,db,redis,kubeconfig}` 挂进 **平台 pod**，
  **没有挂** `/nfs-shared/hai-platform/image`；而 `images load` 必须能在平台进程里 `stat` 到 tar
  （`IMAGE_TAR_NOT_FOUND` 是契约的一部分，FR-03）。
- 裁决：103 的 `override.toml` 取 `image_path = '/nfs-shared/hai-platform/workspace/image'`
  （该目录同时被平台 pod 与三个计算节点可见）。生产环境应给平台 pod 挂上镜像资产根再把配置改回去。
- 影响：与用例 §2.1/§2.4 文档里写的 `/nfs-shared/hai-platform/image` 不同；
  脚本 `smoke_images.sh` / `e2e_images.sh` 一律从 `override.toml` 读取 `image_path`，不硬编码。
- **对上传通道的额外要求（本分支）**：`cluster_base_path` 必须落在**平台 pod 可见**的 `image_path` 之下
  （HC-13）；否则会出现「上传成功但 `load` 报 `PATH_ESCAPE`」的割裂体验（R-12）。

### 5.3 `loader_backend=task|registry` 的 P0 行为 —— 只登记，不假执行

- 决策：非 `register` 后端下 `load` 仍登记一行，状态为 `processing` 且 **`path` 保持空**
  （设计 §4.1 实现修正 I18：`processing` 阶段不得把 tar 路径写进 `path`），
  同时打印 WARNING 说明执行体未实现。任何「假装 loaded」的写法都判为缺陷。
- 影响：`task`/`registry` 路径记为**未实现/未验证**，不阻塞验收（AC-09 允许）。

### 5.4 客户端 `--force`

- 需求 §8 Q-3 要求「`deleted` 行重新 `load` 需显式 `--force`」，但设计 §6.1 只列了 `-i/--image`。
- 裁决：服务端接受 `force`（query 或 body），客户端新增 `--force`（只加长选项，避免短选项冲突）；
  不传时旧形态零变化（CMP-01）。
- **上传通道沿用同一选项名但语义不同**：`images push --force` 表示「忽略『已在集群』判定强制重传」
  （与 `load --force` 的「忽略已删除行」刻意区分，帮助文本必须写清，设计 §6.4）。

### 5.5 本分支文档集与旧分支文档集冲突 —— **以本分支为准**

- 旧分支 `feature/hai-cli-images-server-design` 的文档把上传通道写成「**P1，可选、未开工**」，
  把「手工放盘 + `load`」当成唯一入口；本分支文档集把上传通道写成**唯一上传主入口**（`push`）。
- 裁决：两条链路都保留（`push` = 主入口，手工放盘 = 兼容/运维旁路），但**默认路径与文档口径以本分支为准**；
  代码行为上 `load` 的契约（含 `IMAGE_TAR_NOT_FOUND`）**不得变化**（CMP-08）。
- 影响：README 索引、客户端帮助文本、Checklist 附录 B 的冒烟顺序都要以 `push` 为先。

### 5.6 上传落点与 `no_zip` —— 由 Q-9/Q-10 与 ADR-I13 决定，不另立裁决

- `cluster_base_path` 落在 `image_path` 之下（Q-9）；`cloud_base_path` = `{group}/shared/images/{user}/{name}`（Q-10）；
  上传相对路径为 `<file>.tar`。
- **`no_zip` 恒为 true**（ADR-I13）：`sync_to_cluster.py:121-124` 对 `*.zip` 会先落到 `{cluster_base_path}/.hfai/`
  再解包，若沿用 workspace 的默认 zip 行为，共享盘上会出现 `xxx.tar.zip`，而 `ctr images import` 要的是 tar 本身。

---

## 6. GATE 勾选对照

| ID | 结论 | 证据 |
| --- | --- | --- |
| GATE-01 | ✅ Q-1~Q-8 全部冻结（旧分支，本分支不复议） | §2 |
| GATE-02 | ✅ Q-4 与 ADR-I3 一致；Q-3 与 ADR-I8 删除语义一致 | §2 / §5.4 |
| GATE-03 | ✅ 契约冻结且已实现：API-15~API-18（旧分支逐字段对齐）+ **API-19 已上线**（Checklist 附录 A.6 的请求/响应逐字段落地） | Checklist 附录 A.1~A.4 + **A.6**；`api/resource/image/default.py` |
| GATE-04 | ✅ Q-5/Q-6/Q-7 有结论且支撑 ADR-I2/I4 | §2 / §4 |
| GATE-05 | ✅ R-2 定案：initContainer 专属挂载 | §4 |
| GATE-06 | ✅ 零回归面清单：`conf/utils.py`、`cloud_storage/utils.py:get_base_path`、`one/one_etc/core.toml` + 上传通道的 `workspace`/`env` 语义面（CMP-07） | S9-2 重跑 + 用例 §7.2 |
| GATE-07 | ✅ 迁移策略：`db_schemas/035`（P0，幂等 fail-soft）+ **`db_schemas/036`（本分支，`alter type file_type add value if not exists 'image'`）** | §1.1 / 迁移文件 |
| GATE-08 | ✅ 交付范围含运行面（link 脚本 + 挂载种子 + 可配置基础镜像 + 节点前置） | §4 / `one/hai-up.sh` seed |
| GATE-09 | ✅ Q-9~Q-12 已按推荐值**冻结并实现**，103 端到端验证通过（**PASS=33 WARN=1 FAIL=0**） | §3 / test-report §9.2 |

---

## 7. 实施过程中的偏差回写

| 阶段 | 偏差 | 回写位置 |
| --- | --- | --- |
| P0（旧分支实现期） | 9 个缺陷 D1~D9，其中 **D7 引出新风险 I19**（长 init 被 `unschedulable` 看门狗打断） | [images-server-test-report.md](images-server-test-report.md) §6 |
| 本分支（S9-1 并入） | 31 个 P0 文件与旧分支**逐字节一致**（`git diff 491e5ce` 为空）；确认后提交 `6bc81e2` | 任务列表 §3.10、test-report §9.1 |
| 本分支（S9-2 重跑） | 四套输出与旧分支期望值逐项一致（30/0、33 passed、44/0、26/0、回归全绿） | test-report §9.1 |
| 本分支（S8 实现期） | 4 个缺陷：**D10** API-19 漏 `import os` → 500；**D11** 客户端 provider 默认 `oss` vs 服务端 `s3`；**D13** stage2 落盘窗口期导致 `load` 竞态；**D12** E2E 脚本自身的列名/终态判定错误。另登记 **D14**（环境限制：静态 AK/SK 不强制 prefix → 设计 §15 R-14）与 **R-15**（越界负例留下 `stage2_running` 幂等短路） | test-report §6 / §9.2 |
| 本分支（S8 验证） | `e2e_images_push.sh` **PASS=33 WARN=1 FAIL=0**；开关一致性、md5 一致、幂等、任务可区分输出、还原均有实测 | test-report §9.2 |
| 后续（并入与实现期） | 任何与本文档集不符的实现偏差**必须在同一提交里回写本节** | 本节 |

> 维护规则：决策一旦落地实现，就把「建议冻结」改为「已实现」并附实测证据链接；
> **禁止**只改代码不改本文（这会让 Q-9~Q-12 的落点/key 与实现漂移，直接导致 HC-13 类事故）。
