# HAI Platform · `hai-cli images` 实施决策记录（S0 出口）

> **文档定位**：任务列表 [images-server-task-list.md](images-server-task-list.md) §3.1 的 **S0 交付物**，
> 对应 Checklist [images-server-checklist.md](images-server-checklist.md) 的 **GATE-01/02/03/04/05/07**。
> 本文只记录**决策与裁决**，不重复需求/设计条文；每条决策都给出落点文件与验收口径。
>
> **基线**：`feature/hai-cli-images-server-design` @ `b85c139`（103 工作树于本阶段开始时
> `git status` 为空、`HEAD=b85c139`，因此文件内容与该提交逐字节一致）。
> 本文写于实施之前，实施过程中的偏差一律回写本文件（§5）。

---

## 1. S0-3 基线核对（6 个镜像相关文件的 md5）

| 文件 | `b85c139` 的 md5 |
| --- | --- |
| `client/commands/hfai_image.py` | `023a17b5c9242c8edae1c53a80cee7dc` |
| `client/api/image_api.py` | `5916befd78c2581d96576243deae23e0` |
| `server_model/user_impl/user_image/default.py` | `764ce156e0874b705a031516d2a16371` |
| `server_model/user_impl/user_image/implement.py` | `c15507c0477e84959e510905007638ef` |
| `server_model/selector/train_image_selector.py` | `5339fe70ba4d1284418f94e429faab61` |
| `server_model/user_data/table_config.py` | `bc5c146a06b7579d9cc4ad2c862ba800` |

同批记录的另 5 个关键文件（本特性会改到，便于复核）：

| 文件 | `b85c139` 的 md5 |
| --- | --- |
| `api/resource/image/default.py` | `94dbec86f4312ff126d59f00420b3bc5` |
| `api/register/implement.py` | `50ad81727abb7e92959b3f41bb1bea44` |
| `launcher.py` | `edf5eeed0a79a801cf461a10c212138a` |
| `experiment_manager/manager/init_manager.py` | `a164189cc7ad4714de20a24c7c811d20` |
| `one/hai-up.sh` | `2c93efacbd8050961ebcad96165e6fb7` |

复核命令：`git show b85c139:<path> | md5sum`。

---

## 2. Q-1 ~ Q-8 决策冻结（GATE-01/02/04/07）

| ID | 决策 | 与 ADR 的对应 | 落点文件 | 验收 |
| --- | --- | --- | --- | --- |
| **Q-1** | 数据面主线用 **`register` 后端**：`load` 只校验+登记，状态同步置 `loaded`；真正 import 推迟到 pod 启动时由 link 脚本完成 | ADR-I2 | `[image].loader_backend='register'`；`server_model/user_impl/user_image/implement.py` | AC-09（103 无 registry 可端到端） |
| **Q-2** | 唯一索引由 `(image_tar)` 改为 **`(shared_group, image_tar)`**，新迁移 `035` 幂等重建；`on conflict` 目标同步改 | ADR-I8 / R-4 | `db_schemas/035.table_train_image_alter.sql`；`train_image_selector.a_upsert_image` | DB-03 / TC-DB-02 |
| **Q-3** | **允许同名不同 tar**：不同 `image_tar` 各自成行，保留历史；`delete` 作用于该 `image` 的全部行；已被删除的行重新 `load` 需显式 `--force` | ADR-I8 | `train_image_selector.a_delete_by_group_image`；`implement.async_load(force=...)` | API-11 / API-12 / TC-A14 |
| **Q-4** | 加载执行主体复用**平台任务**（`task_id` 列 + `update_status` 桩的既有模型）；P0 只实现 `register`，`task`/`registry` 执行体留 P1 | ADR-I3 | `implement.async_load` 的后端分支 | 记录为已知限制（§4.6） |
| **Q-5** | `/data_local` 由部署创建，**同时**把 hostPath 改为 `DirectoryOrCreate` 兜底，并纳入部署自检 | ADR-I4 | `experiment_manager/manager/init_manager.py`（initContainer 卷） | DEV-19 / OPS-04 |
| **Q-6** | initContainer 基础镜像改为 `[image].load_helper_image`，103 用节点已有的 `docker.io/library/busybox:latest` | ADR-I4 | `init_manager.py`；`one/one_etc/core.toml` | CFG-05 / DEV-18 |
| **Q-7** | 103 **不部署内网 registry**；只验证「registry 不可达 + `register` 后端仍成功」 | ADR-I2 | `docs/haiplatform/scripts/e2e_images.sh` §0 | AC-09；路径 2 记为「未验证」 |
| **Q-8** | **新增 `user_name` 列**记录「谁加载的」；权限仍是「组内共享、组内可删他人、禁止跨组」 | ADR-I8 | `db_schemas/035.*`；`table_config.TrainImageTable.columns` | DB-02 / SEC-05 |

---

## 3. R-2 定案（GATE-05）：link 脚本如何访问节点容器运行时

**结论：把运行时通路做成 `[image]` 下的三个配置项，并且只挂进 initContainer（不挂主容器）。**

| 配置键 | 语义 | 103 取值 |
| --- | --- | --- |
| `containerd_socket` | 节点 containerd socket（以 `Socket` 类型 hostPath 挂到 initContainer 的 `/run/containerd/containerd.sock`） | `/var/snap/microk8s/common/run/containerd.sock` |
| `runtime_bin_dir` | 提供 `ctr` 的宿主目录（只读挂到 `/host-bin`，脚本优先用 PATH 里的 `ctr`，否则用 `/host-bin/ctr`） | `/snap/microk8s/current/bin` |
| `image_mount_root` | 镜像 tar 所在共享根，**按同一路径**挂进 initContainer（`HFAI_IMAGE_WEKA_PATH` 就在其下） | `/nfs-shared/hai-platform/workspace/image`（见 §4.2） |

**为什么不用「把 socket 登记为 mount_point（storage 行）」**：storage 行是**任务级**挂载，
会同时把节点运行时 socket 挂进**主容器**（违反 SEC-07 最小权限，也扩大攻击面）。
改成 initContainer 专属卷后：主容器零新增挂载；socket/ctr/镜像根三项留空即回到旧行为（向后兼容）。

**基础镜像选择**：节点上 `docker.io/library/busybox:latest` 已存在（三节点实测），
busybox + 宿主 `ctr` 二进制即可完成 `ctr -n k8s.io images import`，无需拉取任何镜像（I17② 闭环）。

---

## 4. 文档间不一致的裁决

### 4.1 `image` 是否自动补 `:latest` —— **以设计 §4.1「实现修正 I6b」为准：不补**

- 冲突双方：设计 §4.1「实现修正 I6b」（**不得**自动补 tag，理由：任务侧 K2 是逐字节比较）
  vs Checklist **DEV-07** / 用例 **TC-U02 / TC-A02**（"无 tag 补 `:latest`"）。
- 裁决：**不补 tag**。`image` 原样保存（tar basename 去 `.tar` 后缀，或用户 `--image` 显式给定），
  并在 `images list` 中以 `registry/shared_group/image` 形式展示——这正是用户应当**原样**传给
  `-i` 的第三段。补 `:latest` 会让 `-i registry/<group>/demo`（不带 tag）永远匹配不上。
- 影响：TC-U02/TC-A02 的期望值改为「派生 `demo`」；单元测试 `test_u02_derive_image_name` 显式断言该口径。
- 后续：需求/用例文档的这三处文字待文档责任人修订（**本次不改需求/用例文件**，与任务列表 §6 的既有做法一致）。

### 4.2 103 的 `image_path` 位置 —— 用 `{workspace}/image`，并记录部署前提

- 实测：平台 StatefulSet 只把 `/nfs-shared/hai-platform/{workspace,log,db,redis,kubeconfig}` 挂进 **平台 pod**，
  **没有挂** `/nfs-shared/hai-platform/image`；而 `images load` 必须能在平台进程里 `stat` 到 tar
  （`IMAGE_TAR_NOT_FOUND` 是契约的一部分，FR-03）。
- 裁决：103 的 `override.toml` 取 `image_path = '/nfs-shared/hai-platform/workspace/image'`
  （该目录同时被平台 pod 与三个计算节点可见）。生产环境应给平台 pod 挂上镜像资产根再把配置改回去。
- 影响：与用例 §2.1/§2.4 文档里写的 `/nfs-shared/hai-platform/image` 不同；
  脚本 `smoke_images.sh` / `e2e_images.sh` 一律从 `override.toml` 读取 `image_path`，不硬编码。
- 登记为部署前提（Checklist ENV-02 的证据改为「平台 pod 可见的镜像根」）。

### 4.3 `loader_backend=task|registry` 的 P0 行为 —— 只登记，不假执行

- 决策：非 `register` 后端下 `load` 仍登记一行，状态为 `processing` 且 **`path` 保持空**
  （设计 §4.1 实现修正 I18：`processing` 阶段不得把 tar 路径写进 `path`），
  同时打印 WARNING 说明执行体未实现。任何「假装 loaded」的写法都判为缺陷。
- 影响：`task`/`registry` 路径在 P0 记为**未实现/未验证**，不阻塞 P0 验收（AC-09 允许）。

### 4.4 客户端 `--force`

- 需求 §8 Q-3 要求「`deleted` 行重新 `load` 需显式 `--force`」，但设计 §6.1 只列了 `-i/--image`。
- 裁决：服务端接受 `force`（query 或 body），客户端新增 `--force`（只加长选项，避免短选项冲突）；
  不传时旧形态零变化（CMP-01）。

---

## 5. GATE 勾选对照

| ID | 结论 | 证据 |
| --- | --- | --- |
| GATE-01 | ✅ Q-1~Q-8 全部冻结 | §2 |
| GATE-02 | ✅ Q-4 与 ADR-I3 一致；Q-3 与 ADR-I8 删除语义一致 | §2 / §4.4 |
| GATE-03 | ✅ 接口契约冻结（API-15~API-18） | Checklist 附录 A；实现逐字段对齐 |
| GATE-04 | ✅ Q-5/Q-6/Q-7 有结论且支撑 ADR-I2/I4 | §2 / §3 |
| GATE-05 | ✅ R-2 定案：initContainer 专属挂载 | §3 |
| GATE-06 | ✅ 零回归面清单：`conf/utils.py`、`cloud_storage/utils.py:get_base_path`、`one/one_etc/core.toml` | 见 S6-4 回归结果（workspace/env 脚本） |
| GATE-07 | ✅ 迁移策略：新增 `db_schemas/035.table_train_image_alter.sql`（幂等、fail-soft），Q-8 采纳加列 | §2 / 迁移文件 |
| GATE-08 | ✅ 交付范围含运行面（link 脚本 + 挂载种子 + 可配置基础镜像 + 节点前置） | §3 / `one/hai-up.sh` seed |

## 6. 实施过程中的偏差回写

见本文末节由实施阶段追加的「偏差与实测」小节（S6/S7 阶段填写）。
