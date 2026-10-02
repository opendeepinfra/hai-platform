# hai-cli images(用户自定义镜像)客户端 / 服务端现状逆向分析

> **本分支说明（`feature/hai-cli-images-rustfs-design`，基线 `feature/hai-cli-env-server-design` @ `33a5b26`）**
>
> - **本次实现的镜像上传主入口 = `hai-cli images push <本地 tar>`**：复用 `workspace`/`env` 既有的 RustFS/S3 流水线
>   （API-01 签发 STS → 客户端直传对象存储 → API-05 stage2 落盘到 `image_path` → API-06 轮询状态），
>   落盘成功后自动登记（API-15）。手工把 tar 放到共享盘再 `images load` 仅作为**兼容/运维旁路**保留。
> - **来源**：控制面（API-15~API-18）与运行面（`marsv2/scripts/link_hfai_image.sh`、`init_manager.py` 注入、`storage` 挂载种子）
>   的设计沿用分支 `feature/hai-cli-images-server-design` 上**已实现并在 103 实测通过**的结论，证据见
>   [images-server-test-report.md](images-server-test-report.md)（被测 tag `f2cb559`）。
> - **状态**：上传通道（FR-16~FR-20 / 设计 §3.5·§4.6·§5.6·§6.4·§7.5·§9.5 / 用例 §4.11 UP 组 + E2E-09/10 /
>   Checklist 阶段 17）在本文档集中是**本次必须交付的主入口**（不再是「P1 可选、未开工」），**尚未实现、尚未验证**。
> - **标签约定**：文中 `P0` / `P1` 只用于标注**来源与阶段**（P0 = 控制面 + 运行面，P1 = 上传通道），**不代表可选项**。
> - **资产状态**：`docs/haiplatform/scripts/` 的 images 相关脚本与 `tests/images/` **尚未并入本分支**，
>   并入计划见 [images-server-task-list.md](images-server-task-list.md) §3.10；文中引用它们是**目标交付物**而非现有文件。

---

> **分析对象**：`hai-cli images`（`hfai images`）命令族 —— 用户自定义镜像的 `list` / `load` / `delete`（本分支新增 `push`），
> 以及服务端 `train_image` 域（路由、selector、数据表、DDL）与任务提交期的镜像校验链路。
>
> **结论一句话**：`images` 是一条**三段式**链路，三段完成度依次递减 ——
> **① 控制面半成品**（`images list` 能用但 `user_images` 恒为空；`images load/delete` 在客户端就抛
> `AttributeError`（审计缺陷 C-3），服务端连路由都没有（实测 **404**），`train_image` 表**零写入、零行**）；
> **② 提交面完整**（`registry/group/image:tag` 三段校验 + `status='loaded'` 白名单逻辑齐备）；
> **③ 运行面结构完整但缺关键脚本** —— launcher 会查 `train_image` 并把 `path` 作为
> `HFAI_IMAGE_WEKA_PATH` 注入，每个计算 pod 起 busybox initContainer 执行
> `/marsv2/scripts/link_hfai_image.sh`，而**该脚本在本仓库中根本不存在**（§4.6 R3）。
> 因此**只修控制面不足以让自定义镜像跑起来**：这是一个「schema 与具名桩齐备、四处断链 + 缺一个运行期脚本」的特性。
>
> **本分支补充结论（上传入口缺失）**：上述三段之外还有一条**此前完全没有被任何文档盘点的第四段 —— 上传面**。
> 控制面与运行面已在分支 `feature/hai-cli-images-server-design` 上实现并 **103 实测通过**（AC-01，task 36），
> 但从「用户本地的一个 tar」到「`train_image.path` 指向的共享盘落点」之间**没有任何面向 images 的上传入口**：
> 客户端没有 `push`、`FileType` 没有 `IMAGE`、同步白名单没有 image、DDL 枚举没有 `image`。
> 本分支把这条缺口补齐为 `hai-cli images push <本地 tar>`（复用 `workspace`/`env` 的 RustFS/S3 流水线，
> 只加 `file_type=image` 分支），并把「手工放盘 + `images load`」降为**兼容/运维旁路**。
> 上传面的完整盘点见 **§4.7**，这是本文档相对旧分支版本最核心的改写。
>
> **原始审计与 §9 实测的证据基线与时点（旧分支证据来源）**：`fireflyer@192.168.100.103:~/hai-platform` @ `e03c42c`
> （工作树含未提交改动，本次涉及的 6 个镜像相关文件与本地仓库逐字节一致，已用 `md5sum` 核对）；
> §9 的运行面实测在本分支 doc 集中统一标注为**被测 tag `f2cb559`**；客户端运行时为
> `/usr/local/lib/python3.10/dist-packages/hfai`（对应同一提交 `7589fb1` 的镜像内代码）。审计日期 **2026-10-02**。
>
> **本文行号基线（当前分支）**：`feature/hai-cli-images-rustfs-design` @ `33a5b26`。
> §1–§4.6 的**代码现状**行号已在 `33a5b26` 工作树上逐条复核；§4.7 的**上传链路**证据行号同样逐条复核
> （命令与结果见文末「证据索引」与 §4.7）。§9 的实测记录原样保留旧分支证据来源，**本分支尚未重跑**。
>
> **与同目录文档的关系**：
> - [../workspace/hai-cli-workspace-analysis.md](../workspace/hai-cli-workspace-analysis.md)、
>   [../env/hai-cli-env-analysis.md](../env/hai-cli-env-analysis.md) 是 `workspace` / `env` 两个**已收口或已成体系**特性的深挖；
>   本分支的 `images push` 正是复用它们的上传流水线，二者是**上游依赖**；
> - [../hai-cli-client-server-audit.md](../hai-cli-client-server-audit.md) 是**全局横切审计**：本文的 `C-3` 即出自该文 §3.4/§8，
>   本文沿用该 ID 并**补充它没有覆盖的服务端侧结论**（审计全文**没有**任何 `images` 的 `S-x` 条目，也没有出现 `user_images`）；
> - 本分支的 `images` 家族文档见同目录 `images-server-requirements.md` / `images-server-design.md` /
>   `images-server-test-cases.md` / `images-server-checklist.md` / `images-server-task-list.md` /
>   `images-server-decisions.md` / `images-server-test-report.md`（并入进度见 task-list §3.10）。
> - 本文是 `images` 特性的**单特性深挖**，与审计呈「点 → 面」互补关系。
>
> **方法**：只读代码审计 + **103 真实环境实测**（跑真实 `hai-cli`、直连真实 `HTTP` 接口、查真实 `psql` 表）。
> **未修改任何源文件**。实测命令与原始输出见 §9。
>
> **⚠️ 阅读须知（当前工作树 vs 目标交付物）**：§1–§4.6 的代码现状描述的是**当前工作树 `33a5b26`**；
> 旧分支已实现的控制面（API-15~API-18）与运行面（`link_hfai_image.sh` + 挂载种子 + initContainer 修复）
> **尚未并入本工作树**，属本分支 doc 集承接的**目标交付物**（见各 sibling 文档），因此本文照实记录其「当前未并入」状态。
> 本分支**新增**的上传通道（`images push`）同理：§4.7 记录缺口，交付计划见 `images-server-design.md` §3.5/§5.6/§6.4。

---

## 1. 结论速览

| # | 判定项 | 结论 | 关键证据 |
| --- | --- | --- | --- |
| 1 | **上传入口（本分支核心缺口 + 本次主入口）** | ❌→🔨 **此前任何文档都未盘点、代码里根本不存在**：客户端无 `push`、`FileType` 无 `IMAGE`、同步白名单无 image、PG 枚举无 `image`。本次以 `hai-cli images push <本地 tar>` 复用 `workspace`/`env` 的 RustFS/S3 流水线作为**唯一上传主入口**，手工放盘 + `images load` 降为**兼容/运维旁路** | **§4.7**；`client/commands/hfai_image.py:12-99`；`client/api/image_api.py:6-35`；`conf/utils.py:23-35`；`cloud_storage/service/sync_to_cluster.py:55-56`；`db_schemas/010.table_user_downloaded_files.sql:8` |
| 2 | 命令面 | ✅ 注册了 3 个子命令 `list` / `load` / `delete`（本分支目标为 4 个，新增 `push`） | `client/hfai_cli.py:12,65`；`client/commands/hfai_image.py:12-17,33,79,92` |
| 3 | `images list` 客户端 | ✅ 可用（真实调用成功、表格渲染正常） | `client/api/image_api.py:6-13`；§9-1 实测 |
| 4 | `images list` 服务端 | ⚠️ **只返回内建镜像**：`user_images` 被**硬编码为 `[]`** | `server_model/user_impl/user_image/default.py:12-17`（`:16`）；§9-2 实测 `"user_images": []` |
| 5 | `images load` 客户端 | ❌ **完全不可用**：`AttributeError: 'UserImage' object has no attribute 'async_load'` | `client/api/image_api.py:23`；`client/model/user_impl/default.py:5-8`；§9-3 实测 |
| 6 | `images delete` 客户端 | ❌ **完全不可用**：`AttributeError: ... no attribute 'async_delete'` | `client/api/image_api.py:34`；§9-4 实测 |
| 7 | 契约层 | ❌ `IUserImage` **只声明了 `async_get`** —— 缺失不是实现漏写，是**接口没定义** | `base_model/base_user_modules/default.py:20-22` |
| 8 | `images load/delete` 服务端 | ❌ **路由不存在**，实测 HTTP **404 `{"success":0,"msg":"Not Found"}`** | `api/register/implement.py:71`（`ugc` 组只注册了 `list`）；§9-2 实测 |
| 9 | 服务端具名桩 | ⚠️ `hfai_image_load/update_status/list/delete` **4 个桩齐备但一个都没注册**（审计 §4.3 类别 B「死桩」） | `api/resource/image/default.py:3-16` |
| 10 | 数据表 | ⚠️ `public.train_image` 表**存在**（DDL + 唯一索引 + `updated_at` 触发器齐备），但**0 行**、**全仓零写入路径** | `db_schemas/017.table_train_image.sql:1-35`；`server_model/user_data/table_config.py:81-86`；§9-5/§9-6 实测 |
| 11 | 读路径 | ⚠️ `TrainImageSelector` 有 3 个读方法，但**只有任务校验用到**（`status='loaded'`），**列表接口从不调用** | `server_model/selector/train_image_selector.py:44-47,50-56`；`api/operation/default.py:9-24` |
| 12 | 任务侧消费 | ✅ 逻辑完整：`registry/group/image:tag` 三段校验 + `status='loaded'` 白名单 | `api/operation/default.py:9-24`；`api/operation/implement.py:282-295` |
| 13 | 路径配置 | ❌ 无 `FileType.IMAGE`、无 `image_path` 配置项 —— 「tar 在共享盘的哪个根下」**没有单点定义**（上传通道要在本分支补这个单点，见 §4.7-2） | `conf/utils.py:23-35`；`cloud_storage/utils.py:445,464-512`；`one/one_etc/core.toml:110-120` |
| 14 | 103 测试环境 | ❌ **集群内无任何 registry**（无 registry pod / svc），DDL 默认 `registry.high-flyer.cn` **不可达** | §9-7 实测；`db_schemas/017.table_train_image.sql:7` |
| 15 | **运行时消费（launcher 侧）** | ✅ **存在**：`train_image:*` 任务会查表并把 `HFAI_IMAGE` + `HFAI_IMAGE_WEKA_PATH`(= `path` 列) 注入 manager | `launcher.py:59-61,144-147`；`server_model/task_impl/single_task_impl.py:79-81` |
| 16 | **运行时消费（pod 侧）** | ⚠️ 结构完整：每个计算 pod 加 busybox initContainer 执行 link 脚本，挂宿主 `/data_local` | `experiment_manager/manager/init_manager.py:347-360`；`server_model/task_impl/single_task_impl.py:325` |
| 17 | **link 脚本** | ❌ **`marsv2/scripts/link_hfai_image.sh` 在本仓库不存在**（0 处定义 / 3 处引用，且 `one/hai-up.sh:289-301` 挂载种子里没有它）→ initContainer 必然失败、pod 卡 Init（旧分支已实现，本工作树未并入） | §4.6 R3；§9-8 实测 |
| 18 | **103 节点前置** | ❌ `/data_local` **在节点上不存在**（hostPath 无 `type` → 不会自动创建）；节点上只有 `docker.io/library/busybox:latest`，**没有** `registry.high-flyer.cn/google_containers/busybox:latest`；该域名被解析到 `198.18.0.77`（代理段地址） | §9-8 实测 |
| 19 | 端到端 | ❌ **三段全断或半断**：控制面断（`load` 进不去/`list` 看不见/`delete` 删不掉）、运行面缺脚本 → **无一条链路可闭环**；且第四段（上传面）此前根本不存在 | §5 场景矩阵 S1–S11；§4.7 |

**一句话结论（现状）**：`images` 的**控制面**（客户端 3 子命令 + 服务端 1 读接口 + DDL）只完成了「内建镜像只读展示」，
**写入面（load/delete/update_status）在客户端、契约层、服务端路由、DB 写入四处同时断链**；
任务侧消费逻辑反而是完整的 —— 即「**能用的那条路要求 `train_image` 表里有一行 `status='loaded'`，但没有任何代码能造出这一行**」。

**一句话结论（本分支改写主轴 —— 上传入口）**：控制面/运行面已在旧分支 `feature/hai-cli-images-server-design` 上实现并
**103 实测通过**（AC-01 task 36），但**此前没有任何面向 images 的上传入口** —— 用户手上的 tar 无路进集群共享盘，
于是即使控制面全修好，`train_image` 表也只能靠「手工放盘 + `images load`」由运维代劳。
本分支把 `hai-cli images push <本地 tar>` 定为**唯一上传主入口**（复用 `workspace`/`env` 的 RustFS/S3 流水线，
只加 `file_type=image` 分支），`images load` 退为**兼容/运维旁路**。

---

## 2. 命令面：3 个子命令与「在哪儿跑」的区别

> **本分支口径**：本节记录**当前工作树**的命令面现状（3 个子命令）；本分支要交付的第 4 个子命令 `push` 见 §4.7-5。

`images` 是一个 `asyncclick` group，挂在顶层 CLI 上：

```python
# client/hfai_cli.py
from hfai.client.commands.hfai_image import images   # :12
cli.add_command(images)                              # :65
```

| 子命令 | 参数 | 语义（docstring） | 客户端行为 | 服务端调用 |
| --- | --- | --- | --- | --- |
| `images list` | `-a/--all` | 列举用户组在萤火二号上的镜像列表及状态 | 渲染两张 rich 表 | `POST /ugc/user/train_image/list` ✅ |
| `images load` | `<image_tar>` | 加载镜像 tar 包到萤火二号 | `os.path.exists` → `abs` → **`async_load` ❌** | **无** |
| `images delete` | `<image>` | 删除镜像以释放空间 | **`async_delete` ❌** | **无** |
| `images push`（🔨 本分支新增） | `<本地 tar>` | **上传本地 tar 到集群共享盘并自动登记（主入口）** | 复用 `workspace`/`env` 上传实现 | API-01 → RustFS/S3 直传 → API-05 → API-06 → API-15 |

**关键推论（决定 `image_tar` 的取值口径）**：三个子命令都用了同一个
`WorkspaceHandleHfaiCommandArgs`（`client/commands/hfai_image.py:20-30`），其参数帮助文本明确写了两套语义：

> `image_tar`：「用户要加载进萤火的镜像 TAR 包。**在用户本地调用为 workspace 中的路径，在萤火上调用则为其共享存储中的路径**」

即 `images` 支持**两种调用形态**（与 `hai-cli env` 的「一个工具、三种形态」同源）：

| 形态 | 运行位置 | `os.path.exists(image_tar)` 检查的是 | 该形态能否工作 |
| --- | --- | --- | --- |
| 本地（登录节点） | 集群外/登录机，共享盘**已挂载** | 共享盘上的绝对路径 | ❌ 卡在 `async_load` |
| 集群内（任务容器里） | 任务容器，共享盘已挂载 | 共享盘上的绝对路径 | ❌ 卡在 `async_load` |

> **本分支的补正**：上表两行的「本地路径」语义本身就是**上传入口缺失的症状** —— 帮助文本假定用户
> 「已经/un 自己把 tar 放到了 workspace 或共享存储」，却没有提供把 tar 送上去的命令。
> 本分支的 `images push <本地 tar>` 直接以**本地文件**为输入，不再要求用户先手工放盘（设计 §6.4）。

> **澄清（避免误判）**：类名 `WorkspaceHandleHfaiCommandArgs` 与 `client/commands/utils.py:67`
> （`elif subcommand in ['images', 'venv', 'workspace']: _commands = ugc_commands`）**都不构成
> 本地/集群的执行分派**：
> - `utils.py:49-93` 的 `format_commands` 只做 **`--help` 文本分组**（把 `images` 归到
>   `UGC Commands` 段），`utils.py:67` 是全仓唯一引用该列表的地方，且作用于**顶层命令名**；
> - `WorkspaceHandleHfaiCommandArgs`（`hfai_image.py:20-30`）只重写 `format_options`，
>   **作用是把 `image_tar` / `image` 两个参数的帮助文本写清楚**，与 workspace 同步无关；
> - 真正的「本地 / 集群」分派在 `client/commands/hfai_python.py:109-134`
>   （`--` → `CLUSTER`、`++` → `SIMULATE`、都不带 → `LOCAL`），**`images` 完全不经过它**。
>
> 因此 `images` 的「本地路径 / 共享盘路径」双语义**只存在于帮助文本里**，客户端没有做任何
> 上下文判定 —— 这也意味着 `image_tar` 的路径口径**必须由服务端统一校验**（见 I9；上传通道落地后
> 该口径由 `image_path` 单点定义，见 §4.7-2）。

---

## 3. 客户端实现盘点

### 3.1 文件与职责

| 层 | 文件 | 内容 | 状态 |
| --- | --- | --- | --- |
| 命令层 | `client/commands/hfai_image.py` | `images` group + 3 子命令 + 2 张 rich 表渲染 | ✅ 完整（缺 `push`，§4.7-5） |
| 命令参数层 | `client/commands/hfai_image.py:20-30` | `WorkspaceHandleHfaiCommandArgs`（本地/集群双语义帮助） | ✅ 完整 |
| 业务层 | `client/api/image_api.py` | `fetch_images` / `load_image_tar` / `delete_image_by_name` | ⚠️ 后两个调用不存在的方法；**无上传调用** |
| 模块层 | `client/model/user_impl/default.py` | `class UserImage(IUserImage)`，**只有 `async_get`** | ❌ 缺 `async_load` / `async_delete` |
| 接口层 | `base_model/base_user_modules/default.py:20-22` | `IUserImage.async_get()` | ❌ 接口本身没有 load/delete 声明 |
| 传输层 | `client/api/api_utils.py:63-136` | `async_requests`（重试 / 超时 / `success` 断言） | ✅ 完整（细节见 I10） |

### 3.2 三个调用的真实代码路径（逐字）

```python
# client/api/image_api.py
async def fetch_images(**kwargs):                    # :6
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_get()            # → POST /ugc/user/train_image/list
    return result.get('result', {}).get('mars_images', []), result.get('result', {}).get('user_images', [])

async def load_image_tar(tar, **kwargs):             # :16
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_load(tar)        # :23  ← AttributeError
    print(result['msg'])

async def delete_image_by_name(image_name, **kwargs):# :27
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_delete(image_name)# :34 ← AttributeError
    print(result['msg'])
```

```python
# client/model/user_impl/default.py（全文 8 行）
class UserImage(IUserImage):
    async def async_get(self):
        url = f'{mars_url()}/ugc/user/train_image/list?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url, retries=3, timeout=60)
```

**结论**：`images load` / `images delete` **不是在服务端失败，而是在客户端 Python 属性查找阶段就抛异常**
（§9-3/§9-4 实测栈顶为 `AttributeError`）。这是审计缺陷 **C-3**（P0），本文沿用该 ID。

> **本分支补注（避免与「上传入口」混淆）**：`image_api.py` 里**没有**任何把本地文件送到对象存储的调用；
> 本分支要在同一文件新增上传 API（`push`），其实现**不在客户端重写一遍分片/断点逻辑**，而是复用
> `plugins/haiworkspace/haiworkspace/client/workspace_api.py::push`（§4.7-8）。

### 3.3 `images list` 的渲染与两个**已存在的逻辑瑕疵**

`list_images`（`client/commands/hfai_image.py:33-76`）渲染两张 `rich` 表：

| 表 | 数据源 | 列 |
| --- | --- | --- |
| 萤火二号内建镜像 | `mars_images`（按 `quota` 倒序） | `image`(+`(default)` 标记) / `default_python` / `cuda` / `supported_hf_envs` / `environments` |
| 用户自定义镜像 | `user_images` | `image` / `status` / `shared_group` / `image_tar` / `updated_at` |

用户表的 `image` 列由三个字段拼接而成，**这一拼接方式即服务端契约**：

```python
i_name = os.path.join(i['registry'], i['shared_group'], i['image'])   # :63 → registry/group/image:tag
```

由此**反向推出服务端必须提供的字段**（缺一即 `KeyError`）：
`registry` / `shared_group` / `image` / `status` / `image_tar` / `updated_at`（共 6 个）。

**瑕疵 1（去重方向自相矛盾，I7）**：客户端想表达「同一镜像名有多条记录时以最新的为准」：

```python
if i_name not in last_img_status:
    last_img_status[i_name] = i['status']          # :64-65 「首次见到」即被当作基准
if i_name in last_img_status and i['status'] != last_img_status[i_name]:
    i['status'] = f"{last_img_status[i_name]} by new tar({i['status']})"   # :67-68
```

注释写「以最新的为准」，但代码取的是**首次见到**的那条。而服务端候选查询
`a_find_user_group_images` 用的是 `.sort_values('updated_at')`（**升序**，`train_image_selector.py:46`），
于是「首次见到 = 最旧」→ **实际基准是最旧那条，与注释意图相反**。修法有二：服务端按 `updated_at DESC`
返回（推荐，客户端零改动），或客户端改为「后者覆盖前者」。**该判断必须在设计期钉死**，否则同一份数据
在两种顺序下会显示不同的 `status`。

**瑕疵 2（`-a/--all` 的过滤口径依赖 `status` 字符串包含关系）**：

```python
if 'deleted' in i['status'] and not show_all:      # :69
    continue
```

即隐藏规则是**子串匹配 `'deleted'`**，而不是枚举相等。这要求服务端 `status` 的取值集合里
「已删除」必须**拼写包含 `deleted`**（例如 `deleted`、`deleted by new tar(...)`），
否则 `-a/--all` 语义失效。**服务端状态词表由此被客户端代码隐式约束**（见 I5）。

> 另注：`mars_table` 的 `default_python`/`cuda` 取自 `config.python`/`config.cuda`，
> 而 103 实测 `hai_base` 的 `config` **只有 `python`、没有 `cuda`** → 显示 `unknown`（§9-1）。
> 这是 `train_environment.config` 数据填充问题，**不属于 images 特性缺陷**，此处仅记录事实。

---

## 4. 服务端实现盘点

### 4.1 路由面：`ugc` 组里只有 1 条镜像路由

```python
# api/register/implement.py:71
app.post('/ugc/user/train_image/list')(aq_optimized_resource.get_train_images)
```

| 路由 | 处理函数 | 现状 |
| --- | --- | --- |
| `POST /ugc/user/train_image/list` | `api/query/optimized/resource.py:14-19` → `user.image.async_get()` | ✅ 已注册、可访问 |
| `POST /ugc/user/train_image/load` | —— | ❌ **未注册**，实测 404 |
| `POST /ugc/user/train_image/delete` | —— | ❌ **未注册**，实测 404 |
| `POST /ugc/user/train_image/update_status` | —— | ❌ **未注册**，无任何调用方 |

服务端**具名桩**已由原作者留好，但**一个都没挂路由**（`api/resource/image/default.py` 全文 16 行）：

```python
async def hfai_image_load():            # :3
    return {'success': 1, 'msg': 'not implemented'}
async def hfai_image_update_status():   # :7
    return {'success': 1, 'msg': 'not implemented'}
async def hfai_image_list():            # :11
    return {'success': 1, 'data': [], 'msg': 'not implemented'}
async def hfai_image_delete():          # :15
    return {'success': 1, 'msg': 'not implemented'}
```

`api/resource/image/implement.py` 只有 `from .default import *` + `from .custom import *`（无覆盖），
所以按三层约定（审计 §2.3）这 4 个属**类别 B「无任何入口（死桩 / 私有扩展点）」**：
客户端不调用、也没有路由。**它们是本设计要填充的 4 个接缝**（对应设计 API-15~API-18）。

> **推论**：4 个桩的**顺序与命名**（`load` → `update_status` → `list` → `delete`）就是原作者心目中
> 的接口清单，且 `update_status` 的存在**反证**了「加载是一次异步任务、由执行方回报状态」的设计意图
> —— 这与 `train_image` 表里的 `task_id` / `status` 两列完全对应（见 §4.4）。
>
> **注意（上传通道不在这个清单里）**：这 4 个桩**全是「登记/状态/列表/删除」语义，没有一个是「上传」**。
> 原作者留下的接缝里**不含上传入口**，这正是 §4.7 所述缺口的来源：既有控制面设计假定 tar
> **已经在共享盘上**（`load` 的 docstring 明说），上传被默认成「用户自己想办法」。

### 4.2 读路径：`user_images` 被硬编码为空

```python
# server_model/user_impl/user_image/default.py（全文 17 行）
class UserImageExtras(IUserImage):
    async def async_get(self: UserImage):
        return {
            'mars_images': await self.async_get_train_images(),
            'user_images': [],           # ← :16 硬编码空列表，TrainImageTable 从不被读
        }
```

`mars_images` 侧是真的（`implement.py:10-19`）：从 `train_environment` 表取内建镜像，
再用 `user.quota.train_environments` 过滤，并补 `quota` 字段（显式 `int(...)` 转掉 `np.int64`）。

**关键事实**：`TrainImageSelector.a_find_user_group_images(shared_group)`（**正是为列表页准备的那个方法**，
返回全部所需 6 字段 + `created_at`/`path`/`task_id`）**在整个仓库里没有任何调用方**：

| selector 方法 | 定义 | 调用方 |
| --- | --- | --- |
| `a_find_user_group_images` | `train_image_selector.py:44-47` | ❌ **无**（本应是 `user_images` 的数据源） |
| `a_find_user_group_image_urls` | `:50-56` | ✅ `api/operation/default.py:20`（任务提交校验） |
| `a_find_one` | `:33-41` | ❌ 无 |
| `find_one` | `:26-31` | ❌ 无 |

> **上传通道落地后的直接收益**：`push` 每成功一次就有一次 INSERT/upsert（API-15），
> `train_image` 表第一次有了**由用户操作自然产生**的行；一旦 `user_images` 接通（I2 修），
> K5 的「让用户跑 `images list` 自查」才第一次不是误导。

### 4.3 任务侧消费：逻辑完整，且**反向定义了服务端必须满足的契约**

这是 `images` 特性里**唯一闭环的一段**：

```python
# api/operation/implement.py:282-295
if task_schema.resource.image is not None and '/' in task_schema.resource.image:
    template = 'train_image:' + task_schema.resource.image.split('/')[-1]
    train_image = task_schema.resource.image
else:
    template = task_schema.resource.image or 'default'
    train_image = None
if (err_msg := await check_environment_get_err(train_image, template, user)) is not None:
    return fatal_response(err_msg)
```

```python
# api/operation/default.py:9-24
if train_image is None:
    ...  # 内建镜像：查 train_environment + 配额
else:
    if len(train_image.split('/')) != 3:
        return 'train_image 格式不正确, 请检查. 仅支持镜像 URL, 请参考 hfai client 文档.'
    valid_image_urls = await TrainImageSelector.a_find_user_group_image_urls(
        shared_group=user.shared_group, status='loaded')
    if train_image not in valid_image_urls:
        return f'用户所在的组 [{user.shared_group}] 不存在镜像 [{train_image}] 或镜像仍在加载, 请使用命令 `hfai images list` 检查'
```

由这段**确定性逻辑**可反推出 5 条硬契约（设计文档必须遵守）：

| # | 契约 | 来源 |
| --- | --- | --- |
| K1 | 自定义镜像 URL 必须是 `registry/shared_group/image` **恰好 3 段**（`image` 必须是 `name:tag`，**自身不含 `/`**） | `len(train_image.split('/')) != 3` |
| K2 | 三段拼接结果必须**逐字节等于** `registry + '/' + shared_group + '/' + image` | `a_find_user_group_image_urls` 的连接方式 |
| K3 | 可被任务使用的镜像，`status` 必须**精确等于** `'loaded'` | `status='loaded'` |
| K4 | 校验用的是**提交者自己的 `user.shared_group`** —— 跨组镜像天然不可见 | `shared_group=user.shared_group` |
| K5 | 报错文案已**把 `images list` 写进用户指引**（「请使用命令 `hfai images list` 检查」）→ `list` 必须能真实反映 `train_image` 的加载状态，否则该指引是误导 | 报错字符串 |

> **K5 的重要性**：任务提交失败时服务端让用户去跑 `images list` 自查；而当前 `list` 的 `user_images`
> 恒为空 → **用户按指引自查只会看到「没有镜像」，永远查不出原因**。这是一个**跨特性的可观测性断链**。
> （该推论已在 103 上**实测确认**，见 §9-9。）

> **K2 的推论（设计期必须遵守）**：K2 是**逐字节**比较，因此服务端**不得**对 `image` 做任何「友好归一化」
> （例如把 `demo` 自动补成 `demo:latest`）—— 一旦补了，用户在 `-i` 里写不带 tag 的 3 段 URL 就**永远匹配不上**。
> 这条推论已写入需求硬约束 HC-05 与设计 §4.1「实现修正 I6b」。

> **与上传通道的关系（本分支新增）**：K1–K5 校验的是**已登记行**；上传通道要保证「落盘 → 登记」写出来的
> `image_tar` / `image` / `path` / `shared_group` / `registry` 五列能满足 K1–K4，否则任务侧依旧拒绝。
> 因此 UP 组用例（用例 §4.11）与 E2E-09/10 必须**在上传成功后立刻提交一次任务**（而非只断言 200）。

### 4.4 数据面：`train_image` 表（schema 齐备、零行、零写入）

```sql
-- db_schemas/017.table_train_image.sql:1-35
create table if not exists public.train_image
(
    image_tar   varchar not null,
    image       varchar default ''::character varying not null,
    path        varchar default ''::character varying not null,
    shared_group varchar,
    registry    varchar default 'registry.high-flyer.cn'::character varying,
    status      varchar default 'processing'::character varying,
    task_id     integer default 0,
    created_at  timestamp not null default current_timestamp,
    updated_at  timestamp not null default current_timestamp
);
create unique index if not exists train_image_image_uindex on public.train_image (image_tar);   -- :24-25 仅 image_tar 唯一
-- + trigger_update_train_image_updated_at（before update → updated_at = current_timestamp）
```

列语义（DDL 注释逐字）：
`image_tar` 唯一键 · `image` 镜像名字 · `path` **镜像在 weka 上的路径**（`:18`）· `shared_group` 哪个 group 可以共享这个镜像 ·
`registry` 默认 `registry.high-flyer.cn` · `status` 默认 `processing` · `task_id` 关联任务。

**由 schema 读出的设计意图（强证据）**：

| 列 | 读出的意图 |
| --- | --- |
| `path` = 「镜像在 **weka** 上的路径」 | tar 位于**集群共享存储**，不是客户端本地上传的临时路径 → 服务端**必须校验路径落在配置的共享根下** |
| `status` 默认 `'processing'` + `task_id` + `update_status` 桩 | 加载是**异步任务**；`processing` → `loaded`/`failed` 由任务回报 |
| `registry` 有默认值 | 设计假定存在**内网 registry**（`registry.high-flyer.cn`）→ 加载动作的终点是「推送进 registry」 |
| `image_tar` **全局**唯一（不含 `shared_group`） | 同一个 tar 路径**全集群只能登记一次** → 两个组加载同一个 tar 会**唯一键冲突**（设计期必须裁决，见 I13） |
| 表**没有 `user_name` 列** | 归属单位是**组**而非人；与 `delete` docstring「用户也可以删除自己组内的其他用户的镜像」**一致**（组内共享、组内可删） |

**实测（§9-5/§9-6）**：`select * from train_image` → **0 rows**；
全仓检索 `TrainImageTable` 的写入调用 → **没有任何 INSERT/UPDATE 路径**。
即：**该表既无数据、也无代码能产生数据**。

> **上传通道如何改变这张表（本分支新增）**：`push` 成功后 API-15 的自动登记正是**第一个由用户操作
> 自然触发的写入路径**——它把「表零写入」从设计问题变成一条必须实现的流水线：
> `image_tar`（去重后的 key/落点）、`image`（`name:tag`）、`path`（`image_path` 下的落点）、
> `shared_group`（提交者组）、`registry`（默认值或配置）五列都要在登记时一次性写对（设计 §5.6 / ADR-I14）。

### 4.5 路径配置：没有 `IMAGE` 类型的单点定义

`FileType`（`conf/utils.py:23-35`）只有 `DATASET:25 / WORKSPACE:27 / ENV:29 / DOC:31 / PYPI:33 / WEBSITE:35`
—— **没有 `IMAGE`**；
`get_base_path()`（`cloud_storage/utils.py:445`）也没有镜像分支（分支见 `:464/:470/:478/:495/:500/:505`，
`file_type=image` 会落到 `:511-512` 的 `else: raise ClientException(f'非法文件类型 {file_type}')`）；
`one/one_etc/core.toml` 的 `[cloud.storage.service]`（`:110-120`）里没有 `image_path`。
**结论**：DDL 说 tar 在「weka 上」，但**「weka 的哪个根」在代码与配置里都没有定义** → 服务端无法做
「路径必须落在共享根内」的校验（安全需求 SEC 的落点因此缺失）。这是设计必须补的单点（对齐 env 特性 §3.1 的做法），
**也是上传通道的第一个前置条件**（详见 §4.7-1/§4.7-2）。

### 4.6 ⚠️ 运行时消费链路：**存在、且比控制面更完整，但缺一个关键脚本**

这是本次分析**最重要的发现**，也是审计完全没有覆盖的一段。自定义镜像一旦在 `train_image` 表里有
`status='loaded'` 的行，任务提交后**运行时会真的去用它**，链路如下：

```python
# ① launcher.py:59-61 —— 带缓存的镜像信息查询（同步读 train_image）
@cached(cache=Cache(maxsize=1024))
def get_image_info(image_name):
    # 在这里查询数据库，获取 image 的信息，这样的好处是，以后可以和议会的集成起来
    return TrainImageSelector.find_one(os.path.basename(image_name))

# ② launcher.py:144-147 —— 把镜像的「URL」与「weka 路径」注入 manager 的 env
if task.backend.startswith('train_image:'):
    train_image_info = get_image_info(task.backend[len('train_image:'):])
    env.append(get_env_var(key='HFAI_IMAGE',           value=train_image_info.image_url))  # registry/group/image:tag
    env.append(get_env_var(key='HFAI_IMAGE_WEKA_PATH', value=train_image_info.path))       # ← path 列，语义在此确定
```

```python
# ③ single_task_impl.py:79-81 —— 自定义镜像走 user_defined 分支
if self.task.backend.startswith('train_image:'):
    # 用户自定义镜像时, image URI 由 launcher 查数据后通过 env 指定给 manager, 此处无需处理
    return Munch(user_defined=True, image=os.environ.get('HFAI_IMAGE'), config={})

# ④ single_task_impl.py:325 —— 透传给 manager 的 pod schema
'link_hfai_image': train_environment.user_defined,
```

```python
# ⑤ experiment_manager/manager/init_manager.py:347-360 —— 每个计算 pod 多一个 initContainer「link」
if node_schema.link_hfai_image:
    # 使用从 launcher 传来的数据, 避免查询数据库
    envs = [get_env_var(key=ee, value=os.environ.get(ee)) for ee in ['HFAI_IMAGE', 'HFAI_IMAGE_WEKA_PATH']]
    init_containers = [client.V1Container(
        name=f'{CONTAINER_NAME}-load-image',
        image='registry.high-flyer.cn/google_containers/busybox:latest',
        image_pull_policy=CONF.try_get('manager.image_pull_policy', default='IfNotPresent'),
        env=envs,
        volume_mounts=volume_mounts + [client.V1VolumeMount(name='data-local', mount_path='/data_local')],
        command=['/bin/sh'],
        args=['/marsv2/scripts/link_hfai_image.sh'],       # ← 该文件全仓不存在
    )]
    volumes += [client.V1Volume(name='data-local', host_path=client.V1HostPathVolumeSource(path='/data_local'))]
```

**由此可读出 3 件事（R1/R3 是事实，R2 是强推断 —— 因为脚本本体不存在，只能由挂载与命名反推）**：

| # | 结论 | 证据 |
| --- | --- | --- |
| R1 | **`train_image.path` 的语义 = 「镜像在 weka 上的目录/文件位置」**，且它**不是 tar 包路径**，而是供 pod 侧「链接」用的镜像位置（`HFAI_IMAGE_WEKA_PATH`）。DDL 注释「镜像在 weka 上的路径」与此完全吻合 | ② 注入 `HFAI_IMAGE_WEKA_PATH = path`；⑤ 把它交给 link 脚本 |
| R2 | **自定义镜像的运行时可用性靠「按节点 link」而非「registry 拉取」**：每个计算 pod 起一个 busybox initContainer，把 weka 上的镜像「链接」进节点本地（宿主 `/data_local` 挂进 initContainer，说明链接的目标是节点本地的镜像存储目录）。这也解释了为何任务 pod 侧**完全不需要访问 registry** | ⑤ 的 `data-local` hostPath `/data_local` + 脚本名 `link_hfai_image` |
| R3 | ❌ **`marsv2/scripts/link_hfai_image.sh` 在本仓库中不存在**（`marsv2/scripts/` 下 11 个文件，无此文件；`one/hai-up.sh:289-301` 的 `storage`/mount_point 种子列表里**也没有**它；全仓 `grep link_hfai_image` 只命中 3 处引用、0 处定义）→ initContainer 会以 `sh: /marsv2/scripts/link_hfai_image.sh: not found` 失败，**pod 卡在 Init**，自定义镜像任务**根本起不来**（旧分支已实现该脚本，本工作树未并入） | `ls marsv2/scripts/`；`one/hai-up.sh:289-301`；`grep -rn link_hfai_image .` |

> **推论（本特性真正的复杂度所在）**：`images` 不是「补一个上传接口」那么简单。它是一条**四段式**链路，
> 各段在本仓库的完成度**依次递减**：
> 1. **控制面**（客户端 3 子命令 + 服务端 1 读接口 + DDL）：**部分存在**，`load/delete` 全断（§3、§4.1–4.4）；
> 2. **提交面**（`train_image` 校验 + `backend=train_image:<tag>` + `config_json.train_image`）：**完整**（§4.3）；
> 3. **运行面**（launcher 查表注入 env + 按节点 link initContainer）：**结构完整、关键脚本缺失**（本节 R3）；
> 4. **上传面**（本地 tar → RustFS/S3 → `image_path` → 自动登记）：**此前完全不存在**（本分支新增，§4.7）。
>
> 因此**只修控制面并不能让自定义镜像跑起来**：`status='loaded'` 造出来之后，任务仍会卡在 R3；
> 而**没有上传面，`status='loaded'` 这一行只能靠运维手工放盘代劳** —— 这是本分支新增上传通道的直接动因。
> 设计必须把「补 `link_hfai_image.sh` + 挂载种子 + 可用的 busybox 镜像」与「补上传通道」一并计入交付范围
> （见风险 I16/I17 与 §6.1 的 R-9~R-12）。

> **另一个运行面发现**：`train_environment` 侧**已有**一个同名机制可供对照 ——
> `validate_image.sh`（`marsv2/scripts/validate_image.sh`，**存在**且在 `one/hai-up.sh:295` 有挂载种子）
> 首行即 `[[ $MARSV2_TASK_BACKEND == train_image:* ]] || exit 0`，由
> `marsv2/entrypoints/system_scope.sh:16` 调用。即：**「自定义镜像的依赖校验」这一段已经实现并落地，
> 只有「把镜像 link 进节点」这一段缺失**。设计可**照抄 `validate_image.sh` 的落地方式**
> （文件放 `marsv2/scripts/` + 在 `one/hai-up.sh` 的 `storage` 里加一行种子），这是本方案成本最低、
> 最贴合仓库既有约定的落地路径。

> **103 环境注意**：`registry.high-flyer.cn/google_containers/busybox:latest` 是**内网地址**，
> 在 103 上不可达（§9-7）；而 `CONF.manager.image_pull_policy` 在 103 已被 `override.toml` 设为
> `IfNotPresent` → 若该镜像未被预拉到节点，initContainer 会 `ImagePullBackOff`。
> 设计需给出可配置的 busybox 镜像地址（见风险 I17）。

### 4.7 上传入口盘点（本分支的核心缺口）

> **本节是本分支相对旧分支版本的核心新增。** 所有行号均在当前工作树
> `feature/hai-cli-images-rustfs-design` @ `33a5b26` 上用 `grep -n` / `sed -n` **逐条复核**，
> 未沿用旧 images 分支文档的行号。**上传通道在旧分支 `feature/hai-cli-images-server-design` 上未实现**，
> 本分支实现（FR-16~FR-20 / 设计 §3.5·§4.6·§5.6·§6.4·§7.5 / ADR-I11~I14）。

**先给一句话**：既有 RustFS/S3 流水线（API-01 签发 STS → 客户端直传 → API-05 stage2 落盘 → API-06 轮询状态）
**本身是完整的、且已被 `workspace`/`env` 两个特性实机验证**；缺的不是流水线，而是**镜像类型没有接入这条流水线**
—— 从枚举成员、路径单点、stage2 白名单、DB 枚举到客户端命令，**每一层都少一个 `image` 分支**。

| # | `文件:行`（`33a5b26` 实测复核） | 现状 | 影响 | 目标改动 |
| --- | --- | --- | --- | --- |
| 1 | `conf/utils.py:23-35` | `class FileType(str, Enum)` 只有 `DATASET:25` / `WORKSPACE:27` / `ENV:29` / `DOC:31` / `PYPI:33` / `WEBSITE:35`，**没有 `IMAGE` 成员** | 「文件类型」是整条上传流水线的主键维度（STS 授权前缀、bucket 选择、stage2 分支、落库枚举全由它派生）；`image` 在类型层面不存在 ⇒ 下游每一层都无处落笔 | 新增 `IMAGE = 'image'`（FR-16；设计 §3.5） |
| 2 | `cloud_storage/utils.py:445`（`get_base_path`）；分支 `:464`(WORKSPACE)/`:470`(ENV)/`:478`(DATASET)/`:495`(DOC)/`:500`(PYPI)/`:505`(WEBSITE)；兜底 `:511-512` `else: raise ClientException('非法文件类型 {file_type}')` | **没有 IMAGE 分支**：`file_type=image` 必然落到兜底 `else` 抛「非法文件类型 image」 | 「tar 落到共享盘哪个根」**没有单点定义** ⇒ STS 授权前缀、stage2 落盘目录、任务侧 `path` 三处无法同源；也无法做「路径必须在镜像根内」的校验（I9 的根因） | 新增 IMAGE 分支：`image_path = CONF.cloud.storage.service.image_path`；`cluster_base_path = f'{image_path}/...'`；给出 `cloud_base_path`（去重 key）；并 `check_is_subpath(image_path, cluster_base_path)`（对照 `:469`/`:477`；设计 §3.5 / ADR-I11） |
| 3 | `cloud_storage/service/sync_to_cluster.py:55-56` | `submit_to_cluster` 的**白名单只放 `(FileType.WORKSPACE, FileType.ENV)`**，否则 `raise WorkspaceError(ErrorCode.INVALID_PARAM, f'不支持同步 {file_type} 类型')`（同文件 `:45-51` 是 env 专属的 `check_env_push_enabled` 闸门，说明「新类型加分支」是既有模式） | 即便 API-01 签发了 STS、客户端把对象传进了 bucket，**stage2（API-05）也会在入口直接 400** —— 对象永远落不到 `image_path` | 白名单加 `FileType.IMAGE`；如需灰度，照 `:45-51` 加 `check_image_push_enabled`（设计 §4.6 / §7.5） |
| 4 | `db_schemas/010.table_user_downloaded_files.sql:8` | `create type file_type as enum ('workspace', 'dataset', 'env', 'doc', 'pypi', 'website')` —— **无 `image`**；而 stage2 会在 `sync_to_cluster.py:103-104` 调 `user.aio_db.set_sync_status(file_type, ...)` 落库 | 上传状态落库必然报 PG 原生错 `invalid input value for enum file_type`，**且失败点在「对象已上传成功」之后** → 产生「对象在 bucket 里、状态表写不进、`image_path` 也没有」的半成品 | 新增 alter 型迁移（当前 `db_schemas/` 最大编号 `034` → 新 `035`；幂等风格参照 `db_schemas/032.table_host_flags.sql`）：`alter type file_type add value if not exists 'image'`（设计 §6.4 / ADR-I12） |
| 5 | `client/commands/hfai_image.py:12-17,33-76,79-90,92-99`；`client/api/image_api.py:6-13,16-25,27-35` | 客户端 `images` group 只有 `list`/`load`/`delete`，**没有 `push`**；API 层只有 `fetch_images`/`load_image_tar`/`delete_image_by_name`，**没有任何上传调用** | 「本地 tar → 对象存储」**在客户端没有入口** —— 这就是「上传入口缺失」最直观的表现。用户唯一能想到的动作是「手工 scp 到共享盘」，但共享盘对多数用户不可写/不可见 | 新增 `images push <本地 tar>` 子命令 + `push` API：**复用** `workspace`/`env` 的上传实现（`plugins/haiworkspace/haiworkspace/client/workspace_api.py::push` → `workspace_util.py:327 push_to_cluster`），只加 `file_type=image` 分支（FR-16/FR-19；设计 §6.4） |
| 6 | `client/commands/hfai_image.py:79-89`（`load_image`） | `load_image` 只做 `os.path.exists(image_tar)` → `abspath` → `load_image_tar`；docstring（`:82-84`）原文写「tar包应该在萤火二号上共享目录下的，外部用户需要先把 tar 包上传上来操作」 | `images load` **只接受共享盘上已存在的 tar**，本身不承担上传递送 —— 客户端语义层面就把「上传」外包给了用户；`push` 落地后 `load` 应降为**兼容/运维旁路** | `load` 保留（手工放盘后登记）；帮助文本与 README 把 `push` 标为主入口；`push` 成功后自动登记（Q-12；FR-18；设计 §6.4） |
| 7 | `cloud_storage/service/sts.py:26-53`（`issue_sts_token`）；`cloud_storage/utils.py:284-304`（`get_bucket_name`）；`api/register/implement.py:76`（API-01 路由） | `:33-34` 用 `get_base_path(user_name, shared_group, name, file_type, GROUP_SHARED)` 取 `cloud_base_path`，`:44` 把它作为 `cloud_api.get_access_token(bucket_name, cloud_base_path, ttl)` 的**授权前缀**；bucket 由 `get_bucket_name` 决定（非 public 类型统一落 `CONF.cloud.storage.private_bucket`，`:303`） | STS 的授权面**完全由 `get_base_path` + `file_type` 决定**：没有 `FileType.IMAGE` ⇒ 要么签不出镜像前缀的凭证，要么只能签成 workspace/env 前缀（**拿错前缀的凭证去写镜像 = 越权面 + 落点错误**） | 第 1/2 项落地后，**API-01 零改动**即获得「只允许写 `image` 前缀」的 STS；bucket 可复用 `private_bucket`（设计 §3.5/§4.6；ADR-I11） |
| 8 | `plugins/haiworkspace/haiworkspace/client/workspace_api.py:108-155`；`client/command.py:48-60`；对照 `cloud_storage/utils.py:464-469` | `push(...)` 已有 `file_type: str = FileType.WORKSPACE` 形参（`:110`），`elif file_type == FileType.ENV`（`:125-131`）就是 env 做过的**同型适配**；`else: print('不支持的file_type: {file_type}'); return False`（`:132-134`）。**但**默认 `FileType.WORKSPACE` 的落点是 `{workspace_path}/{group}/{username}/workspaces/{name}`（`cloud_storage/utils.py:464-469`），**不在 `image_path` 之下** | 直接把 tar 当 workspace `push`：stage2 会落在 workspace 根；补齐 `image_path` 校验后，`check_is_subpath(image_path, cluster_base_path)` 会判 **`PATH_ESCAPE`**（错误码定义 `cloud_storage/service/errors.py:11-16`；现有用法 `sync_to_cluster.py:127-129`）→「传上去了但登记不了/任务用不了」的割裂体验（R-12） | 照 env 的方式在 `workspace_api.push` 加 `elif file_type == FileType.IMAGE`，把 `provider/local_path/remote_path/name` 指向 tar；镜像专属命令 `images push` 走**同一实现**（FR-16/FR-19；设计 §5.6/§6.4；ADR-I14） |

**结论（补齐方式）**：缺口**不是**「缺一条新的上传流水线」，而是「**既有流水线少一个 `file_type=image` 分支**」：

1. **复用**：API-01（`api/register/implement.py:76`）/ API-05（`:79`）/ API-06（`:80`）三条路由、STS 签发、
   客户端分片/断点/校验、stage2 落盘与状态机，**一行不改**即可被镜像复用 —— 这正是 `env` 已经验证过的做法
   （`FileType.ENV` 走的就是同一套：`sync_to_cluster.py:45-51` 的 env 闸门 + `workspace_api.py:125-131` 的 env 分支）；
2. **只加一个分支**：`conf/utils.py` 加 `IMAGE`（§4.7-1）→ `get_base_path` 加 IMAGE 分支并定义 `image_path`（§4.7-2）
   → `sync_to_cluster` 白名单加 `IMAGE`（§4.7-3）→ DDL 枚举补 `image`（§4.7-4）→ 客户端加 `push`（§4.7-5/8）
   → 落盘成功后 API-15 自动登记（§4.4、§4.6 的运行面消费由此第一次有真实数据）；
3. **编号沿用**：需求 **FR-16~FR-20**、设计 **§3.5 / §4.6 / §5.6 / §6.4 / §7.5 / §9.5**、决策 **ADR-I11~I14**、
   用例 **§4.11 UP 组 + E2E-09/10**、Checklist **阶段 17**（这些编号沿用 `images-server-*.md` 文档集，本文不另立编号）；
4. **可观测性**：`push` 的成功/失败必须能区分「上传失败」与「上传成功、登记失败」两种错误面（FR-20 / R-9），
   否则用户看到的现象与 K5 指引一样无法自查。

---

## 5. 端到端链路判定矩阵

| # | 场景 | 客户端 | 服务端 | 判定 |
| --- | --- | --- | --- | --- |
| S1 | `hai-cli images list`（看内建镜像） | ✅ | ✅ | ✅ **可用**（§9-1） |
| S2 | `hai-cli images list`（看用户镜像） | ✅ 渲染 | ❌ `user_images` 恒 `[]` | ❌ **永远为空**（§9-2） |
| S3 | `hai-cli images load <tar>` | ❌ `AttributeError` | ❌ 无路由 | ❌ **两端皆断**（§9-3） |
| S4 | `hai-cli images delete <image>` | ❌ `AttributeError` | ❌ 无路由 | ❌ **两端皆断**（§9-4） |
| S5 | 加载任务回报状态 | ❌ 无调用方 | ❌ 无路由/无桩实现 | ❌ **不存在** |
| S6 | 提交任务使用自定义镜像 `-i registry/g/i:t` | ✅ 发送 | ✅ 校验逻辑完整（K1–K4） | ⚠️ **逻辑可用但永不通过**：表里不可能有 `status='loaded'` 的行 |
| S7 | 按服务端指引自查（报错让用户跑 `images list`） | ✅ | ❌ 空列表 | ❌ **指引误导**（K5） |
| S8 | 删除后释放 registry / 共享盘空间 | ❌ | ❌ | ❌ **不存在**（`delete` 语义未定义） |
| S9 | 任务运行时取镜像信息（launcher 查表 → 注入 env） | —— | ✅ 代码完整 | ⚠️ **仅在表里有行时触发**；因 S3/I4，永不触发 |
| S10 | 计算 pod 启动时把自定义镜像 link 进节点 | —— | ❌ **脚本缺失** + 节点无 `/data_local` + busybox 镜像参照不匹配 | ❌ **即使 S6 通过也必失败**（§4.6 R3、§9-8） |
| S11 | **`hai-cli images push <本地 tar>` → RustFS/S3 → `image_path` → API-15 登记 → 任务侧 K1–K5 校验 → pod link** | 🔨 **本次新增** | 🔨 **本次新增**（复用 API-01/API-05/API-06 + API-15 登记） | 🔨 **上传链路在旧分支未实现，本分支实现**；它是 S6→S9→S10 的「水源」，闭环后 S6 才第一次可能通过 |

> **判定**：**S1 是当前唯一完全可用的场景**。S6 是「管道通了但没有水源」——任务侧代码正确，
> 却因为没有任何写入路径而**永远命中「镜像仍在加载」的错误分支**。S9/S10 进一步说明：
> **即使补上写入路径让 S6 通过，运行面仍会因 I16/I17 失败** —— 因此本特性的 DoD 不能只写
> 「`images list/load/delete` 可用」，必须写「**端到端能跑通一个自定义镜像任务**」（需求 AC-01）。

> **上传链路（S11）补充判定（本分支新增）**：S11 是**唯一能把 S6 从「永不通过」变成「可能通过」的入口**。
> 它在旧分支 `feature/hai-cli-images-server-design` 上**未实现**（旧分支的 103 实测用「手工放盘」造数据，
> 见 §9 及其被测 tag `f2cb559`），本分支实现。S11 的内部还要分三段独立判定：
> **① 上传段**（`push` → 对象存储，判据 API-06 终态 `FINISHED`）；
> **② 落盘段**（stage2 → `image_path`，判据文件真实存在于任务可见的共享盘路径上）；
> **③ 登记段**（API-15 → `train_image` 出现 `status='loaded'` 且 K1–K4 满足，判据是紧接着提交一次任务并成功）。
> 只有三段全绿，S11 才算通过（用例 §4.11 UP 组 + E2E-09/10）。

---

## 6. 风险清单

> ID 命名空间：本文用 **`I1`–`I19`**（`I` = Image），与审计的 `C-x`（客户端缺陷）/ `S-x`（服务端缺陷）、
> `env` 分析的 `E1`–`E13`、workspace 的 `F1`–`F10` **均不共享编号**。`C-3` 为审计既有 ID，本文沿用不改号。
> `I19` 为旧分支运行面实测暴露的新风险（见下），本分支沿用该 ID，不另造号。

| ID | 等级 | 风险 | 证据 | 建议 |
| --- | --- | --- | --- | --- |
| **I1** | 高 | **`async_load` / `async_delete` 在客户端与接口层双重缺失**（= 审计 C-3），两个子命令 100% 不可用 | `client/api/image_api.py:23,34`；`client/model/user_impl/default.py:5-8`；`base_model/base_user_modules/default.py:20-22`；§9-3/§9-4 | 需求 FR-01；设计 §6.1（补接口 + 实现 + URL） |
| **I2** | 高 | **服务端 `user_images` 硬编码 `[]`**，`TrainImageTable` 的读路径（`a_find_user_group_images`）**全仓无调用方** → 列表页永远是空的 | `server_model/user_impl/user_image/default.py:16`；`train_image_selector.py:44-47` | 需求 FR-02；设计 §5.2 |
| **I3** | 高 | **`load`/`delete` 无服务端路由**（实测 404），4 个具名桩全部未注册 | `api/resource/image/default.py:3-16`；`api/register/implement.py:71`；§9-2 | 需求 FR-03/FR-05；设计 §4（API-15/API-18） |
| **I4** | 高 | **`train_image` 表零写入路径**（无任何 INSERT/UPDATE），`status` 永远到不了 `'loaded'` → 任务侧 S6 永远失败 | 全仓检索 `TrainImageTable` 写调用为空；§9-6 | 需求 FR-03；设计 §5.2/§5.3；**本分支补上传入口后由 API-15 产生第一次真实写入（§4.7）** |
| **I5** | 高 | **状态机未定义**：`status` 无枚举、无合法值集合、无迁移规则。但客户端用**子串** `'deleted'` 过滤、任务侧用**精确** `'loaded'` 白名单，两头口径不一致 | 客户端 `hfai_image.py:69`；任务侧 `api/operation/default.py:20`；DDL 默认 `'processing'` | 需求 FR-04；设计 §7.3（状态机表） |
| **I6** | 高 | **`image` 名字（`name:tag`）的来源未定义**：客户端 `load` 只传 tar 路径，服务端无从得知镜像名；而任务侧要求 `image` **不含 `/`** 且与 `registry`/`shared_group` 拼成 3 段 | `hfai_image.py:79-89`；`api/operation/default.py:17-20` | 需求 FR-03/FR-07；设计 §4.1（入参含 `image`）、§6.2；**`push` 的入参必须同时携带「本地 tar」与「镜像名」（§4.7-5）** |
| **I7** | 中 | **去重方向自相矛盾**：客户端取「首次见到」为准，而数据源按 `updated_at` **升序** → 实际以**最旧**为基准，与注释「以最新的为准」相反 | `hfai_image.py:64-68`；`train_image_selector.py:46` | 设计 §5.2（服务端改 `DESC`）+ 用例 TC-C0x 断言顺序 |
| **I8** | 中 | **`user_images` 序列化风险**：`a_find_user_group_images` 用 `df.to_dict('records')` 返回 `task_id` 等 **numpy 标量**，FastAPI `jsonable_encoder` **无法编码 `np.int64`** → 一旦按 I2 接通就会 500 | `train_image_selector.py:47`；对照 `user_image/implement.py:17` 显式 `int(...)` 的既有规避 | 设计 §5.2（出口归一化）；用例 TC-A0x |
| **I9** | 中 | **路径无单点定义、无校验**：无 `FileType.IMAGE`/`image_path`，服务端无法校验 tar「落在共享根内」→ 任意路径（含越权读）可被登记 | `conf/utils.py:23-35`；`cloud_storage/utils.py:445,464-512`；`one/one_etc/core.toml:110-120` | 需求 SEC-01/HC-0x；设计 §3（路径约定）+ `check_is_subpath` 复用；**§4.7-1/§4.7-2 即本分支的落地项** |
| **I10** | 中 | **失败路径的用户体验是 Python 栈**：`load`/`delete` 走 `async_requests` 默认 `assert_success=[1]`，`success=0` 时**抛异常**，`print(result['msg'])` 永不执行 → 用户看到 `Exception('请求失败: ...')` 而非友好提示 | `api_utils.py:100-104`；`image_api.py:24-25`；§9-3/§9-4 栈形态 | 设计 §6.2（`allow_unsuccess`/捕获后打印 `msg`）；**`push` 必须区分「上传失败」与「登记失败」（FR-20 / R-9）** |
| **I11** | 高 | **103 环境无内网 registry**（无 registry pod/svc），DDL 默认 `registry.high-flyer.cn` 不可达 → 「推送进 registry」的数据面**在当前环境不可验证** | §9-7 实测；`db_schemas/017.table_train_image.sql:7` | 需求 Q-1/CMP-0x；设计 §13 ADR-I2（loader 后端可选）+ 部署 registry 或 `node_local` 模式；**上传通道不依赖 registry（tar → 共享盘），与本条正交** |
| **I12** | 中 | **`load` 是变更型调用但客户端默认只重试 1 次、服务端无幂等键设计**：一旦补上重试（如 list 的 `retries=3`）就会重复建表/重复建任务 | `image_api.py:23` vs `:12`（`retries=3`） | 需求 FR-06/NFR-0x；设计 §5.2（按 `image_tar` 幂等 upsert）；**`push` 的「重传」同样需要幂等键（ADR-I13）** |
| **I13** | 中 | **`image_tar` 唯一索引不含 `shared_group`** → 两个组加载同一个共享盘 tar 会**唯一键冲突**；且 `delete` 的软删语义（是否让出唯一键）未定义 | `db_schemas/017.table_train_image.sql:24-25` | 需求 Q-2；设计 §7.2（upsert 冲突处理）；**上传通道的「已在集群」判定也依赖这个唯一键（ADR-I13）** |
| **I14** | 中 | **跨组越权面**：`load`/`delete` 的用户入参是镜像名，`delete` 按 docstring 允许删本组他人镜像；若服务端不校验 URL 中的 `shared_group == user.shared_group`，可跨组删除/登记 | `hfai_image.py:92-99`；DDL 无 `user_name` 列 | 需求 SEC-02；设计 §10 |
| **I15** | 低 | **空间回收缺失**：`delete` 后 registry tag、共享盘 tar、镜像缓存均不处理，`-a` 只隐藏行不释放空间，与 docstring「以释放空间」不符 | `hfai_image.py:92-99` 文案 | 需求 FR-05/非目标；设计 §7.3(P2)；**直接放大 R-10（共享盘被吃满）** |
| **I16** | **高** | **运行期脚本缺失**：`marsv2/scripts/link_hfai_image.sh` 全仓 0 处定义（3 处引用），`one/hai-up.sh:289-301` 的 `storage` 挂载种子里也没有它 → 自定义镜像任务的 initContainer 报 `not found`，**pod 卡在 Init**。**这是「只修控制面也跑不起来」的根因**（旧分支已实现，本工作树未并入） | `init_manager.py:358`；`ls marsv2/scripts/`；`one/hai-up.sh:289-301`；`grep -rn link_hfai_image .` | 需求 FR-08；设计 §7（运行面）+ §5.4（脚本契约） |
| **I17** | 高 | **运行期节点前置不满足**（103 实测）：① `/data_local` 在节点上**不存在**，而 hostPath 未指定 `type` → kubelet 不会创建，**挂载失败**；② 节点上只有 `docker.io/library/busybox:latest`，而 initContainer 要 `registry.high-flyer.cn/google_containers/busybox:latest`（**不同引用 → 触发拉取**），该域名被解析到 `198.18.0.77`（代理/保留段地址，实际不可达）→ `ImagePullBackOff` | §9-8 实测；`init_manager.py:351,360` | 需求 FR-08/CMP-0x；设计 §9.1（可配置镜像地址）、§13 ADR-I4 |
| **I18** | 中 | **`path` 列语义未文档化、且与 tar 路径混淆**：运行期把 `path` 当 `HFAI_IMAGE_WEKA_PATH` 喂给 link 脚本（= **镜像在 weka 上的位置**），而 `images load` 的入参是 **tar 包路径**。两者是不同概念，若不区分，控制面会写错 `path`，运行期 link 必然失败 | `launcher.py:147`；DDL 注释 `path` =「镜像在 weka 上的路径」（`db_schemas/017.table_train_image.sql:18`） | 需求 FR-07；设计 §3（路径/概念单点）；**上传通道的落点与 `path` 必须同源（R-12）** |
| **I19** | **高** | **长 initContainer 被 unschedulable 看门狗打断**：首个 ~1 GB tar 的节点侧导入耗时 >1 min，而 `manager.unschedulable_timeout_Ms=1`（60s）；`check_unschedulable` 把「Pending + `Initialized=False`」判成 BUILDING → `STOP_CODE.UNSCHEDULABLE(33)`，**任务链每 ~110s 被重启一次**，直到某个 incarnation 的导入恰好跑完。E2E 首轮 task 25→36 才 `succeeded`（AC-01），属「靠重启碰巧跑通」 | `experiment_manager/manager/check_unschedulable.py:55-68,82`；旧分支证据见 `images-server-test-report.md`（被测 tag `f2cb559`，task 25..36） | 修复：`check_unschedulable` 显式放行「initContainer 正在运行」的 pod（那是正在导入镜像，不是调度不出去）；已提交旧分支 `5f844b4`，**尚无复测记录**；E2E-09/10 断言「任务链零重启」（需求 FR-08；Checklist 阶段 17） |

### 6.1 上传入口风险（本分支新增；编号沿用设计文档 §15 的 `R-9`~`R-12`，**不并入 I 系列**）

> 上传通道的风险在设计文档里已有编号（设计 §15 的 `R-9`~`R-12`），本节**沿用设计编号**、不新造 `I-ID`，
> 只做「证据 → 影响 → 处置」的本分支落地映射。

| 编号（沿用设计 §15） | 风险 | 在本工作树/本分支的具体表现 | 处置落点 |
| --- | --- | --- | --- |
| **R-9** | **大 tar 失败难定位** | 镜像 tar 动辄 GB 级；「客户端上传失败」「上传成功但 stage2 落盘失败」「落盘成功但 API-15 登记失败」三种失败面在客户端都表现为一次非零退出，用户无法区分，而三者处置完全不同 | FR-20 要求客户端分别呈现（API-06 状态 + API-15 登记结果）；`image_api.py` 上传调用不得沿用 `assert_success` 裸断言（I10 的同型修正）；用例 UP 组覆盖三种失败 |
| **R-10** | **共享盘容量被吃满** | `image_path` 是集群共享盘上的镜像根；`delete` 目前不真正回收（I15），`push` 会把大 tar 持续堆进该根 → 一旦吃满，影响的是**同共享盘上的所有特性**（workspace/env） | 设计 §3.5 / §9.5：登记前做容量/配额校验；`delete` 真释放（FR-05 的 P2 项）；Checklist 阶段 17 增加容量检查项 |
| **R-11** | **枚举迁移** | 第 4 项：`file_type` 是 PG enum，补 `image` 需要 `alter type ... add value`；而本仓库 DB 迁移机制是**每次容器启动按文件名顺序全量重放 `db_schemas/*.sql`**，任何非幂等写法都会在第二次启动炸掉 | 设计 §6.4 / ADR-I12：新增 `db_schemas/035`（当前最大 `034`），用幂等 alter（参照 `032.table_host_flags.sql`）；`alter type ... add value if not exists 'image'` 的事务边界需在设计里定死 |
| **R-12** | **落点与 `image_path` 不同源** | 第 8 项：`workspace push` 默认落点在 workspace 根，直接复用会把 tar 放到 `image_path` 之外；补齐校验后会以 `PATH_ESCAPE` 失败（`cloud_storage/service/errors.py:11-16`），形成「传上去了但登记不了」 | 设计 §3.5 / §5.6 / ADR-I14：`image_path` 单点定义后，**STS 授权前缀 / stage2 落盘目录 / 客户端展示的落点 / `train_image.path` 四者同源**（HC-13）；`workspace_api.push` 加 `file_type=image` 分支取自同一 `get_base_path` |

---

## 7. 与同目录既有文档的差异与补充

> **本分支口径**：凡旧分支文档把「控制面/运行面」写成待开工、或把「手工放盘 + `load`」当作唯一入口的表述，
> 本文一律按本分支结论校正：**控制面 + 运行面 = 旧分支已实现并 103 实测通过的既有结论（本工作树尚未并入）；
> 上传通道 = 本分支必须交付的主入口（此前未实现、未盘点）**。

| 项 | 审计/既有文档的结论 | 本文的补充 |
| --- | --- | --- |
| 客户端缺陷 | 审计 §3.4/§8 记录了 **C-3**（P0，`AttributeError`），并归入「修客户端硬 bug（成本最低）」 | ✅ 一致，本文沿用 C-3；**新增** I10（失败提示形态）、I6（镜像名来源缺口） |
| 服务端缺陷 | 审计 **无任何 images 的 `S-x`**；§4.3 把 4 个 `hfai_image_*` 归为**类别 B 死桩** | ⚠️ **补充 S 侧结论**：除「桩」之外，**真正的问题是 `user_images` 硬编码 `[]`（I2）与表零写入（I4）**；桩只是表象，且这 4 个桩正是要实现的目标接口 |
| `user_images` | 审计全文**未出现** `user_images` | 本文首次定位到硬编码空列表（`user_image/default.py:16`）——**这是 `images list` 永远为空的第一因** |
| 缺口矩阵 | 审计 §5 的 6 行缺口**不含 images**（因为 `load/delete` 是客户端 AttributeError，不表现为「调用缺路由」） | ⚠️ **本文认为这是矩阵的一个盲区**：`images load/delete` 一旦修好客户端，就会立刻退化成「客户端会调用但服务端未注册」的**第 7 行缺口**。建议审计 §5 增行 |
| 任务侧 | 审计 §4.2 未单列 train_image 校验 | 本文 §4.3 把该校验**反向提炼为 5 条硬契约 K1–K5**，作为设计输入 |
| **运行面** | ❌ **审计完全未覆盖**：既没有 `link_hfai_image`、也没有 `HFAI_IMAGE*`、也没有 launcher 查表注入 env 这段 | ⚠️ **旧分支版本的本文最大补充**：§4.6 完整还原「launcher → manager env → 按节点 link initContainer」链路，并发现 **`link_hfai_image.sh` 缺失（I16）**。这直接改变了工作量判断：**不是「补 2 个接口」，而是「补控制面 + 补一个运行期脚本 + 修节点前置」** |
| **上传入口** | ❌ **审计与旧分支版本的本节都完全未盘点**：没有出现 `images push`、`FileType.IMAGE`、`image_path`、`file_type=image` 缺口，旧版本文档默认「tar 已经在共享盘上」 | ⚠️ **本分支版本的最大补充**：新增 **§4.7 上传入口盘点**，逐条给出 8 处缺口的 `文件:行` 证据与目标改动，并明确「复用既有 RustFS/S3 流水线、只加 `file_type=image` 分支」的补齐方式；§5 增行 **S11**；§6.1 引用设计 §15 的 R-9~R-12 |
| 上传通道 vs 手工放盘 | 旧分支口径：`images load` 的 docstring 要求「tar 包应该在萤火二号上共享目录下的」 | 🔄 **校正**：`push` 为**唯一上传主入口**（`push` 成功 → 自动登记），手工放盘 + `load` 降为**兼容/运维旁路**；两条链路都保留，但**默认路径与文档口径以 `push` 为先**（决策 Q-9/Q-12） |
| 状态标签 | 旧分支版本可能出现「P1 上传通道 = 可选、未开工」的写法 | 🔄 **校正**：`P0`/`P1` 只标注**来源与阶段**（P0 = 控制面 + 运行面，P1 = 上传通道），**不代表可选项**；上传通道是本分支必须交付的主入口（本文档集贯穿此约定） |

---

## 8. 证据索引

> 下列行号**均已在当前工作树 `feature/hai-cli-images-rustfs-design` @ `33a5b26` 复核**；
> §9 的实测记录保留旧分支证据来源（被测 tag `f2cb559`），本分支尚未重跑。

**代码 —— 客户端：**
- 客户端命令：`client/commands/hfai_image.py:12-17,20-30,33-76,79-90,92-99`；`client/hfai_cli.py:12,65`；`client/commands/utils.py:67`
- 客户端业务/模块/接口：`client/api/image_api.py:6-13,16-25,27-35`；`client/model/user_impl/default.py:5-8`；`base_model/base_user_modules/default.py:20-22`
- 客户端传输层：`client/api/api_utils.py:63-136`（`assert_success` 断言在 `:100-104`）；`client/api/api_config.py`

**代码 —— 服务端：**
- 服务端路由/接入：`api/register/implement.py:71`（images list）；`api/query/optimized/resource.py:14-19`
- 服务端领域层：`server_model/user_impl/user_image/default.py:12-17`；`server_model/user_impl/user_image/implement.py:10-19`
- 服务端 selector：`server_model/selector/train_image_selector.py:26-31,33-41,44-47,50-56`
- 服务端数据表：`server_model/user_data/table_config.py:73-86`；`db_schemas/017.table_train_image.sql:1-35`
- 服务端桩：`api/resource/image/default.py:3-16`；`api/resource/image/implement.py`
- 任务侧校验：`api/operation/implement.py:282-295`；`api/operation/default.py:9-24`
- 路径/配置：`conf/utils.py:23-35`；`cloud_storage/utils.py:445,464-512`（`get_base_path`）、`:284-304`（`get_bucket_name`）；`one/one_etc/core.toml:110-120`
- DB 访问硬约束（设计必须遵守）：`server_model/user_impl/aio_user_db/default.py:20-28`（禁止 `%s::type`；字面 `%` 写 `%%`；参数只能 tuple + 枚举 `.value`）
- **运行时消费链路（§4.6）**：`launcher.py:59-61`（`get_image_info` 缓存查表）、`launcher.py:144-147`（注入 `HFAI_IMAGE` / `HFAI_IMAGE_WEKA_PATH`）、`server_model/task_impl/single_task_impl.py:79-81`（`user_defined` 分支）、`:325`（`link_hfai_image`）、`experiment_manager/manager/init_manager.py:347-360`（busybox initContainer + `/data_local` hostPath + `link_hfai_image.sh`）
- **运行期脚本现状**：`marsv2/scripts/` 共 11 个文件、**无 `link_hfai_image.sh`**（但有可对照的 `validate_image.sh`，由 `marsv2/entrypoints/system_scope.sh:16` 调用）；挂载种子在 `one/hai-up.sh:289-301`（`storage` 记录中无 link 脚本）
- **上传链路（§4.7，本分支新增，行号已复核）**：
  - `conf/utils.py:23-35`（`FileType` 无 `IMAGE`）
  - `cloud_storage/utils.py:445`（`get_base_path`）、`:464-469`（WORKSPACE 落点）、`:470-477`（ENV 落点）、`:478-494`（DATASET）、`:495-499`（DOC）、`:500-504`（PYPI）、`:505-509`（WEBSITE）、`:511-512`（兜底 `else: raise ClientException('非法文件类型')`）
  - `cloud_storage/service/sync_to_cluster.py:40-56`（`submit_to_cluster`；`:45-51` env 闸门、`:55-56` 白名单）、`:65-66`（`get_base_path`）、`:103-104`（`set_sync_status` 落库）、`:107-108`（`makedirs(cluster_base_path)`）、`:127-129`（`check_is_subpath` → `PATH_ESCAPE`）
  - `db_schemas/010.table_user_downloaded_files.sql:8`（`file_type` 枚举无 `image`）；`db_schemas/` 当前最大编号 `034`；幂等 alter 范例 `db_schemas/032.table_host_flags.sql`
  - `cloud_storage/service/sts.py:26-53`（`issue_sts_token`；`:33-34` 取 `cloud_base_path`、`:38` bucket、`:44` 授权前缀）
  - `api/register/implement.py:76,79,80`（API-01 `/ugc/get_sts_token`、API-05 `/ugc/sync_to_cluster`、API-06 `/ugc/sync_to_cluster/status`）
  - `plugins/haiworkspace/haiworkspace/client/workspace_api.py:108-110`（`push` 的 `file_type` 形参）、`:119-124`（WORKSPACE 分支）、`:125-131`（ENV 分支 = 同型适配范例）、`:132-134`（`else` 拒绝）、`:140`（`file_type` 透传）；`plugins/haiworkspace/haiworkspace/client/command.py:48-60`（`--file_type` 选项）；`plugins/haiworkspace/haiworkspace/client/workspace_util.py:327`（`push_to_cluster`）、`:129-135`（`get_sts_token` 客户端）
  - `cloud_storage/service/errors.py:11-16`（`ErrorCode.PATH_ESCAPE`）

**数据表：**
- `server_model/user_data/table_config.py:73-86`（`TrainEnvironmentTable:73-77`、`TrainImageTable:81-86`）；`db_schemas/009.table_train_environment.sql`、`db_schemas/017.table_train_image.sql`
- 迁移机制：`deploy/dbs/files/init_postgresql.sh`（每次容器启动按文件名顺序全量重放 `db_schemas/*.sql`）；`db_schemas/` 当前最大编号 `034`；alter 型迁移范例 `db_schemas/032.table_host_flags.sql`（`add column if not exists`）
- 数据填充：`one/hai-up.sh:321`（`train_environment:<env>` 配额种子）、`:340-345`（`train_environment` 行种子）、`:287-301`（`storage`/mount_point 种子）

**103 实测：** 见 §9（原始命令与输出；证据来自旧分支部署，被测 tag `f2cb559`）。

---

## 9. 103 真实环境实测记录

> **证据来源与时点**：本节全部命令与输出来自**旧分支部署**（`feature/hai-cli-images-server-design`，被测 tag `f2cb559`），
> **本分支（`feature/hai-cli-images-rustfs-design`）尚未重跑**。原样保留以便复核与对照。
>
> 环境：`fireflyer@192.168.100.103`，MicroK8s v1.21.13 + containerd 1.4.13，4 节点（`k8s-master` invalid、
> `k8s-slave01/03` training、`k8s-slave02` jupyter_cpu），`hai-platform-0` 1/1 Running。
> 客户端 `hai-cli` 位于 `/usr/local/bin`，身份必须为 `fireflyer`（`/home/fireflyer/.hfai/conf.yml`，`url = http://10.205.52.200`）。

**1) `images --help`（命令面确认）**
```
Usage: hai-cli images COMMAND <argument>... [OPTIONS]
  用户自定义镜像的管理接口
Task Manage Cmds:
  list  列举用户组在萤火二号上的镜像列表，以及镜像在萤火二号上的状态
Other Commands:
  delete  删除萤火二号上的镜像，以释放空间 ...
  load    加载镜像 tar...
```

**2) `images list`（S1 ✅ / S2 ❌）**
```
萤火二号内建镜像
| image          | default_python | cuda    | supported_hf_e… | environments   |
| hai_base(defa… | /usr/bin/pyth… | unknown |                 | WS_URL=ws://h… |
用户自定义镜像
| image | status | shared_group | image_tar | updated_at |      ← 0 行
```

**3) 直连真实接口（服务端契约快照）**
```
$ curl -s -X POST "http://10.205.52.200/ugc/user/train_image/list?token=<token>"
{"success":1,"result":{
  "mars_images":[{"env_name":"hai_base",
                  "image":"registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:7589fb1",
                  "schema_template":"",
                  "config":{"python":"/usr/bin/python3.8","environments":{...}},
                  "quota":1}],
  "user_images":[]                       ← I2 实证
}}

$ curl -s -X POST ".../ugc/user/train_image/load?token=<token>"    → {"success":0,"msg":"Not Found"}   [HTTP 404]  ← I3
$ curl -s -X POST ".../ugc/user/train_image/delete?token=<token>"  → {"success":0,"msg":"Not Found"}   [HTTP 404]  ← I3
$ curl -s -X POST ".../query/user/quota/list?token=<token>"        → {"train_environments":["hai_base"], ...}  [HTTP 200]
```

**4) `images load`（S3 ❌，客户端即断）**
```
$ touch /tmp/fake-image.tar && sudo -u fireflyer hai-cli images load /tmp/fake-image.tar
  ...
  File ".../hfai/client/commands/hfai_image.py", line 87, in load_image
    await load_image_tar(abs_image_tar)
  File ".../hfai/client/api/image_api.py", line 23, in load_image_tar
    result = await user.image.async_load(tar)
AttributeError: 'UserImage' object has no attribute 'async_load'
```

**5) `images delete`（S4 ❌）**
```
$ sudo -u fireflyer hai-cli images delete registry.high-flyer.cn/haiadmin/foo:1
  File ".../hfai/client/api/image_api.py", line 34, in delete_image_by_name
    result = await user.image.async_delete(image_name)
AttributeError: 'UserImage' object has no attribute 'async_delete'. Did you mean: 'async_get'?
```

**6) 数据库实测（I4 实证：表存在但零行）**
```
$ kubectl -n hai-platform exec hai-platform-0 -- psql -U root -d mars_db -c "select * from train_image;"
 image_tar | image | path | shared_group | registry | status | task_id | created_at | updated_at
-----------+-------+------+--------------+----------+--------+---------+------------+------------
(0 rows)

$ ... -c "select env_name, image, config from train_environment;"
 hai_base | registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:7589fb1 | {"python": "/usr/bin/python3.8", ...}
(1 row)
```

**7) 无内网 registry（I11 实证）**
```
$ kubectl get svc -A | grep -iE "registry|5000"      → （空）
$ kubectl get pods -A                                → 无 registry 工作负载
$ command -v docker ctr crictl                        → /usr/bin/docker · /usr/bin/ctr · /usr/bin/crictl
                                                        （无 skopeo / buildah / podman / nerdctl）
$ kubectl get nodes -o wide                           → 4 节点，CONTAINER-RUNTIME 均为 containerd://1.4.13
```

> **数据面可行性（设计输入）**：103 上各节点均有 `containerd` + `ctr`，**但没有 registry**。
> 因此「`docker load` → `docker push` 到内网 registry」这条生产路径在本环境**无法端到端验证**；
> 任何可验证的方案必须包含一个**不需要 registry 的加载后端**（详见设计 §13 ADR-I2）。
> **上传通道与之正交**：`images push` 只把 tar 送到共享盘（RustFS/S3 → `image_path`），不需要 registry。

**8) 运行面节点前置实测（I16 / I17 实证）**

103（`fireflyer-0003`）是 **Multipass 宿主**（`mpqemubr0` = `10.205.52.1/24`），4 个 k8s 节点是它上面的
Multipass VM（`k8s-master`/`k8s-slave01..03`）。因此**节点内的探测必须走 `multipass exec`**：

```
$ multipass list
Name          State    IPv4
k8s-master    Running  10.205.52.154
k8s-slave01   Running  10.205.52.222
k8s-slave02   Running  10.205.52.16
k8s-slave03   Running  10.205.52.213

$ multipass exec k8s-slave01 -- ls -ld /data_local
ls: cannot access '/data_local': No such file or directory          ← I17①：link initContainer 的 hostPath 目标不存在
                                                                      （V1HostPathVolumeSource 未指定 type → kubelet 不创建 → 挂载失败）

$ multipass exec k8s-slave01 -- sudo microk8s ctr images ls | grep -i busybox
docker.io/library/busybox:1.36      ...                             ← 只有 docker.io 的 busybox
docker.io/library/busybox:latest    ...                             ← 与 initContainer 要求的引用不一致
（无 registry.high-flyer.cn/google_containers/busybox:latest）

$ multipass exec k8s-slave01 -- getent hosts registry.high-flyer.cn
198.18.0.77     registry.high-flyer.cn                             ← 198.18.0.0/15 为代理/保留段，实际不可达 → 拉取必失败（I17②）
```

**9) 任务提交校验实测（S6 / K5 实证）** —— 服务端的自查指引当前必然是误导

用真实 `hai-cli` 提交一个使用自定义镜像的任务（该镜像在 `train_image` 里没有行）：

```
$ sudo -u fireflyer hai-cli whoami
  haiadmin | haiadmin       ID 10020   Shared Group  hfai   Role  in     ← 组名是 hfai（非 haiadmin）

$ cat > /tmp/probe_img.py <<'EOF'
print("PROBE_OK")
EOF
$ sudo -u fireflyer hai-cli python /tmp/probe_img.py -- --image registry.high-flyer.cn/haiadmin/demo:v1 -n 1
  ...
  File ".../hfai/client/api/api_utils.py", line 75, in async_requests
    raise Exception(f'请求失败: [exception: {str(e)}] [result: {result}]')
Exception: 请求失败: [exception: 用户所在的组 [hfai] 不存在镜像
  [registry.high-flyer.cn/haiadmin/demo:v1] 或镜像仍在加载,
  请使用命令 `hfai images list` 检查]
  [result: {'success': 0, 'msg': '用户所在的组 [hfai] 不存在镜像 ... 请使用命令 `hfai images list` 检查'}]
```

**这条实测同时证明了三件事**：

1. **S6 判定成立**：任务侧校验**确实在跑**，并且**确实拒绝**了自定义镜像（因为表中无 `status='loaded'` 的行）——
   与分析 §4.3 的推理完全一致；
2. **K5 的「指引误导」被实证**：服务端让用户去跑 `hai-cli images list` 自查，而该命令的 `user_images`
   **恒为空**（§9-2）→ 用户按官方指引操作**永远查不出原因**。这使 I2 从「列表不显示」升级为
   **「官方排障路径失效」**，是本次修复最直接的收益点；
3. **I10 的适用范围比预期更广**：不只是 `images load/delete`，**任务提交**的失败也是以
   `Exception('请求失败: ...')` 的**裸 Python 栈**呈现给用户（`api_utils.py:75`），
   而不是友好提示 —— 这一条属于全局客户端体验问题，本特性只负责 `images` 子命令范围内的修正（§6.2）。

> **环境事实修正**：103 上真实用户为 `haiadmin`，**`shared_group = hfai`**（`hai-cli whoami` 实测）。
> 因此本文档示例中的镜像 URL 应为 `registry.high-flyer.cn/hfai/<name>:<tag>`；
> `registry.high-flyer.cn/haiadmin/...` 只出现在上文的**原始命令记录**中（保留不改，以保持实测可复现）。

**10) 客户端与仓库一致性核对（分析方法说明）**
```
$ md5sum ~/hai-platform/{client/commands/hfai_image.py,client/api/image_api.py,
    server_model/user_impl/user_image/default.py,server_model/user_impl/user_image/implement.py,
    server_model/selector/train_image_selector.py,server_model/user_data/table_config.py}
  vs 本地仓库同路径 → 6/6 逐字节一致
```
即：本次分析结论**同时适用于 103 部署（`e03c42c` + 未提交改动）与本地仓库**（旧分支证据来源）。

**11) 一键复现（旧分支资产，本工作树尚未并入）**

旧分支把上述证据固化为一支只读脚本，可在 103 上一条命令复现。**注意：本工作树当前不存在该脚本**
（`docs/haiplatform/scripts/probe_images.sh` 未并入本分支，属目标交付物，并入计划见
`images-server-task-list.md` §3.10），因此下列命令在**旧分支**上执行：

```bash
# 在 103 上（脚本只读，不改平台状态）
cd ~/hai-platform && bash docs/haiplatform/scripts/probe_images.sh
# 2026-10-02 实测基线：PASS=4 FAIL=6
#   失败项：link_hfai_image.sh 不存在 / 挂载种子未登记 / 节点无 /data_local /
#           节点无 busybox 引用 / images load 抛 AttributeError / images delete 抛 AttributeError
```

实施完成后重跑同一脚本，期望 **FAIL=0**（说明见 `docs/haiplatform/scripts/README.md`）。

> **本分支待补的复现项**：上传通道（`push`）尚无对应的一键脚本，计划随上传通道一并并入
> （`images-server-task-list.md` §3.10；脚本名与用例编号见 `images-server-test-cases.md` §4.11）。

---

*本文档由只读代码审计 + 103 真实环境实测生成，未修改任何源文件。
上传链路（§4.7）的证据行号已在 `feature/hai-cli-images-rustfs-design` @ `33a5b26` 上逐条复核并写入正文；
§1–§4.6 的代码现状行号同样已在 `33a5b26` 复核。*

*§9 实测记录保留**旧分支证据来源**（`feature/hai-cli-images-server-design`，被测 tag `f2cb559`），
原样不改；**本分支尚未重跑**。原始实测记录时间：2026-10-02 · 环境：`fireflyer@192.168.100.103` ·
服务端 `hai-platform-0` 1/1 Running。*
