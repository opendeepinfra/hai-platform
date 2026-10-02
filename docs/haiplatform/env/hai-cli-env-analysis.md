# hai-cli env(haienv)客户端 / 服务端现状逆向分析

> **分析对象**:`hai-cli env` 子命令,即 `plugins/haienv` 提供的 `haienv` 插件,以及与之相关的 `client/api/venv_api.py`、`cloud_storage` 的 `FileType.ENV` 分支、任务运行时的 `HAIENV_PATH` / `source haienv`。
>
> **结论一句话**:客户端**本地生命周期闭环完整**;**跨端(本地 → 集群)上传链路是半成品**;服务端**只有"任务运行时消费"这一半**;两端之间的路径约定与"环境注册表"没有设计。
>
> **与 [../workspace/hai-cli-workspace-analysis.md](../workspace/hai-cli-workspace-analysis.md) 的关系**:那份文档分析 `hai-cli workspace`,其中 §4.8 与风险 F7 提到 env 只是"预留链路"。本文把 env 作为**独立特性**做完整逆向,并补齐服务端设计视角。

---

## 1. 结论速览

| # | 判定项 | 结论 | 关键证据 |
| --- | --- | --- | --- |
| 1 | `hai-cli env` 命令是否存在 | 存在(`haienv` 插件的别名) | `client/commands/utils.py:29-39`、`client/hfai_cli.py:72-83` |
| 2 | 客户端本地生命周期 | **完整**:create / list / remove / config / source 激活 / 代码内激活 | `plugins/haienv/**` |
| 3 | 客户端上传到集群 | **不存在命令**,只有死代码库函数 | `plugins/haienv/haienv/client/cli.py:19-22`、`client/api/venv_api.py:10-46` |
| 4 | 客户端上传链路的正确性 | **不可用**(枚举字符串化 + 无调用方 + 无注册 + 子进程可执行文件拼错) | `client/api/venv_api.py:24-25`、`plugins/haiworkspace/haiworkspace/client/workspace_api.py:125` |
| 5 | 服务端任务运行时加载 env | **完整** | `server_model/task_impl/single_task_impl.py:60-61,150-158` |
| 6 | 服务端数据面 ENV 支持 | **部分**:路径与传输有,上传入口无 | `cloud_storage/utils.py:467-473`、`cloud_storage/service/sync_to_cluster.py:47` |
| 7 | 服务端 env 上传入口 | **不存在**:桩 + 未注册路由 | `api/resource/storage/default.py:9-10`、`api/register/implement.py:67-83` |
| 8 | 环境注册语义 | **缺失**:上传后 `source haienv` 发现不了 | `plugins/haienv/haienv/client/model.py:16-28`、`plugins/haienv/haienv/haienv` |
| 9 | 路径约定一致性 | **不一致**:数据面落盘路径 ≠ 运行时搜索根 | `cloud_storage/utils.py:472` vs `single_task_impl.py:61` |
| 10 | 独立设计文档 | **不存在**,仅有 workspace 文档中的 P1 一行契约 | `../workspace/workspace-server-requirements.md:34,278`、`../workspace/workspace-server-design.md:285` |

---

## 2. 命令面:一个工具,三种调用形态

`haienv` 不是单一 CLI,而是三种入口共用一套本地数据模型:

| 形态 | 入口 | 说明 |
| --- | --- | --- |
| ① `hai-cli env <sub>` | `client/hfai_cli.py:72-83` 把插件注册为子命令 `env`(截掉 `hai` 前缀),实际 `os.system` 执行 `/usr/local/bin/haienv` | 帮助里呈现为 `env     haienv`(`docs/_sources/guide/tutorial.md.txt:37-42`) |
| ② `haienv <sub>` | `plugins/haienv/haienv/haienv` 直接执行时自造 `/tmp/haienv` 引导脚本,调 `haienv.client.cli:cli` | 与 ① 同一条代码路径 |
| ③ `source haienv <name>` | 同一个 shell 脚本被 `source` 时走另一分支 | 环境加载器,不是 CLI |

此外还有第四种**进程内**形态:`import haienv; haienv.set_env('name')`(`plugins/haienv/haienv/__init__.py:1`)。

> **注意**:`plugins/haienv/haienv/haienv` 是**文件名无后缀**的 shell 脚本(setup.py 的 `scripts=['haienv/haienv']`),因此 `plugins/haienv/haienv/` 目录下同时存在 `haienv`(脚本)、`haienv.py`(库)、`__init__.py`。

---

## 3. 客户端实现盘点

### 3.1 命令与文件清单

| 子命令 | 命令层 | 业务层 |
| --- | --- | --- |
| `create` | `plugins/haienv/haienv/client/command.py:27-45` | `client/api.py:16-133` |
| `list` | `command.py:48-94` | `client/api.py:136-145` |
| `remove` | `command.py:97-104` | `client/api.py:148-170` |
| `config show` | `command.py:115-132` | 直接用 `Haienv.select` |
| `config clear` | `command.py:135-150` | `Haienv.update` |
| `config append` | `command.py:153-169` | `Haienv.update` |
| `push` | **不存在** | `client/api/venv_api.py:10-46`(**无调用方**) |

`create` 的硬校验(`command.py:41-43`):

- 仅 Linux;
- 必须能找到 `nvcc`;
- **仅支持 CUDA 11.1 / 11.3**(字面量比较,其它版本直接 assert 失败);
- 集群环境(`TASK_NAME != NO_CLUSTER`)下 conda channel 必须**只有 `defaults`**(`client/api.py:43-49`)。

这三条是历史包袱,直接限制了该功能在新镜像上的可用性。

### 3.2 本地数据模型:每用户一个 SQLite

```
$HAIENV_PATH/                     # 默认 $HOME;集群内 = /hf_shared/hfai_envs/<user>
├── venv.db                       # SQLite,表 haienv(key=环境名, value=pickle(HaienvConfig))
├── <name>_0/                     # conda prefix
│   ├── activate                  # 生成的 shell 激活脚本
│   ├── pip.conf
│   └── lib/pythonX.Y/site-packages/
└── <name>_1/ ...
```

- 路径前缀:`plugins/haienv/haienv/client/model.py:9-13`(`HAIENV_PATH` 优先,回退 `$HOME`)。
- DB 路径与建表:`model.py:16-28`;序列化用 `SqliteDict`(`plugins/haienv/haienv/client/sqlite_dict.py`),值 = `pickle.dumps(HaienvConfig, protocol=4)`。
- **兼容迁移**:旧版 `venv` + `venv_config` 两表在首次读写时合并成 `haienv` 表(`model.py:82-100`);DB 无写权限时降级走旧表(`old_version`)。
- 目录唯一化:`get_haienv_path` 生成 `{name}_{suffix}`(`model.py:140-163`)。

### 3.3 激活链路

**shell 侧**(`plugins/haienv/haienv/haienv`):

1. `HAIENV_ROOT = dirname(HAIENV_PATH)`;遍历其下**每个用户目录**的 `venv.db`;
2. 命中 `$HAIENV_PATH` 所属用户则短路;否则取第一个匹配;
3. 把 `extra_search_dir / extra_search_bin_dir / extra_environment` 写进临时配置文件并 `source`;
4. 最后 `source $HFAI_ENV_CERTAIN_PATH/activate` 并前置 `$.../bin` 到 `PATH`。

**Python 侧**(`haienv.py:20-102`):`set_env` 做同样搜索,然后直接改 `sys.path` / `os.environ`;`get_envs` 返回全量列表。

**关键推论**:环境是否"可见",唯一判据是**目标用户的 `venv.db` 里有没有这个 key**。目录存在但 DB 无记录 = 环境不存在。

### 3.4 与任务提交的衔接

| 环节 | 代码 | 说明 |
| --- | --- | --- |
| 用户指定 | `HF_ENV_NAME` / `HF_ENV_OWNER` 环境变量 | 支持 `system[zwt]` 形式 |
| 客户端打包进 schema | `base_model/base_task.py:332-362` | 写入 `options.py_venv` |
| 服务端生成启动脚本 | `server_model/task_impl/single_task_impl.py:150-158` | `source haienv <name> [-u <owner>]` |
| 服务端注入搜索根 | `single_task_impl.py:60-61` | `HAIENV_PATH=/hf_shared/hfai_envs/<user>` |

兼容列表 `py3-202105 / py3-202111 / py38-*` 会被改写(`single_task_impl.py:154-157`);`source` 失败时追加 `|| echo "no valid env found"`,**服务端不校验环境存在性**。

### 3.5 上传链路(半成品)

```
client/api/venv_api.py:push_venv
  ├─ ① Haienv.select(venv_name)                          # 本地查配置
  ├─ ② 拒绝 extend == 'True'                             # 明确不支持扩展环境上传
  ├─ ③ POST /ugc/update_cluster_venv?token&venv_name&py   # 换取集群路径
  └─ ④ os.system("... workspace push --file_type {FileType.ENV} --env_* ...")
```

该链路共有 **5 处断点**(E1–E4 与 E13),任一处未修都无法端到端:

| 断点 | 证据 | 后果 |
| --- | --- | --- |
| **E1 无 CLI 入口** | `plugins/haienv/haienv/client/cli.py:19-22` 只注册 4 个子命令 | 用户无法触发 |
| **E2 `push_venv` 无调用方** | 全仓 grep 仅定义处命中;`client/api/__init__.py` 不导出 | 死代码 |
| **E3 枚举字符串化** | `venv_api.py:25` 的 f-string `{FileType.ENV}` → 字面量 `'FileType.ENV'`;`workspace_api.py:125` 用 `file_type == FileType.ENV` 比较 → False | 打印"不支持的 file_type" |
| **E4 无注册步骤** | 上传只走文件同步;`venv.db` 从不更新 | 集群侧 `env list` / `source haienv` 找不到 |
| **E13 子进程可执行文件拼错** | `venv_api.py:24` 用 `f"{sys.argv[0]} workspace push ..."`;入口若为插件 `env`,则 `argv[0]` 是 `/usr/local/bin/haienv`,拼出 `haienv workspace push` = 未知子命令 | 退出码非 0 → 报"上传venv失败" |

> **E13 说明**:该拼法在旧的 `hai-cli venv push`(hfai 主 CLI,`argv[0]=='hai'`)下成立;一旦挂到 `hai-cli env`(插件)下必然失败。**即使修好 E2/E3 与服务端端点,不修这一条链路仍然不通。**

其余同类风险:`venv_api.py:25-26` 还拼了 `--file_type/--env_provider/--env_local_path/--env_remote_path`,这些是 `haiworkspace` 的隐藏选项,可执行文件应解析为 `haiworkspace`(见设计 §6.2)。

> **E3 实测**:`class FileType(str, Enum)`(`conf/utils.py:29-31`)在 Python 3.14 下 `f'{FileType.ENV}'` 仍为 `'FileType.ENV'`(非 `StrEnum`,不继承 `__format__` 的值语义)。这与 workspace 分析的 **F2** 是同一个根因。

其余客户端问题:

- **E5** `list` 的 `-a/--all`(`show_all`)声明后从未使用(`command.py:50-52`);
- **E6** `list -u <自己>` 会把结果归入 `others`,标题语义错乱(`command.py:76-83`);
- **E7** `push_venv` 直接取 `result['path']`(`venv_api.py:23`),未防御 `path=None`(桩函数正是返回 `path: None`);
- **E8** `client/api/venv_api.py:7` 反向依赖插件包 `haienv`,插件未安装时该模块 import 即失败(当前因无调用方而未暴露)。

### 3.6 workspace 侧对 ENV 的复用

`plugins/haiworkspace/haiworkspace/client/workspace_api.py:125-127` 的 `file_type == FileType.ENV` 分支:

- 不查 `.hfai/workspace.yml`;
- 直接使用 `--env_provider / --env_local_path / --env_remote_path`;
- 额外排除 `['activate', 'pip.conf']`(排除项在 diff 与 zip 两处同时生效)。

对应 CLI 选项在 `plugins/haiworkspace/haiworkspace/client/command.py:48-53`,**全部 `hidden=True`**,即不面向普通用户。

---

## 4. 服务端实现盘点

### 4.1 存在且完整:任务运行时消费

见 §3.4。服务端对 env 的职责只有两条:**注入搜索根**、**生成 `source` 命令**。这部分在功能上是完整的,但在健壮性上缺"环境存在性预校验"(任务会在启动脚本里静默降级)。

### 4.2 存在但只到数据面:ENV 类型

| 环节 | 代码 |
| --- | --- |
| 枚举 | `conf/utils.py:29-31` `FileType.ENV = 'env'` |
| 集群落地路径 | `cloud_storage/utils.py:467-473` |
| 传输放行 | `cloud_storage/service/sync_to_cluster.py:47`、`sync_from_cluster.py:52` |
| 删除放行 | `cloud_storage/service/delete.py:25` |
| 客户端 ENV 字符串归一化 | `cloud_storage/service/compat.py:25-52`(**能识别 `FileType.ENV` 形式**) |

`get_base_path` 的 ENV 分支:

```python
env_base_path = CONF.cloud.storage.service.env_path          # core.toml: '/hf_shared'
cluster_base_path = f'{env_base_path}/{group}/shared/hfai_envs/{username}/{name}'
cloud_base_path   = f'{group}/shared/hfai_envs/{username}/{name}'
check_is_subpath(env_base_path, cluster_base_path)
```

注意:**集群目标路径由服务端 `get_base_path` 决定,客户端传入的 `--env_remote_path` 只被用来取 basename 当 `name`**(`cloud_storage/api.py:158-179` + `api/resource/cloud_storage/default.py:112-125`)。

### 4.3 不存在:上传入口

| 项 | 现状 |
| --- | --- |
| 实现 | `api/resource/storage/default.py:9-10` 返回 `{'success': 1, 'msg': 'not implemented', 'path': None}` |
| 路由 | `api/register/implement.py:67-83` 的 `ugc` 段**没有** `/ugc/update_cluster_venv` |
| 签名 | 桩函数不接收 `Request`,而客户端是 `?token=&venv_name=&py=`(`venv_api.py:22`)→ 即便注册也会 422 |
| 私有层可能实现 | `api/register/implement.py:4` 的 `from .custom import *`,由 `base_model/utils.py:15-61` 的 `CustomFinder` 注入空模块或平台代码目录的同名文件 |

**结论**:开源仓内该端点不存在;生产环境是否由私有 `custom.py` 实现,需与部署层确认(下文设计按"开源仓可自建"处理)。

### 4.4 镜像构建期的平台基础环境

`one/release.sh:15-18` 在训练镜像构建阶段执行:

```bash
export HAIENV_PATH=/hf_shared/hfai_envs/platform
mkdir -p /hf_shared/hfai_envs/platform && chmod 777 /hf_shared/hfai_envs
echo Y | haienv create hai202207 --no_extend
```

说明三件事:

1. **平台基础环境位于 `platform` 这个"伪用户"目录下**,而任务运行时的搜索根是 `/hf_shared/hfai_envs`,靠"遍历所有用户目录"被发现;
2. 平台镜像内**确实安装了 `haienv` 包**(`one/build_cli.sh` 会构建全部 `plugins/hai*` 轮子,`Dockerfile:75-77` 统一 `pip install /tmp/hai*.whl`)——这是服务端复用 `Haienv.insert` 写注册表的前提;
3. `/hf_shared/hfai_envs` 被 `chmod 777`,每用户子目录权限由谁创建决定 → 服务端写他人 `venv.db` 的**权限问题必须显式设计**。

---

## 5. 端到端链路判定矩阵

| # | 场景 | 客户端 | 服务端 | 判定 |
| --- | --- | --- | --- | --- |
| S1 | 开发容器内 `haienv create` → 任务指定 `HF_ENV_NAME` | ✅ | ✅ | **可用**(env 直接建在共享盘) |
| S2 | 代码内 `haienv.set_env()` | ✅ | — | **可用**(需 `HAIENV_PATH` 可达) |
| S3 | `hai-cli env list`(本用户) | ✅ | — | **可用**(本机/开发容器) |
| S4 | `hai-cli env list -u <他人>` | ⚠️ 依赖共享盘且无路径校验 | — | **仅开发容器可用** |
| S5 | 本地(集群外)建 env → 推到集群 | ❌ 无命令 + 枚举 bug | ❌ 无端点 | **不可用** |
| S6 | 推送后任务 `source haienv <name>` | — | ❌ 无注册 | **不可用** |
| S7 | 推送 `extend=True` 的环境 | ❌ 客户端明确拒绝 | — | **设计上不支持** |

---

## 6. 风险清单

| ID | 等级 | 风险 | 证据 | 建议 |
| --- | --- | --- | --- | --- |
| **E1** | 高 | `hai-cli env push` 不存在,用户无入口 | `plugins/haienv/haienv/client/cli.py:19-22` | 新增 `push` 子命令(需求 FR-01) |
| **E2** | 高 | `push_venv` 死代码,且 `FileType.ENV` 字符串化必然失败 | `client/api/venv_api.py:10-46`、`conf/utils.py:29-31` | 用 `.value`,并补调用方(FR-01/FR-02) |
| **E3** | 高 | 服务端无 `/ugc/update_cluster_venv` | `api/resource/storage/default.py:9-10`、`api/register/implement.py:67-83` | 实现并注册(FR-03,API-11) |
| **E4** | 高 | **路径约定不一致**:数据面落盘 `{env_path}/{group}/shared/hfai_envs/{user}/{name}` vs 运行时搜索根 `dirname(HAIENV_PATH)=/hf_shared/hfai_envs` | `cloud_storage/utils.py:472`、`single_task_impl.py:61` | 统一约定(设计 ADR-E1) |
| **E5** | 高 | **无注册表写入**:上传后 `venv.db` 不更新,环境不可见 | `plugins/haienv/haienv/client/model.py:16-28`、`haienv` 脚本搜索逻辑 | 新增注册接口(FR-04,API-13) |
| **E6** | 中 | `push_venv` 不校验 `success`/`path`;上传成功但注册失败无法区分 | `client/api/venv_api.py:22-24` | 分级返回与错误码(FR-06) |
| **E7** | 中 | 共享盘跨用户目录权限未知,服务端写他人 `venv.db` 可能失败 | `one/release.sh:16-17`(仅顶层 777) | 注册前的权限自检 + 明确失败语义(SEC-03) |
| **E8** | 中 | 服务端与客户端 `haienv` 包版本偏移会导致 pickle 反序列化失败 | `sqlite_dict.py`(pickle)、`Dockerfile:75-77` | 版本兼容校验(设计 ADR-E4) |
| **E9** | 中 | `list_haienv` / `set_env` 把 `user` 直接拼进路径,无 `..` 校验 | `client/api.py:137-142`、`haienv.py:27-43` | 路径校验(SEC-02) |
| **E10** | 低 | `create` 硬编码 CUDA 11.1/11.3,新镜像不可用 | `command.py:41-43` | 放宽为"存在即可"或配置化 |
| **E11** | 低 | `list -a/--all` 无效;`-u 自己` 归类错 | `command.py:50-52,76-83` | 顺手修 |
| **E12** | 低 | 文档只描述"支持推送到集群",未说明入口缺失 | `docs/_sources/guide/environment.md.txt:76` | 补文档或实现 |
| **E13** | 高 | 子进程命令用 `sys.argv[0]` 拼接;挂到 `env` 插件后 `argv[0]` 是 `haienv`,拼出的 `haienv workspace push` 非法 | `client/api/venv_api.py:24-26` | 显式解析 `haiworkspace` 可执行文件(设计 §6.2) |

---

## 7. 与 workspace 文档的差异

| 项 | workspace | env |
| --- | --- | --- |
| 特性文档 | 9 份(`docs/haiplatform/workspace/`) | 无(本文是首份) |
| 服务端接口 | 12 个(9 个 P0 已实现) | 1 个(P1,未实现) |
| 数据模型 | Postgres(`user_sync_status` 等) | 客户端 SQLite `venv.db`,服务端无 |
| 传输通道 | 复用 `cloud_storage` + S3 | **同样复用**,只是入口缺失 |
| 任务侧 | `oss://` 解析与挂载 | `HAIENV_PATH` + `source haienv`(已完整) |

**关键判断**:env 的上传通道**不需要新造传输层**,`cloud_storage` 的 ENV 分支已经就绪;真正缺的是"**入口 + 路径对齐 + 注册**"三件事,工作量远小于 workspace。

---

## 8. 证据索引

代码:

- `client/commands/utils.py:26-53`(`PLUGIN_LIST` / 插件派发)、`client/hfai_cli.py:72-83`
- `plugins/haienv/haienv/client/{cli,command,api,model,sqlite_dict}.py`
- `plugins/haienv/haienv/haienv`(shell 激活器)、`plugins/haienv/haienv/haienv.py`、`plugins/haienv/setup.py`
- `client/api/venv_api.py`
- `plugins/haiworkspace/haiworkspace/client/{command,workspace_api}.py`
- `conf/utils.py:29-31`、`base_model/base_task.py:332-362`
- `server_model/task_impl/single_task_impl.py:60-61,150-158`
- `cloud_storage/utils.py:434-442,444-500`、`cloud_storage/api.py:158-179`
- `cloud_storage/service/{__init__,compat,context,errors,sync_to_cluster,sync_from_cluster,delete}.py`
- `api/resource/storage/default.py:9-10`、`api/register/implement.py:4,67-83`、`api/depends/implement.py:110-141`
- `base_model/utils.py:15-61`(`CustomFinder`)
- `one/build_cli.sh`、`one/release.sh:15-18`、`one/one_etc/core.toml:113`、`Dockerfile:73-77`

文档:

- `docs/_sources/guide/environment.md.txt:1-120`(官方使用说明)
- `docs/_sources/guide/tutorial.md.txt:37-109`
- `docs/_sources/cli/ugc.rst.txt:9-11`(Sphinx 从 `haienv.client.cli` 生成)
- `docs/haiplatform/workspace/hai-cli-workspace-analysis.md:238-246,531,567-569`
- `docs/haiplatform/workspace/workspace-server-{requirements,design,test-cases,task-list}.md`(API-11 / P1-3 / TC-J14)
