# hai-cli env 服务端程序设计

> **文档定位**:`docs/haiplatform/env/` 三件套之三(分析 → 需求 → **设计**)。
> **前置阅读**:[hai-cli-env-analysis.md](hai-cli-env-analysis.md)(现状与风险 E1–E12)、[env-server-requirements.md](env-server-requirements.md)(FR/NFR/SEC/OPS/CMP/HC 与验收 AC-01~AC-12)。
> **写作约定**:本文只描述**本仓库可实现**的部分;凡依赖部署私有 `custom.py` 的地方显式标注。行号引用与仓库当前分支一致。

---

## 1. 结论与设计概要

一句话:**不新造传输层**。env 上传复用 `hai-cli workspace push --file_type env` 与 `/ugc/sync_to_cluster`,只需补齐 4 件事:

1. **客户端入口**:`hai-cli env push <name>`(新子命令);
2. **客户端修复**:`FileType` 取值 `.value`(修 F2/E3)+ 正确解析 workspace 可执行文件(修 E13);
3. **服务端两个接口**:`API-11` 预检并推导路径、`API-13` 写集群侧注册表;
4. **路径对齐**:数据面 ENV 落盘目录 = 任务运行时 `HAIENV_PATH` 的父目录下的 `<user>/<name>`。

排期规模约 **6.5 人日**(P0 5 人日 + 文档/灰度 1.5 人日),明显小于 workspace(≈18 人日)。

---

## 2. 总体架构

```
┌───────────────── 本地机(集群外) ─────────────────┐
│  $HAIENV_PATH(默认 $HOME)                       │
│  ├── venv.db            ← 本地注册表(SQLite)     │
│  └── myenv_0/           ← conda prefix           │
│                                                  │
│  hai-cli env push myenv                          │
│      └─ client/api/venv_api.py:push_venv         │
│           ├─① Haienv.select → 本地配置            │
│           ├─② POST /ugc/update_cluster_venv      │──┐
│           ├─③ haiworkspace push --file_type env  │  │
│           │     └─ 复用 workspace 传输链路        │  │
│           └─④ POST /ugc/register_cluster_venv    │──┼─┐
└──────────────────────────────────────────────────┘  │ │
                                                       │ │
┌──────────────── ugc-server / cloud-storage ──────────┼─┼──────────┐
│  api/register/implement.py  (注册 2 条 /ugc 路由)     │ │          │
│  api/resource/storage/default.py  (接入层:契约/鉴权) │ │          │
│  cloud_storage/service/env_registry.py  (领域层)  ◄───┘ │          │
│      ├─ validate_env_name / derive_env_path             │          │
│      └─ register_env → 共享盘 venv.db  ◄────────────────┘          │
│  cloud_storage/service/sync_to_cluster.py (既有,FileType.ENV)      │
└────────────────────────────────────────────────────────────────────┘
             │                                    ▲
             ▼ 对象存储(S3/OSS) + 集群共享盘        │ 读 venv.db
┌──────────────────────── 集群共享盘 ────────────────┴────────────────┐
│  /hf_shared/hfai_envs/            ← env_root = dirname(HAIENV_PATH)  │
│  ├── platform/{venv.db, hai202207_0/}   ← 镜像构建期基础环境         │
│  └── <user>/{venv.db, myenv_0/}         ← 上传 + 注册的产物          │
└──────────────────────────────────────────────────────────────────────┘
             │
             ▼ 任务容器: export HAIENV_PATH=/hf_shared/hfai_envs/<user>
                source haienv myenv → 读 venv.db 解析路径 → source .../activate
```

**分层纪律**(沿用 workspace 的 ADR-11/ADR-12 与 `server_model/task_impl/workspace_resolver.py` 的导入规范):

- 领域层 `cloud_storage/service/**` 不 import fastapi、不注册路由、不做网络/Redis 连接(惰性);
- `server_model/task_impl/**` 顶层**不 import** `cloud_storage.*`(该包 `__init__` 会拉起 FastAPI app);
- 路径常量单点定义在 `conf/utils.py`(纯函数,只有 `os`/`re`/惰性 `CONF`),两端都从这里取(见 §3.4)。

---

## 3. 路径约定(本设计的核心)

### 3.1 术语与单点定义

| 名称 | 定义 | 当前取值 |
| --- | --- | --- |
| `env_path` | 配置项,env 家族的父根 | `[cloud.storage.service] env_path = '/hf_shared'`(`one/one_etc/core.toml:113`) |
| `env_root` | `{env_path}/hfai_envs` | `/hf_shared/hfai_envs` |
| `user_env_dir` | `{env_root}/{username}` = **`dirname(HAIENV_PATH)`** | `/hf_shared/hfai_envs/<user>` |
| 注册表 | `{user_env_dir}/venv.db`,表 `haienv` | 客户端 `haienv` 读写 |
| env 目录 | `{user_env_dir}/{name}_{suffix}` | `{name}_0` 起 |

新增纯函数(建议放 `conf/utils.py`,理由见 ADR-E3):

```python
ENV_DIR_NAME = 'hfai_envs'
ENV_NAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$')

def get_env_path() -> str:            # env_path,默认 /hf_shared
def get_env_root() -> str:            # {env_path}/hfai_envs
def get_user_env_dir(user) -> str:    # {env_root}/{user}
def get_env_registry_path(user) -> str  # {user_env_dir}/venv.db
def get_env_dir_name(name, suffix=0) -> str  # f'{name}_{suffix}'
```

### 3.2 现状不一致(必须修)

| 来源 | 路径 |
| --- | --- |
| 数据面 `get_base_path` ENV 分支(`cloud_storage/utils.py:467-473`) | `/hf_shared/<group>/shared/hfai_envs/<user>/<name>` |
| 任务运行时(`server_model/task_impl/single_task_impl.py:61`) | `HAIENV_PATH=/hf_shared/hfai_envs/<user>` |

### 3.3 目标约定

```
env_root            = /hf_shared/hfai_envs
user_env_dir        = /hf_shared/hfai_envs/<user>
注册表               = /hf_shared/hfai_envs/<user>/venv.db
env prefix          = /hf_shared/hfai_envs/<user>/<name>_<suffix>
HAIENV_PATH(任务)   = /hf_shared/hfai_envs/<user>        # = user_env_dir
S3 key(不变)         = <group>/shared/hfai_envs/<user>/<name>
```

`cloud_storage/utils.py:get_base_path` 的 ENV 分支改为:

```python
elif file_type == FileType.ENV:
    env_root = get_env_root()                                   # /hf_shared/hfai_envs
    cluster_base_path = f'{env_root}/{username}/{name}'         # ★ 与 user_env_dir 对齐
    cloud_base_path   = f'{group}/shared/hfai_envs/{username}/{name}'  # S3 key 保持不变
    check_is_subpath(env_root, cluster_base_path)
```

**注意**:`get_base_path` 返回的 `(cluster_base_path, cloud_base_path)` 是两个独立计算的字符串,只改 cluster 侧不影响 bucket 布局,存量对象无需迁移(且 env 上传链路历史上从未成功,无历史包袱)。

### 3.4 单点与自检

- `server_model/task_impl/single_task_impl.py:60-61` 的硬编码改为 `f'{get_env_root()}/{self.task.user_name}'`,该文件已 `from conf import CONF, FileType`,追加导入即可,**不引入 cloud_storage 依赖**。
- 启动自检(OPS-01):`get_env_root()` 与 `get_base_path(..., FileType.ENV)` 产出的路径前缀比较;不一致时打印 ERROR 并给出建议配置值,不阻断启动。

---

## 4. 接口契约

### 4.1 API-11 `POST /ugc/update_cluster_venv`(预检)

```http
POST /ugc/update_cluster_venv?token=<token>&venv_name=myenv&py=3.8&extend=False
```

| 项 | 内容 |
| --- | --- |
| 鉴权 | `Depends(get_ugc_user)`(`api/depends/implement.py:110-141`) |
| 处理 | ① 名称白名单校验;② `extend=True` → 拒绝;③ 计算目标目录:注册表命中 → 复用其 `path`;**注册表未命中但磁盘上已有 `{name}_{suffix}` → 复用最小后缀**(N3,见下);否则取 `_0`;④ **写权限探测**(见 §4.4) |
| 成功 | `{'success': 1, 'path': '/hf_shared/hfai_envs/<user>/myenv_0', 'exists': false, 'reused': true, 'cloud_path': '<group>/shared/hfai_envs/<user>/myenv_0', 'haienv_version': '1.4.1+e03c42c'}`（`cloud_path` 为 C-6 新增；`reused` / `haienv_version` 为 M3 新增，见下） |
| 失败 | `INVALID_PARAM` / `FEATURE_DISABLED` / `ENV_REGISTRY_NOT_WRITABLE` / **`ENV_REGISTRY_READ_FAILED`** / `UNAUTHORIZED` |
| 幂等 | 相同 `venv_name` 返回同一 `path`;**`exists=true` 表示「已注册可用」**(目录在但未注册仍为 `false`,避免客户端据此跳过上传);`reused=true` 表示本次复用了已存在的目录(幂等重试) |
| 兼容 | 旧形态 `?token&venv_name&py`(无 `extend`)必须可用;新增字段是**追加**的,老客户端忽略即可 |

> **实现修正 C-6（必须遵守）**：`--env_remote_path` 在客户端**不是集群文件系统路径，而是对象存储 key 前缀**
> （`workspace_util.upload_files`：`dst_file = f'{remote_path}/{f.path}'`）。因此 API-11 必须额外返回
> `cloud_path = get_base_path(..., FileType.ENV)[1]`，客户端用它作 `--env_remote_path`；若误用集群路径，
> 对象会写到 `nfs-shared/...` 而服务端 stage2 按 `{group}/shared/hfai_envs/...` 读取 → `404 Not Found`。
> `path` 仍用于展示与 API-13 注册（两者 basename 必须相同，服务端据此推导 `name`）。
>
> **实现修正 C-7**：`workspace_api.push` 的 ENV 分支**不能**把 `activate` 放进 `exclude_list` —— 集群侧 env
> 目录必须自带 activate，否则任务里 `source haienv <name>` 报 `<prefix>/activate: No such file or directory`。
> conda 生成的 activate 用 `${BASH_SOURCE[0]}` 推导环境自身路径，换到集群路径仍可用。
>
> **实现修正 N3（M3，幂等）**：旧实现的「取第一个空闲后缀」在
> 「① 上传成功 → ② API-13 注册失败/客户端中断 → ③ 用户重试」时会分配 `name_1`，
> **把整份环境重传一遍**（103 实测复现 `xxx_0 → xxx_1`）。现在：只要磁盘上已有同名目录就复用
> 最小后缀，重试落在同一目录、同一批对象 key 上；语义与客户端 `get_haienv_path`（同名目录复用）一致。
> 复现脚本：`docs/haiplatform/scripts/check_env_idempotent.sh`。
>
> **实现修正 N3b（注册表读失败 fail-closed）**：`_read_registry` 过去把所有异常吞成 `{}`，
> 于是「注册表读不出来」等价于「这个名字没注册过」→ 同样产生重复上传。现在：
> 整表读不出来 → `ENV_REGISTRY_READ_FAILED`；只有**部分**条目反序列化失败时降级逐行读取，
> 把读不出来的 key 显式暴露，并对这些名字 fail-closed（避免覆盖或重复上传）。

`path` 的 **basename 即最终目录名**:客户端把它作为 `--env_remote_path` 传入,`workspace_api.push` 取 `os.path.basename(env_remote_path)` 当 `name`(`plugins/haiworkspace/haiworkspace/client/workspace_api.py:126`),服务端 `submit_to_cluster` 再按 `get_base_path` 落盘。因此**只要 §3.3 对齐,API-11 返回的路径与真实落盘路径必然一致**。

### 4.2 API-13 `POST /ugc/register_cluster_venv`(注册)

```http
POST /ugc/register_cluster_venv?token=<token>
Content-Type: text/plain                      # 与 workspace 一致:JSON 放在 text/plain body 里

{"venv_name":"myenv","path":"/hf_shared/hfai_envs/<user>/myenv_0","py":"3.8",
 "extra_search_dir":[],"extra_search_bin_dir":[],"extra_environment":[]}
```

| 项 | 内容 |
| --- | --- |
| 处理 | ① 校验名称;② 校验 `path` 落在 `get_user_env_dir(user)` 之下(`check_is_subpath`);③ **有上限地等待目录可见**(NFS 属性缓存,见下);④ 加文件锁;⑤ 用镜像内 `haienv` 包写 `venv.db` 的 `haienv` 表;⑥ 回读校验 |
| 成功 | `{'success': 1, 'registered': true, 'path': '...', 'db': '/hf_shared/hfai_envs/<user>/venv.db', 'haienv_version': '1.4.1+e03c42c'}` |
| 失败 | `INVALID_PARAM` / `FORBIDDEN`(path 越界)/ `PATH_ESCAPE` / `ENV_REGISTRY_WRITE_FAILED` / **`ENV_REGISTRY_READ_FAILED`** / `FEATURE_DISABLED` |
| 幂等 | `REPLACE INTO haienv`;同名同路径重复注册无副作用,`registered` 表示"当前存在" |
| 前置 | 客户端在 `haiworkspace push` **退出码为 0** 之后调用(此时 stage2 已完成,集群目录就绪) |

> **NFS 目录可见性（M3）**：宿主与 Pod 是不同的 NFS 客户端（`lookupcache=all`、`acdirmax` 默认 60s），
> 在宿主侧创建的目录 Pod 可能**最长几十秒**看不到（103 实测约 28s）。因此第 ③ 步做有上限的轮询：
> 默认 10s，可用 `cloud.storage.service.env_register_isdir_wait_seconds` 调整（0=只看一次，上限 120s）。
> 超时后仍然报 `INVALID_PARAM`，并在消息里明确提示「直接重试 `env push`」——
> 重试会复用同一目录与同一批对象 key（N3），不会多占后缀。

> **为什么拆成两个接口而不是"同步完成后自动注册"**:见 ADR-E2。核心原因是自动注册需要在服务端保存"待注册元数据"的中间状态(Redis/DB),而拆分后两个接口都无状态、可独立重试、与客户端超时解耦。

### 4.3 (P2)API-14 `GET /ugc/cluster_venv/list`

`?token=&user=<可选>` → `{'success':1,'data':[{user,haienv_name,path,extend,extend_env,py}]}`。仅服务端读注册表,供无共享盘场景;本期不实现(需求 Q-5)。

### 4.4 写权限探测(AC-06 的关键)

`user_env_dir` 由**用户自己**在开发容器里 `haienv create` 时创建(`get_haienv_path`→`os.makedirs`),默认权限 `755 <user>:<user>`;而 **ugc-server 以平台账号运行**,很可能无法写 `venv.db`。因此:

1. 镜像构建期的 `chmod 777 /hf_shared/hfai_envs`(`one/release.sh:17`)只覆盖根目录,不覆盖用户目录;
2. API-11 预检时执行**探测**:在 `user_env_dir` 下 `mkstemp` + 删除,失败即返回 `ENV_REGISTRY_NOT_WRITABLE`,**在浪费带宽上传前暴露问题**;
3. 修复路径(任选其一,建议 ①):
   - ① 客户端 `haienv create`/首次 push 时把 `user_env_dir` 置为 `0o777`(或 `0o775` + 与平台同组),`mode` 只影响权限位,不改语义(HC-01 允许);
   - ② 运维侧对存量用户目录批量 `chmod 777`;
   - ③ 服务端改用"平台所有的并行注册表"(**否决**:违反 HC-02,会让 `source haienv` 有两套判据)。

---

## 5. 服务端模块设计

### 5.1 文件清单

| 动作 | 文件 | 内容 |
| --- | --- | --- |
| 新增 | `cloud_storage/service/env_registry.py` | 名称校验、路径推导、注册表读写、权限探测、读回校验 |
| 新增 | `cloud_storage/service/env_paths.py`(可选) | 若不愿放 `conf/utils.py`,则在此单点定义路径函数(**纯 `conf` 依赖**) |
| 修改 | `conf/utils.py` | 新增 §3.1 的 5 个纯函数(推荐位置,ADR-E3) |
| 修改 | `cloud_storage/service/errors.py` | 新增 `ENV_ALREADY_EXISTS`、`ENV_REGISTRY_WRITE_FAILED`、`ENV_REGISTRY_NOT_WRITABLE`、`ENV_PATH_MISMATCH` |
| 修改 | `cloud_storage/service/__init__.py` | 导出 `derive_env_path` / `register_env` / `env_registry_self_check` |
| 修改 | `cloud_storage/service/context.py` | 新增 `get_env_root()`(内部调 `conf.utils`)与自检辅助 |
| 修改 | `cloud_storage/utils.py:467-473` | ENV 分支 cluster/cloud 路径解耦 |
| 修改 | `api/resource/storage/default.py:9-10` | 用真实实现替换桩(放 `default.py` 以便 `custom.py` 覆盖,HC-08) |
| 修改 | `api/register/implement.py`(`ugc` 段) | 注册 2 条路由 |
| 修改 | `server_model/task_impl/single_task_impl.py:60-61` | `HAIENV_PATH` 改用 `get_env_root()` |
| 修改 | `one/one_etc/core.toml` | `env_path` 注释补充语义 |

### 5.2 领域层核心签名

```python
# cloud_storage/service/env_registry.py
def env_registry_self_check() -> dict: ...            # OPS-01,启动时调用

def validate_env_name(name: str) -> str: ...          # 非法 → WorkspaceError(INVALID_PARAM)

async def derive_env_path(user, venv_name: str, py: str, extend) -> dict:
    """API-11 领域实现。返回 {'path':..., 'exists': bool}。不写注册表。"""

async def register_env(user, venv_name, path, py,
                       extra_search_dir, extra_search_bin_dir,
                       extra_environment) -> dict:
    """API-13 领域实现。写 {user_env_dir}/venv.db 的 haienv 表并回读校验。"""
```

实现要点:

1. **名称白名单**:`ENV_NAME_RE = ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`;显式拒绝 `..`、`/`、空白、超长(SEC-04)。
2. **路径推导**:先读注册表;命中则复用其 `path`;否则扫描 `{user_env_dir}/{name}_*`,取第一个空闲后缀(`suffix=0,1,2,...`),与客户端 `get_haienv_path`(`plugins/haienv/haienv/client/model.py:140-163`)保持一致。
3. **注册表写入**(关键):

```python
def _write_registry_sync(username, name, path, py, extra_search_dir, extra_search_bin_dir, extra_environment):
    from haienv.client.model import Haienv, HaienvConfig   # 惰性 import(见 ADR-E4)
    db_path = get_env_registry_path(username)
    haienv_config = HaienvConfig(path=path, extend='False', extend_env='', py=py,
                                 extra_search_dir=list(extra_search_dir or []),
                                 extra_search_bin_dir=list(extra_search_bin_dir or []),
                                 extra_environment=list(extra_environment or []))
    with _registry_lock(db_path):                          # fcntl.flock(<db>.lock)
        Haienv.insert(haienv_name=name, haienv_config=haienv_config,
                      outside_db_path=db_path)
    got = Haienv.select(haienv_name=name, outside_db_path=db_path)   # 回读校验
    assert got is not None and got.path == path
```

- **必须复用客户端的 `HaienvConfig` 类**:`SqliteDict` 用 `pickle.dumps(value, protocol=4)`(`plugins/haienv/haienv/client/sqlite_dict.py`),客户端 `loads` 时需要能 import 同一个类;手写 pickle 或自定义类会导致客户端反序列化失败(CMP-03)。
- 平台镜像已安装 `haienv`(`one/build_cli.sh` 构建全部 `plugins/hai*`,`Dockerfile:75-77` 统一安装),因此该 import 在 ugc-server 进程内可用。
- **惰性 import**:`haienv/__init__.py` → `haienv.py` 在模块导入期会读 `HAIENV_PATH`/`HOME` 并 `makedirs`(`plugins/haienv/haienv/haienv.py:12-17`),放在函数内 import 避免污染服务端启动路径(ADR-E4)。
- **阻塞 I/O**:注册是同步 sqlite 写,接入层用 `asyncio.to_thread` 或既有 `asyncwrap` 包装(NFR-04)。
- 失败一律转 `WorkspaceError(ENV_REGISTRY_WRITE_FAILED, ...)`,**不抛裸 500**(HC-05)。

4. **权限探测**:`_probe_writable(user_env_dir)`:`tempfile.mkstemp(dir=...)` + `os.remove`,异常 → `ENV_REGISTRY_NOT_WRITABLE`。

### 5.3 接入层

```python
# api/resource/storage/default.py(替换现有桩)
async def update_cluster_venv(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    p = request.query_params
    result = await derive_env_path(user, p.get('venv_name') or '',
                                   p.get('py') or '', p.get('extend'))
    result['success'] = 1
    return result

async def register_cluster_venv(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    body = await parse_json_body(request)
    result = await register_env(user, body.get('venv_name'), body.get('path'),
                                body.get('py'), body.get('extra_search_dir'),
                                body.get('extra_search_bin_dir'),
                                body.get('extra_environment'))
    result['success'] = 1
    return result
```

- 复用 `cloud_storage/service` 的 `normalize_enum` / `parse_json_body` / `WorkspaceError` 与 `api/app.py:225` 的 `/ugc/*` 统一错误改写(CON-3)。
- 路由注册(`api/register/implement.py` 的 `if 'ugc' in REG_SERVERS:` 段):

```python
app.post('/ugc/update_cluster_venv')(ar_storage.update_cluster_venv)
app.post('/ugc/register_cluster_venv')(ar_storage.register_cluster_venv)
```

> **不要**放在 `ar_cloud_storage.*` 下:该模块的 9 个函数与客户端 workspace 契约一一对应,混入 env 会破坏"接口 ↔ 函数"的对应关系。

---

## 6. 客户端设计

### 6.1 新增 `env push` 子命令

`plugins/haienv/haienv/client/command.py`:

```python
@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.argument('haienv_name', required=True, metavar='haienv_name')
@click.option('--force', is_flag=True, default=False, help='强制覆盖集群侧同名文件')
@click.option('-n', '--no_checksum', is_flag=True, default=False)
@click.option('-z', '--no_zip', is_flag=True, default=False)
@click.option('-d', '--no_diff', is_flag=True, default=False)
@click.option('-p', '--provider', default='', help='云端存储 provider,默认取 $CLOUD_STORAGE_PROVIDER 或 oss')
@click.option('--proxy', default='')
@click.option('-l', '--list_timeout', type=click.IntRange(5, 7200), default=300)
@click.option('-s', '--sync_timeout', type=click.IntRange(5, 21600), default=1800)
@click.option('-o', '--cloud_connect_timeout', type=click.IntRange(60, 43200), default=120)
@click.option('-t', '--token_expires', type=click.IntRange(900, 43200), default=1800)
@click.option('-m', '--part_mb_size', type=click.IntRange(10, 10240), default=100)
async def push(haienv_name, ...):
    """把本地虚拟环境推送到集群（仅支持非 extend 环境）"""
    result = await push_venv(haienv_name=haienv_name, ...)
    print(result['msg'])
    if not result['success']:
        sys.exit(1)
```

`plugins/haienv/haienv/client/cli.py:19-22` 增加 `cli.add_command(push)`。

> 短选项冲突检查:`create` 已占用 `-p/--py`;`push` 用 `-p/--provider` 会与 `create` 冲突吗?**不会**(不同子命令命名空间独立),但与 `hai-cli workspace push` 的 `-p/--part_mb_size` 语义不同,故 `push` 的 provider 用长选项 `--provider`、分片大小用 `-m`,避免 E10/E11 那类"同名不同义"的坑。

### 6.2 修复 `client/api/venv_api.py`

| 问题 | 修法 |
| --- | --- |
| E3 `{FileType.ENV}` → `'FileType.ENV'` | 一律 `.value`:`--file_type {FileType.ENV.value}`;全仓同类 f-string 一并排查(workspace 侧 F2) |
| E13 `sys.argv[0]` 不再是 `hai-cli` | `os.system` 内改为解析 workspace 可执行文件:`os.environ.get('HAI_WORKSPACE_BIN')` → `shutil.which('haiworkspace')` → `os.path.join(sysconfig.get_path('scripts'), 'haiworkspace')`;或直接 `from hfai.client.commands.utils import PLUGIN_LIST` 取 `PLUGIN_LIST['haiworkspace']` |
| E7 未校验 `path` | `path = result.get('path')`;为空 → 返回 `success=0` 的明确 `msg` |
| E8 反向依赖插件 | 保留(插件与主 CLI 同镜像安装),但改为**函数内惰性 import**,避免 `hfai.client.api` 导入期失败 |
| 缺注册 | push 成功后调 `POST /ugc/register_cluster_venv`(§4.2) |

> **E13 是本设计新发现的风险**:`hai-cli env push` 走的是插件进程,`sys.argv[0]` 是 `/usr/local/bin/haienv`;原代码 `f"{sys.argv[0]} workspace push ..."` 会变成 `haienv workspace push ...`(未知子命令→非 0→报"上传venv失败")。**即使修好 F2 和端点,不修这一条链路仍然失败。**

### 6.3 分级结果与幂等(FR-06/NFR-06)

```
上传失败            → success=0,'上传失败:<原因>'                     exit 1
上传成功 + 注册失败 → success=0,'环境已上传但注册失败,可重试:env push X'  exit 1   ← 关键区分
全部成功            → success=1,'上传并注册成功,可用 source haienv X'   exit 0
```

- 幂等:重复执行时 ③ 会打印"数据已同步,忽略本次操作"(`workspace_util.py:368-370`)并正常返回;④ 的 `REPLACE` 保证注册不重复。
- **不回滚**已上传对象与集群目录(NFR-06):注册失败只是"不可见",补齐注册即可,不必重传。

---

## 7. 任务侧

| 项 | 设计 |
| --- | --- |
| `HAIENV_PATH` | 改为 `f'{get_env_root()}/{self.task.user_name}'`,值不变但来源单点化(§3.4) |
| `source haienv` | **语义不变**(HC-02):仍靠 `venv.db` 解析,见 `plugins/haienv/haienv/haienv` 的搜索逻辑 |
| 基础环境 | `platform` 目录仍被"遍历所有用户"发现,零改动 |
| 诊断增强(FR-11,P2) | `single_task_impl.py:158` 的 `|| echo "no valid env found"` 扩为打印 `env=<name> owner=<owner> HAIENV_PATH=$HAIENV_PATH`,并把失败降级为**显式 WARNING**(不改变任务成败语义) |
| 提交期预校验(NFR,可选 P1) | 任务创建时若 `options.py_venv` 非空,可做一次注册表存在性检查,不存在则**只告警**不拒绝(避免误伤共享盘未挂载的场景) |

---

## 8. 端到端时序

```mermaid
sequenceDiagram
    autonumber
    participant U as 用户(本地机)
    participant H as haienv 插件
    participant V as client/api/venv_api
    participant W as haiworkspace push
    participant S as ugc-server
    participant FS as 共享盘 / 对象存储

    U->>H: hai-cli env push myenv
    H->>H: Haienv.select(myenv) 校验存在且 extend=False
    V->>S: POST /ugc/update_cluster_venv?token&venv_name=myenv&py=3.8&extend=False
    S->>FS: 读取 user_env_dir(注册表 + 目录扫描) + 写权限探测
    S-->>V: {'success':1,'path':.../myenv_0,'exists':false}
    V->>W: haiworkspace push --file_type env --env_local_path --env_remote_path --provider
    W->>FS: ① 分片上传对象存储 ② POST /ugc/sync_to_cluster ③ 轮询 status 至 finished
    FS-->>W: stage2 finished(集群目录已就绪)
    W-->>V: exit 0
    V->>S: POST /ugc/register_cluster_venv {venv_name,path,py,extra_*}
    S->>FS: flock + REPLACE INTO venv.db(haienv) + 回读校验
    S-->>V: {'success':1,'registered':true}
    V-->>U: 上传并注册成功,可用 source haienv myenv

    Note over U,FS: 任务侧
    FS-->>U: 任务容器 HAIENV_PATH=/hf_shared/hfai_envs/<user>;source haienv 命中 venv.db
```

**一页简图**:

```
本地 venv.db ──┐
本地 myenv_0/ ─┴─ (1) API-11 预检 ─→ 目标路径 ─ (2) workspace push ─→ 对象存储 ─→ 集群目录
                                                                              │
                                                          (3) API-13 注册 ────┘
                                                                              ▼
                                                      共享盘 venv.db(haienv 表) ─→ 任务 source haienv
```

---

## 9. 配置、灰度、回滚与可观测

### 9.1 配置

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `cloud.storage.service.env_path` | `/hf_shared` | env 家族父根;`env_root = {env_path}/hfai_envs` |
| `cloud.storage.service.env_push_enabled` | `true` | 灰度总开关 |
| `cloud.storage.service.env_push_enabled_users` | `[]` | 白名单(空=全量) |
| `cloud.storage.service.env_push_enabled_groups` | `[]` | 白名单(组) |
| `cloud.storage.service.env_name_regex` | `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` | 可收紧不可放宽到含 `/` |

### 9.2 灰度(OPS-02/AC-10)

`check_env_push_enabled(user)`:复用 `cloud_storage/service/context.py:check_feature_enabled` 的写法;关闭时两接口均返回 `FEATURE_DISABLED`,**不写库、不改文件**。

### 9.3 回滚(OPS-03/AC-11)

- 一级回滚:配置置 `env_push_enabled=false` + **重启 ugc_server 进程**(`supervisorctl restart ugc_server`,
  103 实测 4–5s;`CONF` 在进程启动时加载,不是热重载)→ 新请求被拒,已有环境不受影响;
  **数据面也必须被同一个开关挡住**(`sync_to_cluster` 的 `file_type=env` 分支,实现修正 N4),
  否则「关掉控制面」只是半次回滚;
- 演练脚本:`docs/haiplatform/scripts/env_rollback_drill.sh`(默认 8 条断言:三条写入路径全被拒 +
  `venv.db` md5/key 不变 + 已注册环境仍可读 + 恢复后可用 + `override.toml` 复原);
  `DRILL_L2=1` 时追加二级回滚演练(13 条断言:两条路由 404、workspace 路由不受影响、
  客户端给出「接口不存在」可读结论、恢复后 API-11 可用);
- 二级回滚:去掉 `api/register/implement.py` 的两行路由注册 → 客户端 push 在 ①/④ 报"接口不存在";
- **无脏数据**:注册表是既有 `venv.db` 的既有表,回滚不产生遗留表/列(HC-03)。

> 运维细节:override.toml 是**以文件 bind mount** 进 Pod 的
> (`one/hai-up.sh`: `${HAI_PLATFORM_PATH}/override.toml:/etc/hai_one_config/override.toml`)。
> `sed -i` 会 rename 出新 inode,容器内仍读旧内容 → **必须原地截断重写**(保持 inode),演练脚本已按此实现。

### 9.4 可观测(NFR-05)

| 指标 | 类型 | 标签 |
| --- | --- | --- |
| `env_push_requests_total` | Counter | `api`(`update_cluster_venv`/`register_cluster_venv`)、`result`、`code` |
| `env_register_duration_seconds` | Histogram | `result`(**成功与失败都观测**) |
| `env_registry_write_failures_total` | Counter | `reason`(permission/locked/import/assert/unknown) |
| `env_registry_read_failures_total` | Counter | `reason`(decode_all/decode_partial) — N3 新增 |

日志字段:`user`、`env`、`path`、`code`、`elapsed_ms`;`token=` 掩码沿用 `api/app.py:104-107`。
启动自检还会打印 `haienv_version`(ADR-E4 版本耦合可见)。

> `api` 标签取实现里的接口名(比设计初稿的 `update`/`register` 更明确,且 103 实测即如此);
> 103 无 Prometheus/Grafana,看板以 `docs/haiplatform/scripts/env_metrics.sh`(命令行汇总,
> 含成功率/P99/失败 reason 与 5% 阈值判定)交付,告警规则以配置即代码
> `docs/haiplatform/scripts/env_alerts.yml` 交付,生产集群有 Prometheus 时直接 `kubectl apply`。

---

## 10. 安全设计

| 面 | 措施 |
| --- | --- |
| 身份 | 只用 token(`get_ugc_user`);忽略 body/query 里的 username/group(SEC-01/HC-04) |
| 越权 | 注册 `path` 必须落在 `get_user_env_dir(user)` 之下并过 `check_is_subpath`(SEC-02/SEC-04) |
| 注入 | 名称白名单 + 拒绝 `..`/`/`/空白;不使用 shell 拼接服务端命令 |
| 权限 | 注册前写权限探测,失败返回明确 `code`(SEC-03) |
| SQLite | `flock` 串行化 + `REPLACE`;写后回读校验;不执行任何用户可控 SQL(表名固定) |
| 日志 | token 掩码;错误 msg 不含完整 token(SEC-05) |
| 客户端 | `list_haienv`/`set_env` 的 `-u` 参数做 `..` 与 `/` 校验(修 E9,SEC-06) |
| 传输 | 沿用 workspace 既有 STS/分片上传,不新增密钥面 |

---

## 11. 兼容性设计

| 场景 | 行为 |
| --- | --- |
| 老客户端(无 push) | 不调用新接口,零影响(CMP-01) |
| 客户端仍传 `FileType.ENV` 字面量 | 服务端 `normalize_enum` 能识别(`cloud_storage/service/compat.py:25-52`),但**客户端必须修**(CMP-02) |
| 私有 `custom.py` 已实现同名函数 | 实现放 `default.py`,私有层可覆盖(HC-08/ADR-4) |
| `haienv` 客户端/服务端版本不同 | 见 ADR-E4:**已实现**「失败安全」——① 整表读不出来 → `ENV_REGISTRY_READ_FAILED`,不写;② 单行读不出来 → 该名字 fail-closed,其余名字不受影响;③ API-11/API-13 返回 `haienv_version`,客户端基础版本不一致时提示,`HAIENV_STRICT_VERSION=1` 时直接中止(见 §11 下方) |
| 共享盘未挂载(纯对象存储部署) | API-13 无法写注册表 → 返回明确 `code`;API-14(P2)为后续方案 |
| `platform` 基础环境 | 不受影响(CMP-05) |

> **版本偏移的可操作口径(M3)**:注册表的值是 `pickle(protocol=4)` 的 `haienv.client.model.HaienvConfig`
> (ADR-E4),因此「镜像内 haienv 版本」与「客户端 haienv 版本」必须同基础版本(如都是 `1.4.1`)。
> 判定入口:启动日志的 `env path check: OK … haienv_version=…`、API-11 返回体、客户端提示。
> 完整端到端验证(CMP-04)见 `docs/haiplatform/env/env-server-test-report.md`。

---

## 12. 测试要点映射

| 用例组 | 覆盖 | 对应验收 |
| --- | --- | --- |
| U-组 单元 | `validate_env_name`、`derive_env_path`(后缀分配/复用)、`register_env`(幂等/越界/写失败)、路径函数 | AC-07 / AC-08 |
| A-组 接口 | API-11/API-13 的正常/边界/鉴权/灰度/旧形态 | AC-01 / AC-09 / AC-10 |
| P-组 路径一致性 | §3 的四方对照(配置、`get_base_path`、API-11、`HAIENV_PATH`) | AC-02 |
| E-组 E2E | create → push → 任务 source → import;重复 push;注册失败注入 | AC-03 / AC-04 / AC-05 / AC-06 |
| R-组 回归 | workspace 7 子命令全量回归(因改 `get_base_path` 与 `FileType`) | 回归矩阵 |
| S-组 安全 | 路径穿越、越权 path、token 缺失/过期、并发注册 | AC-07 |
| O-组 运维 | 权限探测、回滚演练、自检输出 | AC-10 / AC-11 |

> **R-组不可省**:`get_base_path` 与 `conf/utils.py` 是 workspace 主链路共用代码,必须跑 workspace 的既有 E2E(参考 `docs/haiplatform/scripts/e2e_workspace.sh`)。

---

## 13. 架构决策记录(ADR)

| ID | 决策 | 备选 | 理由 |
| --- | --- | --- | --- |
| **ADR-E1** | 统一约定为 `env_root = {env_path}/hfai_envs`,**改数据面** ENV 的 `cluster_base_path`,运行时 `HAIENV_PATH` 值不变 | 改运行时 `HAIENV_PATH` | 改运行时需同步改镜像 ENV、`one/release.sh`、任务脚本,影响面大;数据面 ENV 从未成功过,无历史包袱 |
| **ADR-E2** | 预检(API-11)与注册(API-13)**拆成两个接口**,由客户端在传输成功后显式调用 | 服务端在 `sync_to_cluster` 完成时自动注册 | 自动注册需在服务端保存"待注册元数据"的中间状态(Redis/DB),并把注册与传输耦合;显式调用无状态、可独立重试、与客户端 `sync_timeout` 解耦 |
| **ADR-E3** | 路径函数放 `conf/utils.py`(纯函数 + 惰性 `CONF`) | 放 `cloud_storage/service/env_paths.py` / `server_model/task_impl/env_resolver.py` | 两端都要用:`server_model` 顶层**禁止** import `cloud_storage.*`(`workspace_resolver.py` 已确立该纪律),`cloud_storage` 也不宜依赖 `server_model`;`conf` 是唯一无副作用的中立层 |
| **ADR-E4** | 注册表写入**复用镜像内 `haienv` 包**(惰性 import),而不是手写 pickle | 自定义序列化 / 直接 SQL | `SqliteDict` 用 `pickle protocol=4`,客户端 `loads` 时必须能 import 同一个 `HaienvConfig` 类;复用是唯一与 CMP-03 兼容的做法。代价是版本耦合 → 用"类签名探测 + 失败不写"兜底 |
| **ADR-E5** | API-11 增加**写权限探测**,不通过则提前失败 | 上传完再失败 | env 目录可能达 GB 级,失败必须前移;同时把"服务端写用户目录"的权限假设显式化 |
| **ADR-E6** | `env list / remove / config` **保持本地实现**,不服务端化 | 全部服务端化 | 它们读的是共享盘 SQLite,服务端化会引入第二套判据(违反 HC-02),且无收益 |
| **ADR-E7** | 实现放 `api/resource/storage/default.py`(可覆盖),不放 `implement.py` | 放 `implement.py` | 与 ADR-4 一致,保留私有层覆盖能力 |

---

## 14. WBS 与里程碑

| 阶段 | 内容 | 依赖 | 估时 |
| --- | --- | --- | --- |
| **S0 契约冻结** | 确认 Q-1(私有层是否已实现)、Q-2(路径方案)、错误码表 | — | 0.5 d |
| **S1 路径单点化** | `conf/utils.py` 5 个纯函数 + `get_base_path` ENV 分支 + `single_task_impl` 改用 + 自检 | S0 | 0.5 d |
| **S2 领域层** | `env_registry.py`:名称校验/路径推导/权限探测/注册/回读/锁 | S1 | 1.5 d |
| **S3 接入层** | 两个 `/ugc` 接口 + 路由注册 + 错误码 + 灰度 + 指标 | S2 | 1 d |
| **S4 客户端** | `env push` 子命令 + `push_venv` 重写(含 `.value` 与 workspace 可执行文件解析)+ 分级结果 | S0 | 1 d |
| **S5 联调** | 真实环境跑通 create→push→任务 source;注册失败注入;权限修复 | S3,S4 | 1 d |
| **S6 回归与文档** | workspace 7 子命令回归 + `ugc.rst`/`environment.md` 更新 | S5 | 0.5 d |
| **S7 灰度上线** | 开关、指标看板、Runbook、回滚演练 | S6 | 0.5 d |

**关键路径**:S0 → S1 → S2 → S3 → S5 → S6 → S7(≈ 6.5 人日)。

**里程碑**

| 里程碑 | 判定 |
| --- | --- |
| M1 契约可用 | S1–S3 完成:两个接口可被 curl 调通,返回结构与 §4 一致 |
| M2 端到端可用 | S5 完成:本地 push → 任务 `source haienv` → import 成功(AC-03/AC-04) |
| M3 可上线 | S6/S7 完成:回归全绿、开关与回滚演练通过、文档更新 |

---

## 15. 风险与开放问题

| ID | 风险/问题 | 等级 | 处置 |
| --- | --- | --- | --- |
| R-1 | 私有 `custom.py` 已实现 `/ugc/update_cluster_venv`,与本设计冲突 | 中 | S0 联调确认(Q-1);以私有实现为准,本设计退化为契约对齐 + 客户端修复 |
| R-2 | 服务端无权写用户 `venv.db`(目录 `755 <user>`) | 高 | ADR-E5 预检前移 + §4.4 修复路径 ① |
| R-3 | `haienv` 包版本偏移导致 pickle 不兼容 | 中 | ADR-E4:类签名/版本探测;失败不写并给出运维提示 |
| R-4 | 客户端/服务端同时写 `venv.db`(开发容器内用户手工 create 与 push 并发) | 低 | `flock` + `REPLACE`;极端情况以回读校验兜底 |
| R-5 | `get_base_path` 改动影响 workspace 主链路 | 中 | 只改 ENV 分支;R-组回归必跑 |
| R-6 | `extend=True` 环境不支持上传,用户预期落差 | 中 | CLI 明确报错 + 文档说明(FR-07/FR-10) |
| R-7 | 无共享盘部署(纯对象存储)不可用 | 低 | API-14(P2)预留 |
| R-8 | 集群 `env_path` 与本设计不一致的存量部署 | 中 | OPS-01 启动自检告警 + Q-2 决策 |

**开放问题**(与需求 §8 对应):Q-1 私有层现状、Q-2 路径方案、Q-3 失败语义、Q-4 是否允许覆盖同名环境、Q-5 API-14 是否本期、Q-6 是否同批修 workspace 侧 F2。

---

## 16. 附:相关代码索引

| 主题 | 位置 |
| --- | --- |
| 客户端命令 | `plugins/haienv/haienv/client/{cli,command,api,model,sqlite_dict}.py` |
| 激活器 | `plugins/haienv/haienv/haienv`、`plugins/haienv/haienv/haienv.py` |
| 上传库函数 | `client/api/venv_api.py` |
| ENV 传输分支 | `plugins/haiworkspace/haiworkspace/client/workspace_api.py:125-127`、`cloud_storage/service/sync_to_cluster.py:47` |
| 路径推导 | `cloud_storage/utils.py:467-473` |
| 接入层范式 | `api/resource/cloud_storage/default.py`、`api/resource/storage/default.py` |
| 鉴权 | `api/depends/implement.py:110-141` |
| 错误码 | `cloud_storage/service/errors.py` |
| 扩展点 | `base_model/utils.py:15-61`(`CustomFinder`)、`api/register/implement.py:4` |
| 任务侧 | `server_model/task_impl/single_task_impl.py:60-61,150-158`、`server_model/task_impl/workspace_resolver.py`(导入纪律范例) |
| 镜像/构建 | `one/build_cli.sh`、`one/release.sh:15-18`、`Dockerfile:73-77` |
