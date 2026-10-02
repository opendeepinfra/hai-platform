# HAI Platform · `hai-cli env`（haienv）服务端功能测试用例集

> **文档定位**:`docs/haiplatform/env/` 四件套之三(《[分析](hai-cli-env-analysis.md)》→《[需求](env-server-requirements.md)》→《[设计](env-server-design.md)》→ **用例**)。
> **被测对象**:`API-11 /ugc/update_cluster_venv`、`API-13 /ugc/register_cluster_venv`、`cloud_storage/service/env_registry.py`、`conf/utils.py` 的 env 路径函数、`cloud_storage/utils.py:get_base_path` 的 ENV 分支、客户端 `hai-cli env push` 与 `client/api/venv_api.py`。
> **不在范围**:`env create/list/remove/config` 的本地功能（非目标,需求 §1.3）;env 内容的 GC / 配额 / 审计（P2）。
> **前提**:接口号沿用 workspace 编号空间——`API-11` 为修订版,`API-13` 为新增,`API-14` 为 P2 预留;需求 ID 见 [env-server-requirements.md](env-server-requirements.md) §3;验收 ID 为 `AC-01~AC-12`。

---

## 1. 测试范围与策略

### 1.1 分层模型

| 层 | 名称 | 被测对象 | 依赖与环境 | 通过标准（准出） |
| --- | --- | --- | --- | --- |
| **L1** | 单元测试 | `cloud_storage/service/env_registry.py`（名称校验/路径推导/权限探测/注册/回读）、`conf/utils.py` 的 env 路径函数、`get_base_path` ENV 分支 | pytest + `pytest-asyncio`;**不启动 FastAPI**;`venv.db` 用 `tmp_path`;`haienv` 包以真实安装版本参与（**不 mock `HaienvConfig`**） | `env_registry` 行覆盖率 ≥ 85%;`validate_env_name` / 路径推导 / `check_is_subpath` 组合为纯函数 → **100% 分支覆盖**;`tmp_path` 用完即清 |
| **L2** | 接口契约测试 | `api/resource/storage/default.py` 的两条 `/ugc/*` 路由 + 全局错误改写（`api/app.py:225`） | 单机 `SERVER=ugc`,`uvicorn_server.py :8083`;`provider=localfs`;`env_path=/tmp/hai-test`;HTTP 客户端须能伪造 `Content-Type: text/plain` 的 JSON Body | 全部响应体含 `success`（HC-05）;`text/plain` / `application/json` / 裸 query 三种形态行为一致;错误码与设计 §4 表逐项一致 |
| **L3** | 端到端测试 | 真实 `hai-cli env push` + `haiworkspace push --file_type env` + `ugc-server` + 共享盘（`env_root`）+ 任务容器 `source haienv` | 客户端容器 + `ugc-server` + `operating-server/launcher` + 共享盘 + 对象存储（`localfs` 或真实 S3/OSS）;**客户端与服务端必须安装同一份 `haienv` 轮子** | 「本地造 env → `env push` → 任务 `source haienv` → `import` 环境内独有包」全链路无异常（AC-03/AC-04）;重复 push 上传字节数 = 0 |
| **L4** | 兼容与灰度测试 | 老客户端（无 `push`）、枚举串客户端（`file_type=FileType.ENV`）、灰度开关、私有 `custom.py` 覆盖、`haienv` 版本偏移 | 两套客户端二进制 + 两组服务端配置 + 一组「私有层已实现同名函数」的模拟部署 | 兼容矩阵（设计 §11）逐行成立;灰度外用户得到 `success=0 + FEATURE_DISABLED`;私有覆盖生效且本仓实现不被误用 |

### 1.2 策略要点

1. **客户端事实优先**:断言以客户端真实行为为准。凡「服务端看起来对但客户端会失败」的形态（枚举字面量、`sys.argv[0]` 拼错、`path=None`）一律判失败——分析报告 E2/E3/E7/E13 的教训。
2. **两条 provider 路径**:`localfs`（§2.1,必跑、离线可重复）与真实 S3/OSS（§2.2,发布前必跑一次）。除 STS 与分片行为外,两组断言必须等价。
3. **双向可判定**:每个用例同时给出「接口层可见结果」与「落盘 / `venv.db` 侧真实结果」。**只验响应不验副作用**正是现状桩函数能「返回 success=1」的原因（分析报告 §4.3）。
4. **路径一致性是本特性的头号风险**:`P` 组独立成组,并作为发布门禁（设计 ADR-E1、需求 AC-02）。
5. **注册表用真实 `haienv` 包**:`SqliteDict` 是 `pickle protocol=4`（`plugins/haienv/haienv/client/sqlite_dict.py`）,mock 掉 `HaienvConfig` 会让「客户端能否反序列化」这一核心断言失效（CMP-03）。
6. **不产生 GPU 依赖**:CI 不执行 `haienv create`（其硬校验 CUDA 11.1/11.3,`plugins/haienv/haienv/client/command.py:41-43`),统一用 fixture 直接构造 prefix 与 `venv.db`（§2.5）。

---

## 2. 测试环境与数据准备

### 2.1 环境拓扑（路径 1:`provider=localfs`,必跑）

| 组件 | 部署 | 关键配置 |
| --- | --- | --- |
| PostgreSQL | 复用 workspace 测试库(**本特性零 DDL**,HC-03) | 仅需 `user` / `user_group` 等既有表 |
| Redis | 本地 `:6379`,`db=2`(与 workspace 的 `db=1` 隔离) | 同步状态键前缀 `localfs` |
| ugc-server | `SERVER=ugc`,`uvicorn_server.py :8083` | `MARSV2_MANAGER_CONFIG_DIR=/etc/config` |
| 对象存储 | `provider=localfs`,`localfs_root=/tmp/hai-test/localfs` | 与 workspace 共用实现 |
| **env 根** | 本地目录 `/tmp/hai-test/hfai_envs` 充当 `env_root` | `cloud.storage.service.env_path = /tmp/hai-test`(设计 §3.1) |
| 任务侧变量 | `HAIENV_PATH=/tmp/hai-test/hfai_envs/<user>` | 由 `get_env_root()` 推导,禁止手写 |
| `haienv` 包 | 与客户端同一轮子 | `one/build_cli.sh` 产物;`pip install /tmp/haienv-*.whl` |

### 2.2 环境拓扑（路径 2:真实 S3/OSS,发布前必跑）

在 §2.1 基础上替换 `provider=s3`(或 `oss`)与真实端点/凭据。需额外验证:ENV 对象的**真实分片上传**(100 MB 阈值)、`activate`/`pip.conf` 的排除是否生效(`workspace_api.py:125-127`)、集群共享盘上的真实 `venv.db` 写入权限。用例范围 = SMOKE 集 + `TC-C*` + `TC-P*` + `TC-I01/I03`。

### 2.3 测试用户与权限

| 标识 | 角色 | 用途 | 关键属性 |
| --- | --- | --- | --- |
| `T_A` / U-A | 普通用户 | 主路径 | `env_root/U-A/` 目录属主 = U-A,**初始 755**(复现 R-2 权限问题) |
| `T_B` / U-B | 普通用户 | 越权/隔离 | 与 U-A 同 `shared_group` |
| `T_C` / U-C | 普通用户 | **无写权限场景** | `env_root/U-C/` 设为 `555`,供 `ENV_REGISTRY_NOT_WRITABLE` 用例 |
| `T_ADMIN` | ops | 运维视角 | 用于 `custom.py` 覆盖验证 |

服务端进程账号(平台账号)与 U-A **不是同一账号**,这是 §4.4 权限用例的前提;若测试机上平台账号 = 用户账号,须显式用 `sudo -u` 或容器拆分复现。

### 2.4 `[cloud.storage]` 配置样例

```toml
# /etc/config/override.toml —— provider=localfs(CI/本地)
[cloud.storage]
provider = 'localfs'

[cloud.storage.service]
env_path = '/tmp/hai-test'          # → env_root = /tmp/hai-test/hfai_envs
workspace_path = '/tmp/hai-test/workspace'
breakpoint_info_path = '/tmp/hai-test/breakpoints'
legacy_param_compat = true
env_push_enabled = true
env_push_enabled_users = []
env_push_enabled_groups = []
env_name_regex = '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'
```

### 2.5 env fixture 构造（替代 `haienv create`）

```bash
# 1) 目录骨架(模拟 conda prefix,只需 activate 与 lib/pythonX.Y/site-packages)
USER_ENV=/tmp/hai-test/hfai_envs/U-A
mkdir -p "$USER_ENV/myenv_0/lib/python3.8/site-packages"
printf 'export HF_ENV_NAME=myenv\n' > "$USER_ENV/myenv_0/activate"
chmod 777 "$USER_ENV"

# 2) 本地注册表(客户端侧),用真实 haienv 包写入,保证 pickle 兼容
python3 - <<'PY'
from haienv.client.model import Haienv, HaienvConfig
Haienv.insert(haienv_name='myenv',
              haienv_config=HaienvConfig(path='/tmp/hai-test/hfai_envs/U-A/myenv_0',
                                         extend='False', extend_env='', py='3.8'),
              outside_db_path='/tmp/hai-test/hfai_envs/U-A/venv.db')
PY

# 3) 断言 fixture 生效(source 可解析)
HAIENV_PATH=$USER_ENV bash -c 'source haienv myenv && echo OK'
```

> **D-1 基线**:`env_root` 已建、U-A 有 `myenv_0/` 且 `venv.db` 有 `myenv`、服务端已配 §2.4。以下用例中「基线环境」均指此。

### 2.6 环境内独有包(用于任务侧断言)

在步骤 1) 的 `site-packages` 放一个**唯一名字**的假包,避免与基础环境混淆:

```bash
PKG="$USER_ENV/myenv_0/lib/python3.8/site-packages/haienv_probe_unique"
mkdir -p "$PKG"; echo "VALUE = 'env-push-ok'" > "$PKG/__init__.py"
```

任务侧断言用 `python3 -c "import haienv_probe_unique as m; assert m.VALUE=='env-push-ok'"`。

---

## 3. 用例总览

| 组 | 名称 | 用例数 | 主要覆盖需求 | 层级分布 |
| --- | --- | --- | --- | --- |
| U | 单元（路径 / 校验 / 注册） | 14 | FR-05/07/08 · NFR-03/04 · HC-02/03 | L1 |
| A | 接口契约（API-11 / API-13） | 20 | FR-03/04/07/08/12 · SEC-01 · HC-05 | L1/L2 |
| P | 路径一致性 | 6 | FR-05 · AC-02 | L2/L3 |
| C | 客户端 push 链路 | 12 | FR-01/02/06 · AC-03/05/06 | L2/L3 |
| REG | 注册表 | 10 | FR-04/09 · CMP-03 · HC-02 | L1/L2/L3 |
| S | 安全 | 10 | SEC-01~SEC-06 | L2/L3 |
| F | 并发 / 幂等 / 故障 | 8 | NFR-03 · OPS-05 | L2/L3 |
| O | 兼容 / 配置 / 运维 | 10 | CMP-01~CMP-05 · OPS-01~OPS-05 · FR-12 | L4 |
| T | 任务侧 | 6 | FR-09/11 · AC-03/AC-04 | L1/L3 |
| L | 可观测性 | 5 | NFR-05 · SEC-05 | L2/L3 |
| I | 性能与容量 | 5 | NFR-01/02 · AC-05 | L2/L3 |
| **合计** | | **106** | 见 §12 追溯表 | |

> 另有 §5 的 **8 个端到端场景（E2E-01~E2E-08）** 与 §6 的 **8 条故障注入（FI-01~FI-08）**,由上述用例组合而成,不重复计数。

---

## 4. 详细用例

> 表头说明:`层级` = L1/L2/L3/L4;基础 URL 简写 `$API`;「基线环境」= §2.1 + §2.3 + §2.4 + §2.5-D1。

### 4.1 U 组 · 单元（TC-U01~TC-U14）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-U01 | P0 | L1 | `env_path=/tmp/hai-test` | 调 `get_env_root()` / `get_user_env_dir('U-A')` / `get_env_registry_path('U-A')` | → `/tmp/hai-test/hfai_envs` / `.../U-A` / `.../U-A/venv.db`;三者前缀一致(设计 §3.1) | FR-05 |
| TC-U02 | P0 | L1 | 同上 | `env_path` 含尾斜杠 / 相对路径 / 未配置(缺省) | → 归一化为绝对路径且无双斜杠;未配置时取默认 `/hf_shared`;不得因尾斜杠产出 `//` | FR-05 |
| TC-U03 | P0 | L1 | 纯函数 | `validate_env_name` 遍历合法集 `['a','myenv','my-env','a.b_c','A1']` 与非法集 `['','.','..','a/b','../x','/abs','a b','a'*65,'中文名','a\nb']` | → 合法集原样返回;非法集全部 `WorkspaceError(code=INVALID_PARAM)`;**`..` 与 `/` 必须拒绝** | FR-08, SEC-04 |
| TC-U04 | P0 | L1 | 基线环境 | `derive_env_path(U-A,'myenv',...)` | → 命中注册表,返回 `exists=true` 且 `path` == 注册表中的路径(复用,不新分配后缀) | FR-03 |
| TC-U05 | P0 | L1 | `env_root/U-A` 下存在 `new1_0/` | `derive_env_path(U-A,'new1',...)` | → 返回 `.../new1_1`,`exists=false`(后缀分配与客户端 `get_haienv_path` 一致,客户端 `model.py:140-163`) | FR-03 |
| TC-U06 | P0 | L1 | 目标目录不可写(`555`) | `derive_env_path(U-A,'x',...)` | → `WorkspaceError(ENV_REGISTRY_NOT_WRITABLE)`,**不得**返回成功(设计 ADR-E5) | FR-03, SEC-03 |
| TC-U07 | P0 | L1 | 基线环境 | `register_env(U-A,'myenv2',path='/tmp/hai-test/hfai_envs/U-A/myenv2_0','3.8',[],[],[])` | → 返回 `registered=true`;`venv.db` 中 `haienv['myenv2']` 存在且 `path/py/extend('False')` 正确 | FR-04 |
| TC-U08 | P0 | L1 | TC-U07 之后 | 重复调用 `register_env` 同参数 | → 仍 `success`;`venv.db` 中该 key **只有一条**(`REPLACE`);`SELECT COUNT(*)` 不增长 | NFR-03 |
| TC-U09 | P0 | L1 | 基线环境 | `register_env` 传 `path='/tmp/evil/myenv_0'`(不在用户目录下) | → `WorkspaceError(PATH_ESCAPE)` 或 `FORBIDDEN`;**`venv.db` 不被修改** | SEC-02, FR-08 |
| TC-U10 | P0 | L1 | 基线环境 | `register_env` 传 `path='/tmp/hai-test/hfai_envs/U-B/myenv_0'`(他人目录) | → 拒绝,且 U-B 的 `venv.db` 不存在/未变 | SEC-02 |
| TC-U11 | P0 | L1 | `venv.db` 只读(`444`) | `register_env` | → `WorkspaceError(ENV_REGISTRY_WRITE_FAILED)`,错误信息含目标路径,不含 token | SEC-03, SEC-05 |
| TC-U12 | P0 | L1 | 基线环境 | 用**客户端** `Haienv.select(outside_db_path=..., haienv_name='myenv2')` 读服务端写入的记录 | → 返回 `HaienvConfig`,字段与 TC-U07 写入一致(**跨进程 pickle 兼容**) | CMP-03 |
| TC-U13 | P0 | L1 | 已存在旧版 `venv`/`venv_config` 表的 DB | `register_env` 触发 `update_venv_to_haienv` 迁移(`model.py:82-100`) | → 迁移成功且新 key 写入 `haienv` 表;旧表数据不丢 | CMP-03 |
| TC-U14 | P1 | L1 | 基线与 `env_path` 不匹配的配置 | `env_registry_self_check()` | → 返回含 `ok=false` 与建议值;仅告警不抛异常(OPS-01) | OPS-01 |

### 4.2 A 组 · 接口契约（TC-A01~TC-A20）

#### 4.2.1 API-11 `POST /ugc/update_cluster_venv`（FR-03）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A01 | P0 | L2 | 基线环境 | `POST $API/ugc/update_cluster_venv?token=T_A&venv_name=myenv&py=3.8&extend=False` | → HTTP 200,`success=1`,`path` == `/tmp/hai-test/hfai_envs/U-A/myenv_0`,`exists=true`;响应体含 `success` 键 | API-11, HC-05 |
| TC-A02 | P0 | L2 | 基线环境 | **旧形态**:省略 `extend` 参数 | → 与 TC-A01 等价(`extend` 缺省 False),HTTP 200 | CMP-01 |
| TC-A03 | P0 | L2 | 基线环境 | 新名字 `venv_name=newenv` | → `exists=false`,`path` 以 `/newenv_0` 结尾,**且不创建任何目录**(预检只读) | FR-03 |
| TC-A04 | P0 | L2 | 基线环境 | `extend=True` | → `success=0`,`code=INVALID_PARAM`,`msg` 说明不支持扩展环境 | FR-07 |
| TC-A05 | P0 | L2 | 基线环境 | `venv_name` 分别取 `''`、`../x`、`a/b`、`a b`、`'a'*65` | → 五者均 `success=0` + `INVALID_PARAM`,HTTP 200,且**无副作用** | FR-08 |
| TC-A06 | P0 | L2 | U-C 目录 `555` | 用 `T_C` 调用 | → `success=0`,`code=ENV_REGISTRY_NOT_WRITABLE`,`msg` 含运维提示 | SEC-03, ADR-E5 |
| TC-A07 | P0 | L2 | 基线环境 | 缺 `token` / 传过期 token / 传不活跃用户 token | → 三者均 `success=0` + `UNAUTHORIZED`,HTTP 401 或 403(与 workspace 一致) | SEC-01 |
| TC-A08 | P1 | L2 | 基线环境 | 同一请求重复 3 次 | → 3 次响应**完全相同**(含 `path`),无时间戳/随机后缀 | NFR-03 |
| TC-A09 | P1 | L2 | `legacy_param_compat=true` | `py` 缺省 / `extend='false'`(小写字符串) | → 前者按默认 py 处理;后者归一化为 False → `success=1`(兼容层 `compat.py:25-52`) | CMP-02 |
| TC-A10 | P1 | L2 | 灰度 `env_push_enabled=false` | 调用 | → `success=0` + `FEATURE_DISABLED`;不写任何文件 | FR-12, OPS-02 |

#### 4.2.2 API-13 `POST /ugc/register_cluster_venv`（FR-04）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A11 | P0 | L2 | 基线环境 + 目录 `myenv2_0/` 已存在 | `POST $API/ugc/register_cluster_venv?token=T_A`,body(JSON):`{"venv_name":"myenv2","path":".../U-A/myenv2_0","py":"3.8","extra_search_dir":[],"extra_search_bin_dir":[],"extra_environment":[]}` | → HTTP 200,`success=1`,`registered=true`,`db` == `/tmp/hai-test/hfai_envs/U-A/venv.db` | API-13 |
| TC-A12 | P0 | L2 | 同 TC-A11 | body 三种 Content-Type:`text/plain` / `application/json` / 省略 | → 三者结果等价(兼容层 `parse_json_body`) | CMP-02 |
| TC-A13 | P0 | L2 | 基线环境 | `venv_name` 非法(`../x`)、`path` 缺失、`py` 缺失 | → `success=0` + `INVALID_PARAM`,逐项给出可读 `msg`;`venv.db` 不变 | FR-08 |
| TC-A14 | P0 | L2 | 基线环境 | `path='/tmp/hai-test/hfai_envs/U-B/myenv_0'`(**他人**目录) | → `success=0` + `FORBIDDEN`/`PATH_ESCAPE`;U-B 的 `venv.db` 未被创建或未被修改 | SEC-02 |
| TC-A15 | P0 | L2 | 基线环境 | `path='/tmp/evil'`、`path='../../etc'`、`path` 含 URL 编码的 `%2e%2e/` | → 三者均拒绝;错误码 `PATH_ESCAPE`(校验前已解码) | SEC-04 |
| TC-A16 | P0 | L2 | U-C 目录 `555` | 用 `T_C` 注册 | → `success=0` + `ENV_REGISTRY_WRITE_FAILED`(非 500 裸错误) | SEC-03, HC-05 |
| TC-A17 | P0 | L2 | TC-A11 之后 | 重复提交同一 body | → `success=1`,`registered=true`,记录数不增长;响应字段稳定 | NFR-03 |
| TC-A18 | P0 | L2 | 基线环境 | 传 `extra_search_dir=['/opt/x','/opt/y']` 等三个列表 | → 注册表 `HaienvConfig` 三字段与入参逐项相等(**列表不得被字符串化**) | FR-04 |
| TC-A19 | P1 | L2 | 灰度关闭 | 调用 | → `FEATURE_DISABLED`,不写库 | FR-12 |
| TC-A20 | P1 | L2 | 基线环境 | 缺 token / 过期 token | → `UNAUTHORIZED`,HTTP 401/403,响应体含 `success` | SEC-01 |

### 4.3 P 组 · 路径一致性（TC-P01~TC-P06）

**本组是 AC-02 的判定依据,发布门禁必跑。**

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-P01 | P0 | L1/L2 | 基线环境 | 对同一 `(user='U-A', group, name='myenv')` 分别取:①`get_base_path(...)[0]`(cluster)②API-11 的 `path` ③`f'{get_env_root()}/U-A'` | → ①的 dirname == ③,且 ② 在 ③ 之下;**三者同源** | FR-05, AC-02 |
| TC-P02 | P0 | L2 | 基线环境 | 读 `cloud_base_path`(S3 key) | → 仍为 `<group>/shared/hfai_envs/U-A/myenv`(S3 布局不变,设计 §3.3) | CMP-05 |
| TC-P03 | P0 | L2 | `env_path` 改为 `/hf_shared`(生产取值) | 重复 TC-P01 | → 三者仍一致(不依赖具体取值) | FR-05 |
| TC-P04 | P0 | L2 | 基线环境 | 触发启动自检 | → `get_env_root()` 与 `get_base_path` 前缀一致时打印 `env path check: OK`;人为制造不一致 → 打印 ERROR + 建议值且**不阻断启动** | OPS-01 |
| TC-P05 | P0 | L1 | 纯函数 | `check_is_subpath(env_root, '{env_root}/U-A/x')` 与 `check_is_subpath(env_root, '{env_root}/../etc')` | → 前者通过,后者抛 `ClientException`(`cloud_storage/utils.py:434-442`) | SEC-04 |
| TC-P06 | P1 | L3 | 真实部署 | 在真实共享盘上执行 §2 路径四方对照(配置 / `get_base_path` / API-11 / `$HAIENV_PATH`) | → 四方一致;并记录实际取值到测试报告(替代「看代码推断」) | AC-02 |

### 4.4 C 组 · 客户端 push 链路（TC-C01~TC-C12）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-C01 | P0 | L2 | 基线环境 | `hai-cli env --help` | → 帮助中出现 `push` 子命令与全部参数;`env push --help` 无 `-h` 报错 | FR-01 |
| TC-C02 | P0 | L2 | 基线环境 | `hai-cli env push myenv --provider localfs` | → 依次发起 API-11 → `haiworkspace push --file_type env` → API-13;退出码 0;**日志中不出现 `haienv workspace push`**(E13 回归) | FR-01, FR-02 |
| TC-C03 | P0 | L2 | 基线环境 | 抓取 `haiworkspace push` 的实际命令行 | → `--file_type` 的值为字面量 `env`(**不是 `FileType.ENV`**);`--env_local_path` 为本地 prefix;`--env_remote_path` 为 API-11 返回值 | FR-02 |
| TC-C04 | P0 | L2 | 不存在的环境名 | `hai-cli env push not_exist` | → 本地即失败(不发起任何网络请求),提示"未找到名为…的虚拟环境",退出码 1 | FR-06 |
| TC-C05 | P0 | L2 | `extend='True'` 的环境 | `hai-cli env push ext_env` | → 本地拒绝,提示"暂不支持上传 extend 模式的 venv",退出码 1,**不发起 API-11** | FR-07 |
| TC-C06 | P0 | L2 | API-11 返回 `path=None` | mock 服务端返回(或指向桩) | → 客户端明确报错并退出 1,**不**执行 `workspace push`;不抛 `KeyError` 堆栈 | FR-06(E7 回归) |
| TC-C07 | P0 | L2 | 上传阶段失败(如 provider 不可达) | `env push` | → 输出"上传失败:<原因>",退出码 1;**不调用 API-13** | FR-06 |
| TC-C08 | P0 | L2 | 上传成功、注册失败(服务端返回 `ENV_REGISTRY_WRITE_FAILED`) | `env push` | → 输出区分"**已上传但注册失败,可重试**",退出码 1;集群目录与对象**保留**(NFR-06) | FR-06, AC-06 |
| TC-C09 | P0 | L3 | TC-C08 之后修复权限 | 重新 `env push` | → 打印"数据已同步,忽略本次操作";仅重试注册;最终 `success=1`;上传字节数 = 0 | FR-06, AC-05 |
| TC-C10 | P1 | L2 | 基线环境 | `--no_zip --no_diff --force --proxy=...` 组合各传一次 | → 参数正确透传到 `haiworkspace push`(逐项核对命令行) | FR-01 |
| TC-C11 | P1 | L2 | 基线环境 | 检查 `/tmp/<name>.zip` 是否残留 | → 成功与失败路径均不残留临时 zip(`workspace_util.py:438,445`) | NFR-06 |
| TC-C12 | P1 | L3 | 真实 S3 | 完整 push 一个含 3 万小文件 + 1 个 1.2 GB 文件的 env | → 分片上传生效;`activate`/`pip.conf` **未上传**(排除项,`workspace_api.py:125-127`);结束后注册成功 | FR-04 |

### 4.5 REG 组 · 注册表（TC-REG-01~TC-REG-10）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-REG-01 | P0 | L1 | 空 `venv.db` | `register_env` 首次写入 | → 自动 `CREATE TABLE IF NOT EXISTS "haienv"`;`sqlite_master` 中表名正确 | FR-04 |
| TC-REG-02 | P0 | L2 | TC-REG-01 | `sqlite3 <db> "select key from haienv"` | → 含 `myenv`;**value 为 BLOB**(pickle),非 TEXT/JSON | HC-02 |
| TC-REG-03 | P0 | L1 | 基线环境 | 服务端写入后,客户端 `Haienv.select` 读取 | → 字段全等(TC-U12 的端到端版) | CMP-03 |
| TC-REG-04 | P0 | L2 | 基线环境 | 客户端 `hai-cli env list` | → 新注册环境出现在"自己创建的环境"表;列 `user/haienv_name/path/extend/extend_env/py` 与注册值一致 | FR-09, AC-04 |
| TC-REG-05 | P0 | L2 | 基线环境 | 用 `T_B` 调 API-14(P2 若实现)/直接读共享盘 | → 只能读到 `env_root/U-B/venv.db`;不得返回 U-A 的内容 | SEC-02 |
| TC-REG-06 | P0 | L1 | 基线环境 | 服务端写入后立刻“**回读校验**”失败注入(写入后篡改 path) | → 服务端检出不一致,返回 `ENV_REGISTRY_WRITE_FAILED`,不回 `success=1` | 设计 §5.2 |
| TC-REG-07 | P0 | L1 | 并发:两个线程同时对同一用户注册不同 env | 同时执行 | → 两条记录均写入成功;无 `database is locked` 泄漏到响应(失败也要有明确 code) | R-4, NFR-03 |
| TC-REG-08 | P1 | L1 | 并发:同 env 同 path 两次注册 | 同时执行 | → 记录数 = 1;无异常 | NFR-03 |
| TC-REG-09 | P1 | L2 | `haienv` 包版本被替换为不兼容版本 | 触发注册 | → 返回 `ENV_REGISTRY_WRITE_FAILED`;**不得**写入不可反序列化的记录(ADR-E4) | R-3, CMP-03 |
| TC-REG-10 | P1 | L2 | 基线环境 | `register_env` 传 `py=''` | → 明确 `INVALID_PARAM` 或按默认值处理,行为在文档中固定(不可静默写入空 py) | FR-04 |

### 4.6 S 组 · 安全（TC-S01~TC-S10）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-S01 | P0 | L2 | 基线环境 | 请求体/查询串伪造 `username=U-B&group=x`(仅 `token=T_A`) | → 忽略伪造字段,按 T_A 处理 | SEC-01, HC-04 |
| TC-S02 | P0 | L2 | 基线环境 | API-13 传 `path` 指向 U-B(TC-A14) | → 拒绝,且 U-B `venv.db` mtime 不变 | SEC-02 |
| TC-S03 | P0 | L2 | 基线环境 | `path` 遍历:`.../U-A/../../U-B/x`、`.../U-A/%2e%2e/U-B`、`....//U-A` | → 全部拒绝(`PATH_ESCAPE`) | SEC-04 |
| TC-S04 | P0 | L2 | 基线环境 | `venv_name` 注入:`a';DROP TABLE haienv;--`、`a" OR 1=1` | → `INVALID_PARAM`;表结构不变(**无用户可控 SQL 拼接**) | SEC-04 |
| TC-S05 | P0 | L2 | U-C 无权限 | 注册 | → `ENV_REGISTRY_WRITE_FAILED`,响应不含服务器绝对路径以外的敏感信息 | SEC-03 |
| TC-S06 | P0 | L2 | 基线环境 | 检查服务端 stdout/日志 | → `token=` 被掩码为 `token=***`;`msg` 中无完整 token | SEC-05 |
| TC-S07 | P0 | L2 | 客户端本机 | `hai-cli env list -u '../../etc'`、`set_env('x[../../etc]')` | → 客户端在拼路径前拒绝并给出可读错误(修 E9) | SEC-06 |
| TC-S08 | P1 | L2 | 基线环境 | 用 A 的 token 注册,但 body 中 `path` 指向 A 之外**且**含符号链接(软链指向 U-B) | → 拒绝;`check_is_subpath` 走 `resolve()` 语义 | SEC-02, SEC-04 |
| TC-S09 | P1 | L2 | 基线环境 | 超大 body(>1 MB) / 超长 `extra_environment` 列表(1000 项) | → 明确 `INVALID_PARAM`/`PAYLOAD_TOO_LARGE`,不 OOM、不超时 | SEC-04 |
| TC-S10 | P1 | L2 | 基线环境 | API-13 在响应中回显请求 `path` | → 允许(客户端需要),但**不得**回显 token 或其他用户路径 | SEC-05 |

### 4.7 F 组 · 并发 / 幂等 / 故障（TC-F01~TC-F08）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-F01 | P0 | L2 | 基线环境 | 同一用户并发 5 次 `env push`(不同 env) | → 全部成功或全部给出明确 code;`venv.db` 5 条记录完整;无记录丢失 | NFR-03 |
| TC-F02 | P0 | L2 | 基线环境 | 同一 (env,path) 并发 5 次注册 | → 记录数 = 1;所有响应一致 | NFR-03 |
| TC-F03 | P0 | L2 | 基线环境 | 注册进行中强杀 ugc-server(多 worker `ugc=2`) | → 重启后 `venv.db` 处于一致状态(要么有记录要么无);无半写记录 | OPS-05 |
| TC-F04 | P0 | L3 | 传输 stage2 进行中 | 客户端 `Ctrl-C` 中断 | → 集群目录不完整但**未注册**,`source haienv` 找不到(安全失败);重跑 push 可续传 | NFR-06 |
| TC-F05 | P0 | L2 | 上传成功、注册超时 | 客户端超时后重试 | → 幂等成功;不产生重复记录 | FR-06 |
| TC-F06 | P1 | L1 | `venv.db` 被 `flock` 占用 | 注册 | → 等待或返回明确 code;不得静默失败仍回 `success=1` | R-4 |
| TC-F07 | P1 | L2 | `env_root` 所在文件系统只读 | API-11 预检 | → `ENV_REGISTRY_NOT_WRITABLE`(前移失败) | ADR-E5 |
| TC-F08 | P1 | L2 | 服务端重启(未完成注册) | 重启后重试 `env push` | → 走"数据已同步",仅补注册,成功 | OPS-05 |

### 4.8 O 组 · 兼容 / 配置 / 运维（TC-O01~TC-O10）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-O01 | P0 | L4 | 老客户端二进制(无 `env push`) | 跑完整 workspace 流程 + 任务提交 | → 零回归;不调用新接口 | CMP-01 |
| TC-O02 | P0 | L4 | 客户端仍发送 `file_type=FileType.ENV` 字面量 | 直接调 `sync_to_cluster` | → 服务端 `normalize_enum` 归一化为 `FileType.ENV`;链路可用(但正式路径仍要求客户端修 FR-02) | CMP-02 |
| TC-O03 | P0 | L4 | 模拟部署:私有 `api/resource/storage/custom.py` 定义同名 `update_cluster_venv` | 调 API-11 | → **私有实现生效**,本仓 `default.py` 版本被覆盖(HC-08) | HC-08, ADR-E7 |
| TC-O04 | P0 | L4 | 灰度 `enabled_users=['U-B']` | U-A / U-B 各调一次 | → U-B 成功;U-A `FEATURE_DISABLED` | FR-12, OPS-02 |
| TC-O05 | P0 | L2 | 配置 `env_path` 缺失 | 启动 + 调 API-11 | → 自检告警取默认 `/hf_shared`;接口不 500 | OPS-01 |
| TC-O06 | P1 | L2 | `env_name_regex` 收紧为 `^[a-z][a-z0-9_]{0,15}$` | 用 `MyEnv` / 长名调用 | → 按配置拒绝(`INVALID_PARAM`),说明可配置生效 | FR-08 |
| TC-O07 | P1 | L2 | 运维手册条目 | 按手册手工修复某用户 `venv.db`(删错误 key) | → 步骤可复现;`env list` 与 `source haienv` 结果随之一致 | OPS-04 |
| TC-O08 | P1 | L2 | 一键关闭开关 | 关闭 → 重启 ugc-server | → 接口返回 `FEATURE_DISABLED`;已注册环境仍可 `source` | OPS-02, OPS-03 |
| TC-O09 | P1 | L2 | 移除两行路由注册 | 重启 | → `/ugc/update_cluster_venv` 404;客户端报明确错误;无脏数据 | OPS-03, AC-11 |
| TC-O10 | P1 | L2 | 平台基础环境 `platform/hai202207_0` | 任务 `source haienv hai202207` | → 不受本次改动影响(基础环境仍可发现) | CMP-05 |

### 4.9 T 组 · 任务侧（TC-T01~TC-T06）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-T01 | P0 | L1 | — | 检查 `single_task_impl.sys_environments['HAIENV_PATH']` | → `== get_env_root()/<user>`,与数据面同源(不再是硬编码字面量) | FR-05 |
| TC-T02 | P0 | L3 | 完成一次成功 push | 提交任务 `HF_ENV_NAME=myenv` | → 任务启动脚本含 `source haienv myenv`;`echo $HAIENV_PATH` 指向 `env_root/U-A` | FR-09 |
| TC-T03 | P0 | L3 | TC-T02 | 任务内 `python3 -c "import haienv_probe_unique as m; assert m.VALUE=='env-push-ok'"` | → 通过(环境真正生效,而非仅 `source` 未报错) | AC-03 |
| TC-T04 | P0 | L3 | 跨用户:`HF_ENV_OWNER=U-B` | 提交任务 | → 解析为 `source haienv <name> -u U-B`,命中 U-B 的注册表 | FR-09 |
| TC-T05 | P0 | L3 | 未注册的环境名 | 提交任务 | → 任务启动打印可诊断信息(env 名 / owner / `HAIENV_PATH`)而非仅 `no valid env found`;任务不因诊断信息失败 | FR-11 |
| TC-T06 | P1 | L3 | 兼容老环境名 `py38-202207` | 提交任务 | → 仍走 `single_task_impl.py:154-157` 的改写分支,行为不变 | CMP-01 |

### 4.10 L 组 · 可观测性（TC-L01~TC-L05）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-L01 | P0 | L2 | 基线环境 | 成功 + 失败各调一次两个接口 | → `env_push_requests_total` 按 `api/result/code` 正确累加 | NFR-05 |
| TC-L02 | P0 | L2 | 注册成功 | 观察直方图 | → `env_register_duration_seconds` 有观测值 | NFR-05 |
| TC-L03 | P0 | L2 | 注入写失败 | 观察计数 | → `env_registry_write_failures_total{reason=permission}` +1 | NFR-05 |
| TC-L04 | P0 | L2 | 基线环境 | 检查日志字段 | → 含 `user/env/path/code/elapsed_ms`;`token` 掩码 | SEC-05 |
| TC-L05 | P1 | L2 | 大量请求 | 观察日志量 | → 不因逐请求 INFO 打爆日志(降为 DEBUG 或采样) | NFR-05 |

### 4.11 I 组 · 性能与容量（TC-I01~TC-I05）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-I01 | P0 | L2 | 基线环境 | 对 API-11 压测 200 QPS × 60 s | → P99 < 100 ms;无 5xx;`env` 目录不增长(只读预检) | NFR-01 |
| TC-I02 | P0 | L2 | 基线环境 | 对 API-13 压测 100 QPS × 60 s(不同 env 名) | → P99 < 300 ms;`venv.db` 记录数 == 成功请求数;无 `database is locked` | NFR-02 |
| TC-I03 | P0 | L3 | 1.2 GB env | 完整 push | → 成功;注册耗时 < 300 ms;总时长与 workspace 同量级 | NFR-02 |
| TC-I04 | P1 | L2 | 单用户 200 个 env | `derive_env_path` 遍历 | → 响应仍 < 100 ms(不得全量扫描大目录;必要时按注册表短路) | NFR-01 |
| TC-I05 | P1 | L2 | 注册表 5 MB / 1000 条 | 注册 + 客户端 `env list` | → 两端耗时在可接受范围(< 1 s);无超时 | NFR-02 |

---

## 5. 端到端场景（E2E）

| ID | 优先级 | 场景 | 步骤 | 通过判据 |
| --- | --- | --- | --- | --- |
| **E2E-01** | P0 | 本地首次 push → 任务生效(**主场景**) | ①按 §2.5 造 `myenv`(含探针包)②`hai-cli env push myenv --provider localfs` ③集群侧 `env list` ④提交任务 `HF_ENV_NAME=myenv` ⑤任务内 import 探针包 | ②退出码 0 且输出"上传并注册成功";③出现该环境;⑤断言通过(AC-03/AC-04) |
| **E2E-02** | P0 | 增量 push(第二次 0 字节) | 紧接 E2E-01 再 push 一次 | 打印"数据已同步,忽略本次操作";上传字节数 = 0;注册记录数 = 1(AC-05) |
| **E2E-03** | P0 | 修改环境内容后再 push | 在 prefix 内新增一个文件 → push | 仅新文件被上传;注册不重复;任务侧可见新文件 |
| **E2E-04** | P0 | 注册失败后重试 | 先把 `env_root/U-A` 置 `555` → push(应上传成功 + 注册失败);恢复 `777` → 再 push | 第一次:提示"已上传但注册失败,可重试";第二次:仅补注册即成功(AC-06) |
| **E2E-05** | P0 | 多用户隔离 | U-A 与 U-B 各 push 同名 `myenv` | 两份互不覆盖;`env_root/U-A/venv.db` 与 `U-B/venv.db` 各自记录;任务按 owner 解析正确 |
| **E2E-06** | P0 | 中断恢复 | push 到 stage1 中途 `Ctrl-C` → 重新 push | 重跑成功;无重复对象(checksum/tagging 命中);集群目录完整 |
| **E2E-07** | P0 | 灰度 + 老客户端 | 灰度仅放行 U-B;U-A 用老客户端 | U-A 老客户端零回归;U-A 新客户端 `FEATURE_DISABLED`;U-B 全链路成功(AC-10) |
| **E2E-08** | P1 | 大环境(3 万文件 + 1.2 GB) | 完整 push → 任务生效 | 分片上传与排除项生效;注册成功;全链路总时长记录在案 |

---

## 6. 异常与故障注入矩阵

| ID | 注入点 | 手法 | 期望行为 | 关联用例 |
| --- | --- | --- | --- | --- |
| FI-01 | API-11 预检写权限 | `chmod 555 env_root/U-C` | `ENV_REGISTRY_NOT_WRITABLE`,**不进入上传** | TC-A06, TC-F07 |
| FI-02 | 注册写库 | `chmod 444 venv.db` | `ENV_REGISTRY_WRITE_FAILED`;客户端区分"已上传未注册" | TC-A16, TC-C08 |
| FI-03 | 注册并发 | 两进程同时注册同用户不同 env | 均成功或明确 code;无丢失 | TC-REG-07 |
| FI-04 | `haienv` 包损坏 | 替换为不兼容版本 | 失败且不写坏 DB | TC-REG-09 |
| FI-05 | 传输中断 | stage1 中断 / stage2 超时 | 未注册 → `source haienv` 安全失败;可重跑 | TC-F04 |
| FI-06 | 服务端重启 | 注册前后各杀一次 ugc-server | 无半写记录;可重试补齐 | TC-F03, TC-F08 |
| FI-07 | 路径穿越 | 构造 `..`/软链/编码绕过 | 全部拒绝且无副作用 | TC-S03, TC-S08 |
| FI-08 | 灰度关闭 | 动态改配置 | `FEATURE_DISABLED`,已注册环境不受影响 | TC-O08 |

---

## 7. 优先级与回归矩阵

### 7.1 集合定义

| 集合 | 内容 | 触发时机 |
| --- | --- | --- |
| **SMOKE** | TC-U01/U03/U04、TC-A01/A03/A04/A07/A11/A13/A14、TC-P01、TC-C02/C03/C04/C08、TC-T01、TC-O01 | 每次构建 |
| **REG(回归)** | SMOKE + U 组全部 + A 组全部 + P 组全部 + C 组全部 + REG 组全部 + S 组全部 | 每个 PR |
| **RELEASE** | REG + F 组 + O 组 + T 组 + L 组 + I 组 + 全部 E2E + FI 矩阵 | 发布前 |

### 7.2 workspace 回归(不可省)

因改动落在 `conf/utils.py` 与 `cloud_storage/utils.py:get_base_path`(**workspace 主链路共用**),RELEASE 集必须串跑 workspace 既有 E2E:

```bash
bash docs/haiplatform/scripts/e2e_workspace.sh   # 期望 19/19
bash docs/haiplatform/scripts/smoke_ugc.sh       # 期望 8/8
```

---

## 8. 缺陷分级

| 级别 | 判定 | 示例 |
| --- | --- | --- |
| **致命(Critical)** | 数据损坏 / 越权 / 不可恢复 | `venv.db` 被写坏导致 `source haienv` 全面失效;跨用户写入成功;路径穿越成功 |
| **严重(Major)** | 主链路不可用或与设计契约不符 | 路径三方不一致(AC-02 失败);上传成功但注册静默失败仍报成功;"已上传未注册"无法区分 |
| **一般(Minor)** | 非主链路、有绕行 | `extra_search_dir` 顺序变化;`exists` 字段在个别分支缺失;日志字段不全 |
| **轻微(Trivial)** | 文案/体验 | `msg` 措辞、帮助文本、指标命名 |

**准出**:致命/严重 = 0;一般 ≤ 2 且均有绕行方案;性能门槛(NFR-01/02)全部达标。

---

## 9. 需求追溯反向表

| 需求 | 用例 |
| --- | --- |
| FR-01（`env push` 入口） | TC-C01, TC-C02, TC-C10 |
| FR-02（`.value` 修复） | TC-C02, TC-C03 |
| FR-03（API-11 预检） | TC-U04~U06, TC-A01~A06, TC-A08~A10 |
| FR-04（API-13 注册） | TC-U07~U13, TC-A11~A19, TC-REG-01~03, TC-REG-10 |
| FR-05（路径对齐） | TC-U01/U02, TC-P01~P06, TC-T01 |
| FR-06（分级与幂等） | TC-C04, TC-C06~C09, TC-F05 |
| FR-07（拒绝 extend） | TC-A04, TC-C05 |
| FR-08（名称/路径校验） | TC-U03, TC-A05, TC-A13, TC-O06, TC-S03/S04 |
| FR-09（可见性） | TC-T02~T04, TC-REG-04 |
| FR-10（文档） | 文档评审（见 Checklist DOC） |
| FR-11（任务侧诊断） | TC-T05 |
| FR-12（灰度） | TC-A10, TC-A19, TC-O04, TC-O08 |
| NFR-01（API-11 性能） | TC-I01, TC-I04 |
| NFR-02（API-13 性能） | TC-I02, TC-I03, TC-I05 |
| NFR-03（幂等） | TC-U08, TC-A08, TC-A17, TC-REG-07/08, TC-F01/F02 |
| NFR-04（不阻塞事件循环） | TC-I01/I02 期间的 `/ugc/get_sync_status` 并发可用性 |
| NFR-05（指标） | TC-L01~L03, TC-L05 |
| NFR-06（不回滚） | TC-C08/C09/C11, TC-F04 |
| SEC-01 | TC-A07, TC-A20, TC-S01 |
| SEC-02 | TC-U09/U10, TC-A14, TC-S02, TC-S08 |
| SEC-03 | TC-U06/U11, TC-A06/A16, TC-S05 |
| SEC-04 | TC-U03, TC-A15, TC-P05, TC-S03/S04/S09 |
| SEC-05 | TC-U11, TC-S06, TC-S10, TC-L04 |
| SEC-06 | TC-S07 |
| OPS-01 | TC-U14, TC-P04, TC-O05 |
| OPS-02/03 | TC-O04, TC-O08, TC-O09 |
| OPS-04 | TC-O07 |
| OPS-05 | TC-F03, TC-F08, TC-L03 |
| CMP-01 | TC-A02, TC-O01, TC-T06 |
| CMP-02 | TC-A09, TC-A12, TC-O02 |
| CMP-03 | TC-U12/U13, TC-REG-02/03/09 |
| CMP-04 | TC-C03 |
| CMP-05 | TC-P02, TC-O10 |
| HC-02 | TC-REG-02, TC-REG-04 |
| HC-03 | 全组（无 DDL）:`db_schemas/` 无新增文件 |
| HC-05 | TC-A01, TC-A07, TC-A16, TC-A20 |
| HC-08 | TC-O03 |
| AC-01 | TC-A01, TC-A11 |
| AC-02 | TC-P01~P06 |
| AC-03 / AC-04 | E2E-01, TC-T02~T04, TC-REG-04 |
| AC-05 | E2E-02, TC-C09 |
| AC-06 | E2E-04, TC-C08 |
| AC-07 | TC-U03/U09, TC-A05/A14/A15 |
| AC-08 | TC-A04, TC-C05 |
| AC-09 | TC-A02, TC-A12, TC-O01/O02 |
| AC-10 | E2E-07, TC-A10/A19, TC-O04 |
| AC-11 | TC-O09 |
| AC-12 | Checklist DOC 阶段 |

---

## 10. 与需求文档 §6 验收标准的对应

| 验收 | 本文件判定位置 |
| --- | --- |
| AC-01 契约一致 | §4.2 全部 |
| AC-02 路径三方一致 | §4.3（发布门禁） |
| AC-03 / AC-04 端到端与可见性 | §5 E2E-01 + §4.9 |
| AC-05 幂等 | §5 E2E-02 + TC-C09 |
| AC-06 失败分级 | §5 E2E-04 + TC-C08 |
| AC-07 安全 | §4.6 |
| AC-08 拒绝 extend | TC-A04 / TC-C05 |
| AC-09 兼容 | §4.8 TC-O01/O02 + TC-A02/A12 |
| AC-10 灰度 | E2E-07 + TC-O04 |
| AC-11 回滚 | TC-O09 |
| AC-12 文档 | Checklist DOC 阶段 |
