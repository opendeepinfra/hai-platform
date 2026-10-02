# hai-cli env 服务端实现需求说明

> **文档定位**:`docs/haiplatform/env/` 三件套之二(分析 → **需求** → 设计)。
> **前置阅读**:[hai-cli-env-analysis.md](hai-cli-env-analysis.md)。
> **编号约定**:沿用 `../workspace/` 系列的编号空间——接口号从 `API-11` 续起(与 workspace 的 API-11 同一接口、语义扩展),新增接口用 `API-13` 起,避免与 workspace 的 `API-12`(`/ugc/cloud_storage/usage`)冲突;需求号用 `FR/ NFR/ SEC/ OPS/ CMP/ HC`,与 workspace 文档不共享编号。
> **追溯**:每条 `FR` 都能在 [env-server-design.md](env-server-design.md) 与后续用例文档中找到落点,见 §11。

---

## 1. 背景与目标

### 1.1 现状(摘自分析报告)

- 客户端 `haienv` 插件**本地闭环完整**(create / list / remove / config / source 激活 / 代码内 `set_env`),但**没有 `push` 子命令**;唯一的上传函数 `client/api/venv_api.py:push_venv` 无调用方,且因 `FileType.ENV` 被 f-string 插值成字面量 `'FileType.ENV'` 而必然失败(分析报告 E2/E3)。
- 服务端**运行侧完整**(`HAIENV_PATH` 注入 + `source haienv` 生成),**数据面 ENV 类型已就绪**(`cloud_storage` 的路径与传输分支),但**上传入口只有桩**(`api/resource/storage/default.py:9-10`)且**未注册路由**(`api/register/implement.py:67-83`)。
- 两处设计级缺陷:①**路径约定不一致**(数据面落盘 `{env_path}/{group}/shared/hfai_envs/{user}/{name}` vs 运行时搜索根 `/hf_shared/hfai_envs`);②**没有环境注册表写入**,上传后 `source haienv` 无法发现该环境。

### 1.2 目标

| ID | 目标 |
| --- | --- |
| **G1** | 打通"本地(集群外)创建 env → 推送到集群 → 任务内 `source haienv <name>` 可用"的端到端闭环 |
| **G2** | 在不改变 `haienv` 激活语义(唯一判据仍是 `venv.db`)的前提下,让集群侧注册表被正确写入 |
| **G3** | 数据面落盘路径与任务运行时搜索根**统一**,由配置与常量单点定义 |
| **G4** | 复用既有 `cloud_storage` 传输通道与 `/ugc/*` 接入层约定,不新造传输层、不引入 Postgres DDL |
| **G5** | 失败可区分(上传失败 vs 注册失败)、可重试、可观测、可灰度回滚 |

### 1.3 非目标(Out of Scope)

- **不**把 `env create / list / remove / config` 改造成服务端 API;这四个子命令继续是本地行为(设计 ADR-E2)。
- **不**支持 `extend=True` 环境的上传(客户端已明确拒绝,保持该语义,见 FR-07)。
- **不**做 env 内容的 GC / 配额 / 审计 / 计费(登记为 P2,分析报告 L 组风险)。
- **不**做跨集群 env 复制(与 workspace 一致,靠对象存储中转隐式支持)。
- **不**修改 `source haienv` / `Haienv.select` 的搜索算法(硬约束 HC-02)。
- **不**重构 `cloud_storage/api.py` 的既有无前缀路由契约(硬约束 HC-07)。

---

## 2. 角色与场景

| 角色 | 场景 |
| --- | --- |
| 平台用户(本地机) | 本地 `haienv create myenv --no_extend` → `hai-cli env push myenv` → 提交任务时 `HF_ENV_NAME=myenv` |
| 平台用户(开发容器) | 直接在共享盘 `haienv create`,无需 push;`hai-cli env list` 查看自己与他人的环境 |
| 任务容器 | 启动脚本 `source haienv <name> [-u <owner>]`,按 `venv.db` 解析 |
| 平台运维 | 配置 env 根路径、灰度开关、观察注册失败率 |

---

## 3. 需求总览

### 3.1 功能需求(FR)

| ID | 需求 | 优先级 | 落点 |
| --- | --- | --- | --- |
| **FR-01** | 客户端新增 `hai-cli env push <haienv_name>` 子命令,参数对齐 `push_venv` 既有形参(`--force / --no_checksum / --no_zip / --no_diff / --list_timeout / --sync_timeout / --cloud_connect_timeout / --token_expires / --part_mb_size / --provider / --proxy`),并接入 `plugins/haienv/haienv/client/cli.py` | P0 | 设计 §6.1 |
| **FR-02** | 修复 `FileType` 序列化:凡拼入命令行或查询串的枚举一律取 `.value`(至少覆盖 `client/api/venv_api.py:25`;建议同步修 workspace 侧 F2 影响面) | P0 | 设计 §6.2 |
| **FR-03** | 服务端实现并注册 `API-11 POST /ugc/update_cluster_venv`:token→用户,入参 `venv_name`、`py`、`extend`;做**预检**(名称合法、非 extend、目标不冲突),返回 `{'success':1,'path':<集群 env 目录绝对路径>,'exists':<bool>}` | P0 | 设计 §4.1 |
| **FR-04** | 服务端实现并注册 `API-13 POST /ugc/register_cluster_venv`:把已上传成功的 env 写入集群侧**目标用户**的 `venv.db`(`haienv` 表),使 `source haienv <name>` 与 `hai-cli env list` 立即可见 | P0 | 设计 §4.2 |
| **FR-05** | 统一 env 路径约定:数据面 ENV 的**集群落盘路径**必须等于 `dirname(HAIENV_PATH)/<user>/<name>`(即与运行时搜索根一致);配置项 `[cloud.storage.service] env_path` 的语义在文档与代码中单点定义 | P0 | 设计 §3.3 / §13 ADR-E1 |
| **FR-06** | 客户端 push 结果分级:能区分"上传失败"与"上传成功但注册失败",注册失败时给出**可重试**的明确提示,且重复执行 `push` 幂等(不重复上传、不产生重复注册项) | P0 | 设计 §6.3 |
| **FR-07** | 服务端拒绝 `extend=True` 环境的上传请求(与客户端既有拒绝语义一致),返回 `success=0` + `code=INVALID_PARAM` | P0 | 设计 §4.1 |
| **FR-08** | 服务端对 `venv_name` 做白名单式校验(不允许 `/`、`..`、空白、超长),最终路径必须通过 `check_is_subpath` 校验 | P0 | 设计 §4.4 |
| **FR-09** | 注册成功后,同一集群内的任务 `source haienv <name>` 必须成功;客户端 `env list` 必须能列出该环境 | P0(Acceptance) | 设计 §4.2 / §8 |
| **FR-10** | 更新文档:`docs/_sources/cli/ugc.rst.txt` 与 `docs/_sources/guide/environment.md.txt` 补充 push 用法、前置条件(CUDA/conda 限制)与失败排查 | P1 | 设计 §14(S6) |
| **FR-11** | 任务启动脚本在 `source haienv` 失败时输出可诊断信息(环境名、搜索根、owner),而非仅 `no valid env found` | P2 | 设计 §7 |
| **FR-12** | 提供灰度开关(按用户/用户组)与一键关闭能力,关闭时 `env push` 返回 `FEATURE_DISABLED` | P1 | 设计 §9.2 |

### 3.2 非功能需求(NFR)

| ID | 需求 |
| --- | --- |
| **NFR-01** | `API-11` 是**无状态预检**,单次响应 P99 < 100 ms(不触碰共享盘大目录遍历) |
| **NFR-02** | `API-13` 单次响应 P99 < 300 ms(含 SQLite 写与 `fsync`) |
| **NFR-03** | 两个接口都必须**幂等**:同参数重复调用不产生副作用累积(注册用 `REPLACE`) |
| **NFR-04** | 服务端任一新逻辑不得阻塞事件循环(共享盘 I/O 走 `asyncwrap` 或线程池) |
| **NFR-05** | 提供指标:注册成功/失败计数(按 `code` 分标签)、注册耗时直方图 |
| **NFR-06** | 客户端 `push` 在注册失败时**不得**回滚已上传的对象与集群目录(传输成功即保留) |

### 3.3 安全需求(SEC)

| ID | 需求 |
| --- | --- |
| **SEC-01** | 两个接口的身份**只**来自 `token`(`api/depends.get_ugc_user`),忽略客户端传入的 `username/group` |
| **SEC-02** | 用户**只能**操作自己的 env 根目录;服务端不得接受任意 `path` 参数写入,注册路径必须由服务端按 `env_root/<user>/<name>` 自行推导 |
| **SEC-03** | 注册前必须校验目标目录/`venv.db` 的写权限;无权限时返回明确 `code`,不抛裸 500 |
| **SEC-04** | 所有路径参数经 `check_is_subpath` / 名称白名单校验,禁止 `..` 与绝对路径注入 |
| **SEC-05** | 日志中 `token=` / `access_token=` 必须掩码(复用 `api/app.py:104-107`),错误信息不得回显完整 token |
| **SEC-06** | 客户端 `list_haienv` / `set_env` 使用 `-u <user>` 拼路径前必须做 `..` 与 `/` 校验(修 E9) |

### 3.4 运维需求(OPS)

| ID | 需求 |
| --- | --- |
| **OPS-01** | `env_path` 与 `HAIENV_PATH` 的对应关系在**启动自检**中校验;不一致时打印 ERROR 并给出建议值(不阻断启动) |
| **OPS-02** | 灰度:开关关闭时两个接口返回 `FEATURE_DISABLED`;已注册环境不受影响 |
| **OPS-03** | 回滚:关闭路由注册即可回到现状(客户端 push 报"接口不存在"),不产生脏数据 |
| **OPS-04** | 提供运维手册条目:如何手工修复某用户的 `venv.db`(只读校验 + 删除错误 key) |
| **OPS-05** | 注册失败必须落日志(用户、env 名、目标路径、异常),便于事后补登记 |

### 3.5 兼容需求(CMP)

| ID | 需求 |
| --- | --- |
| **CMP-01** | 老客户端(无 `env push`)行为不变:不调用新接口即无影响 |
| **CMP-02** | 保留 `legacy_param_compat` 语义:服务端归一化枚举时能识别 `'FileType.ENV'` 形式(`cloud_storage/service/compat.py:25-52` 已具备),但**客户端仍必须修 FR-02** |
| **CMP-03** | 服务端写入的 `venv.db` 记录必须能被当前客户端 `haienv` 正确反序列化(pickle 兼容) |
| **CMP-04** | `plugins/haiworkspace` 的 `--file_type env / --env_*` 隐藏选项语义不变,仅由 `env push` 内部调用 |
| **CMP-05** | 不改变 `one/release.sh` 平台基础环境(`platform/hai202207`)的构建方式 |

### 3.6 硬约束(HC)

| ID | 约束 | 原因 |
| --- | --- | --- |
| **HC-01** | 不改动 `haienv create/list/remove/config` 四个既有子命令的语义与输出格式 | 兼容既有用户与文档 |
| **HC-02** | 不修改 `source haienv` / `Haienv.select` / `get_envs` 的搜索算法;环境的唯一注册表仍是 `{HAIENV_PATH}/venv.db` 的 `haienv` 表 | 避免同一环境两套判定 |
| **HC-03** | 服务端不新增 Postgres 表/列;注册信息落在共享盘 SQLite(与客户端同源) | 仓库无自动迁移框架(见 workspace DB 审计 §4/§7) |
| **HC-04** | `/ugc/*` 身份只来自 token;不得信任 body/query 里的 username/group | SEC-01 |
| **HC-05** | 所有响应体必须带 `success` 字段;业务失败用 `{'success':0,'code','msg'}`,HTTP 默认 200 | 客户端 `async_requests` 先断言 `'success' in result` |
| **HC-06** | 领域层(`cloud_storage/service/*`)**禁止** import fastapi / 注册路由 | 沿用 ADR-11/ADR-12 分层 |
| **HC-07** | 不改变 `cloud_storage/api.py` 既有无前缀路由的路径与入参 | COMP-03 |
| **HC-08** | 可被部署私有 `*/custom.py` 覆盖的方法必须放在 `default.py` 侧(`*Extras` 基类风格),不放 `implement.py` | ADR-4 |

---

## 4. 接口清单

### 4.1 API-11 修订版:`POST /ugc/update_cluster_venv`

| 项 | 内容 |
| --- | --- |
| 用途 | **预检 + 路径推导**:返回该 env 在集群侧的落盘目录,并给出是否已存在 |
| 路由 | `api/register/implement.py` 的 `if 'ugc' in REG_SERVERS:` 段 |
| 实现位置 | `api/resource/storage/default.py`(可被 `custom.py` 覆盖),业务在 `cloud_storage/service/env_registry.py` |
| 鉴权 | `Depends(get_ugc_user)` |
| 入参 | query: `token`(必填)、`venv_name`(必填)、`py`(必填)、`extend`(可选,默认 `False`) |
| 出参 | `{'success': 1, 'path': '/hf_shared/hfai_envs/<user>/<name>_<suffix>', 'exists': false}` |
| 失败 | 「`INVALID_PARAM`」(名非法 / extend=True)、「`FEATURE_DISABLED`」、「`UNAUTHORIZED`」 |
| 幂等 | 是(`exists=True` 时返回同一路径) |
| 兼容 | 客户端旧调用形态 `?token&venv_name&py` **必须继续可用**(`extend` 缺省即 False) |

> **与旧桩的差异**:旧桩 `async def update_cluster_venv()` 不接收 `Request`,注册后必然 422;本需求要求签名含 `request: Request`。`path` 的 basename 必须是最终目录名,因为客户端把它作为 `--env_remote_path` 传入后,服务端只取 basename 当 `name`(`cloud_storage/api.py:158-179`)。

### 4.2 API-13:`POST /ugc/register_cluster_venv`

| 项 | 内容 |
| --- | --- |
| 用途 | 把上传成功的 env **登记**到目标用户集群侧 `venv.db`,使 `source haienv` / `env list` 可见 |
| 路由 | 同 API-11 段 |
| 实现位置 | `api/resource/storage/default.py` + `cloud_storage/service/env_registry.py` |
| 鉴权 | `Depends(get_ugc_user)` |
| 入参 | body(JSON):`venv_name`、`path`、`py`、`extra_search_dir[]`、`extra_search_bin_dir[]`、`extra_environment[]`;`path` 必须经服务端校验落在 `env_root/<user>/` 下 |
| 出参 | `{'success': 1, 'registered': true, 'path': '<最终路径>', 'db': '<venv.db 绝对路径>'}` |
| 失败 | 「`INVALID_PARAM`」、「`FORBIDDEN`」(path 越界)、「`PATH_ESCAPE`」、「`INTERNAL_ERROR`」(写库失败,附 `msg`) |
| 幂等 | 是(`REPLACE INTO haienv`);重复注册同名同路径不报错 |
| 前置条件 | 由客户端在 `workspace push` **退出码为 0** 之后调用(此时 stage2 已完成,集群目录已就绪) |

### 4.3 (P2)API-14:`GET /ugc/cluster_venv/list`

| 项 | 内容 |
| --- | --- |
| 用途 | 服务端视角列出某用户(或本用户)集群侧 env,供跨集群/无共享盘场景使用 |
| 入参 | `token`、可选 `user` |
| 出参 | `{'success':1,'data':[{'user','haienv_name','path','extend','extend_env','py'}]}` |
| 优先级 | P2(现状 `env list` 走共享盘已可用;本接口仅为无盘场景预留) |

### 4.4 统一约定

| 项 | 约定 |
| --- | --- |
| 鉴权 | `/ugc/*` 一律 `token` → `get_ugc_user`(`api/depends/implement.py:110-141`) |
| 枚举归一化 | 服务端用 `cloud_storage/service/compat.py:normalize_enum`;客户端必须传 `.value` |
| 错误体 | `{'success':0,'code':<ErrorCode>,'msg':<中文可读>}`,HTTP 默认 200 |
| 错误码 | 复用 `cloud_storage/service/errors.py:ErrorCode`,新增 `ENV_ALREADY_EXISTS`、`ENV_REGISTRY_WRITE_FAILED`、`ENV_PATH_MISMATCH` |
| 路径校验 | `check_is_subpath`(`cloud_storage/utils.py:434-442`)+ 名称白名单 `^[A-Za-z0-9._-]{1,64}$` |
| 日志 | 结构化: `user`、`env`、`path`、`code`、`elapsed_ms` |

---

## 5. 状态与数据

| 数据 | 位置 | 说明 |
| --- | --- | --- |
| 环境配置 | `{env_root}/<user>/venv.db` → 表 `haienv` | key=环境名,value=`pickle(HaienvConfig, protocol=4)`;服务端复用镜像内 `haienv` 包写入 |
| 环境文件 | `{env_root}/<user>/<name>_<suffix>/` | conda prefix;由 `workspace push --file_type env` 落盘 |
| 同步状态 | 既有 Postgres `user_sync_status` | 直接复用 `sync_to_cluster` 链路,**不新增表/列** |
| 平台基础环境 | `{env_root}/platform/hai202207_0` | 镜像构建期产物,只读 |

> **env_root 单点定义**:`env_root = dirname(HAIENV_PATH)`,任务侧为 `/hf_shared/hfai_envs`(`server_model/task_impl/single_task_impl.py:61`);配置侧 `[cloud.storage.service] env_path = '/hf_shared'`(`one/one_etc/core.toml:113`)。两者由 **FR-05** 强制对齐。

---

## 6. 验收标准(DoD)

| ID | 验收项 | 判定方式 |
| --- | --- | --- |
| **AC-01** | 契约:`/ugc/update_cluster_venv` 与 `/ugc/register_cluster_venv` 均可被真实 token 调用,返回结构与 §4 一致 | 接口测试 |
| **AC-02** | 路径:API-11 返回的 `path` 与 `cloud_storage` 的实际集群落盘目录、任务运行时搜索根**三者一致** | 环境实测(见 §7) |
| **AC-03** | 端到端:本地 `haienv create m --no_extend` → `hai-cli env push m` → 提交任务 `HF_ENV_NAME=m` → 任务内 `python -c "import <env 内独有包>"` 成功 | E2E |
| **AC-04** | 可见性:注册后 `hai-cli env list` 能列出该环境;`source haienv m` 返回 0 | E2E |
| **AC-05** | 幂等:连续两次 `push` 第二次「数据已同步,忽略本次操作」且无重复注册项 | E2E |
| **AC-06** | 失败分级:人为让注册写库失败时,客户端输出"上传成功,注册失败,可重试"而非"推送失败" | 故障注入 |
| **AC-07** | 安全:`venv_name=../../etc` / `path=/tmp` 均被拒绝且无副作用 | 安全测试 |
| **AC-08** | 拒绝 extend:上传 `extend=True` 环境返回 `INVALID_PARAM` | 接口测试 |
| **AC-09** | 兼容:旧调用形态 `?token&venv_name&py`(无 extend)可用;老客户端行为不变 | 回归 |
| **AC-10** | 灰度:开关关闭时两接口返回 `FEATURE_DISABLED`,且不写库 | 配置测试 |
| **AC-11** | 回滚:关闭路由注册后系统回到现状,无脏数据 | 演练 |
| **AC-12** | 文档:`ugc.rst` / `environment.md` 已补充用法与限制 | 文档评审 |

---

## 7. 路径一致性验证方法(AC-02 的实测步骤)

```bash
# 1) 配置侧
grep -n "env_path\|HAIENV_PATH" one/one_etc/core.toml server_model/task_impl/single_task_impl.py

# 2) 数据面落盘
python -c "from cloud_storage.utils import get_base_path; from conf.utils import FileType; \
print(get_base_path('<user>','<group>','m',FileType.ENV))"

# 3) 接口返回
curl -s -X POST "http://127.0.0.1:8083/ugc/update_cluster_venv?token=<token>&venv_name=m&py=3.8"

# 4) 运行时搜索根(任务内)
echo $HAIENV_PATH && ls -l "$(dirname $HAIENV_PATH)"
```

**通过判据**:步骤 ② 的 `cluster_base_path` 的 `dirname` 等于步骤 ④ 的 `dirname(HAIENV_PATH)`,且步骤 ③ 的 `path` 在其下。

---

## 8. 待确认决策

| ID | 问题 | 建议 | 影响 |
| --- | --- | --- | --- |
| **Q-1** | 生产环境是否已由私有 `api/register/custom.py` 实现 `/ugc/update_cluster_venv`? | 先联调确认,若已存在则以私有实现为准,本设计退化为"契约对齐 + 客户端修复" | 工作量 |
| **Q-2** | 集群 `env_path` 是否允许调整?若不希望改动数据面路径,则改为改任务侧 `HAIENV_PATH` | 倾向改数据面(改动面小,见设计 ADR-E1) | 路径约定 |
| **Q-3** | 注册表写入失败时的用户可见语义 | 明确区分"上传成功 + 注册失败,可重试" | 体验 |
| **Q-4** | 是否允许用户**覆盖**已存在的同名 env | 默认拒绝覆盖(`force` 才允许),避免误伤他人环境 | 数据安全 |
| **Q-5** | P2 的 `env list` 服务端接口是否本期做 | 本期不做,走共享盘 | 范围 |
| **Q-6** | 是否顺手修 workspace 侧 F2(`get_sync_status` 等 3 处调用) | 建议同批修,同一根因 | 回归面 |

---

## 9. 交付物清单

| 交付物 | 路径 |
| --- | --- |
| 逆向分析 | `docs/haiplatform/env/hai-cli-env-analysis.md` |
| 需求说明(本文) | `docs/haiplatform/env/env-server-requirements.md` |
| 程序设计 | `docs/haiplatform/env/env-server-design.md` |
| 测试用例(后续) | `docs/haiplatform/env/env-server-test-cases.md` |
| 上线 Checklist(后续) | `docs/haiplatform/env/env-server-checklist.md` |

---

## 10. 术语

| 术语 | 含义 |
| --- | --- |
| env / haienv | 用户级 Python 虚拟环境(conda prefix + 元数据) |
| env_root | 集群侧所有用户 env 的父目录 = `dirname(HAIENV_PATH)` = `/hf_shared/hfai_envs` |
| 注册表 | `{env_root}/<user>/venv.db` 的 `haienv` 表 |
| 预检 | API-11:只读地校验并推导目标路径 |
| 注册 | API-13:写入注册表,使环境"可见" |
| extend 环境 | 从当前基础环境继承 `sys.path` 的环境(`extend='True'`),本特性不支持上传 |

---

## 11. 追溯矩阵

| 需求 | 接口 / 落点 | 设计章节 | 验收 |
| --- | --- | --- | --- |
| FR-01 / FR-02 / FR-06 | 客户端 `plugins/haienv` + `client/api/venv_api.py` | §6.1–§6.3 | AC-05 / AC-06 / AC-09 |
| FR-03 / FR-07 / FR-08 | API-11 | §4.1 / §4.4 | AC-01 / AC-02 / AC-07 / AC-08 |
| FR-04 / FR-09 | API-13 | §4.2 / §8 | AC-01 / AC-03 / AC-04 |
| FR-05 | `cloud_storage/utils.py:get_base_path` + 配置 | §3.3 / §13 ADR-E1 | AC-02 |
| FR-10 | 文档 | §14(S6) | AC-12 |
| FR-11 | 任务侧启动脚本 | §7 | AC-03 |
| FR-12 | 灰度开关 | §9.2 | AC-10 / AC-11 |
| NFR-01..06 | 接口与客户端 | §4 / §6.3 / §9.4 | AC-05 / 性能测试 |
| SEC-01..06 | 接口与客户端 | §10 | AC-07 |
| OPS-01..05 | 自检 / 手册 | §3.4 / §9 | AC-10 / AC-11 |
| CMP-01..05 | 兼容层 | §11 | AC-09 |
| HC-01..08 | 全局硬约束 | §2 / §11 / §13 | 代码评审 |
