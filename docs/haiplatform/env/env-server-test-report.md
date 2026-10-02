# HAI Platform · `hai-cli env`（haienv）实现与 103 实测记录

> **文档定位**:`docs/haiplatform/env/` 五件套之五(分析 → 需求 → 设计 → 用例 → Checklist → **实测记录**)。
> **前置阅读**:[env-server-design.md](env-server-design.md)(落地清单与 ADR)、[env-server-test-cases.md](env-server-test-cases.md)(L1–L4 用例)、[env-server-checklist.md](env-server-checklist.md)(DEV/E2E/REG 检查项)。
> **本文回答三个问题**:①代码改了哪些文件、关键实现决策是什么;②在 `fireflyer@192.168.100.103` 上跑出了什么结果(可复现命令 + 原始数字);③实现期**新发现**了哪些缺陷、怎么修的。
> **基线**:仓库分支 `feature/hai-cli-env-server-design`,源码基线提交 `e03c42c`(workspace 服务端实现),env 实现为未提交工作树改动。

---

## 1. 结论速览

| 项 | 结果 |
| --- | --- |
| L1 单元测试 | `tests/env/test_env_registry.py` **40 passed / 1 skipped**;`cloud_storage/service/env_registry.py` 行覆盖率 **100%**(225/225,UT-01 要求 ≥85%) |
| L2 接口冒烟 | `smoke_env.sh` **PASS=20 FAIL=0**(API-11 正常/**cloud_path 合规**/旧形态/幂等、API-13 注册/幂等/越界/非法名、鉴权、注册表反序列化、`source haienv`) |
| 客户端单测 | `test_client_push.py` **10 passed** + `test_haienv_create_cuda.py` **14 passed**（共 24） |
| L3 端到端 | `e2e_env.sh` **PASS=16 FAIL=0**（本地在**集群外** `/tmp`）：`env push` → **RustFS 期望 key 命中** → 集群落盘 + 注册 → 二次 push 幂等 → 任务 `succeeded` 且 `PROBE_OK`（`HAIENV_PATH=/nfs-shared/hai-platform/workspace/hfai_envs/haiadmin`） |
| workspace 回归 | `smoke_ugc.sh` **8/8**、`e2e_workspace.sh all` **19/19**(改动落在共用代码 `conf/utils.py` / `cloud_storage/utils.py`,回归必跑) |
| 一键复现 | `verify_env.sh` → **验证汇总: PASS=6 FAIL=0**(l1_unit / l1_client / l2_smoke / l3_e2e / reg_ugc / reg_workspace) |
| 零 DDL | `git status --short db_schemas/` 为空(HC-03 / GATE-05 / REG-01) |
| 单点化 | `grep -rn hfai_envs --include=*.py` 只剩 `conf/utils.py`(定义)与 `cloud_storage/utils.py` 的 S3 key(CMP-05 要求不变) |
| 缺陷（实现期新发现 7 + 分析报告落地 1） | **C-1** 客户端 wheel 缺 `hfai/conf.utils`;**C-2** `env push` 子进程多一个 `workspace` 词;**C-3** `asyncio.to_thread` 在 py3.8 不存在;**C-4** `build_hai.sh` 用管道吞掉镜像构建失败;**C-5** 构建机 `archive.ubuntu.com` 不可达导致 docker build 卡死;**C-6** `--env_remote_path` 用了集群路径 → 对象 key 错、stage2 404;**C-7** ENV 排除 `activate` → 集群侧环境不可 `source`;**C-8**（= 分析报告 E10）`haienv create` 的 CUDA 门禁过窄 → 默认支持 **CUDA 11.x（含 11.5）** |
| 任务侧 | `HAIENV_PATH` 已单点化;任务容器需额外挂载 `env_root`(见 §5.4,已脚本化) |

---

## 2. 落地清单

### 2.1 服务端

| 文件 | 动作 | 内容 |
| --- | --- | --- |
| `conf/utils.py` | 改 | 新增 env 路径**单点定义**:`ENV_DIR_NAME` / `DEFAULT_ENV_PATH` / `ENV_NAME_RE` / `normalize_env_path` / `get_env_path` / `get_env_root` / `get_user_env_dir` / `get_env_registry_path` / `get_env_dir_name`;惰性 `CONF`,导入期无副作用(该模块被 `conf/__init__.py` star-import) |
| `cloud_storage/utils.py` | 改 | `get_base_path` 的 `FileType.ENV` 分支:cluster 侧改为 `{env_root}/{user}/{name}`(与 `dirname(HAIENV_PATH)` 对齐),**S3 key 布局不变** |
| `cloud_storage/service/env_registry.py` | **新增** | 领域层:名称白名单校验 / 预检与后缀分配 / 写权限探测 / `flock`+`REPLACE` 写注册表并回读校验 / OPS-01 自检(225 行,覆盖率 100%) |
| `cloud_storage/service/errors.py` | 改 | 新增 `ENV_ALREADY_EXISTS` / `ENV_REGISTRY_NOT_WRITABLE` / `ENV_REGISTRY_WRITE_FAILED` / `ENV_PATH_MISMATCH` |
| `cloud_storage/service/context.py` | 改 | `get_env_path/get_env_root/get_user_env_dir/get_env_registry_path`、`check_env_push_enabled`(灰度)、`get_env_name_regex`(含 `/` 的配置自动退回内置白名单) |
| `cloud_storage/service/__init__.py` | 改 | 导出 `validate_env_name` / `derive_env_path` / `register_env` / `env_registry_self_check` |
| `cloud_storage/metrics.py` | 改 | `env_push_requests_total{api,result,code}` / `env_register_duration_seconds{result}` / `env_registry_write_failures_total{reason}` |
| `api/resource/storage/default.py` | 改 | **替换桩**:`update_cluster_venv`(API-11)/ `register_cluster_venv`(API-13)/ `startup_env_check`(OPS-01);放 `default.py` 保留 `custom.py` 覆盖能力(HC-08) |
| `api/register/implement.py` | 改 | `ugc` 段注册两条路由 + startup 自检钩子 |
| `server_model/task_impl/single_task_impl.py` | 改 | `HAIENV_PATH` / `MARSV2_VENV_PATH` 改用 `get_user_env_dir(user)`,去掉 `/hf_shared/...` 硬编码;`source haienv` 失败时打印 `env=<name> owner=<owner> HAIENV_PATH=$HAIENV_PATH`(FR-11,不改变任务成败语义) |
| `one/one_etc/core.toml` | 改 | `env_path` 语义注释(新增 `env_root = {env_path}/hfai_envs`)+ 新增 `env_push_enabled` / `env_push_enabled_users` / `env_push_enabled_groups` / `env_name_regex` |

### 2.2 客户端

| 文件 | 动作 | 内容 |
| --- | --- | --- |
| `client/api/venv_api.py` | **重写** | `push_venv`:`Haienv.select` 校验(不存在 / extend)→ **API-11 预检**(校验 `path` 非空,E7)→ `haiworkspace` 子进程上传(`.value` 修 E3、可执行文件解析修 E13)→ **API-13 注册**;三级结果「上传失败 / 已上传但注册失败(可重试) / 上传并注册成功」;上传前尽力 `chmod 777` 用户 env 目录(设计 §4.4 路径 ①) |
| `plugins/haienv/haienv/client/command.py` | 改 | 新增 `push` 子命令(参数对齐设计 §6.1:provider 用长选项,分片大小 `-m`);`list` / `config show` 的 `-u` 参数加路径校验(SEC-06/E9) |
| `plugins/haienv/haienv/client/cli.py` | 改 | 注册 `push` |
| `plugins/haienv/haienv/client/model.py` | 改 | 新增 `check_user_name`(拒绝 `..` / `/` / `\` / NUL / 首尾空白) |
| `plugins/haienv/haienv/client/api.py` | 改 | `list_haienv` 拼路径前校验;`create_haienv` 尽力把用户 env 目录放宽到 777 |
| `plugins/haienv/haienv/haienv.py` | 改 | `set_env` / `get_envs` 校验 `-u`(E9) |
| `plugins/haiworkspace/haiworkspace/client/workspace_util.py` | 改 | 枚举串一律取 `.value`(`FileType` / `SyncDirection` / `SyncStatus`,修 F2/Q-6) |
| `client/install.sh` | 改 | **无条件**生成空 `hfai/conf/flags/custom.py`(原实现只在 `external=true` 时生成,导致构建出的 `hai-cli` 一执行就 `ModuleNotFoundError`) |

### 2.3 测试与运维脚本

| 文件 | 内容 |
| --- | --- |
| `tests/env/test_env_registry.py` | L1:U/P/REG/S/灰度 边界共 41 条(不 mock `HaienvConfig`,注册表读写走真实 `haienv` 包) |
| `tests/env/test_haienv_create_cuda.py` | `haienv create` 的 CUDA 门禁 14 条（11.x 全放行 / 非 11.x 拒绝 / 覆盖规则 / nvcc 缺失） |
| `tests/env/test_client_push.py` | 客户端:10 条(E3/E7/E13 + 分级 + 注册 body + C-6 缺 cloud_path 回归) |
| `docs/haiplatform/scripts/patch_env_override.py` | 幂等写 `env_path` + `env_push_*` |
| `docs/haiplatform/scripts/mount_env_root.sh` | 把 `env_root` 挂进任务容器(storage 表 Directory 记录) |
| `docs/haiplatform/scripts/env_fixture.py` | 造 env fixture(真实 `haienv` 写注册表 + 可用 `activate` + 探针包) |
| `docs/haiplatform/scripts/build_cli_local.sh` | 宿主机直接构建/安装 `hai-cli`/`haienv`/`haiworkspace` wheel(含 wheel 自检) |
| `docs/haiplatform/scripts/deploy_pod_dev.sh` | 联调快通道:源码 tar 进运行中的 pod + 重启 `ugc_server` |
| `docs/haiplatform/scripts/smoke_env.sh` | L2 冒烟 19 项 |
| `docs/haiplatform/scripts/e2e_env.sh` | L3 端到端（强制本地在集群外 + 断言 S3 key） |
| `docs/haiplatform/scripts/verify_env.sh` | 一键验证 6 步(L1/客户端/L2/E2E/两套回归) |
| `docs/haiplatform/scripts/build_hai.sh` | **修 C-4**:`| tail` 改为 `| tee + pipefail`,并新增「镜像必须存在」校验 |
| `docs/haiplatform/scripts/patch_dockerfile.py` | **修 C-5**:构建期 apt 源换 aliyun;7 条规则补 `skip_if` 标记(补丁幂等) |

---

## 3. 关键实现决策(与设计文档的差异说明)

1. **同步 I/O 用 `loop.run_in_executor`,不用 `asyncio.to_thread`** —— 平台镜像与 ugc-server 都是 Python 3.8,`asyncio.to_thread` 3.9 才有(实测见 §7 C-3)。设计 §5.2 写的是「`asyncio.to_thread` 或既有 `asyncwrap`」,实现取前者语义 + 3.8 兼容写法。
2. **`_read_registry` 先判 `os.path.exists(db)` 再 `Haienv.select`** —— `sqlite3.connect` 会**创建**空文件,而 API-11 必须只读(TC-A03「不创建任何目录/文件」)。
3. **`env_registry` 不 import `cloud_storage.utils`(模块级)** —— 只在 `register_env_sync` / `env_registry_self_check` 内部惰性 import,避免领域层导入期拉起 fastapi(HC-06)。
4. **`register_env` 额外拒绝 `path == user_env_dir`** —— `check_is_subpath` 允许相等,但注册点必须是 `{user_env_dir}/<name>_<suffix>`;同时 `py` 为空返回 `INVALID_PARAM`(TC-REG-10)。
5. **`env_name_regex` 配置含 `/` 时自动退回内置白名单** —— 需求允许收紧、不允许放宽到含路径分隔符(SEC-04)。
6. **`_probe_writable` 允许创建用户 env 目录** —— 首次 push 的用户目录可能还不存在;创建失败或探测写失败都返回 `ENV_REGISTRY_NOT_WRITABLE`,把失败前移到上传之前(ADR-E5)。
7. **`env_root` 在 103 取 `/nfs-shared/hai-platform/workspace`** —— 任务容器只挂 `.../workspace/{user}`,共享的 `hfai_envs` 必须单独挂载(§5.4)。
8. **日志字段统一 `user=<user_name>`** —— 仓库既有约定是 `user={user.user_name}`,实现时最初直接打了 `User` 对象(`user=user_name: haiadmin`),已改为 `_user_name(user)`;实测日志:
   `[ENV] 注册成功 user=haiadmin env=smokeenv path=/nfs-shared/hai-platform/workspace/hfai_envs/haiadmin/smokeenv_0 db=… elapsed_ms=101`(满足 NFR-05 / TC-L04 的 `user/env/path/code/elapsed_ms`)。
9. **`verify_env.sh` 把 6 步串成一条命令** —— 每步日志落 `/tmp/verify_env_<step>.log`,整体退出码 != 0 即失败;报告中的所有数字都由它产出。

---

## 4. 测试环境(103 实测)

| 项 | 取值 |
| --- | --- |
| 平台 Pod / manager 镜像 | `registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:envtest5`(由本工作树构建;`override.toml` 的 `manager_image` 已同步);联调期用 `deploy_pod_dev.sh` 覆盖源码 |
| `env_path`(运行时配置) | `/nfs-shared/hai-platform/workspace` → `env_root = /nfs-shared/hai-platform/workspace/hfai_envs` |
| 对象存储 | RustFS(`provider=s3`,`http://192.168.100.103:19000`) |
| 客户端 | `hai-cli` / `haienv` / `haiworkspace` 由 `build_cli_local.sh` 构建安装(`1.0.0+e03c42c` / `1.4.1+e03c42c`) |
| 测试用户 | `haiadmin`(token 取自 `~/.hfai/conf.yml`) |
| L1 运行方式 | pod 内 `python3 -m pytest tests/env/test_env_registry.py`(`MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config`) |
| 覆盖率工具 | pod 内无 `coverage`/`pytest-cov`(`pip install` 的 coverage 7.x + PyO3 会 `PyO3 modules ... initialized once`),改用 `sys.settrace` + `dis` 自算行覆盖率 |
| 启动自检实测 | 日志:`env path check: OK env_root=/nfs-shared/hai-platform/workspace/hfai_envs HAIENV_PATH=/nfs-shared/hai-platform/workspace/hfai_envs/__env_self_check__`(OPS-01/AC-02 的配置侧证据) |

### 4.1 复现命令

```bash
ssh fireflyer@192.168.100.103

# 运行时配置 + 任务容器挂载
sudo python3 ~/hai-platform/docs/haiplatform/scripts/patch_env_override.py
bash ~/hai-platform/docs/haiplatform/scripts/mount_env_root.sh

# 服务端(联调快通道)
bash ~/hai-platform/docs/haiplatform/scripts/deploy_pod_dev.sh

# 客户端
bash ~/hai-platform/docs/haiplatform/scripts/build_cli_local.sh

# 用例
sudo kubectl -n hai-platform exec hai-platform-0 -- sh -c \
  "cd /high-flyer/code/multi_gpu_runner_server && MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config \
   python3 -m pytest tests/env/test_env_registry.py -q"
HAIENV_PATH=$(mktemp -d) python3 -m pytest ~/hai-platform/tests/env/test_client_push.py -q
bash ~/hai-platform/docs/haiplatform/scripts/smoke_env.sh http://10.205.52.200
bash ~/hai-platform/docs/haiplatform/scripts/e2e_env.sh
```

---

## 5. 分层测试结果

### 5.1 L1 单元(41 条)

```
tests/env/test_env_registry.py::test_tc_u01_path_functions PASSED
... (U 组 16 条 + 灰度/extend 4 条 + P 组 3 条 + REG 组 3 条 + S 组 2 条 + 边界/防御 12 条)
40 passed, 1 skipped  (skip: 以 root 运行无法用 chmod 555 造不可写目录,已用「monkeypatch tempfile.mkstemp 抛异常」等价覆盖)
env_registry.py: 可执行行=225 已覆盖=225 未覆盖=0 行覆盖率=100.0%
```

要点用例与结论:

| 用例 | 断言 | 结论 |
| --- | --- | --- |
| TC-U01/U02 | 路径函数同源 + 归一化(尾斜杠/重复分隔符/相对路径/默认 `/hf_shared`) | PASS |
| TC-U03 | 合法集原样返回;`''`/`.`/`..`/`a/b`/`../x`/`a b`/超长/中文/换行/`a\\b`/` a` 全部 `INVALID_PARAM` | PASS |
| TC-U04/U05 | 注册命中复用路径;未注册分配第一个空闲后缀且**只读预检不创建目录** | PASS |
| TC-U06 | 写权限探测失败 → `ENV_REGISTRY_NOT_WRITABLE`(root 场景用注入法) | PASS |
| TC-U07/U12 | 注册字段正确 + `HaienvConfig` 可反序列化 + `haienv` 表 value 是 BLOB | PASS |
| TC-U08/REG-07/08 | 幂等 `REPLACE`;并发(5 线程)注册不同/相同 env 无丢记录、无 `database is locked` 泄漏 | PASS |
| TC-U09/U10/A14/A15 | `/tmp/evil`、`../etc`、他人目录一律 `PATH_ESCAPE` 且不写他人 DB | PASS |
| TC-U11/A16 | 写库失败 → `ENV_REGISTRY_WRITE_FAILED`,msg 含目标路径、不含 token | PASS |
| TC-U13 | 旧版 `venv`/`venv_config` 表触发迁移且旧数据不丢 | PASS |
| TC-U14/P04 | 自检一致 `ok=true`;人为制造不一致 `ok=false` + 建议值且不抛异常 | PASS |
| TC-P01/P02/P03/P05 | `get_base_path(cluster)` 的 dirname == `dirname(HAIENV_PATH)` == `user_env_dir`;S3 key 不变;`env_path=/hf_shared` 亦成立;`check_is_subpath` 拒绝穿越 | PASS |
| TC-A04/A10/A19/O04/O06 | extend 拒绝;灰度关闭/白名单外 `FEATURE_DISABLED` 且不写库;正则可收紧、含 `/` 的配置被忽略 | PASS |

### 5.2 L2 接口冒烟(20 项)

```
=== SMOKE_ENV 结果: PASS=20 FAIL=0 ===
```

覆盖明细:API-11 新环境/旧形态(无 extend)/三次幂等响应一致/extend 拒绝/5 类非法名/缺 token(HTTP 403 带 `success=0`)、
API-13 `text/plain` 与 `application/json` 注册/幂等记录数不增长/`exists=true` 复用路径/`/tmp/evil` 等 3 类越界/他人目录/非法名/缺 py、
注册表客户端反序列化、`hai-cli env list` 可见、`source haienv` + 探针包 import、伪造 `username/group` 被忽略。

### 5.3 客户端单测(24 项 = push 10 + CUDA 门禁 14)

```
tests/env/test_client_push.py ..........          [10 passed]   # E3/E7/E13 + 分级 + 注册 body + C-6 回归
tests/env/test_haienv_create_cuda.py ..............  [14 passed]   # C-8：CUDA 11.x（含 11.5）默认放行
24 passed
```

E3(`--file_type env` 而非 `FileType.ENV`)、E13(插件二进制 `haiworkspace push` / 主 CLI `hai-cli workspace push` 两条分支都有断言,且不含 `haienv workspace push`)、
E7(`path=None` → 明确失败且**不执行上传**)、C04(环境不存在本地失败)、C05(extend 本地拒绝且不调 API-11)、C07(上传失败不调注册)、
C08(「已上传但注册失败,可重试」)、C09(注册 body 为 JSON 且 `extra_search_dir` 不被字符串化)全部通过。

### 5.4 任务容器挂载(`env_root`)

任务容器默认只挂载 `/nfs-shared/hai-platform/workspace/{user_name}`(`mars_db.storage`),而 `env_root` 是所有用户共享的父目录,
**不在该挂载点之下**。实测首次跑 E2E 时任务内 `$HAIENV_PATH` 确实不存在 → 已用 `mount_env_root.sh` 插入等价挂载记录:

```
 /nfs-shared/hai-platform/workspace/{user.user_name} | ... | {public} | {} | Directory | f | add | t
 /nfs-shared/hai-platform/workspace/hfai_envs        | ... | {public} | {} | Directory | f | add | t
```

> 生产环境应走 `/operating/mount_point/create`(需 `ops`/`cluster_manager` 角色)或编排仓库落地;脚本里的 SQL 只是 103 上的等价复现手段。

### 5.5 workspace 回归

```
bash smoke_ugc.sh http://10.205.52.200        →  SMOKE 结果: PASS=8 FAIL=0
bash e2e_workspace.sh all                     →  E2E 结果统计: PASS=19 FAIL=0
```

---

## 6. L3 端到端（本地在集群外）

`e2e_env.sh` 执行：在**集群共享盘之外**（`/tmp/hai-env-e2e/<user>/<name>_0`）造环境（真实 `haienv` 写本地
`venv.db` + 探针包 `haienv_probe_unique`）→ `hai-cli env push` → 对象存储 → 集群落盘 + 注册 →
第二次 push 幂等 → 任务 `HF_ENV_NAME=e2eenv` 内 `source haienv` + 探针 import。

```
--- 1) 造集群外本地环境（不在共享盘上：/tmp/hai-env-e2e/haiadmin/e2eenv_0）
PASS | 本地 fixture 就绪
--- 2) 第一次 push（E2E-01 / AC-03）
PASS | 第一次 push 退出码 0 且提示「上传并注册成功」
PASS | 确实执行了上传（未命中「数据已同步」）
PASS | E13 无回归（未出现 haienv workspace push）
--- 3) 对象存储 key 与集群落盘 + 注册表（AC-03/AC-04）
api11 path=/nfs-shared/hai-platform/workspace/hfai_envs/haiadmin/e2eenv_0
api11 cloud_path=hfai/shared/hfai_envs/haiadmin/e2eenv_0
PASS | API-11 返回对象存储前缀且 basename 与集群目录一致（C-6）
s3 hfai/shared/hfai_envs/haiadmin/e2eenv_0/e2eenv_0.zip -> FOUND
PASS | 对象已落到 RustFS 期望 key
PASS | 集群侧 prefix 与探针包存在（stage2 落盘）
PASS | 集群侧注册表恰有 1 条记录
--- 4) 第二次 push（E2E-02 / AC-05：幂等、数据已同步）
PASS | 第二次 push 幂等成功
PASS | 第二次 push 未重复上传（数据已同步，忽略本次操作）
PASS | 重复 push 未产生重复注册项
PASS | hai-cli env list 可见（AC-04）
PASS | 集群侧 source haienv + 探针包 import（AC-04）
--- 5) 任务侧：HF_ENV_NAME=e2eenv 提交任务（TC-T02/T03 / AC-03）
PASS | 任务 pod 状态 succeeded
PASS | 任务内 source haienv 生效且探针包 import 成功（AC-03）
PASS | 任务内 HAIENV_PATH 与数据面同源（TC-T01）
=== E2E_ENV 结果: PASS=16 FAIL=0 ===
```

任务容器内实测输出（决定性的 AC-03 证据）：

```
[2026-10-02 14:39:18.841134] HAIENV_PATH= /nfs-shared/hai-platform/workspace/hfai_envs/haiadmin
[2026-10-02 14:39:18.841221] HF_ENV_NAME= e2eenv
[2026-10-02 14:39:18.846990] PROBE_VALUE= env-push-ok
[2026-10-02 14:39:18.847075] PROBE_OK
```

最终在镜像 `envtest5`（= 本工作树）上重跑 `verify_env.sh`：**PASS=6 FAIL=0**
（`l1_unit` 40 passed/1 skipped、`l1_client` 24 passed、`l2_smoke` 20/20、`l3_e2e` 16/16、`reg_ugc` 8/8、`reg_workspace` 19/19）。

> **用例有效性**：第一版 `e2e_env.sh` 把「本地」env 直接建在集群 `env_root` 下，`haiworkspace push`
> 判定「数据已同步」而**完全跳过上传**，于是对象 key 错误（C-6）与 `activate` 被排除（C-7）两个缺陷都被掩盖。
> 现在用例强制：本地在 `/tmp`、push 前清空集群侧、并断言「未命中数据已同步」。

### 6.1 env 到底上传到哪里（三处落点）

| 层 | 位置 | 实测值 |
| --- | --- | --- |
| 对象存储（RustFS/S3） | `<private_bucket>/<cloud_path>/<dir>.zip`，`cloud_path = {group}/shared/hfai_envs/{user}/{dir}` | `hai-platform-private` bucket：`hfai/shared/hfai_envs/haiadmin/e2eenv_0/e2eenv_0.zip`（默认 zip 模式；`--no_zip` 时是该前缀下的散文件） |
| 集群共享盘（数据面落盘） | `{env_root}/{user}/{dir}`，`env_root = {env_path}/hfai_envs` | `/nfs-shared/hai-platform/workspace/hfai_envs/haiadmin/e2eenv_0`（stage2 解压后删除 `.hfai/*.zip`） |
| 集群注册表（可见性） | `{env_root}/{user}/venv.db` 的 `haienv` 表（pickle `HaienvConfig`） | `/nfs-shared/hai-platform/workspace/hfai_envs/haiadmin/venv.db` → key `e2eenv` |
| 任务运行时 | `HAIENV_PATH={env_root}/{user}` + `source haienv <name>` 读注册表解析出上表第 2 行路径 | 任务日志 `HAIENV_PATH=/nfs-shared/hai-platform/workspace/hfai_envs/haiadmin` |

S3 对象名 = 本地目录 basename（`e2eenv_0.zip`），服务端按**同一份文件清单**拼 key
（`sync_to_cluster.py:112` `key = os.path.join(cloud_base_path, fname)`），因此只要客户端用的是
`cloud_path`（C-6 修复点）就必然一致。

> **为什么必须先重建镜像**：任务侧 `HAIENV_PATH` 由 **manager 容器**计算，而 manager 跑的是
> `override.toml` 的 `manager_image`。只覆盖 `hai-platform-0` 的源码（`deploy_pod_dev.sh`）不影响 manager 镜像，
> 实测任务内仍是旧值 `/hf_shared/hfai_envs/haiadmin`；`build_hai.sh` + `redeploy_local.sh`（会同步 `manager_image`）后才生效。

## 7. 实现期新发现的缺陷(4 个)

| ID | 现象 | 根因 | 修法 | 证据 |
| --- | --- | --- | --- | --- |
| **C-1** | 安装自建 wheel 后 `hai-cli` 一执行就 `ModuleNotFoundError: No module named 'hfai.conf.flags.custom'` | `client/install.sh` 只在 `external=true` 时写空 `conf/flags/custom.py`;而 `base_model` 的 `CustomFinder` 只接管 `hfai/client/**` 与 `hfai/base_model/**` 下的 `custom`,`hfai/conf/flags/` 必须真实存在该文件(镜像内的 `hai-cli` 同样受影响) | `install.sh` 无条件生成空 `custom.py` | `build_cli_local.sh` 的 wheel 自检 + `hai-cli --version` |
| **C-2** | `env push` 报 `Error: No such command 'workspace'`,随后「上传venv失败」 | `hai-cli workspace push` 会被派发成 `haiworkspace push`(插件自身即子命令集合),解析出插件路径后再补 `workspace` 词就多了一层 | `_build_push_cmd` 按可执行文件分支:主 CLI 补 `workspace`,插件不补 | 实测 `env push` 成功;`test_tc_c02_dispatch_plugin_vs_main_cli` |
| **C-3** | `asyncio.to_thread` 在 pod(python 3.8)内 `AttributeError` | `asyncio.to_thread` 是 3.9+ API,设计文档只是「建议」 | 自实现 `_to_thread`(`loop.run_in_executor` + `functools.partial`) | L1 `test_async_wrappers` |
| **C-4** | 镜像构建**失败**但 `build_hai.sh` 仍打印 `ALL DONE: <tag>` | `sudo docker buildx build … 2>&1 \| tail -40` 的退出码是 `tail` 的,`set -e` 失效 | 改 `\| tee <log> \| tail -40` 并 `set -o pipefail`,再加「镜像必须存在」校验 | 本次构建取消时实测:日志有 `ERROR: failed to build … Canceled` 却输出 `ALL DONE` |
| **C-8**（= 分析 E10） | `haienv create` 只认 CUDA 11.1/11.3：CUDA 11.5/11.8 的镜像**默认无法创建环境**（报「目前haienv只支持cuda 11.1和cuda 11.3」） | `command.py:43` 用 `any(v in nvcc_out for v in ['11.1','11.3'])` 做字面量子串匹配 | 抽出 `get_cuda_version()` / `check_cuda_version()`，默认正则 `^11\.\d+$`（**11.x 全部小版本，含 11.5**），可用 `HAIENV_CUDA_VERSION_RE` 覆盖；顺带把重复的 `nvcc -V` 调用合并为一次 | 见 §7.1 前后对照表；`tests/env/test_haienv_create_cuda.py` **14 passed** |
| **C-6** | 集群外 env push：对象被写到 `nfs-shared/.../<dir>.zip`，服务端 stage2 读 `{group}/shared/hfai_envs/...` → **404 Not Found**，push 判定失败（但对象与空目录已留下） | 客户端 `--env_remote_path` 用的是 API-11 返回的**集群文件系统路径**，而该参数在客户端是**对象存储 key 前缀**（`workspace_util.upload_files` 里 `dst_file = f'{remote_path}/{f.path}'`），服务端则自行用 `get_base_path(..., FileType.ENV)` 推导 cloud 侧前缀 → 两边不一致 | API-11 增加返回 `cloud_path`（对象存储前缀，服务端推导）；客户端用它作 `--env_remote_path`，缺该字段时明确失败（不静默用集群路径）；新增客户端回归用例 `test_tc_c10_missing_cloud_path` | E2E 第 3 步 `s3 .../e2eenv_0.zip -> FOUND`；修复前同一步为 `MISSING (404)` |
| **C-7** | 集群外 env push 成功后，任务里 `source haienv` 报 `<prefix>/activate: No such file or directory` → 环境不可用（AC-03 失败） | `workspace_api.push` 的 ENV 分支 `exclude_list = ['activate', 'pip.conf']` 把激活脚本排除了，集群侧没人再生成它 | 去掉该排除项（conda 生成的 activate 用 `${BASH_SOURCE[0]}` 推导环境自身路径，换到集群路径仍可用；仅 `PIP_CONFIG_FILE`/`PYTHONUSERBASE` 仍指向创建时的绝对路径，不影响 source 与 import） | 修复前任务日志 `.../e2eenv_0/activate: No such file or directory`；修复后 `HF_ENV_NAME= e2eenv` + `PROBE_OK` |
| **C-5** | docker build 第一步 `apt-get update` 卡住 15+ 分钟(索引都拉不完) | 103 上 `archive.ubuntu.com` / `security.ubuntu.com` 基本不可达(实测 `curl` 超时),而 `mirrors.aliyun.com` / `mirrors.tuna.tsinghua.edu.cn` 有 ~3 MB/s | `patch_dockerfile.py` 新增规则:构建时 `sed` 改写 `/etc/apt/sources.list` 到 aliyun 镜像;同时把 7 条规则都补上 `skip_if` 标记,让补丁**幂等** | 换源后同一构建 6s 拉完 11.3 MB 索引;整镜像构建 ~16 分钟(含 apt/pip) |

> 另有 2 项属环境/流程经验(非产品缺陷):①任务容器需要额外挂载 `env_root`(§5.4);②只覆盖 pod 源码不足以验证任务侧(§6)。

---

### 7.1 C-8 前后对照（CUDA 版本门禁）

```
CUDA   旧逻辑(字面量 11.1/11.3)     新默认(check_cuda_version)
11.0   REJECT                      ACCEPT
11.1   ACCEPT                      ACCEPT
11.3   ACCEPT                      ACCEPT
11.5   REJECT                      ACCEPT      ← 本次诉求
11.8   REJECT                      ACCEPT
12.0   REJECT                      REJECT
10.2   REJECT                      REJECT
```

覆盖规则实测（`HAIENV_CUDA_VERSION_RE`）：`^12\.` → 12.9 ACCEPT；`^11\.(1|3|5)$` → 11.1/11.3/11.5 ACCEPT、11.8 REJECT。
本机（103）真实 `nvcc` 为 **CUDA 12.9**，默认门禁按其规则拒绝并给出「设置 `HAIENV_CUDA_VERSION_RE`」的提示；
`NVCC_CMD` 指向 11.5 的伪造 nvcc 时 ACCEPT（`tests/env/test_haienv_create_cuda.py::test_check_uses_nvcc_command_when_no_output`）。

## 8. 验收对照(AC-01 ~ AC-12)

| 验收 | 判定 | 证据 |
| --- | --- | --- |
| AC-01 契约一致 | ✅ | `smoke_env.sh` 19/19(返回结构与设计 §4 逐项断言) |
| AC-02 路径三方一致 | ✅ | L1 `TC-P01/P03`;启动日志 `env path check: OK`;`get_base_path` / `HAIENV_PATH` / API-11 同源 |
| AC-03 端到端 | ✅ | `e2e_env.sh` 16/16（本地在集群外）：RustFS 对象命中期望 key、集群落盘+注册、任务 `succeeded` 且 `PROBE_OK`（§6） |
| AC-04 可见性 | ✅ | `smoke_env.sh` 第 14/15 项(`Haienv.select` 反序列化 + `hai-cli env list` 可见);E2E 第 4 步同断言 |
| AC-05 幂等 | ✅ | L1 `TC-U08`/`REG-07/08`;`smoke_env.sh` 第 9/10 项;`e2e_env.sh` 第二次 push 命中「数据已同步」 |
| AC-06 失败分级 | ✅(注入法) | 客户端单测 C08;服务端 `ENV_REGISTRY_WRITE_FAILED` 注入 L1 `TC-U11` |
| AC-07 安全 | ✅ | L1 S 组 + `smoke_env.sh` 第 5/11/12/13/17 项(越界、非法名、伪造身份) |
| AC-08 拒绝 extend | ✅ | L1 `TC-A04`、客户端 `TC-C05`、`smoke_env.sh` 第 4 项 |
| AC-09 兼容 | ✅ | `smoke_env.sh` 第 2/9/10 项(旧形态无 `extend`);`smoke_ugc.sh` 8/8(枚举串兼容未受影响) |
| AC-10 灰度 | ✅ | L1 `TC-A10`/`TC-O04`(接口级灰度需重启生效,与 workspace 一致,未在本轮做动态改配置演练) |
| AC-11 回滚 | ⏳ 未演练(需移除两行路由注册后重启,属发布演练项) | 设计 §9.3 已给出两级回滚方案;本特性**零 DDL**,回滚无脏数据 |
| AC-12 文档 | ✅ | 本文件 + `environment.md.txt` 新增 `env push` 用法与失败处置 + `ugc.rst.txt` 经 `.. click:: haienv.client.cli:cli` **自动**收录 `push` |

### 8.1 未覆盖/后续项

| 项 | 说明 |
| --- | --- |
| 性能(NFR-01/02, PERF-01/02) | 未做 200/100 QPS 压测;单次接口实测都在毫秒级(日志 `elapsed_ms`) |
| 故障注入矩阵 FI-04 / FI-06 | `haienv` 包版本偏移、服务端重启期间的半写状态未注入验证 |
| 动态灰度(OPS-02 演练) | 需改 `override.toml` 后重启 pod;本轮只验了「关闭即拒绝」的领域层行为 |
| `platform` 基础环境(CMP-05/TASK-07) | 本环境镜像内 `/hf_shared/hfai_envs/platform` 是**空占位目录**(无 `venv.db`),故 103 上不存在可回归的基础环境 |
| `localfs` provider | 103 只有 RustFS(`s3`);`localfs` 路径按设计需单独实现(现状 `build_cloud_api` 直接拒绝) |
| NFS 可见性（环境特性，非产品缺陷） | `smoke_env.sh` 的 fixture 在**宿主机**（NFS 服务端本地路径）建目录、由 **pod**（NFS 客户端）执行 `os.path.isdir`。NFSv4 `lookupcache=all` 的目录属性/负项缓存（≤ `acdirmax`≈60s）会让新建目录在 pod 内短暂「看不到」，表现为 `register_cluster_venv` 返回「目标目录不存在」。脚本已加「等待 pod 侧可见」；真实 push 链路里目录由**服务端自己**在 stage2 创建，不受影响 |
