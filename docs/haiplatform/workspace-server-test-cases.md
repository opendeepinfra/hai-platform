# HAI Platform · `hai-cli workspace` 服务端功能测试用例集

| 项目 | 内容 |
| --- | --- |
| 对象 | `hai-cli workspace` 服务端：`/ugc/*` 9 个接口、`cloud_storage/service/*` 领域层、`localfs`/`oss` provider、状态机与审计、任务侧 `oss://` 解析与挂载 |
| 版本 | v1.0（与需求 v1.0 / 设计 v1.0 同期） |
| 依据 | [`workspace-server-requirements.md`](workspace-server-requirements.md)（FR/API/NFR/SEC/OPS/COMP/CON 与 §11 追溯矩阵）· [`workspace-server-db-audit.md`](workspace-server-db-audit.md)（§8 的 TC-DB-01~TC-DB-12 已并入 §4.13）· [`workspace-server-design.md`](workspace-server-design.md)（§4 契约、§5 领域流程、§6 兼容层、§7 状态、§9 配置、§10 任务侧、§11 审计、§12.3 安全、§13 部署灰度）· [`hai-cli-workspace-analysis.md`](hai-cli-workspace-analysis.md)（客户端实际行为与 F1~F10） |
| 编写方法 | 契约驱动（接口形态 → 断言）＋ 客户端反向工程（以客户端实际发送/期望为准）＋ 故障注入 ＋ 反向追溯（需求 → 用例无空洞） |
| 执行层级 | L1 单元（pytest）· L2 接口（HTTP 契约）· L3 端到端（真实 `hai-cli` + `haiworkspace` 插件）· L4 兼容与灰度 |
| 用例总数 | 详细用例 **194**（A 56 · B 6 · C 30 · D 10 · E 6 · F 8 · G 12 · H 12 · I 10 · J 14 · K 10 · L 8 · **DB 12**）＋ 端到端场景 8（E2E-01~E2E-08），合计 **202**；另含故障注入 FI-01~FI-12、压测 PM-1~PM-6 |
| 判定基准 | 任一用例的「预期结果」均为可判定断言：HTTP 状态码、响应字段值、状态字符串、文件系统路径/属主/权限、对象 key 与 tagging、DB 行内容、指标/日志存在性 |

> 阅读约定：`→` 表示预期结果；`[待确认]` 表示设计未决、需产品/运维输入（汇总见 §12.4）；需求引用格式 `FR-xx`、设计引用格式 `设计 §x.y`。

---

## 1. 测试范围与策略

### 1.1 分层模型

| 层 | 名称 | 被测对象 | 依赖与环境 | 通过标准（准出） |
| --- | --- | --- | --- | --- |
| **L1** | 单元测试 | `cloud_storage/service/*`（`sts`/`cluster_files`/`sync_to_cluster`/`sync_from_cluster`/`delete`/`status`/`recovery`/`compat`）、`workspace_resolver`、`UserDbExtras.set_sync_status/get_sync_status`、`UserDownloadedFiles`、`localfs` provider | pytest + `pytest-asyncio`；Redis/PostgreSQL 用 fixture 真实实例（非 mock）；OSS 用 `localfs`；**不启动 FastAPI** | 领域层行覆盖率 ≥ 85%、整体 ≥ 70%（NFR-07）；`compat.normalize_enum` / `index` 哈希 / 路径校验为纯函数，须 100% 分支覆盖 |
| **L2** | 接口契约测试 | `api/resource/cloud_storage/default.py` 的 9 条 `/ugc/*` 路由（含全局异常改写 `api/app.py`） | 单机 `SERVER=ugc`，`uvicorn` 起 :8083（或 `haproxy` 前置 :80）；PostgreSQL + Redis + `provider=localfs`；HTTP 客户端用 `curl`/`requests`/`aiohttp`（**必须能伪造 `text/plain` Body**） | 全部响应体含 `success`（CON-3）；枚举串/规范形态/裸 Body/包裹 Body 四种组合行为一致；错误码与设计 §4.11 表逐项一致 |
| **L3** | 端到端测试 | 真实 `hai-cli` + `haiworkspace` 插件（**未升级的老客户端**）驱动完整 `init/push/diff/list/pull/download/remove`；任务侧 `oss://` 解析与挂载 | 客户端容器 + `ugc-server` + `operating-server` + 集群共享盘（`workspace_path`）+ 对象存储（`localfs` 或真实 OSS）；`provider` 映射若用 `localfs`，客户端需设计 §3.2 的一行映射改动 | `workspace init → push → diff（无差异）→ list → 提交任务 → 集群路径可见代码 → pull → download <subpath> → remove -f → remove` 全链路无异常（需求 §10.1）；第二次 push 上传字节数 = 0 |
| **L4** | 兼容与灰度测试 | 旧客户端（枚举串 + `text/plain` + `{"file_list":...}`）与新客户端（规范形态）同时在线；`legacy_param_compat` 开/关；`enabled_users/groups` 灰度；`cloud-storage` 独立部署（无前缀路由，COMP-03） | 两套客户端二进制 + 两台服务端配置（开关不同）+ 独立 `cloud-storage` 服务实例 | 设计 §13.4 兼容矩阵 5 行逐行成立；灰度外用户得到 `success=0 + FEATURE_DISABLED`；独立部署既有客户端（`cloud_storage/auth.py` JWT、`Page[FileInfo]` 响应模型）零回归 |

### 1.2 策略要点

1. **客户端事实优先**：所有断言以客户端真实行为为准（分析报告 §7 契约表、F2/F3/F3b）。凡「服务端实现看起来对但客户端会失败」的形态（如 422、裸 `{"detail":...}`、`msg` 非数字）一律判失败。
2. **两条 provider 路径**：`localfs`（§2.1，必跑、可离线、可重复）与真实 OSS（§2.2，发布前必跑一次）。除 STS 与分片行为外，两组用例断言必须等价。
3. **双向可判定**：每个用例同时给出「接口层可见结果」与「落盘/DB/Redis 侧真实结果」，避免只验响应不验副作用（F1 的教训：桩返回空列表也能「成功」）。
4. **真客户端参与**：C/D/E 组的关键用例在 L3 用真实客户端跑一遍；L2 的 curl 形态只是等价复现。
5. **开关矩阵化**：`legacy_param_compat`、`enabled`、`enabled_users/groups`、`no_zip`、`no_checksum`、`no_hfignore` 六个维度的取值组合由 A/J 组显式覆盖，不做「默认值即可」的假设。

---

## 2. 测试环境与数据准备

### 2.1 环境拓扑（路径 1：`provider=localfs`，必跑）

| 组件 | 部署 | 关键配置 |
| --- | --- | --- |
| PostgreSQL | 本地/Docker `mars_db`，含 `user_sync_status`（`db_schemas/011`）与 `user_downloaded_files`（`db_schemas/010`） | 不新增 DDL（CON-9） |
| Redis | 本地 `:6379`，`db=1`（避免污染） | 键前缀 `{PROVIDER}=localfs` 与线上隔离 |
| ugc-server | `SERVER=ugc`，`uvicorn_server.py :8083`，`ugc=2`（CON-10 多 worker 必测） | `MARSV2_MANAGER_CONFIG_DIR=/etc/config` |
| 集群共享盘 | 本地目录 `/tmp/hai-test/workspace` 充当 `service.workspace_path` | 需可 `chown`（测试用 root 或 fakeroot） |
| 对象存储 | `provider=localfs`，`localfs_root=/tmp/hai-test/localfs` | 对象落在 `/tmp/hai-test/localfs/<bucket>/<key>`，tagging 落在 `<key>.__tag__.json`（设计 §9.3） |
| 断点目录 | `/tmp/hai-test/breakpoints/{instance_id}` | 按进程隔离（设计 §5.5） |

### 2.2 环境拓扑（路径 2：真实 OSS，发布前必跑）

在 §2.1 基础上替换：`provider=oss` + 真实 `endpoint/access_key_id/access_key_secret/uid/role_arn/private_bucket`，`localfs_root` 留空。需验证 STS 真实签发（`AssumeRole`）、真实分片上传/下载（100 MB 阈值、4 线程）、真实 tagging（`x-oss-tagging`）。用例范围 = P0 冒烟集 + C13~C24 + I02/I04/I05。

### 2.3 测试用户、用户组与配额

| 主体 | `user_name` | `shared_group` | `user_id` | 用途 |
| --- | --- | --- | --- | --- |
| U-A | `wstest_a` | `wsgrp` | 20001 | 主用例执行者 |
| U-B | `wstest_b` | `wsgrp` | 20002 | 越权（同组他人）用例 |
| U-C | `wstest_c` | `wsgrp2` | 20003 | 跨组越权用例 |
| U-ADMIN | `wsadmin` | `ops` | 20004 | API-10 配额设置（`allowed_groups=['ops','platform']`） |
| U-GRAY | `wsgray` | `wsgrp3` | 20005 | 灰度外用户（OPS-01） |
| U-NOQUOTA | `wsquota0` | `wsgrp` | 20006 | `cloud_storage_quota.download=0`（D07） |

配额准备（PG `quota` 表，`resource='cloud_storage_quota'`）：

```sql
-- 默认额度
insert into quota(user_name, resource, quota, expire_time)
values ('wstest_a', 'cloud_storage_quota', 102400, now() + interval '30 day'); -- 100 GB, 单位 MB
-- 超限用例：预置已用 99 GB
update quota set quota = 102400 where user_name = 'wstest_a' and resource='cloud_storage_quota';
-- 零额度用户
insert into quota(user_name, resource, quota, expire_time)
values ('wsquota0', 'cloud_storage_quota', 0, now() + interval '30 day');
```

`local_path`/`cluster_path` 元数据准备（R4 / J06 专用，含 `&`、`#`、空格、中文）：

```bash
LOCAL_PATH='/Users/张三/my code&dir#1'
```

### 2.4 `[cloud.storage]` 配置样例（设计 §9.1）

```toml
# /etc/config/override.toml —— provider=localfs（CI/本地）
[cloud.storage]
provider = 'localfs'
endpoint = 'file:///tmp/hai-test/localfs'
access_key_id = 'test-ak'
access_key_secret = 'test-sk'
uid = '0'
role_arn = 'hai-platform'
private_bucket = 'hai-platform-private'

[cloud.storage.service]
workspace_path = '/tmp/hai-test/workspace'
env_path = '/tmp/hai-test/hf_shared'
public_dataset_path = ''
private_dataset_path = ''
doc_path = ''
pypi_path = ''
official_website_path = ''
breakpoint_info_path = '/tmp/hai-test/breakpoints'
proxy_endpoint = ''
public_bucket_allowed_users = ''
password = ''
enabled = true
enabled_users = ''
enabled_groups = ''
legacy_param_compat = true
localfs_root = '/tmp/hai-test/localfs'
status_ttl_finished = 1800
max_files_per_request = 10000
max_bytes_per_request = 1099511627776
max_page_size = 1000
recover_on_startup = true
recover_stale_seconds = 600
workers = 4
```

派生路径（用例断言唯一依据，设计 §8.1）：`cluster_base = /tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/demo`，`cloud_base = wsgrp/wstest_a/workspaces/demo`。

### 2.5 数据集构造

**数据集 D1（常规）**：`demo` 工作区，3 文件：

```bash
mkdir -p /tmp/hai-test/local/demo/sub && cd /tmp/hai-test/local/demo
printf 'a' > a.txt; printf 'bb' > sub/b.bin; printf 'ccc' > sub/deep/c.txt
chmod 640 sub/b.bin   # 供 filemode 恢复断言（C13）
```

**数据集 D2（10 万文件 / 大目录，FR-04 / NFR-02/03 / I02/I05）**：

```bash
python3 - <<'PY'
import os
root='/tmp/hai-test/local/big'
for i in range(100):                     # 100 目录 × 1000 文件 = 100000
    d=f'{root}/d{i:03d}'; os.makedirs(d, exist_ok=True)
    for j in range(1000):
        with open(f'{d}/f{j:04d}.txt','wb') as f:
            f.write(b'x'*16)             # 每文件 16 B → 合计约 1.6 MB
PY
```

**数据集 D3（10 GB 单大文件，FR-11 / NFR-02 / I04）**：

```bash
mkdir -p /tmp/hai-test/local/bigfile
# 稀疏文件即可（md5 计算仍是全量读，注意耗时）
truncate -s 10G /tmp/hai-test/local/bigfile/checkpoint.bin
# 或真实随机数据（较慢，用于真实 OSS 分片路径）
# dd if=/dev/urandom of=/tmp/hai-test/local/bigfile/checkpoint.bin bs=1M count=10240
```

**数据集 D4（1000 文件 × 10 MB = 10 GB，NFR-02 端到端）**：

```bash
python3 - <<'PY'
import os
root='/tmp/hai-test/local/ten_g'
os.makedirs(root, exist_ok=True)
blob=b'z'*(1024*1024)
for j in range(1000):
    with open(f'{root}/f{j:04d}.bin','wb') as f:
        for _ in range(10): f.write(blob)
PY
```

**数据集 D5（边界与恶意）**：空目录；单文件 0 字节；文件名含中文/空格/`#`；路径穿越（`../etc/passwd`、`/etc/passwd`）；符号链接 `ln -s /etc/passwd link_out`、`ln -s ./a.txt link_in`；`name` 含 `/`。

### 2.6 服务端发请求：「枚举串客户端」与「规范客户端」

**请求 A —— 枚举串客户端（老客户端实际形态，分析报告 F2 / CON-4 / CON-5）**

```bash
# 注意：Body 为 JSON 字符串 + Content-Type: text/plain; charset=utf-8
curl -sS -X POST \
  'http://127.0.0.1:8083/ugc/sync_to_cluster?token=T_A&name=demo&file_type=FileType.WORKSPACE&no_zip=1' \
  -H 'Content-Type: text/plain; charset=utf-8' \
  --data-binary '{"file_list": {"files": ["a.txt", "sub/b.bin"]}}'
# → {"success":1,...,"index":"<sha256>","dst_path":"/tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/demo"}
```

**请求 B —— 规范客户端（修复后形态）**

```bash
curl -sS -X POST \
  'http://127.0.0.1:8083/ugc/sync_to_cluster?token=T_A&name=demo&file_type=workspace&no_zip=1' \
  -H 'Content-Type: application/json' \
  --data-binary '{"files": ["a.txt", "sub/b.bin"]}'
# → 与请求 A 完全等价的响应体（除 index 依赖的 file_type 归一化值必须相同）
```

**请求 C —— Python 等价复现（与客户端 `workspace_util.py:150,234,270` 同源）**

```python
import asyncio, aiohttp, hashlib

BASE = 'http://127.0.0.1:8083'
TOKEN = 'T_A'

def index_of(name, file_type, files):
    return hashlib.sha256(''.join([TOKEN, name, file_type, *files]).encode()).hexdigest()

async def call(session, path, params, body, wrapper):
    url = f'{BASE}{path}'
    payload = {wrapper: body} if wrapper else body
    # 关键：data=<json str> → aiohttp 自动设置 Content-Type: text/plain; charset=utf-8
    async with session.post(url, params=params, data=__import__('json').dumps(payload)) as r:
        text = await r.text()
        return r.status, __import__('json').loads(text)

async def main():
    async with aiohttp.ClientSession() as s:
        # 1) 枚举串 + text/plain + file_list 外壳
        st, res = await call(s, '/ugc/sync_to_cluster',
                             {'token': TOKEN, 'name': 'demo',
                              'file_type': 'FileType.WORKSPACE', 'no_zip': '1'},
                             {'files': ['a.txt', 'sub/b.bin']}, 'file_list')
        assert st == 200 and res['success'] == 1, res
        expect = index_of('demo', 'workspace', ['a.txt', 'sub/b.bin'])
        assert res['index'] == expect, (res['index'], expect)      # 服务端返回 index 路径

        # 2) 轮询：running 时 msg 必须可 int()
        while True:
            async with s.get(f'{BASE}/ugc/sync_to_cluster/status',
                             params={'token': TOKEN, 'index': expect}) as r:
                st = await r.json()
            if st['status'] == 'running':
                int(st['msg'])                                      # 客户端 workspace_util.py:199
            else:
                break
        assert st['status'] == 'finished', st

asyncio.run(main())
```

**请求 D —— 客户端兜底 index（服务端未回传 index 时，设计 §8.2 / ADR-10）**

```python
# 删除服务端响应中的 index 字段后，客户端按同一算法重算；用例断言两值相等
fallback = hashlib.sha256(''.join([TOKEN, 'demo', 'workspace', 'a.txt', 'sub/b.bin']).encode()).hexdigest()
assert fallback == index_of('demo', 'workspace', ['a.txt', 'sub/b.bin'])
```

**DB 直查辅助（用例断言 DB 行时使用）**

```bash
psql -h 127.0.0.1 -U mars -d mars_db -Atc \
 "select push_status, pull_status, local_path, cluster_path,
         to_char(last_push,'YYYY-MM-DD HH24:MI:SS')
    from user_sync_status where user_name='wstest_a' and file_type='workspace' and name='demo'"
```

**localfs 侧车查看（用例断言对象与 tagging）**

```bash
ls -l /tmp/hai-test/localfs/hai-platform-private/wsgrp/wstest_a/workspaces/demo/
cat /tmp/hai-test/localfs/hai-platform-private/wsgrp/wstest_a/workspaces/demo/a.txt.__tag__.json
# → {"size":"1","source":"client","filemode":"644","md5":"<md5>"}
```

---

## 3. 用例总览

| 组 | 名称 | 用例数 | 主要覆盖需求 | 层级分布 |
| --- | --- | --- | --- | --- |
| A | 接口契约 | 56 | API-01~API-10 · FR-01~FR-08 · FR-14 · FR-18 · FR-19 | L1/L2 为主 |
| B | init 与状态查看流程 | 6 | FR-02 · FR-03 | L2/L3 |
| C | push 全链路 | 30 | FR-05 · FR-09 · FR-10 · FR-11 · FR-20 | L2/L3 |
| D | pull / download | 10 | FR-07 · FR-17 | L2/L3 |
| E | remove | 6 | FR-08 | L2/L3 |
| F | 并发、幂等、崩溃恢复 | 8 | FR-11 · FR-12 · FR-13 | L2/L3 |
| G | 状态机与一致性 | 12 | FR-02 · FR-06 · FR-12 · FR-20 | L1/L2 |
| H | 安全 | 12 | SEC-01~SEC-08 · FR-01/09/17 | L2/L3 |
| I | 性能与容量 | 10 | NFR-01~NFR-10 · FR-04 | L3/L2 |
| J | 兼容、配置与运维 | 14 | FR-14 · FR-19 · OPS-01~07 · COMP-01~06 · API-11/12 | L4 |
| K | 任务侧 `oss://` | 10 | FR-15 · FR-16 | L1/L3 |
| L | 可观测性与审计 | 8 | FR-21 · NFR-06 · OPS-04/05 | L2/L3 |
| **合计** | | **182** | 见 §12 反向追溯表 | |

> 另有 §5 的 8 个端到端场景（E2E-01~E2E-08），由上述用例组合而成，不重复计数；
> 以及 §4.13 的 **12 条 DB 层用例（TC-DB-01~TC-DB-12）**，来源为《[数据库支撑性审计](workspace-server-db-audit.md)》§8，覆盖表结构/枚举/绑定参数/迁移与回滚。
> 详细用例合计 **194**（182 ＋ DB 层 12），加端到端场景 8 条，全量可执行项共 **202**。

---

## 4. 详细用例

> 表头说明：`层级` = L1/L2/L3/L4；`前置` 中「基线环境」指 §2.1 + §2.3 + §2.4 + §2.5-D1 已就绪；`覆盖` 列出需求 ID。

### 4.1 A 组 · 接口契约（TC-A01~TC-A56）

基础 URL 简写为 `$API`，token 简写：`T_A`、`T_B`、`T_ADMIN`。

#### 4.1.1 API-01 `POST /ugc/get_sts_token`（FR-01）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A01 | P0 | L1/L2 | 基线环境；U-A 已 `init` | 1) `POST $API/ugc/get_sts_token?token=T_A&name=demo&file_type=FileType.WORKSPACE` 2) 重复调用第 3 次 | → 3 次均 HTTP 200、`success=1`、含键 `oss`；`oss` 含且仅需 `endpoint/access_key_id/access_key_secret/security_token/bucket` 五个键；`bucket == 'hai-platform-private'`（即 `get_bucket_name(workspace, GROUP_SHARED)`，设计 §4.1 要点①）；缺省 `ttl_seconds` 时按 1800 计算 | FR-01, API-01 |
| TC-A02 | P0 | L2 | 同上 | 1) `ttl_seconds=100` 2) `ttl_seconds=90000` 3) `ttl_seconds=1800` | → 均 `success=1`；①生效 TTL 夹取为 900（响应头/日志中 `ttl=900`）；②夹取为 43200；③保持 1800；**任何取值都不得签发 >43200 的凭证** | FR-01, SEC-02 |
| TC-A03 | P0 | L2 | 同上 | 1) 取响应 `oss.access_key_id` 2) 与 `get_base_path(user,'demo',workspace)[1]` 比较 3) 在 `localfs` 中以该凭证写 `wsgrp/wstest_b/...` | → ①授权前缀 = `wsgrp/wstest_a/workspaces/demo/*`；②写入 U-B 前缀被拒绝（localfs 校验或 OSS 403）；③策略中无 `oss:*` 全量动作、无长期 AK/SK 下发 | FR-01, SEC-02 |
| TC-A04 | P1 | L2 | 基线环境 | 1) `name=demo%2Fx` 2) `name=`（空） 3) `file_type=bogus` 4) 缺失 `token` | → ①②③ 均 `success=0` + `code=INVALID_PARAM` + 非空 `msg`，HTTP 200；④ HTTP 401 + `success=0` + `code=UNAUTHORIZED`；**四者响应体均含 `success` 键**（CON-3） | FR-01, CON-3 |
| TC-A05 | P1 | L2 | 基线环境 | 1) 用 T_A 请求 `name=demo`（属 U-A）2) 用 T_B 请求 `name=demo`（属 U-A） | → ②`success=1` 但 `bucket` 一致、`endpoint` 一致；其授权前缀为 `wsgrp/wstest_b/workspaces/demo/*`（**不是 U-A 的前缀**）；用 T_B 凭证写 U-A 前缀 → 拒绝（403） | FR-01, SEC-01, SEC-02 |


#### 4.1.2 API-02 `POST /ugc/set_sync_status`（FR-02）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A06 | P0 | L2 | 基线环境；U-A 无 `demo` 记录 | 1) `POST $API/ugc/set_sync_status?token=T_A&file_type=FileType.WORKSPACE&name=demo&direction=SyncDirection.PUSH&status=SyncStatus.INIT&local_path=/tmp/hai-test/local/demo&cluster_path=`（`Content-Type: text/plain`，空 Body） | → HTTP 200；响应体 `{"success":1}`；DB：`select push_status,last_push from user_sync_status where user_name='wstest_a' and file_type='workspace' and name='demo'` → `init` 且 `last_push` 非空、`last_push::date = now()::date`；`local_path='/tmp/hai-test/local/demo'`、`cluster_path=''`（**未被清空以外的新增行**） | FR-02, API-02 |
| TC-A07 | P0 | L2 | TC-A06 后 | 1) 同 A06 但 `direction=SyncDirection.PULL&status=SyncStatus.STAGE2_RUNNING&cluster_path=/tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/demo&local_path=` | → DB 同一行：`pull_status='stage2_running'`、`last_pull` 已更新；**`push_status` 保持 `init` 不变**；`local_path` 仍为 `/tmp/hai-test/local/demo`（空值不覆盖旧值，设计 §4.2）；`cluster_path` 已写入 | FR-02, FR-14 |
| TC-A08 | P0 | L2 | TC-A07 后 | 1) 用**规范形态**重放 A07（`file_type=workspace&direction=pull&status=stage2_running`）2) 比较两次调用后的 DB 行 | → 第二次调用后 DB 行与第一次**逐列相同**（`updated_at` 更新除外）→ 归一化等价、幂等（FR-02 幂等，NFR-05） | FR-02, FR-14, NFR-05 |
| TC-A09 | P1 | L2 | 基线环境 | 1) `status=SyncStatus.BOGUS` 2) `direction=bogus` 3) `name=`（空） 4) `name=a%2Fb` | → 四者均 HTTP 200 + `success=0` + `code=INVALID_PARAM` + `msg` 含非法值与可选值列表；DB 无新增行；**不得返回 422 或裸 `{"detail":...}`** | FR-02, FR-14, CON-3 |
| TC-A10 | P1 | L2 | 基线环境；token 过期 | 1) `token=EXPIRED` 2) `token=`（缺） | → 均 HTTP 401、`success=0`、`code=UNAUTHORIZED`、无 DB 写 | FR-02, SEC-01 |

#### 4.1.3 API-03 `POST /ugc/get_sync_status`（FR-03）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A11 | P0 | L2 | U-A 无任何 `demo` 记录 | 1) `POST $API/ugc/get_sync_status?token=T_A&file_type=FileType.WORKSPACE&name=demo` | → HTTP 200 + `{"success":1,"data":[]}`（**不是** `success=0`、不是 404；客户端据此打印「没找到工作区」并终止，设计 §4.3 关键项） | FR-03, API-03 |
| TC-A12 | P0 | L2 | 已按 A06/A07 写入 1 行 | 1) 以 `name=demo` 查询 2) 校验字段集合与格式 | → `data` 长度 1；元素键**恰为** `name/local_path/cluster_path/push_status/last_push/pull_status/last_pull`；`last_push` 匹配 `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`（`to_char` 格式，设计 §7.2）；`push_status='init'`、`pull_status='stage2_running'` | FR-03, API-03 |
| TC-A13 | P0 | L2 | U-A 有 `demo`/`demo2`/`demo3` 三行，`updated_at` 依次递增 | 1) `name=*` 2) `name` 参数缺省 | → 两次均返回 3 条，**顺序严格为 `demo3, demo2, demo`**（`updated_at DESC`，设计 §4.3）；两次结果完全一致（`list` 输出稳定） | FR-03 |
| TC-A14 | P1 | L2 | U-A 有 `demo`；U-B 有 `demo` | 1) 用 T_A 查询 `name=demo` | → 仅返回 U-A 的行（`user_name` 只来自 token，设计 §6.1）；响应中不含 U-B 的 `local_path` | FR-03, SEC-01, SEC-04 |
| TC-A15 | P1 | L2 | 已软删的行（`deleted_at` 非空）存在 | 1) 查询 `name=*` | → 已软删行**不出现**在 `data` 中（`deleted_at is null`，设计 §7.2）；时间字段为 NULL 时 JSON 为 `null`，客户端可直接渲染不报错 | FR-03, FR-08 |

#### 4.1.4 API-04 `POST /ugc/cloud/cluster_files/list`（FR-04）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A16 | P0 | L2 | `cluster_base` 已有 D1 的 3 个文件（`a.txt`/`sub/b.bin`/`sub/deep/c.txt`） | 1) `POST $API/ugc/cloud/cluster_files/list?token=T_A&name=demo&file_type=FileType.WORKSPACE&no_checksum=false&no_hfignore=false&recursive=true&page=1&size=100`，Body `{"file_list":{"files":[]}}`（`text/plain`） | → `success` 隐含于 `items/total` 结构：返回键含 `items,total,page,size,pages`；`total=3`；`page=1`、`size=100`、`pages=1`；每个 item 含 `path,size,last_modified,md5`，`path` 为**相对路径**（如 `a.txt`、`sub/b.bin`）、无前导 `./`、无绝对路径；`md5` 等于本地 `md5sum`（前置：`R6` 需先确认 `fastapi-pagination` 的 `Params(page,size)` 可用） | FR-04, API-04 |
| TC-A17 | P0 | L2 | D2（10 万文件）已落盘 | 1) `size=100&page=1` 2) `page=1000` 3) `size=10000` | → ①`items` 长度 100、`total=100000`、`pages=1000`；②返回第 1000 页（≤100 条）且 `total` 不变；③`size` 被截断为 1000（`items` 长度 ≤1000），响应 `size=1000` | FR-04 |
| TC-A18 | P0 | L2 | D1 已落盘 | 1) `no_checksum=true` 2) `no_checksum=false` | → ①item **不含** `md5` 键（或为 `null`）；②item 含 `md5` 且与 `md5sum` 一致 | FR-04 |
| TC-A19 | P1 | L2 | `cluster_base/.hfignore` 忽略 `*.log`，目录含 `x.log` 与 `y.txt`；另存在 `.hfai/demo.zip` | 1) `no_hfignore=false` 2) `no_hfignore=true` | → ①`items` 不含 `x.log`、**绝不含** `.hfai/demo.zip`（FR-10 过滤要求，设计 §5.4）；②含 `x.log`，仍不含 `.hfai/*` | FR-04, FR-10 |
| TC-A20 | P1 | L2 | D1 已落盘 | 1) Body 为裸体 `{"files": []}` + `application/json` 2) Body 为 `{"file_list":{"files":["sub"]}}` + `text/plain` 3) 空 Body | → 三者均成功返回 `items/total`；②仅返回 `sub/` 下的 2 个文件（子路径限定）；③等价于 `files=[]`（全量），返回 `total=3` | FR-04, FR-14, COMP-02 |
| TC-A21 | P1 | L2 | D1 已落盘并已缓存 | 1) 首次调用（冷）计时 2) 立即重复调用（热）计时 3) 删除 `cluster_base/a.txt` 后翻到第 500 条校验点 | → ①冷调用返回正确 `total`；②热调用 ≤1 s（缓存命中，设计 §4.4）；③首文件不存在时返回 `success=0` + `code=CLIENT_RETRY`（缓存被清，设计 §4.4 一致性项） | FR-04, NFR-01 |

#### 4.1.5 API-05 `POST /ugc/sync_to_cluster`（FR-05）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A22 | P0 | L2 | 对象已存在（A 组前置可用 `localfs` 直接放置对象） | 1) `POST $API/ugc/sync_to_cluster?token=T_A&name=demo&file_type=FileType.WORKSPACE&no_zip=1`，Body `{"file_list":{"files":["a.txt","sub/b.bin"]}}` + `text/plain` | → HTTP 200；`success=1`；`msg='提交同步任务成功'`；键含 `index`（64 位小写 hex）、`dst_path`、`accepted=2`、`skipped=0`；`dst_path == /tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/demo`（**由 token 推导，与请求中的 local_path/cluster_path 无关**） | FR-05, API-05 |
| TC-A23 | P0 | L2 | 同 A22 | 1) 计算期望 `sha256(token+name+'workspace'+'a.txt'+'sub/b.bin')` 2) 与响应 `index` 比较 3) 交换 files 顺序再次提交并比较 | → ②两值逐字节相等；③顺序交换后 `index` **不同**（不排序，设计 §8.2/ADR-10） | FR-05, NFR-05 |
| TC-A24 | P1 | L2 | 同 A22 | 1) `no_zip=1` 2) `no_zip=0` 且 files=`['demo.zip']` 3) 不传 `no_zip` | → ①逐文件落到 `cluster_base/<fname>`；②落到 `cluster_base/.hfai/demo.zip` 后解压并删除临时 zip；③缺省视同 `no_zip=0`（客户端默认 `no_zip=false`，分析报告 §2.2） | FR-05, FR-10 |
| TC-A25 | P0 | L2 | 同 A22 | 1) 提交 `files=[]`（空列表） | → `success=1`、`accepted=0`；立即置 `status=finished`（设计 §5.2）；随后 `GET /ugc/sync_to_cluster/status?index=<返回值>` → `status='finished'`（不得为 `running`） | FR-05 |
| TC-A26 | P1 | L2 | 同 A22 | 1) `files` 含 `../etc/passwd` 2) 含 `/etc/passwd` 3) 含 `sub/../../x` | → 三者均 `success=0` + `code` ∈ {`PATH_ESCAPE`,`INVALID_PARAM`}；`cluster_base` 之外无任何文件被创建（`ls /tmp/hai-test/workspace/wsgrp/wstest_a/..` 无新增） | FR-05, SEC-03 |
| TC-A27 | P1 | L2 | 同 A22 | 1) 用 T_B 提交 `name=demo`（U-A 的工作区） | → `success=1` 但 `dst_path` 为 `.../wsgrp/wstest_b/workspaces/demo`（**用户以 token 为准**，不得写入 U-A 目录）；U-A 目录 mtime 不变 | FR-05, SEC-01 |

#### 4.1.6 API-06 / API-08 状态查询（FR-06）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A28 | P0 | L2 | 已提交一个大文件同步（D3），任务处于运行中 | 1) `GET $API/ugc/sync_to_cluster/status?token=T_A&index=<I>` 2) 同一 index 请求 `sync_from_cluster/status` | → ①`success=1`、`status='running'`、`msg` 为 JSON **number** 或纯数字字符串；`int(msg)` 不抛异常（客户端 `workspace_util.py:199`）；可选 `total` 存在时 `int(total)` 亦不抛异常；②方向键空间不同：`sync_from_cluster` 下同一 index 应查不到该方向（`NOT_FOUND_INDEX` 或对应方向状态），**不得串方向** | FR-06, API-06, API-08 |
| TC-A29 | P0 | L2 | 任务已完成 | 1) `GET .../sync_to_cluster/status?index=<I>`（终态） | → `{"success":1,"status":"finished","msg":""}`；`msg` 为空串（设计 §4.6），客户端不解析空 msg | FR-06 |
| TC-A30 | P0 | L2 | 任务已失败（注入 OSS 5xx 至 10 次重试耗尽） | 1) `GET .../sync_to_cluster/status?index=<I>` | → `{"success":1,"status":"failed","msg":"<可读原因>"}`；`msg` 非空、不含堆栈、不含 AK/SK、不含 token（SEC-05） | FR-06, SEC-05 |
| TC-A31 | P0 | L2 | 任意 index 不存在 | 1) `GET .../sync_to_cluster/status?token=T_A&index=deadbeef` 2) `GET .../sync_from_cluster/status?token=T_A&index=deadbeef` | → 均 HTTP 400 + `success=0` + `code='NOT_FOUND_INDEX'` + `msg='不存在的index'`（设计 §4.6）；**响应体含 `success` 键** | FR-06, CON-3 |
| TC-A32 | P0 | L2 | U-A 提交的 index=`I_A`；U-B 已登录 | 1) 用 T_B 查询 `index=I_A` | → HTTP 403 + `success=0` + `code='FORBIDDEN'`（Redis 中 `owner` 与请求用户不符，设计 §4.6/§12.3 SEC-04）；不泄漏 `I_A` 的任何进度信息 | FR-06, SEC-04, SEC-01 |

#### 4.1.7 API-07 `POST /ugc/sync_from_cluster`（FR-07）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A33 | P0 | L2 | `cluster_base` 已有 D1；配额充足 | 1) `POST $API/ugc/sync_from_cluster?token=T_A&name=demo&file_type=FileType.WORKSPACE`，Body `{"file_infos":{"files":[{"path":"a.txt","size":1,"last_modified":"<ts>","md5":"<md5>"}]}}` + `text/plain` | → `success=1`；键含 `index`（64 hex）、`accepted=1`、`skipped=0`、`upload_mb`（number）；`localfs` 中对象 `wsgrp/wstest_a/workspaces/demo/a.txt` 存在且**内容逐字节等于**集群文件 | FR-07, API-07 |
| TC-A34 | P1 | L2 | 同 A33 | 1) 上传完成后读取对象 tagging | → tagging 键含 `size=<字节数>`、`md5=<与本地一致>`、`source=cluster`、`filemode=<octal>`（如 `644`）；`expire_at` 可选（FR-09） | FR-09, FR-07 |
| TC-A35 | P1 | L2 | 同 A33 | 1) Body 为裸体 `{"files":[{...}]}` + `application/json` 2) `file_infos` 外壳 + `text/plain` | → 两者均 `success=1` 且 `accepted` 相同（FR-14 双外壳；客户端实际用外壳形态） | FR-14, COMP-02 |
| TC-A36 | P0 | L2 | 同 A33 | 1) `files` 含 `{"path":"../x"}` 2) 含 `{"path":"/etc/passwd"}` | → 均 `success=0` + `PATH_ESCAPE`；无对象被上传（`localfs` 前缀下对象数不变） | FR-07, SEC-03 |
| TC-A37 | P1 | L2 | `cluster_base` 内 `link_in -> ./a.txt`、`link_out -> /etc/passwd` | 1) 上传 `link_in` 2) 上传 `link_out` | → ①成功（`realpath` 仍在工作区内）；②`success=0` 或该文件被标记失败并跳过（`submit_from_cluster` 对该文件标记失败，设计 §5.3-1）；`/etc/passwd` 内容**不出现在 bucket** | FR-07, SEC-03 |
| TC-A38 | P1 | L2 | D3 已上传过（同 md5） | 1) 再次提交相同 `file_infos` 2) 观察 `accepted/skipped` 与对象 `last_modified` | → 对象 `last_modified` 不变（tagging `md5` 命中则跳过传输，FR-11；总量 ≤1 GiB 时不启用 `filter_synced_files` 属预期）；账目不重复计入（`user_downloaded_files` 中 `(file_path,file_md5)` 仍为 1 行） | FR-11, FR-07 |

#### 4.1.8 API-09 `POST /ugc/delete_files`（FR-08）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A39 | P0 | L2 | `cluster_base` 有 `a.txt`、`sub/` | 1) `POST $API/ugc/delete_files?token=T_A&name=demo&file_type=FileType.WORKSPACE`，Body `{"file_list":{"files":["a.txt"]}}` | → `success=1`；`cluster_base/a.txt` **不存在**；`sub/` 与其余文件仍在；`user_sync_status` 对应行 `deleted_at` 非空（软删，设计 §4.9） | FR-08, API-09 |
| TC-A40 | P0 | L2 | 同 A39（`sub/` 存在） | 1) `files=["sub"]`（目录） | → `success=1`；`cluster_base/sub` 整个目录被 `rmtree` 删除（`sub/b.bin` 不存在） | FR-08 |
| TC-A41 | P0 | L2 | 同 A39 | 1) `files=[]`（空列表） 2) 检查日志 | → `success=1`；整个 `cluster_base/demo` 被删除；日志中出现 **INFO 级审计**记录，含操作人 `wstest_a`、`name=demo`、files 为空、来源 IP（SEC-07）；**bucket 中的对象不被删除**（分析报告 §4.6/F6） | FR-08, SEC-07 |
| TC-A42 | P1 | L2 | 同 A39 | 1) 重复删除 `a.txt`（已不存在） 2) `files=["../etc"]` 3) 用 T_B 删 U-A 的 `demo` | → ①`success=1`（幂等，设计 §4.9）；②`success=0` + `PATH_ESCAPE` 且 `/etc` 未被触碰；③`success=0` + `FORBIDDEN`（或 `success=1` 但仅影响 U-B 自身目录——以「U-A 目录不变」为判定） | FR-08, SEC-03, SEC-04 |
| TC-A43 | P1 | L2 | 同 A39 | 1) Body 缺省（空 Body） 2) Body 非法 JSON `{oops` | → ①视同 `files=[]` → 删整区 + 审计日志；②`success=0` + `code=INVALID_BODY` + HTTP 200，**不得 500、不得裸 `{"detail":...}`** | FR-08, CON-3, FR-14 |

#### 4.1.9 FR-14 兼容层（TC-A44~TC-A50）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A44 | P0 | L2 | `legacy_param_compat=true`；基线环境 | 1) `file_type=FileType.WORKSPACE` 2) `file_type=workspace` 3) `file_type=WORKSPACE` 4) `file_type=FileType.workspace` 请求 `/ugc/get_sync_status` | → 四者响应体**逐字节相同**（`data` 内容与顺序一致）→ 归一化规则：取最后一个 `.` 之后部分并小写，同时接受大写（FR-14 表） | FR-14, COMP-01 |
| TC-A45 | P0 | L2 | 同上 | 1) `direction=SyncDirection.PUSH&status=SyncStatus.STAGE1_RUNNING` 2) `direction=push&status=stage1_running` 调 `/ugc/set_sync_status` | → 两者写出的 DB 行 `push_status` 均为 `stage1_running`（枚举对象绝不入 SQL，设计 §7.2 要点） | FR-14, FR-02 |
| TC-A46 | P0 | L2 | 同上 | 1) `Content-Type: text/plain; charset=utf-8` + 包裹 Body 调 `/ugc/sync_to_cluster` 2) `Content-Type: application/json` + 裸 Body | → ①`success=1`（手工解析 `Request.body()`，ADR-2/F3b）②`success=1`；两次 `index` 相同（files/name/file_type 一致时） | FR-14, CON-4, COMP-02 |
| TC-A47 | P0 | L2 | 同上 | 1) 请求 `/ugc/set_sync_status` 追加未知参数 `&foo=bar&username=wstest_b&group=wsgrp2&userid=99999` | → HTTP 200 + `success=1`（忽略未知参数，不 422，FR-14 未知参数行）；写入的 `user_name` 仍为 `wstest_a`（`username/group/userid` 一律忽略，SEC-01） | FR-14, SEC-01 |
| TC-A48 | P0 | L2 | `legacy_param_compat=false`（重启生效） | 1) `file_type=FileType.WORKSPACE` 调 `/ugc/get_sync_status` 2) `file_type=workspace` | → ①`success=0` + `code=INVALID_PARAM` + HTTP 200（开关关闭后枚举串被拒，设计 §13.4 第 2 行）②`success=1` | FR-14 |
| TC-A49 | P1 | L2 | `legacy_param_compat=true` | 1) `file_type=FileType.ENV` 调 `/ugc/get_sync_status` 2) Body `{"file_infos":{"files":[]}}` 的裸体变体 `{"files":[]}` | → ①按 `env` 归一化并查询 ENV 类型（不报错）；②正常解析为 `files=[]`（优先取包裹键、缺失时裸体，FR-14 表） | FR-14 |
| TC-A50 | P1 | L2 | 基线环境 | 1) 触发一次 FastAPI 校验失败：`page=abc` 调 `/ugc/cloud/cluster_files/list` 2) 缺失必填 `name` | → 均被全局 `RequestValidationError` 处理器改写为 `{'success':0,'code':'INVALID_PARAM','msg':'请求参数非法: ...'}` + HTTP 200（设计 §6.4/CON-3）；**响应体必须含 `success` 键**（客户端 `async_requests` 断言 `'success' in result`） | FR-14, CON-3 |

#### 4.1.10 API-10（FR-18，P1）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A51 | P1 | L2 | U-ADMIN 属 `ops` 组 | 1) `POST $API/ugc/set_cloud_storage_quota?token=T_ADMIN&user_name=wstest_a&quota_mb=204800&expire_time=2027-01-01 00:00:00` | → `success=1`；`quota` 表 `(wstest_a,'cloud_storage_quota')` 的 `quota=204800`、`expire_time` 正确（设计 §4.10） | FR-18, API-10 |
| TC-A52 | P1 | L2 | U-A 非 `ops`/`platform` 组 | 1) 用 T_A 调 `/ugc/set_cloud_storage_quota` 修改自己的额度 | → HTTP 403 + `success=0`（沿用 `get_internal_api_user_with_token(allowed_groups=['ops','platform'])`）；`quota` 表**未被修改** | FR-18, SEC-04 |
| TC-A53 | P1 | L2 | TC-A51 后 | 1) `quota_mb=-1` 2) `quota_mb=abc` 3) `user_name` 不存在 | → 均 `success=0` + 明确 `msg`；①② 不得写入非法值，③ 返回 `success=0` 而非 500 | FR-18 |

#### 4.1.11 FR-19 配置装载与启动自检（API-19 归属 FR-19）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A54 | P0 | L2 | `[cloud.storage]` **完整**（§2.4） | 1) 启动 ugc-server 2) 抓取 startup 日志 | → 无缺失键告警；`provider/endpoint/.../breakpoint_info_path` 9 个必填键全部就绪（设计 §9.2 `REQUIRED_KEYS`）；任一 `/ugc/*` 接口 `success=1` | FR-19 |
| TC-A55 | P0 | L2 | 删除 `service.workspace_path` 与 `role_arn` 两项后启动 | 1) 启动 2) 调 `/ugc/get_sts_token` 3) 调 `/ugc/set_sync_status` 4) 调 `/ugc/user/nodeport/create`（非云存储接口） | → ①启动日志给出**逐项**缺失清单（含这两项，且不含未缺失的键）；②③ HTTP 200 + `success=0` + `code='CLOUD_STORAGE_NOT_CONFIGURED'` + `msg='云存储未配置: 缺少 ...'`；④其他 ugc 接口**不受影响**（设计 §9.2 关键项）；无 500 裸异常 | FR-19, OPS-02 |
| TC-A56 | P1 | L2 | `provider=localfs` | 1) 触发任一云存储接口 2) 检查响应 `msg` 与日志 | → 响应 `msg` 中标注非生产 provider（如含 `localfs`）且日志有 WARN（设计 §9.2「避免 F5 式假成功」）；`MockApi` 签名已修正为 `(*args, **kwargs)`，调用**不得**出现 `TypeError`（设计 §9.3） | FR-19, COMP-01 |


### 4.2 B 组 · `init` 与状态查看流程（TC-B01~TC-B06）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-B01 | P0 | L3 | 老客户端；干净目录 `/tmp/e2e/demo`；U-A 无 `demo` 记录 | 1) `cd /tmp/e2e/demo && hai-cli workspace init demo`（`-p oss`） | → 客户端退出码 0；生成 `./.hfai/workspace.yml`，含且仅含 `workspace: demo`、`local: /tmp/e2e/demo`、`remote: wsgrp/wstest_a/workspaces/demo`、`provider: oss`（分析报告 §4.2）；DB 出现 `user_sync_status(user_name='wstest_a',file_type='workspace',name='demo')` 且 `push_status='init'`（客户端调用 `/ugc/set_sync_status`，分析报告 §4.3-5） | FR-02, FR-03 |
| TC-B02 | P0 | L3 | TC-B01 后 | 1) 在 `demo` 目录内任意子目录执行 `hai-cli workspace list` | → 输出表格 7 列（`workspace, local_path, cluster_path, push status, last push, pull status, last pull`），`demo` 行为当前工作区（加粗）；`push status = init`；`last push` 非空且格式 `YYYY-MM-DD HH:MM:SS`；退出码 0 | FR-03, API-03 |
| TC-B03 | P0 | L3 | U-A 无任何工作区记录 | 1) `hai-cli workspace list` | → 客户端正常渲染（可能为空表）且**不报错**、退出码 0；服务端 `POST /ugc/get_sync_status` 返回 `{'success':1,'data':[]}`（需求 FR-03 硬性要求；若服务端返回 `success=0` 客户端会抛 `请求失败`） | FR-03 |
| TC-B04 | P0 | L3 | 本地有 `.hfai/workspace.yml` 但服务端**无**记录（手工删除 DB 行） | 1) `hai-cli workspace push --force` | → 客户端在 `get_wc_with_check()` 阶段打印 `没找到工作区 demo` 并返回失败（退出码非 0），**不发任何 OSS 上传请求**（`localfs` 中 `demo` 前缀对象数保持 0）；服务端 `get_sync_status` 响应为 `success=1 & data=[]`（分析报告 §4.4 阶段 0） | FR-03 |
| TC-B05 | P1 | L3 | TC-B01 后；直接调接口 | 1) `POST /ugc/set_sync_status`（`direction=SyncDirection.INIT` 之外的非法组合，如 `direction=SyncDirection.PUSH&status=SyncStatus.INIT`）2) 再 `workspace list` | → ①服务端接受（`init` 是合法成员，方向为 push 时写 `push_status='init'`）；②`list` 中 `push status` 显示 `init`；③重复 `init`（同目录再跑 `init demo`）客户端提示「已经创建」且 DB 行不新增 | FR-02, FR-03 |
| TC-B06 | P1 | L3 | `demo2` 工作区已 init 并被 push 完成 | 1) `hai-cli workspace list`（当前目录不在任何工作区内） | → 表格包含 `demo2`；`push status = finished`、`last push` 为终态时间；排序与后端 `updated_at DESC` 一致（最近更新的工作区在首行） | FR-03 |

### 4.3 C 组 · push 全链路（TC-C01~TC-C30）

> C 组被测链路（分析报告 §4.4、设计 §14.1）：`get_sync_status` → `cluster_files/list`（分页）→ 差异计算 → zip 打包 → `get_sts_token` → `set_sync_status(stage1_running)` → OSS 上传（tagging）→ `set_sync_status(stage1_finished)` → `sync_to_cluster`（分批 50）→ 轮询 status → 集群落盘。

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-C01 | P0 | L3 | 基线环境 + D1 + TC-B01；集群侧为空 | 1) `hai-cli workspace push --force --no_zip` | → 退出码 0，输出「推送成功」；`cluster_base` 下出现 `a.txt`、`sub/b.bin`、`sub/deep/c.txt`，三者 `md5sum` 与本地一致；`localfs` 中对应 3 个对象存在且 tagging 含 `md5`/`size`/`filemode`/`source=client` | FR-05, FR-09 |
| TC-C02 | P0 | L3 | TC-C01 后（集群已有同样文件） | 1) 立即再次 `hai-cli workspace push --force --no_zip` 2) 统计本次上传字节 | → 客户端上传字节数为 **0**（需求 §10.2 增量正确性）；`cluster_files/list` 的 `total=3` 且 3 个文件 md5 与本地一致 → 差异为空；服务端 `sync_to_cluster` 未被调用或 `files=[]`（可接受，但**不得重传对象**：`localfs` 对象 `last_modified` 不变） | FR-04, FR-05 |
| TC-C03 | P0 | L3 | TC-C02 后；修改 `a.txt` 内容（md5 变化、size 不变或变化） | 1) `hai-cli workspace push --force --no_zip` | → 仅 `a.txt` 被上传（对象 `last_modified` 更新），`sub/b.bin`、`sub/deep/c.txt` 的对象 `last_modified` **不变**；客户端打印的 diff 仅含 1 个文件 | FR-04, FR-05 |
| TC-C04 | P0 | L3 | TC-C01 后；`--no_diff` | 1) `hai-cli workspace push --force --no_diff --no_zip` | → 跳过集群遍历（服务端 `cluster_files/list` **未被调用**，可用访问日志断言）；全部本地文件被重新上传（对象 `last_modified` 全部更新）；最终集群侧文件内容与本地一致 | FR-04, FR-05 |
| TC-C05 | P0 | L2/L3 | 集群侧为空；D1 已上传到 bucket | 1) 直接 `POST /ugc/cloud/cluster_files/list?size=100&page=1` 2) 用返回 `items/total` 计算差异 | → ①`items` 为 3 个**真实**文件（**绝不能是空列表**——F1：桩返回空列表会让客户端全量重传）；②`total` 与磁盘文件数一致；③若返回空列表，则客户端将重传全部文件，本用例判**失败** | FR-04 |
| TC-C06 | P0 | L3 | 集群侧已有 `cluster_only.txt`（本地不存在） | 1) `hai-cli workspace push --force --no_zip` 2) `hai-cli workspace diff` | → ①push 成功，`cluster_only.txt` **仍存在**（ADR-7：不做隐式删除）；②`diff` 的「集群未下载」组中列出 `cluster_only.txt` | FR-05 |
| TC-C07 | P1 | L3 | 本地新增 `new.txt` | 1) `hai-cli workspace push`（**不带 `--force`**） | → 客户端打印 diff 并以非 0 退出（`changed_files` 非空且未 force，分析报告 §4.4 阶段 2）；`localfs` 对象数不变、集群侧无 `new.txt` | FR-05 |
| TC-C08 | P0 | L3 | 1000 个小文件（前缀目录 `many/`）；`--no_zip` | 1) `hai-cli workspace push --force --no_zip` 2) 抓取服务端访问日志 | → 集群侧 1000 个文件全部存在且 md5 一致；`POST /ugc/sync_to_cluster` 被调用 **20 次**（每批 50，`ceil(1000/50)`，分析报告 §4.4 阶段 5）；每次 `success=1`、`accepted=50`；最终所有 index 状态为 `finished` | FR-05, FR-17 |
| TC-C09 | P1 | L3 | 服务端 `max_files_per_request` 临时改为 10；本地 30 个新文件 | 1) `hai-cli workspace push --force --no_zip` 2) 观察客户端提示 | → 服务端对超限批次返回 `success=0` + `code='TOO_MANY_FILES'`；客户端打印「推送失败」并重试 3 次（每次间隔 2 s，历时 ≈4 s）；集群侧无部分落盘的半批文件（提交阶段拒绝，设计 §4.5） | FR-05, FR-17 |
| TC-C10 | P1 | L3 | `.hfignore` 含 `*.log`；目录有 `a.log` | 1) `hai-cli workspace push --force --no_zip` 2) 再以 `--no_hfignore` 重跑 | → ①`a.log` 不在集群侧；`cluster_files/list` 的 `items` 也不含 `a.log`（双端同一份 `conf/utils.py`，COMP-04）；②`a.log` 落盘 | FR-04, COMP-04 |
| TC-C11 | P1 | L3 | 本地文件 mode=640（D1 的 `sub/b.bin`） | 1) `--no_zip` push 完成 2) `stat -c '%a'` 检查集群侧文件 | → 集群侧 `sub/b.bin` 权限为 `640`（从 tagging `filemode` 恢复，FR-05 权限与属主）；文件属主 uid == `user.user_id`(20001)（新建目录 `chown` 给目标用户） | FR-05, FR-09 |
| TC-C12 | P1 | L3 | 服务端以 root 运行；U-A 首次 push | 1) push 完成 2) `stat -c '%u:%g'` 检查 `cluster_base` 与子目录 | → `cluster_base` 及新建父目录属主为 20001（`ensure_dir(uid)`）；对已存在目录重复 `chown` **不报错**（FR-13 幂等要求） | FR-05, FR-13 |

#### 4.3.1 FR-09 对象命名与 tagging（TC-C13~TC-C16）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-C13 | P0 | L2 | bucket 中已有 `a.txt` 对象 | 1) 校验对象 key 2) 读取 tagging | → key **恰为** `wsgrp/wstest_a/workspaces/demo/a.txt`（`<group>/<user>/workspaces/<name>/<相对路径>`，FR-09 表）；tagging 含 `size=1`、`source=client`、`filemode=644`、`md5=<md5sum a.txt>`；`expire_at` 可选 | FR-09 |
| TC-C14 | P0 | L2 | `file_type=env`、`name=venv1` 的场景（直接调接口构造） | 1) 上传一个 env 对象并校验 key | → key 为 `wsgrp/shared/hfai_envs/wstest_a/venv1/<rel>`（FR-09 表 env 行）；`dst` 前缀为 `{env_path}/wsgrp/shared/hfai_envs/wstest_a/venv1` | FR-09 |
| TC-C15 | P0 | L2 | 已存在 `a.txt` 对象（md5 相同） | 1) 客户端侧 `--no_zip` 重传 2) 读取对象 tagging（手工将 tagging 删除后重试） | → ①tagging `md5` 命中 → 跳过传输（对象 `last_modified` 不变，FR-11）；②tagging **缺失**时按「无元数据」降级：正常重传、**不报错**（FR-09 兼容行） | FR-09, FR-11 |
| TC-C16 | P1 | L2 | `--no_checksum` 场景（客户端不写 md5） | 1) 上传 2) 读取 tagging 3) 再次 push | → ①tagging **不含** `md5` 键（客户端 `no_checksum` 时不写，分析报告 §4.4-4）；②服务端不因缺 `md5` 报错；③无法用 md5 去重时按 size/存在性差异处理，行为可解释（不产生 `KeyError`） | FR-09 |

#### 4.3.2 FR-10 zip 分发语义（TC-C17~TC-C20）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-C17 | P0 | L3 | 基线环境 + D1；默认 `no_zip=false` | 1) `hai-cli workspace push --force`（不带 `--no_zip`）2) 检查本地 `/tmp/<basename>.zip` 与 bucket | → 客户端生成 `/tmp/demo.zip` 并作为**单一对象**上传到 `wsgrp/wstest_a/workspaces/demo/demo.zip`（FR-10）；请求 `sync_to_cluster` 的 `no_zip` 缺省为 0 | FR-10 |
| TC-C18 | P0 | L2/L3 | bucket 中已有 `demo.zip` 对象（含 `sub/b.bin`） | 1) `POST /ugc/sync_to_cluster?...&no_zip=0`，`files=["demo.zip"]` 2) 等待 `finished` 3) 检查集群侧 | → 集群侧出现解压后的 `a.txt`、`sub/b.bin`；`cluster_base/.hfai/demo.zip` **已被删除**（临时 zip 清理，设计 §5.4）；解压文件权限由 zip `external_attr` 恢复（如 640） | FR-10 |
| TC-C19 | P0 | L2 | 同上 | 1) 解压完成后调 `cluster_files/list?no_hfignore=true&size=1000` | → `items` 中**不含** `.hfai/demo.zip`，也不含任何 `.hfai/*` 路径（FR-10 硬性要求：否则客户端会把 `.hfai/<name>.zip` 判为集群独有文件）；`total` 等于真实工作区文件数 | FR-10, FR-04 |
| TC-C20 | P0 | L2 | 同一 zip 已解压过一次 | 1) 再次提交同一 `demo.zip`（force）2) 检查落盘与临时目录 | → 二次解压幂等：文件内容正确、无 `.hfai/demo.zip` 残留、无权限报错（`chown` 幂等）；若 zip 内路径含 `../evil`，解压**拒绝**该条目（zip slip 防护，以 `check_is_subpath` 校验）且 `cluster_base` 外无新文件 | FR-10, SEC-03 |

#### 4.3.3 FR-11 断点重试（TC-C21~TC-C24）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-C21 | P0 | L2 | D3（10 GB）已上传到 bucket；注入下载第 3 分片失败 2 次 | 1) 提交 `sync_to_cluster` 2) 等待 `finished` 3) 对比集群侧 md5 | → 任务最终 `finished`；集群侧文件 md5 与 bucket 对象一致；日志中出现重试记录（次数 ≤10、间隔 1 s，FR-11）；**不产生重复 chown 报错**、`user_downloaded_files` 无重复记账 | FR-11 |
| TC-C22 | P0 | L2 | 同上，但注入持续失败至 10 次耗尽 | 1) 提交并轮询 2) 读状态接口 | → 状态 `failed`，`msg` 为首个错误原因；其他文件仍成功落盘（单文件失败不影响同批其他文件的可观测性，NFR-04）；DB `push_status='stage2_failed'` | FR-11, NFR-04 |
| TC-C23 | P0 | L2 | 大文件传输中（D3） | 1) 轮询 `GET .../sync_to_cluster/status`，每 1 s 一次 2) 记录 `msg` 序列 | → 每次 `int(msg)` 成功；序列**单调不减**（FR-20 单调性）；相邻两次变化间隔 ≥1 s 或增量 ≥1%（限频，设计 §7.4）；`msg` 收敛到文件总字节数后状态转 `finished` | FR-20, FR-11 |
| TC-C24 | P0 | L2 | 分片阈值 100 MB（`slice_bytes`） | 1) 上传 250 MB 文件 2) 检查 OSS 请求与断点目录 | → 产生 ≥3 个分片（100 MB 阈值/100 MB 分片）；并发线程数 4；断点文件位于 `{breakpoint_info_path}/{instance_id}`（**进程隔离**，设计 §5.5）；重跑时已达分片被跳过 | FR-11 |

#### 4.3.4 push 全链路收尾（TC-C25~TC-C30）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-C25 | P0 | L3 | 基线环境 | 1) `hai-cli workspace push --force --no_zip` 2) 抓取客户端 HTTP 请求序列 | → 请求顺序为：`get_sync_status` → `cluster_files/list` → `get_sts_token` → `set_sync_status(push,stage1_running)` → （OSS 直传）→ `set_sync_status(push,stage1_finished)` → `sync_to_cluster` → 若干 `sync_to_cluster/status`（设计 §14.1）；**所有请求 token 均在查询串**（CON-7） | FR-05, CON-7, API-01~06 |
| TC-C26 | P0 | L3 | 基线环境 | 1) `push` 完成后查 DB | → `user_sync_status.push_status='finished'`、`last_push` 为终态时间；Redis `localfs:sync_to_cluster:<index>:status='finished'` 且 TTL 在 `(1700, 1800]` 区间（终态 1800 s，ADR-6） | FR-06, FR-12 |
| TC-C27 | P0 | L3 | 客户端 `--sync_timeout 5` | 1) 提交一个 10 GB 上传并让服务端延迟（注入 20 s 的传输延迟）2) 观察客户端 | → 客户端在 5 s 后超时退出并报超时（客户端行为）；服务端任务**继续执行至完成**，最终 `status='finished'`（服务端不因客户端放弃而中止）；再次 push 因 md5 命中跳过已传文件 | FR-20, FR-11 |
| TC-C28 | P1 | L2/L3 | 服务端 `status_ttl_finished=60`（临时） | 1) 完成一次同步 2) 等 61 s 后查询状态 | → 终态过期后返回 HTTP 400 + `NOT_FOUND_INDEX`（可判定）；恢复到 1800 后重跑，等待 1800 s 内查询仍返回 `finished`（ADR-6 反向验证） | FR-06 |
| TC-C29 | P1 | L2 | 已存在 `demo` 工作区且集群侧为空 | 1) `POST /ugc/sync_to_cluster?file_type=FileType.WORKSPACE&no_zip=1`（**不带 token**）2) 带失效 token | → 两者 HTTP 401 + `success=0` + `code='UNAUTHORIZED'`；**无对象被下载、`cluster_base` 未被创建**（鉴权先于副作用，SEC-01） | SEC-01, API-05 |
| TC-C30 | P1 | L3 | 本地目录含空目录 `emptydir/`、0 字节文件 `zero.txt` | 1) `push --force --no_zip` | → `zero.txt` 落盘且 size=0、md5=`d41d8cd98f00b204e9800998ecf8427e`；空目录不报错（是否保留空目录允许两种实现，但 `success=1` 且客户端退出码 0） | FR-05 |


### 4.4 D 组 · pull / download（TC-D01~TC-D10）

> 链路（分析报告 §4.5、设计 §5.3）：差异计算（`subpath`）→ `sync_from_cluster`（file_infos，每批 50）→ 服务端集群→bucket + 记账 → 客户端取 STS → OSS 下载 → `chmod`(tagging `filemode`) → `set_sync_status(pull, finished)`。

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-D01 | P0 | L3 | 集群侧有 `ckpt/model.pt`（2 MB，不在本地）；本地为空目录 | 1) `hai-cli workspace pull --force` | → 本地出现 `ckpt/model.pt`，`md5sum` 与集群侧一致；bucket 对象 `wsgrp/wstest_a/workspaces/demo/ckpt/model.pt` 存在且 tagging `source=cluster`；客户端退出码 0 | FR-07, API-07 |
| TC-D02 | P0 | L3 | TC-D01 后 | 1) 立即再 `pull --force` | → 无差异 → `sync_from_cluster` 未被调用（或 `files=[]`）；本地文件 `mtime` 不变；对象 `last_modified` 不变 | FR-07 |
| TC-D03 | P0 | L3 | 集群侧文件 mode=600 | 1) `pull --force` 2) `stat -c '%a'` 检查本地文件 | → 本地文件权限为 `600`（从对象 tagging `filemode` 恢复后 `os.chmod`，分析报告 §4.5-4）；若 tagging 缺 `filemode` → 按默认 umask 落盘且**不报错** | FR-07, FR-09 |
| TC-D04 | P0 | L3 | 集群侧有 `ckpt/a.pt`、`ckpt/b.pt`、`logs/train.log` | 1) `hai-cli workspace download ckpt`（`subpath=ckpt`） | → 仅本地出现 `ckpt/a.pt`、`ckpt/b.pt`；`logs/` **未被下载**；请求 `sync_from_cluster` 的 `file_infos` 仅含 `ckpt/` 下路径（客户端把 `remote_path` 剥前导 `./` 后作为 subpath，分析报告 §4.5-1） | FR-07 |
| TC-D05 | P0 | L3 | 集群侧有 `ckpt/model.pt`；本地已存在**不同内容**的同名文件 | 1) `hai-cli workspace pull`（**不带 `--force`**）2) 带 `--force` 重跑 | → ①客户端打印 diff（本地与集群有差异）并非 0 退出；本地文件**未被覆盖**；②带 `--force` 后被集群内容覆盖，md5 等于集群 | FR-07 |
| TC-D06 | P0 | L3 | 本地存在 `local_only.py`（集群无） | 1) `hai-cli workspace pull --force` | → `local_only.py` **仍存在**（pull 只补缺失/差异，不删本地独有文件，分析报告 §4.5 语义要点）；无文件被删除 | FR-07 |
| TC-D07 | P0 | L2 | U-NOQUOTA（`cloud_storage_quota.download=0`）；集群侧有 10 MB 文件 | 1) `POST /ugc/sync_from_cluster`（body 含该文件 `size=10485760`） | → HTTP 403 + `success=0` + `code='QUOTA_EXCEEDED'`；`msg` 含**已用/本次申请/限额**三要素（FR-17）；无对象被上传；`user_downloaded_files` 无新增 `running` 行；审计日志含超配额拒绝记录（SEC-08） | FR-17, SEC-08, API-07 |
| TC-D08 | P1 | L2 | U-A 已用 99 GB（`user_downloaded_files` 预置 `finished` 行），配额 100 GB | 1) 提交 2 GB 文件 | → 403 + `QUOTA_EXCEEDED`（`used + upload ≥ limit`，设计 §5.3-2）；改为 0.5 GB 文件 → `success=1`、`upload_mb≈512`；两者判定可复现 | FR-17 |
| TC-D09 | P1 | L3 | 集群侧 120 个文件待 pull | 1) `pull --force` 2) 统计请求次数 | → `POST /ugc/sync_from_cluster` 被调用 3 次（`ceil(120/50)`，客户端固定 50/批，FR-17 单批项）；每次 `accepted=50/50/20`；最终本地 120 个文件全部 md5 一致 | FR-07, FR-17 |
| TC-D10 | P1 | L3 | 集群侧有 3 个文件；bucket 侧将其中 1 个对象的 tagging `md5` 改为错值 | 1) `pull --force` 2) 观察客户端是否报错 | → 服务端仍成功上传（md5 不匹配只影响跳过判断，不影响正确性）；客户端下载后本地 md5 与集群一致；`user_downloaded_files` 中该文件 `(file_path,file_md5)` 唯一（upsert 冲突键，设计 §7.2） | FR-07, FR-11 |

### 4.5 E 组 · remove（TC-E01~TC-E06）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-E01 | P0 | L3 | `demo` 已 push 且 `list` 可见；集群侧有 3 文件 | 1) `hai-cli workspace remove demo` 2) 确认二次确认提示 | → 客户端弹出二次确认（SEC-07）；确认后 `POST /ugc/delete_files` 且 `files=[]`；服务端 `success=1`；集群侧 `cluster_base` 整个目录不存在；DB 行 `deleted_at` 非空；本地 `.hfai/workspace.yml` 被删除（仅当是当前工作区，分析报告 §4.6） | FR-08, SEC-07 |
| TC-E02 | P0 | L3 | 集群侧有 `a.txt`、`sub/` | 1) `hai-cli workspace remove demo -f a.txt` | → `cluster_base/a.txt` 删除；`sub/` 与其余文件保留；`success=1`；客户端退出码 0 | FR-08, API-09 |
| TC-E03 | P0 | L3 | 同 E02 | 1) `hai-cli workspace remove demo -f sub` | → `cluster_base/sub` 目录树被删除（含 `b.bin`、`deep/c.txt`）；`a.txt` 保留 | FR-08 |
| TC-E04 | P1 | L3 | 已 remove 过 `demo` | 1) `hai-cli workspace remove demo -f a.txt` 2) 再次整体 `remove demo` | → ①客户端因 `get_sync_status` 无记录而提示工作区不存在并退出（或服务端返回 `success=1` 幂等成功，两者择一但**必须不抛异常**）；②幂等成功，无 500、无 `PathNotFound` 堆栈 | FR-08 |
| TC-E05 | P0 | L2 | U-A 有 `demo`；U-B 登录 | 1) 用 T_B 调 `/ugc/delete_files?name=demo&file_type=FileType.WORKSPACE`（`files=[]`） | → `success=0` + `FORBIDDEN`（或仅删除 U-B 自己的 `demo`）；**U-A 的 `cluster_base` 必须完好**（判定以文件系统实际结果为准）；日志含越权尝试记录 | FR-08, SEC-04 |
| TC-E06 | P0 | L2 | U-A 有 `demo` | 1) 调 `/ugc/delete_files` 且 `files=["../../wsgrp/wstest_b/workspaces/demo"]` 2) `files=["/tmp/hai-test/workspace"]` | → 均 `success=0` + `PATH_ESCAPE`；`/tmp/hai-test/workspace` 与 U-B 目录**未被删除**（`ls` 校验）；无 `rmtree` 越过 `cluster_base` | FR-08, SEC-03 |

### 4.6 F 组 · 并发、幂等、崩溃恢复（TC-F01~TC-F08）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-F01 | P0 | L2/L3 | `ugc=2`（两个 uvicorn worker）；Redis 干净 | 1) 启动服务（`recover_on_startup=true`）2) 抓取两个进程日志 | → 恢复逻辑**仅执行一次**：日志中 `recovery skipped: another worker holds the lock` 出现 ≥1 次，且 `SET NX localfs:recover:<pod_id>` 只有 1 个持有者（设计 §5.6 / CON-10）；无重复恢复提交 | FR-13, CON-10 |
| TC-F02 | P0 | L2 | 构造「崩溃残留」：写入 `localfs:sync_to_cluster:<index>:param:<dead_instance>`，其中 `instance` 不在心跳表且 `created_at` > 600 s | 1) 启动服务 2) 观察 Redis 与集群侧 | → 该 param 被认领：`param.instance` 被改写为当前 `instance_id`，随后以 `force=True` 重新执行；最终 `status='finished'`；已完成的文件靠 tagging `md5` 跳过（设计 §5.6） | FR-13, FR-11 |
| TC-F03 | P0 | L2 | 存活实例的 param（`instance` 在心跳表中且心跳新鲜） | 1) 触发一次恢复扫描 | → 该 param **不被认领**（`instance in live` → continue）；对应任务状态与进度不受影响（防止误抢存活 worker 的任务，ADR-5） | FR-13 |
| TC-F04 | P0 | L2/L3 | 传输进行中（10 GB 上传/下载） | 1) `kill -9 <ugc worker pid>` 2) 立即重启服务 3) 轮询状态至终态 | → 重启后任务自动续跑；状态最终为 `finished`（需求 §10.5）；续跑只补齐缺失分片（`localfs` 对象最终 md5 正确、无重复记账）；`RUNNING_TASKS_GAUGE` 在重启后回到 0（F9 `try/finally`） | FR-13, NFR-05 |
| TC-F05 | P0 | L2 | 注入单文件失败 3 次后成功 | 1) 提交该文件同步 2) 计次观察重试 | → 重试间隔 ≈1 s、总尝试次数 ≤10（FR-11）；**不重复写 DB 记账**：`user_downloaded_files` 中 `(file_path,file_md5)` 仅 1 行；无重复 `chown` 报错 | FR-11 |
| TC-F06 | P0 | L2 | 同一 `index` 正在 `running`（10 GB 任务） | 1) 用**完全相同**的参数再次提交（不带 force）2) 并发（`xargs -P 10`）提交同一批 10 次 | → ①返回 `success=1` + `msg='上一次同步正在进行中，忽略本次请求'`，`accepted=0`（设计 §5.2/§4.5）；②10 次并发中**仅 1 份传输任务**（`RUNNING_TASKS_GAUGE` 峰值 ≤1、worker pool 数 ≤1）；对象最终正确 | NFR-05, FR-12 |
| TC-F07 | P1 | L2 | 10 个不同 index 的同步任务并发提交 | 1) 并发提交 10 个任务 2) 观察 `WorkerPools` | → 前 10 个各占一个 pool（`max_pools=10`），第 11 个走共享池（设计 §12.1）；全部最终 `finished`；无 pool 泄漏（`shutdown_all` 后进程退出干净） | NFR-10, FR-13 |
| TC-F08 | P1 | L2 | 提交循环中注入异常（monkeypatch `run_in_executor` 抛错） | 1) 触发一次同步 2) 检查状态与 gauge | → 状态**不悬挂**：立即置 `failed`（`finalize_failed`，设计 §5.2 `except` 分支）；`cloud_storage_tasks_running` 回落到 0（try/finally，F9）；DB `push_status='stage2_failed'` | FR-13, NFR-04 |

### 4.7 G 组 · 状态机与一致性（TC-G01~TC-G12）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-G01 | P0 | L1 | `AioUserDbExtras.set_sync_status` 可测 | 1) 调 `set_sync_status('workspace','demo','push','stage1_running')` 2) 调 `set_sync_status(...,'stage1_finished')` | → 两次后 `push_status` 依次为 `stage1_running`、`stage1_finished`（状态可正向流转）；`last_push` 单调更新 | FR-02, FR-12 |
| TC-G02 | P0 | L1 | 同上 | 1) 以 `local_path=''`、`cluster_path=''` 调用 `set_sync_status`（已有非空旧值） | → 旧值**被保留**（`coalesce(nullif(excluded.local_path,''), 旧值)`，设计 §7.2）；`deleted_at` 被置回 null（软删行被复活） | FR-02, FR-12 |
| TC-G03 | P0 | L1 | 同上 | 1) 传入 3000 字符的 `local_path` 2) 传入 `status=SyncStatus.STAGE1_RUNNING`（枚举对象） | → ①落库长度恰为 2047（截断）；②SQL 参数只接受 `.value`，传入枚举对象触发 `ValueError`（**必须报错而非静默写入 `SyncStatus.STAGE1_RUNNING`**，设计 §7.2 要点/F2 同源风险） | FR-02, FR-12 |
| TC-G04 | P0 | L1 | 同上 | 1) 连续调 `set_sync_status` 相同参数 5 次 2) 查询 DB 行数 | → 行数恒为 1（主键 `(user_name,file_type,name)` upsert）；每次 `updated_at` 更新、`last_push` 更新；**幂等** | FR-02, NFR-05 |
| TC-G05 | P0 | L1/L2 | 可注入 Redis 阶段 | 1) `set_phase(index, to_cluster, INIT)` 2) `RUNNING` 3) `FINISHED` 4) 读状态接口 | → Redis `localfs:sync_to_cluster:<i>:status` 依次为 `init/running/finished`；接口 `status` 字段映射为 `init/running/finished`（设计 §7.3 表）；`running` 时 `msg` 为数字 | FR-06, FR-12 |
| TC-G06 | P0 | L1/L2 | 同上 | 1) `set_phase(index, to_cluster, FAILED, reason='md5 mismatch')` 2) 读接口 | → Redis 值为 `failed(md5 mismatch)`；接口返回 `status='failed'`、`msg='md5 mismatch'`（客户端 `raise Exception(msg)`，FR-06） | FR-06, FR-12 |
| TC-G07 | P0 | L2 | 任意 index | 1) `get_phase` 读不存在 key 2) 读方向反转 | → ①返回 `none`（不抛异常，不误判为 `init`）；②`is_upload` 参数决定 key 段（`sync_to_cluster` vs `sync_from_cluster`），互不串读 | FR-06 |
| TC-G08 | P0 | L2 | 一个进行中的 to_cluster 任务 | 1) `HSET progress` 写入 3 个对象字节数 2) 读状态接口 | → `msg` = 三个值之和（`hgetall` 求和，设计 §12.1）；`progress` 为 hash 类型；任务结束后该 key **被删除**（FR-12） | FR-12, FR-20 |
| TC-G09 | P0 | L2 | 一个已 `finished` 的任务 | 1) 读 `status` key 的 TTL 2) 读 `progress` key | → ①TTL ∈ (1700, 1800]（`status_ttl_finished`，ADR-6）；②`progress` 已不存在；③`owner` key 与 status 同 TTL | FR-06, FR-12 |
| TC-G10 | P0 | L1 | 模拟 DB 写失败（关闭 PG 或注入异常） | 1) 触发一次同步并让 `set_sync_status` 抛错 2) 检查传输与指标 | → 数据传输**不中断**，最终集群侧文件正确（FR-12：DB 失败不得影响传输）；`cloud_storage_db_failure_total{operation="set_sync_status"}` 计数 +1；日志 `logger.error` 记录（不抛给用户） | FR-12, NFR-06 |
| TC-G11 | P0 | L2 | 大文件传输中 | 1) 采样 `progress` hash 值序列 2) 在写入前尝试写入一个**更小**的值 | → 序列单调不减（仅新值更大时写入，设计 §7.4）；更小值被忽略；`msg` 在接口层为 JSON number（`int` 转换） | FR-20 |
| TC-G12 | P0 | L2 | 完成后立即读 PG 与 Redis | 1) 读取 PG `push_status` 与 Redis `status` 2) 注入 5 s 的 PG 写延迟后重试 | → ①两者均为终态（`finished`/`finished`）；②允许 ≤300 s 不一致（NFR-05），但 300 s 后必须收敛为一致；客户端 `list` 最终显示 `finished` | FR-12, NFR-05 |


### 4.8 H 组 · 安全（TC-H01~TC-H12）

> 手法约定：越权类用例统一「A 造数据 → B 访问 → 断言 403 且 A 的数据零变化」；路径类用例统一「构造逃逸路径 → 断言 `success=0` 且 base 之外文件系统零变化」；泄漏类用例统一扫「响应体 + 应用日志 + 异常栈」三处。

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-H01 | P0 | L2 | U-A / U-B / U-C 均可用 | 1) 以 T_A 调全部 9 个接口，追加 `&username=wstest_b&group=wsgrp2&userid=20003` 2) 以 T_B 调 `get_sync_status&name=*` | → ①所有接口行为与**不带**这三个参数完全一致（写入/读取的 `user_name` 均为 `wstest_a`，DB 行与文件系统路径均指向 U-A）；②T_B 的结果**不含** U-A 的任何工作区；③响应中不出现请求传入的伪造值 | SEC-01 |
| TC-H02 | P0 | L2 | 基线环境 | 1) `POST /ugc/get_sts_token?token=T_A&name=demo&file_type=FileType.WORKSPACE` 2) 解析返回凭证的授权前缀 3) 用该凭证尝试读 `wsgrp/wstest_b/workspaces/demo/a.txt` | → ①前缀 = `wsgrp/wstest_a/workspaces/demo/*`（设计 §4.1 要点③）；②越界读/写被拒（OSS 403 或 localfs 拒绝），**不得**返回他人对象内容；③TTL ∈ [900,43200]；④响应中仅含**临时**凭证（`security_token` 非空），无长期 AK/SK | SEC-02, FR-01 |
| TC-H03 | P0 | L2 | `[cloud.storage.provider]` 含 `localfs_root`/`access_key_secret` | 1) 触发一组成 fail 的请求（非法参数、路径穿越、配额超限）2) `grep -i` 响应体与日志 | → 响应与日志中**不出现** `access_key_secret`、长期 `access_key_id` 明文、`security_token`、用户 `token`（`token=` 已脱敏，`api/app.py:104-107`）；STS 凭证**不入库**（`select` PG 全表无该字段） | SEC-05 |
| TC-H04 | P0 | L2 | U-A 提交 index=`I_A`；U-B 可用 | 1) T_B 查 `I_A` 状态 2) T_B 用 `name=demo` 调 `sync_to_cluster`/`delete_files`/`get_sync_status` | → ①② 全部 403 + `success=0` + `code='FORBIDDEN'`（状态接口 HTTP 403，设计 §4.6）；U-A 的 `cluster_base`、DB 行、Redis 状态**零变化**（前后 `md5sum`/行数对比一致） | SEC-04 |
| TC-H05 | P0 | L2 | `/tmp/hai-test/workspace` 之外有诱饵文件 `/tmp/hai-test/outside.txt` | 1) `sync_to_cluster` `files=["../../outside.txt"]`、`files=["....//outside.txt"]` 2) `sync_from_cluster` `path="sub/../../outside.txt"` 3) `delete_files` `files=["../.."]` 4) `name="..%2F..%2Fetc"` | → ①②③④ 全部 `success=0`（`PATH_ESCAPE`/`INVALID_PARAM`）；诱饵文件存在且内容未变；`cluster_base` 之外的目录 mtime 不变；无任何对象被写入/删除 | SEC-03 |
| TC-H06 | P0 | L2 | 集群侧 `link_out -> /etc/passwd`、`link_dir -> /`（符号链接逃逸） | 1) `sync_from_cluster` 上传 `link_out` 2) `delete_files` 删除 `link_out` 3) `cluster_files/list` 列出该目录 | → ①该文件被拒绝/标记失败（`success=0` 或该文件 `status=failed`），bucket 中**无** `/etc/passwd` 内容（`localfs` 全库 grep 无 `root:x:`）；②删除操作对 `/etc` 无影响（`realpath` 校验）；③tagging 中恢复的 `filemode` 不得为 `0`（不产生 `chmod 000` 意外） | SEC-03, FR-09 |
| TC-H07 | P0 | L2 | U-NOQUOTA 配额 0 | 1) 提交 10 MB 文件 `sync_from_cluster` 2) 检查响应与副作用 | → HTTP 403 + `success=0` + `QUOTA_EXCEEDED`，`msg` 含已用/申请/限额（FR-17）；**无对象上传**、`user_downloaded_files` 无行；审计日志有超配额记录（SEC-08） | SEC-06, FR-17, SEC-08 |
| TC-H08 | P0 | L2 | `max_files_per_request=100`（临时） | 1) 提交 101 个文件 2) 提交 100 个文件 3) `cluster_files/list?size=99999` 4) `cluster_files/list?files=` 含 1001 个子路径 | → ①`success=0` + `TOO_MANY_FILES`；②`success=1`；③`size` 被截断为 1000（响应 `size=1000`，SEC-06 防 `list` 放大）；④超 1000 子路径被拒或截断，响应耗时不随 `files` 长度线性膨胀（限页） | SEC-06 |
| TC-H09 | P0 | L2 | `max_bytes_per_request` 临时改为 100 MB | 1) 提交 1 个 200 MB 文件（`size` 字段声明 200 MB）2) 提交 `size` 字段被篡改为 1 B 的同一文件 | → ①`success=0` + `PAYLOAD_TOO_LARGE`；②不得因客户端声明值小而绕过——以**服务端实际 `stat` 结果**为准（或至少不得落盘超出限额的文件）；两者均不产生部分上传 | SEC-06, FR-17 |
| TC-H10 | P0 | L2 | U-A 的 `demo` 有 3 个文件 | 1) 调 `delete_files` 且 `files=[]`（删整区）2) 检查审计日志 | → 日志中出现 INFO 级记录，字段含操作人 `wstest_a`、`name=demo`、`files=[]`（显式标注整区删除）、来源 IP、时间戳（SEC-07）；审计日志保留策略 ≥90 天（运维侧配置项在 Checklist 登记） | SEC-07, SEC-08 |
| TC-H11 | P1 | L2 | 触发：签发 STS、删工作区、超配额拒绝、越权拒绝 | 1) 收集四类事件的审计记录 2) 校验字段完整性 | → 每类均有独立审计事件，含 `event/user/name/ip/ts/result`；事件可被 `grep` 检索（SEC-08）；日志文件中 `token` 字段已脱敏为掩码 | SEC-08 |
| TC-H12 | P0 | L2 | 全量用例机器可跑 | 1) 对 `ugc_0.log` + 应用 stdout + 全部响应体执行泄漏扫描：`grep -nE 'access_key_secret' -e 'security_token' -e 'token=[^&[:space:]]+' -e 'AKID[A-Za-z0-9]{16,}'`（多 `-e` 模式以规避表格竖线歧义） 2) 检查异常栈响应 | → 扫描结果中除**脱敏掩码**外无命中；任何 4xx/5xx 响应体不含 traceback、SQL 语句、文件绝对路径以外的内部结构（SEC-05）；`cloud_storage` 相关 logger 中 `token` 恒为掩码 | SEC-05, CON-3 |

### 4.9 I 组 · 性能与容量（TC-I01~TC-I10）

> 压测模型统一为：`wrk`/`locust` 或 `asyncio` 并发客户端；`W` 并发数；`D` 数据规模；冷/热两轮；采集 P50/P99 与错误率。指标门槛取自 NFR-01~NFR-10 与设计 §12.1。

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-I01 | P0 | L2 | 基线环境；Redis 预热 | 1) `GET /ugc/sync_to_cluster/status`（已存在 index）W=50、持续 5 min 2) 采集 P99 | → P99 ≤ 100 ms（NFR-01）；错误率 0；无 DB 查询（仅 Redis，设计 §12.1）；`cloud_storage_ugc_request_seconds{api="status"}` 直方图可见 | NFR-01, NFR-06 |
| TC-I02 | P0 | L2/L3 | D2（10 万文件）已落盘 | 1) 冷启动调 `cluster_files/list?size=100&page=1` 计时 2) 立即热调用计时 3) W=10 并发热调用 | → ①冷调用 ≤30 s；②热调用 ≤1 s（缓存命中）；③10 并发热调用 P99 ≤1 s 且 `total=100000` 恒定（NFR-01/FR-04 性能项） | NFR-01, NFR-02, FR-04 |
| TC-I03 | P0 | L2 | 基线环境 | 1) `get_sts_token` W=20、1000 次 2) `get_sync_status` W=20、1000 次 3) `sync_to_cluster` 提交（files=50，不含传输）W=20 | → ①P99 ≤1 s；②P99 ≤200 ms；③P99 ≤500 ms（NFR-01）；三者错误率 <0.1% | NFR-01 |
| TC-I04 | P0 | L2/L3 | D4（1000 文件 / 10 GB）已上传 bucket；内网带宽 ≥100 MB/s | 1) 提交 `sync_to_cluster`（全部 1000 文件）2) 计时到 `finished` 3) 计算吞吐 | → 端到端 ≤10 min（NFR-02）；单实例吞吐 ≥100 MB/s（目标 200 MB/s，与 bucket 带宽相关）；`cloud_storage_synced_file_size_total` 累计 ≥10 GiB | NFR-02 |
| TC-I05 | P0 | L2 | D2（10 万文件） | 1) 全量 `sync_to_cluster`（分 2000 批 × 50）2) 计时并观察内存 | → 全部落盘且 md5 一致；总时长与文件数近线性、无 O(n) 内存增长（NFR-10：内存占用与文件大小/数量无关，流式/分片）；无 `MemoryError`、无 pool 泄漏 | NFR-03, NFR-10 |
| TC-I06 | P1 | L2 | 单工作区 20 万文件（D2 复制 2 份，共约 400 万行元数据的上界测试可抽样到 20 万） | 1) `cluster_files/list` 全量分页遍历 2) 校验 `total` 与页数一致性 | → `total=200000`、`pages=2000`（size=100）；遍历过程无页缺失/重复（`items` 去重后计数 = total）；`page.size` 上限 1000 生效 | NFR-03, FR-04 |
| TC-I07 | P0 | L2 | 单文件失败注入（1/100 文件），其余 99 成功 | 1) 跑完整批次 2) 读状态与指标 | → 任务整体 `failed`（或部分成功语义按设计 §4.5：执行期失败经状态接口暴露）；99 个成功文件在集群侧可验证、1 个失败可定位（NFR-04）；失败率指标只反映该文件 | NFR-04, FR-11 |
| TC-I08 | P1 | L2 | `WORKERS=4`（默认） | 1) 单任务 10 GB 传输时采样 RSS 2) 对比 `WORKERS=8` | → RSS 与文件大小**无关**（10 GB 文件不导致 GB 级 RSS，NFR-10）；`WORKERS` 可通过配置/环境变量生效；worker 数变化不改变正确性 | NFR-10 |
| TC-I09 | P1 | L1 | 单测套件已就绪 | 1) `pytest --cov=cloud_storage --cov=server_model/task_impl/workspace_resolver --cov-report=term` 2) 统计覆盖率 | → 领域层行覆盖率 ≥85%、整体 ≥70%（NFR-07）；`workspace_resolver`/`compat` 分支覆盖 100%；报告归档 | NFR-07, NFR-09 |
| TC-I10 | P1 | L2 | 双部署形态：形态 A（ugc 进程内）与形态 B（独立 `cloud-storage`） | 1) 同一组用例在两种形态各跑一次 2) 对比响应与落盘结果 | → 两形态结果等价（同一领域层，ADR-1）；形态 B 不新增必需中间件（复用 PG/Redis/对象存储，NFR-09）；形态 B 内部转发使用服务间 JWT 而非用户 token（设计 §13.1） | NFR-09, NFR-07 |


### 4.10 J 组 · 兼容、配置与运维（TC-J01~TC-J14）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-J01 | P0 | L4 | 旧客户端二进制（枚举串 + `text/plain` + `{"file_list":...}`）；服务端 `legacy_param_compat=true` | 1) 依次执行 `init/list/push --force --no_zip/diff/pull --force/download <sub>/remove -f <f>/remove` | → 7 个子命令全部成功（退出码 0，无 `请求失败`）；服务端日志中每个请求的 `file_type` 归一化后均为 `workspace`；DB 与文件系统结果符合 C/D/E 组断言 | COMP-01, COMP-02, FR-14, NFR-08 |
| TC-J02 | P0 | L4 | 新客户端（`file_type.value` + `application/json` 裸 Body）；`legacy_param_compat=false` | 1) 跑完整 7 子命令流程 | → 全部成功（设计 §13.4 第 3 行）；服务端不受 `legacy_param_compat=false` 影响（规范形态不需要归一化） | COMP-01, COMP-02, NFR-08 |
| TC-J03 | P0 | L4 | 旧客户端 + `legacy_param_compat=false` | 1) 执行 `push --force` | → `INVALID_PARAM`（HTTP 200，`success=0`）→ 客户端重试 3 次后打印 `推送失败` 并非 0 退出；**判定为预期行为**（用于验证客户端是否已升级，设计 §13.4 第 2 行） | COMP-01, FR-14 |
| TC-J04 | P0 | L4 | 两套客户端 + 同一服务端（`legacy_param_compat=true`） | 1) 旧客户端 push 一批文件 2) 新客户端对同一工作区 pull/diff 3) 反向再来一次 | → 双端看到的 `total/md5` 完全一致（COMP-04：`conf/utils.py` 副本同版本）；`diff` 无差异；zip 打包/解包结果逐字节一致 | COMP-04, NFR-08 |
| TC-J05 | P1 | L4 | 老客户端（只认 7 个字段） | 1) 服务端在 `get_sync_status` 响应中追加字段 `extra_field` 2) 老客户端跑 `list` | → 老客户端正常渲染 7 列、忽略未知字段、退出码 0（COMP-05 响应字段只增不减、老客户端忽略未知字段） | COMP-05 |
| TC-J06 | P0 | L2/L4 | 客户端 `local_path` 含 `&`、`#`、空格、中文（R4：未 URL 编码） | 1) `hai-cli workspace init` 于 `/Users/张三/my code&dir#1` 2) 执行 `push --force --no_zip` | → 传输**不受影响**（集群侧文件正确）；DB 中 `local_path` 可为被截断/破坏的展示值，但 `cluster_path` 与 `name` 必须正确（CON-6：`local_path` 只作展示性元数据，严禁参与路径推导）；服务端**不因该参数报错**、不 500；后续 `diff/list` 可正常执行；此用例同时登记 R4 的已知代价 | COMP-01, CON-6 |
| TC-J07 | P0 | L4 | `[cloud.storage]` 完整 | 1) 启动服务 2) 修改 `service.workspace_path` 后重启 3) 再次 push | → ①启动自检通过；②重启后新路径生效（配置热更为「重启生效」，OPS-02）；③重启前已完成的任务状态仍在 Redis/PG 中可查（不中断已完成任务） | FR-19, OPS-02 |
| TC-J08 | P1 | L4 | 审计开关 `RUN_AUDIT`；`breakpoint_info_path` 可写 | 1) 执行一轮审计（或触发 `run_audit`）2) 检查断点目录与残留 zip 3) 校验 DDL 变更流程 | → ①审计单实例执行（Redis `SET NX localfs:audit:lock`）；②断点目录按进程隔离、清理脚本可清理残留（OPS-06）；③P1 新表 DDL 可在线执行且可回滚（OPS-07）；④指标暴露后端与 `ugc_0.log` 均可用（NFR-07 可维护性） | OPS-06, OPS-07, NFR-07 |
| TC-J09 | P0 | L4 | `enabled=true&enabled_groups=wsgrp3&enabled_users=` | 1) U-GRAY（属 `wsgrp3`）push 2) U-A（属 `wsgrp`）push | → ①U-GRAY `success=1`；②U-A 返回 HTTP 200 + `success=0` + `code='FEATURE_DISABLED'` + 提示「您暂未开通云存储工作区功能」；③客户端打印提示并未 0 退出（非静默失败） | OPS-01, FR-05 |
| TC-J10 | P0 | L4 | 灰度 4 步：`enabled=false` → `enabled_groups=<内部组>` → 试点组 → 全量 | 1) 按设计 §13.2 逐步推进，每步跑 P0 冒烟集 | → ①`enabled=false` 时全部 `/ugc/*` 返回 `FEATURE_DISABLED`，且 ugc-server 其他接口（nodeport/train_image）零回归；②灰度组内成功、组外失败；③每步失败率与时延可观测 | OPS-01, OPS-04 |
| TC-J11 | P0 | L4 | 已完成若干同步；手工上传若干对象到 bucket、写入若干 PG 行 | 1) `enabled=false` 并重启（一级回滚）2) 回滚镜像（二级）3) 单关 `legacy_param_compat`（三级） | → ①接口立即返回 `FEATURE_DISABLED`；②已上传对象、PG 记录、Redis 状态**全部保留**；进行中任务随进程退出，旧版本无恢复逻辑 → 无脏数据（设计 §13.3）；③仅老客户端受影响、新客户端正常 | OPS-03, COMP-01 |
| TC-J12 | P0 | L4 | 监控接线；`cloud_storage_db_failure_total` 可注入 | 1) 制造任务失败率 >5%（5 min）2) 制造积压 >100（提交 150 个任务）3) 制造 bucket 用量 >90% 4) 注入 1 次 DB 失败 | → 四项告警**各自触发**（OPS-04）：失败率、积压、用量、`db_failure_total>0`；`ugc_0.log` 中同步任务日志含 `index` 便于检索（OPS-05）；日志保留 ≥7 天（运维侧配置） | OPS-04, OPS-05 |
| TC-J13 | P1 | L4 | U-A 有 2 个工作区、若干对象；`cloud_storage_quota.download=100 GB` | 1) `GET /ugc/cloud_storage/usage?token=T_A` | → `success=1`；含 `used_mb`（等于 `user_downloaded_files` 中 `status='finished'` 的 size 之和/1 MiB）、`quota_mb`、`file_count`、`workspaces`（长度 2，元素含 `name`）；`used_mb` 与 §TC-A51 设置的额度一致（P1，API-12） | API-12 |
| TC-J14 | P1 | L4 | `file_type=env` 场景 | 1) `POST /ugc/update_cluster_venv` 2) 校验返回路径 | → `success=1` + `path` 指向 env 的集群路径（`{env_path}/<group>/shared/hfai_envs/<user>/<name>`）；**仍受客户端 `FileType.ENV` 字符串化缺陷限制**（需客户端修复后才有端到端意义，分析报告 F7）→ 本用例只验证服务端契约，端到端标记 `[待确认]`（P1，API-11） | API-11 |

### 4.11 K 组 · 任务侧 `oss://`（TC-K01~TC-K10）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-K01 | P0 | L1 | `workspace_path=/tmp/hai-test/workspace`；U-A 的 `demo` 目录存在 | 1) `resolve_workspace_path(user_a, 'oss://wsgrp/wstest_a/workspaces/demo', check_exists=True)` | → 返回 `/tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/demo`（逐字符相等，无尾斜杠）；函数为纯函数（不触碰 Redis/OSS，设计 §3.3 依赖约束） | FR-15 |
| TC-K02 | P0 | L1 | 同上 | 1) `resolve_workspace_path(user_a, 'oss://wsgrp2/wstest_a/workspaces/demo')` 2) `'oss://wsgrp/wstest_b/workspaces/demo'` 3) `'oss://wsgrp/wstest_a/workspaces/'` 4) `'oss://wsgrp/wstest_a/workspaces/a/b'` | → 四者均抛 `TaskSchemaError`（`workspace 不属于当前用户` / `workspace 名称非法`，设计 §10.1）；抛出**不得**发生在任务创建成功之后（任务创建接口返回 `success=0` + 文案） | FR-15 |
| TC-K03 | P0 | L1 | 同上 | 1) `resolve_workspace_path(user_a, 's3://wsgrp/wstest_a/workspaces/demo')` 2) `'OSS://wsgrp/wstest_a/workspaces/demo'` | → ①`TaskSchemaError('不支持的 workspace scheme: s3')`；②scheme 大小写不敏感 → 解析成功（设计 §10.1 `scheme.lower()`） | FR-15 |
| TC-K04 | P0 | L3 | `demo` 已 push 到集群 | 1) 提交 v2 任务，`spec.workspace='oss://wsgrp/wstest_a/workspaces/demo'` 2) 进入 pod 执行 `echo $MARSV2_TASK_WORKSPACE; pwd` | → 环境变量与 `cd` 目录均为 `/tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/demo`（对用户透明，FR-15）；`train.py` 可被执行（entrypoint 相对路径正确） | FR-15 |
| TC-K05 | P0 | L1/L3 | 集群共享盘路径 `/nfs_shared/code`（非 URI） | 1) `resolve_workspace_path(user_a, '/nfs_shared/code')` 2) 提交 `spec.workspace='/nfs_shared/code'` 的任务 | → ①**原样返回**（透明透传，设计 §10.1）；②任务提交结果与改动前**完全一致**（不追加挂载、不改写环境变量）；该用例为「非 URI 路径行为不变」的回归保护 | FR-15 |
| TC-K06 | P0 | L1/L3 | U-A 的 `demo` **未**同步到集群（目录不存在） | 1) `resolve_workspace_path(..., check_exists=True)` 2) 通过任务创建接口提交该 workspace | → ①抛 `TaskSchemaError('workspace [demo] 尚未同步到集群，请先执行 \`hai-cli workspace push\`')`；②任务创建接口返回 `success=0` + 上述文案，**不产生「任务已创建但立即失败」**（设计 §10.1 错误可读）；同时验证 `check_exists=False` 时不抛错（挂载阶段语义） | FR-15 |
| TC-K07 | P0 | L1 | U-A 提交 `spec.workspace='oss://wsgrp/wstest_a/workspaces/demo'` | 1) 调用 `add_runtime_mounts(task_impl)` 2) 检查 `task_impl._runtime_mounts` 末尾项 | → 追加 1 项且字段完全为：`host_path=mount_path=/tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/demo`、`mount_type='DirectoryOrCreate'`、`read_only=False`、`name='workspace-path'`（设计 §10.2） | FR-16 |
| TC-K08 | P0 | L1/L3 | `storage` 表中已有 `mount_path == 集群路径` 的挂载项 | 1) 调 `add_runtime_mounts` 2) 检查挂载项数量 | → **不追加**重复项（`mount_path` 去重，设计 §10.2 要点 3）；pod spec 中 `mountPath` 唯一（否则 k8s 报错）；`personal_storage()` 在 `__init__` 中命中缓存（`storage_df` 为 `cached_property`），不产生额外 SQL | FR-16 |
| TC-K09 | P1 | L3 | `spec.workspace='oss://wsgrp/wstest_a/workspaces/demo'`，pod 已启动 | 1) 检查 pod spec 的 volumes/volumeMounts 2) 在 pod 内写文件 `checkpoint.pt` 3) 回到宿主检查 | → `hostPath.path == mountPath == 集群路径`；`type=DirectoryOrCreate`（路径缺失也能起 pod）；容器内写入的文件出现在宿主 `/tmp/hai-test/workspace/...`（`read_only=False`，训练可写 checkpoint）；`cd {code_dir}` 与 `MARSV2_TASK_WORKSPACE` 语义不变 | FR-16 |
| TC-K10 | P1 | L1 | `spec.workspace` 为空串 / 字段缺失 / 为 `/tmp/hai-test/workspace`（在 `workspace_path` 前缀内但非 URI） | 1) 分别调 `add_runtime_mounts` | → ①空/缺失：直接 return，不追加挂载、不抛错；②前缀匹配但非云存储 URI：允许追加（`cluster_path.startswith(workspace_path)`）或按设计 return——两种实现均判通过，但**必须与 `resolve_workspace_path` 的返回一致**（不得挂载一个未被解析的路径） | FR-16 |


### 4.12 L 组 · 可观测性与审计（TC-L01~TC-L08）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-L01 | P0 | L2 | `/metrics` 已接线（Prometheus 文本格式） | 1) 依次调用 9 个接口各 3 次，其中 1 次故意失败 2) `curl :8083/metrics` 后过滤 `cloud_storage_ugc_request` | → `cloud_storage_ugc_request_total` 按 `api,result,code` 有标签组合可查（成功 3 次记 `success`、失败记对应 `code`）；`cloud_storage_ugc_request_seconds` 直方图含每个 `api` 的 `_bucket/_sum/_count`（设计 §12.2）；**每个接口都有 QPS 与错误率可算**（NFR-06） | NFR-06 |
| TC-L02 | P0 | L2 | 跑一次完整的 push 与 pull | 1) 采集任务类指标 | → 存在 `cloud_storage_sync_task_seconds{direction,file_type}`、`cloud_storage_sync_task_files{direction,file_type}`、`cloud_storage_synced_file_size_total`、`cloud_storage_synced_file_num_total`；任务结束时 `cloud_storage_tasks_running` 回落 0；失败时 `cloud_storage_tasks_failed_total` +1（NFR-06；F9 的 gauge 修复） | NFR-06, FR-13 |
| TC-L03 | P0 | L2 | `ugc_0.log` 可读 | 1) 抓取一次 push 与一次 failed 的日志行 2) 用正则校验 | → 日志格式符合 `[WORKSPACE] user=<> name=<> file_type=<> index=<前10位> action=<> files=<> bytes=<> cost=<ms> result=<ok 或 err:code>`（设计 §12.2）；`index` 出现在日志中便于检索（OPS-05）；`token` 字段为掩码（SEC-05） | NFR-06, OPS-05, SEC-05 |
| TC-L04 | P0 | L2 | 注入 1 次 PG 写失败 | 1) 采集 `cloud_storage_db_failure_total` 2) 检查告警规则 | → `cloud_storage_db_failure_total{operation="set_sync_status"}` +1 且触发告警（OPS-04）；**传输不受影响**（与 TC-G10 联动，判定以文件正确性为准） | NFR-06, OPS-04, FR-12 |
| TC-L05 | P1 | L2 | 集群侧存在 `.hfai/demo.zip`（mtime 25 h 前）与一个 `expire_at` 已过期的对象 | 1) 触发审计 `run_audit()` 2) 检查文件系统与 bucket | → ①`expire_at < now` 的对象被 `batch_delete_objects` 删除（分批 500）；②`<workspace_path>/**/.hfai/*.zip` 且 mtime >24 h 被清理（设计 §11）；③`RUN_AUDIT` 关闭时两者均保留（开关有效） | FR-21 |
| TC-L06 | P1 | L2 | 两个 worker（`ugc=2`）同时到审计时刻 | 1) 同时触发 `run_audit()` 2) 检查日志与 Redis 锁 | → 仅一个实例真正执行（`SET NX localfs:audit:lock` TTL 300 s 续期，设计 §11）；另一实例日志记录跳过；删除操作**不重复执行**、无 `KeyNotFound` 异常 | FR-21, CON-10 |
| TC-L07 | P1 | L2 | 预置 `localfs:sync_to_cluster:<i>:status=running` 且 `updated_at` 为 25 h 前 | 1) 触发审计 2) 检查 Redis 与 worker pool | → 状态被置为 `failed`（设计 §11 悬挂任务清理，频率 15 min）；对应 worker pool 被释放（`shutdown_all`/`finish` 幂等）；`cloud_storage_tasks_running` 回落 | FR-21, FR-13 |
| TC-L08 | P1 | L2 | bucket 中 U-A/U-B 各若干对象 | 1) 触发用量统计 2) 读 `cloud_storage_bucket_usage_size` | → 按 `{group,user,file_type}` 标签聚合出用量（F9：该指标原有定义无写入点，本设计补齐）；数值与 `localfs` 目录实际字节数一致（±<1%）；`file_type=workspace` 与 `env` 分开统计 | FR-21, NFR-06 |

---

### 4.13 DB 层用例（TC-DB-01~TC-DB-12）

> 来源：《[数据库支撑性审计](workspace-server-db-audit.md)》§8。用于验证「表结构与访问层能否支撑实现」，其中 TC-DB-07/08 专门复现 `db/mars_db.py` 的绑定参数陷阱（审计 §4 的 C1/C2）。

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-DB-01 | P0 | L1 | 可连 `mars_db`（只读权限即可） | 1) 执行审计 §9.1 的 ①~④ 段 SQL | → `user_sync_status`、`user_downloaded_files` 均存在；`information_schema.columns` 列集合与 `db_schemas/010,011` 一致；`file_type` 6 个标签、`sync_status` 9 个标签且与 `conf/utils.py` 的 `FileType`/`SyncStatus` **无差集**；两表 `updated_at` 触发器存在；主键与索引定义正确 | CON-9、ENV-01、DB-01 |
| TC-DB-02 | P0 | L2 | U-A 无 `demo` 行 | 1) `set_sync_status(file_type=workspace,name=demo,direction=push,status=init,local_path=/tmp/x,cluster_path=/tmp/y)` 2) 查该行 | → 新增 1 行；`push_status='init'`、`last_push` 非空且 `last_push::date = now()::date`；`local_path`/`cluster_path` 与入参一致；`user_role` 写入 `'internal'/'external'`（varchar，非枚举，不报错）；`updated_at = created_at` | FR-02 |
| TC-DB-03 | P0 | L2 | 接 TC-DB-02 | 1) 以 `direction=pull,status=stage2_running,local_path='',cluster_path=<新值>` 再写一次 2) 对比前后行 | → **行数不变**（同一 PK）；`pull_status='stage2_running'`、`last_pull` 刷新；`push_status` 保持 `init`；**`local_path` 保持旧值**（`COALESCE(NULLIF(...,''))`）；`cluster_path` 更新为新值 | FR-02、NFR-05 |
| TC-DB-04 | P0 | L2 | 执行一次 `pull`（集群→bucket）成功 | 1) 查 `user_downloaded_files` | → 出现对应行：`file_path` 为**集群绝对路径**（`/…/workspaces/demo/…`）、`file_size` 与文件一致、`status='finished'`、`file_md5` 与对象 tagging 的 `md5` 一致、`file_mtime` 与源文件 mtime 字符串一致 | FR-07 |
| TC-DB-05 | P0 | L2 | 接 TC-DB-04 | 1) 修改同一路径文件内容（md5 变化）2) 再 `pull` 一次 3) 统计两种口径 | → 该 `file_path` 出现**第 2 行**（md5 不同，PK 不冲突）；`sum(file_size)`（累计口径）≈ 2×size，按 `file_path` 去重后（净占用口径）= size → **实现必须与产品确认的口径一致**（审计 §5 G3、Checklist DB-07） | FR-17、G3 |
| TC-DB-06 | P1 | L2 | 已有 1 行 | 1) 连续 2 次以完全相同参数写状态 2) 查 `updated_at` 与行数 | → 行数不增；两次均成功（无唯一键冲突异常）；`updated_at` 单调递增（触发器生效） | FR-02 |
| TC-DB-07 | P0 | L1 | 可执行 SQL | 1) 用 `await MarsDB().a_execute('select count(*) from "user_sync_status" where "file_type" = %s::file_type', ('workspace',))` 2) 再用 `... = %s` 重试 | → 第一次**必须失败**（`%s::file_type` → `:p1::file_type` → 绑定参数被解析为 `:p`，SQL 变形/报错，复现审计 §4 C1）；改用 `%s` 或 `CAST(%s AS file_type)` 后成功返回 | §4 C1、DB-06 |
| TC-DB-08 | P1 | L1 | 可执行 SQL | 1) 执行带参数且含 `like 'abc%'` 的 SQL 2) 改写成 `like 'abc%%'` 重试 | → 第一次失败或参数错位（审计 §4 C2）；第二次成功……（若有 `%` 语义需求，须确认 `%%` 在 sqlparams→text 链路上语义正确）；同时验证**无参数**的同类 SQL 不受影响 | §4 C2、DB-06 |
| TC-DB-09 | P1 | L2 | — | 1) `name` 传 2048 个字符 2) `local_path` 传 3000 个字符 | → 服务端按 2047 截断后写入成功；**不出现** `value too long for type character varying(2047)`；`get_sync_status` 能读回 | 设计 §7.2 |
| TC-DB-10 | P1 | L2 | 用一个数据库中不存在的 token | 1) 调 `set_sync_status` | → 401 + `success=0` + `code=UNAUTHORIZED`（`AioUserSelector` 先校验）；**DB 无任何新增行** | SEC-01 |
| TC-DB-11 | P1 | L2 | U-A 有一条 `deleted_at` 非空的行 | 1) `get_sync_status(name='*')` 2) 再 `workspace init` 同名工作区 | → ①该行**不返回**；②upsert 后 `deleted_at` 被置回 `NULL`，该工作区重新出现在 `list` 中 | FR-08、FR-03 |
| TC-DB-12 | P1 | L1 | 仅在采纳 P1 DDL 时执行 | 1) 在预发库执行审计 §6 迁移脚本 2) 执行 §6.4 回滚脚本 3) 再次执行迁移脚本 | → 三次均成功（全部 `if not exists`/`drop if exists`，幂等）；列/索引状态符合预期；`alter table add column` 后**旧代码仍可正常读写**（只增列） | DB-03/04/05 |

---

## 5. 端到端场景（E2E）

> 场景在 L3 环境执行，使用**真实老客户端**（未升级 `haiworkspace` 插件）。每个场景给出编号步骤与「期望系统状态」（接口 + 文件系统 + DB/Redis 三侧）。

### E2E-01 本地首次 push → 提交任务跑通（主场景，P0）

1. 干净本地目录 `/tmp/e2e/ws1`，写入 `train.py`、`sub/data.txt`、`.hfignore`（忽略 `*.pyc`）。
2. `hai-cli workspace init ws1`。
3. `hai-cli workspace push --force --no_zip`。
4. `hai-cli workspace diff`。
5. `hai-cli workspace list`。
6. 用 `spec.workspace=oss://wsgrp/wstest_a/workspaces/ws1` 提交 v2 任务。
7. pod 内 `cat $MARSV2_TASK_WORKSPACE/train.py` 并运行之。

**期望系统状态**

| 侧 | 期望 |
| --- | --- |
| 接口 | `get_sync_status`（`success=1,data=[init]`）→ `cluster_files/list`（`total=0`）→ `get_sts_token`（`success=1,oss.*`）→ `set_sync_status`（`stage1_running`→`stage1_finished`）→ `sync_to_cluster`（`success=1,index,dst_path`）→ `status` 轮询至 `finished`；全部响应含 `success` |
| 文件系统 | `cluster_base = /tmp/hai-test/workspace/wsgrp/wstest_a/workspaces/ws1` 下 `train.py`、`sub/data.txt` 存在且 md5 一致；`.pyc` 不存在；属主 uid=20001；`.hfai/` 无残留 zip |
| 对象存储 | `wsgrp/wstest_a/workspaces/ws1/{train.py,sub/data.txt}` 存在，tagging 含 `size/md5/filemode/source=client` |
| DB/Redis | `user_sync_status.push_status='finished'`；`localfs:sync_to_cluster:*/status` 为 `finished`（TTL 1800 s） |
| 任务 | pod 的 `MARSV2_TASK_WORKSPACE` 与 `pwd` 均为集群路径；`train.py` 可执行且产出文件落在同一路径 |

### E2E-02 增量 push（第二次 0 字节，P0）

1. 承 E2E-01，立即再执行 `hai-cli workspace push --force --no_zip`，开启客户端 verbose 统计上传量。
2. 修改 `train.py` 一行后第三次 push。
3. 修改 `sub/data.txt` 的权限（内容不变，mode 644→600）后第四次 push。

**期望系统状态**：第二次上传字节数 **0**，`cluster_files/list` 的 `total` 与 md5 与本地一致、无对象 `last_modified` 变化；第三次仅 `train.py` 的对象 `last_modified` 更新（其余不变），`diff` 无差异；第四次内容 md5 不变 → **0 字节上传**，但（若客户端把 mode 纳入差异则仅重传该文件；否则不重传）→ 两者均可接受，判定以「集群侧 `sub/data.txt` 内容 md5 不变且任务可读」为准。

### E2E-03 pull 取回 checkpoint（P0）

1. 在集群侧 `cluster_base` 下由任务写出 `checkpoint/model.pt`（2 MB）与 `logs/train.log`。
2. 本地执行 `hai-cli workspace pull --force`。
3. `md5sum` 比对。

**期望系统状态**：bucket 中出现 `wsgrp/wstest_a/workspaces/ws1/checkpoint/model.pt`（tagging `source=cluster`、`md5`、`filemode`）；本地出现同名文件且 md5 一致、权限按 tagging 恢复；`user_downloaded_files` 新增 2 行 `status='finished'`；DB `pull_status='finished'`；本地独有文件（如 `notes.md`）**未被删除**。

### E2E-04 download 单个子路径（P0）

1. 集群侧存在 `checkpoint/a.pt`、`checkpoint/b.pt`、`logs/train.log`。
2. 本地执行 `hai-cli workspace download checkpoint`。

**期望系统状态**：本地仅有 `checkpoint/a.pt`、`checkpoint/b.pt`（`logs/` 未下载）；`sync_from_cluster` 请求的 `file_infos` 仅含 `checkpoint/` 前缀路径（`subpath` 剥前导 `./`，分析报告 §4.5-1）；配额按 2 个文件累计；`pull_status='finished'`。

### E2E-05 多用户隔离（P0）

1. U-A 与 U-B 各自 `init` 同名工作区 `demo`，写入**不同内容**的 `train.py`。
2. 两人各自 `push --force --no_zip`。
3. U-B 尝试 `pull` U-A 的 workspace（通过手工把 `remote` 改成 `wsgrp/wstest_a/workspaces/demo` 后执行 pull）。
4. U-B 尝试 `delete_files` U-A 的 `demo`。

**期望系统状态**：集群侧存在两份**互不影响**的目录（`wsgrp/wstest_a/workspaces/demo`、`wsgrp/wstest_b/workspaces/demo`），内容各自正确；bucket 前缀同理；步骤 3/4 → `success=0` + `FORBIDDEN`（或仅作用于 U-B 自身前缀），U-A 目录与对象**零变化**；两边 DB 行均只反映自己的 `user_name`。

### E2E-06 中断恢复（P0）

1. 提交 10 GB 文件的 `sync_to_cluster`（真实传输中）。
2. 传输到约 50% 时 `kill -9` ugc worker 进程。
3. 重启 ugc-server（`recover_on_startup=true`）。
4. 轮询状态至终态，校验 md5。

**期望系统状态**：重启后 **仅一个** worker 认领恢复（另一 worker 日志 `recovery skipped`）；任务从已完成分片处续传（`localfs` 分片/对象最终 md5 正确，无重复记账、无重复 `chown` 报错）；状态最终 `finished`；`cloud_storage_tasks_running` 回到 0；断点目录中新旧实例子目录隔离（`{breakpoint_info_path}/{instance_id}`）。

### E2E-07 老客户端 + 灰度开关全链路（P0）

1. `enabled=true&enabled_users=wstest_a`，U-A 跑完整 7 子命令流程。
2. 换成 U-GRAY（不在白名单）执行 `push`。

**期望系统状态**：U-A 全链路成功（同 E2E-01~E2E-04）；U-GRAY 得到 `success=0 + FEATURE_DISABLED`，客户端打印未开通提示并非 0 退出；服务端无任何对象/DB/Redis 副作用。

### E2E-08 大工作区（10 万文件）首次 push（P1）

1. 客户端以 D2（10 万文件，约 1.6 MB）执行 `push --force --no_zip`。
2. 记录各阶段耗时（`list` 遍历、zip/逐文件、传输、`sync_to_cluster` 轮询）。
3. 完成后 `diff` 与 `pull` 各跑一次。

**期望系统状态**：`cluster_files/list` 冷调用 ≤30 s、热调用 ≤1 s（NFR-01/FR-04）；`sync_to_cluster` 按 50/批共 2000 次、全部 `success=1`；最终集群侧 10 万文件全部存在且抽样 md5 一致；`diff` 无差异、`pull` 无新增下载；单实例内存无线性膨胀（NFR-10）；总耗时可观测并归档（用于 NFR-02 的趋势对比）。

---

## 6. 异常与故障注入矩阵

> 通用判定：任何注入都不得产生「响应 `success=1` 但数据实际丢失」的假成功（分析报告 F5 教训）；失败必须可通过状态接口或指标定位。

| # | 故障 | 注入手法 | 期望行为（可判定） | 关联用例 |
| --- | --- | --- | --- | --- |
| FI-01 | Redis 不可用 | `redis-cli shutdown`；或在测试中把端口指向黑洞 | 状态接口返回 HTTP 200 + `success=0` + 明确 `msg`（不得 500 裸异常）；`StatusRecorder` 3 次重试后失败并计入 `cloud_storage_db_failure_total`；**数据传输可继续**（Redis 非数据面）；恢复后状态可继续读写 | TC-A31, TC-G10, TC-L04 |
| FI-02 | Redis 超时（高延迟） | `tc qdisc add dev lo root netem delay 800ms` 或 `DEBUG SLEEP` | 状态接口 P99 仍 ≤1 s（不含注入延迟时）或明确返回错误；不出现**无限阻塞**（连接池超时生效，请求 10 s 内有响应，CON-8） | TC-I01, TC-A31 |
| FI-03 | PG 写失败 | `ALTER TABLE user_sync_status RENAME TO ...`（测试库）或注入 `a_execute` 异常 | 传输**不中断**（FR-12）；`cloud_storage_db_failure_total{operation="set_sync_status"}` +1；日志 ERROR；客户端不受影响（`success=1`）；修复表名后状态恢复写入 | TC-G10, TC-L04 |
| FI-04 | OSS 限流 5xx | 用本地代理（`mitm`/`toxiproxy`）对 `localfs` HTTP 层或 `oss` 端点返回 503/429 | 单文件重试 ≤10 次、间隔 1 s（FR-11）；持续失败 → 状态 `failed` + 首个错误原因；成功重试 → 最终 `finished` 且 md5 正确；无重复记账 | TC-C21, TC-C22, TC-A30 |
| FI-05 | 网络中断 30 s | `iptables -A OUTPUT -p tcp --dport <oss> -j DROP`，30 s 后删除 | 客户端侧：`ClientConnectorError` 立即失败或重试后失败（分析报告 §4.9）；服务端侧：重试至恢复后继续；最终数据一致；无状态悬挂（30 s 内 `running`，之后 `finished`/`failed` 明确） | TC-C21, TC-F04 |
| FI-06 | 进程 `kill -9` | `kill -9 $(pgrep -f 'uvicorn.*8083')`（单/双 worker） | 重启后按 §5.6 恢复（心跳+pod 锁）；任务续跑至 `finished`；多 worker 不重复恢复（CON-10）；gauge 归零 | TC-F01, TC-F02, TC-F04, E2E-06 |
| FI-07 | 磁盘满 | 对 `workspace_path` 所在分区 `mount -o remount,size=...` 或写入大文件占满；`localfs_root` 同样注入 | 落盘失败被捕获 → 该文件标记失败 → 任务 `failed` + 可读原因（如 `No space left on device`）；**不得**产生半截文件被当作成功（校验 md5/大小）；清理后重跑可成功 | TC-C22, TC-I07 |
| FI-08 | 配额超限 | `quota.cloud_storage_quota.download=0` 或预置 99 GB 已用 | `submit_from_cluster` 预检即 403 + `QUOTA_EXCEEDED`（含已用/申请/限额）；**无对象上传、无 DB 记账**；审计日志有记录 | TC-D07, TC-D08, TC-H07 |
| FI-09 | 对象被外部删除 | 直接 `rm` `localfs` 中的对象（或 OSS 控制台删除）后触发 `sync_to_cluster` | 下载失败 → 该文件重试 10 次后失败 → 状态 `failed` + 明确原因（`NoSuchKey`）；其余文件仍成功；`cluster_files/list` 的 `total` 反映真实落盘结果；重传（客户端再 push）可自愈 | TC-C22, TC-I07 |
| FI-10 | tagging 丢失 | `rm <key>.__tag__.json`（localfs）或 `delete_object_tagging` | 按「无元数据」降级：**不报错**（FR-09 兼容行）；`filemode` 缺失时按默认权限落盘；`md5` 缺失时退化为 size/存在性比较（可能重传但结果正确）；`expire_at` 缺失 → 审计不回收该对象（不误删） | TC-C15, TC-C16, TC-L05 |
| FI-11 | 心跳表丢失（Redis flush） | `redis-cli flushdb` 后触发恢复 | 退化为时间阈值策略（`recover_stale_seconds=600`，设计 §5.6 兜底）：仅认领 param 写入时间 >600 s 的任务；不误抢存活任务；日志记录降级原因 | TC-F02, TC-F03 |
| FI-12 | 目录在翻页期间被删 | 分页遍历中途 `rm -rf` 某子目录 | 命中一致性校验后返回 HTTP 200 + `success=0` + `code='CLIENT_RETRY'`（清缓存），**不得返回错误的空页**（FR-04） | TC-A21 |

---

## 7. 非功能测试（性能 / 容量 / 稳定性）

### 7.1 压测模型

| 模型 | 描述 | 客户端 | 数据规模 | 并发 | 时长 |
| --- | --- | --- | --- | --- | --- |
| PM-1 状态查询 | 固定 index 轮询（纯 Redis 读） | `locust`/`wrk` | 1 个工作区 | W=50 | 5 min |
| PM-2 目录列表 | 冷/热 `cluster_files/list` | `wrk` | D2 = 10 万文件 | W=1 冷、W=10 热 | 各 5 min |
| PM-3 提交类接口 | `get_sts_token`/`get_sync_status`/`sync_to_cluster`（提交，不含传输） | `asyncio` 并发 | D1 | W=20 | 1000 次/接口 |
| PM-4 端到端传输 | `sync_to_cluster` / `sync_from_cluster` 大文件 | 真实客户端或直调 | D3 = 10 GB 单文件；D4 = 1000×10 MB | 1 / 5 并发 | 至完成 |
| PM-5 规模遍历 | 全量分页 + 全量同步 | 直调 | D2 = 10 万文件；上界 20 万 | W=1 | 至完成 |
| PM-6 稳定性 | 混合负载（PM-1+PM-3+PM-4）持续运行 | 混合 | D1/D4 | W=30 | 24 h |

### 7.2 指标门槛（引用 NFR）

| 指标 | 门槛 | 数据来源 | 用例 |
| --- | --- | --- | --- |
| 状态查询 P99 | ≤100 ms | `cloud_storage_ugc_request_seconds{api="status"}` | TC-I01 |
| `get_sts_token` P99 | ≤1 s | 同上 | TC-I03 |
| `get_sync_status` P99 | ≤200 ms | 同上 | TC-I03 |
| `sync_to_cluster` 提交 P99（不含传输） | ≤500 ms | 同上 | TC-I03 |
| 单实例吞吐 | ≥200 MB/s（下限 100 MB/s，与 bucket 带宽相关） | 传输计时 | TC-I04 |
| 10 000 文件 / 10 GB push 端到端 | ≤10 min（内网 100 MB/s） | 客户端计时 | TC-I04 |
| 目录列表 | 冷 ≤30 s（10 万文件）；热 ≤1 s | 请求计时 | TC-I02 |
| 单工作区规模 | ≤200 000 文件、≤1 TB；单请求 ≤10 000 文件 | 压测配置 | TC-I05, TC-I06 |
| 接口可用性 | ≥99.9%（24 h 稳定性，PM-6） | `cloud_storage_ugc_request_total` 错误率 | TC-I07, §7.3 |
| 状态接口时延（约束） | P99 ≤1 s（CON-8 客户端 10 s 轮询） | 同上 | TC-I01 |
| 单文件失败隔离 | 失败文件可定位，其余文件状态正确 | 状态接口 + 指标 | TC-I07 |
| 内存 | 与文件大小无关（10 GB 文件不产生 GB 级 RSS） | `ps`/`memory_profiler` 采样 | TC-I08 |
| worker 数 | 可配（默认 4，`WORKERS` 生效） | 配置项 | TC-I08 |
| 单测覆盖率 | 领域层 ≥85%、整体 ≥70% | `pytest --cov` | TC-I09 |
| 幂等 | 同 index 并发 10 次 → 仅 1 份任务 | gauge + 对象计数 | TC-F06 |

### 7.3 稳定性测试（PM-6，24 h）

1. 混合负载持续 24 h，每 30 min 记录一次快照（QPS、P99、错误率、gauge、RSS、Redis 内存、断点目录大小）。
2. 期间每 2 h 注入一次 FI-01/FI-04/FI-05（轻量），每 6 h 注入一次 FI-06。
3. **通过标准**：错误率 <0.1%（不含注入窗口）、P99 无持续劣化（末 4 h P99 ≤ 首 4 h 的 1.5 倍）、无内存单调增长（RSS 增长率 <1%/h）、无状态悬挂（`running` 且 TTL 过期的 key 数为 0）、`cloud_storage_db_failure_total` 在注入窗口外不增长。

---

## 8. 安全测试（SEC-01~SEC-08）

| SEC | 手法（具体命令/步骤） | 判定 | 用例 |
| --- | --- | --- | --- |
| SEC-01 身份只来自 token | ① `curl -X POST "$API/ugc/set_sync_status?token=$T_A&file_type=FileType.WORKSPACE&name=evil&direction=SyncDirection.PUSH&status=SyncStatus.INIT&username=wstest_b&group=wsgrp2&userid=20003"` ② 用 T_B 查 `name=*` ③ `grep -n "wstest_b" $LOG` 于 U-A 请求段 | 伪造参数不改变任何落库/落盘归属；响应与日志不出现被伪造的 `username/group` 作为归属 | TC-H01, TC-A47 |
| SEC-02 STS 最小权限 | ① 取 STS 响应 ② 用临时凭证对 `wsgrp/wstest_b/...` 执行 PUT/GET ③ 校验 TTL ④ 校验 policy 前缀 ⑤ 校验无长期 AK/SK | 越界操作被拒（403/拒绝）；TTL ∈[900,43200]；前缀 = `wsgrp/wstest_a/workspaces/demo/*`；响应仅含临时凭证 | TC-A03, TC-A05, TC-H02 |
| SEC-03 路径穿越与软链 | ① `files=["../../etc/passwd"]`、`files=["....//"]`、`path="/etc/passwd"`、`name="a/b"` ② `ln -s /etc/passwd link_out` 后 `sync_from_cluster`/`delete_files` ③ `grep -R "root:x:" $LOCALFS_ROOT` | 全部 `success=0`（`PATH_ESCAPE`/`INVALID_PARAM`）；base 之外零变化；bucket 中无 `/etc/passwd` 内容 | TC-H05, TC-H06, TC-A26, TC-A36, TC-E06 |
| SEC-04 越权访问 | ① B 查 A 的 index ② B 调 `name=A 的 name` 的三个写接口 ③ B 调 `delete_files` ④ 对比 A 侧 mtime/行数 | 全部 403 + `success=0`；A 侧零变化 | TC-H04, TC-A32, TC-A42, TC-E05 |
| SEC-05 敏感信息 | ① 触发 4 类错误响应 ② `grep -nE 'access_key_secret' -e 'security_token' -e 'token=[^&[:space:]]+' ugc_0.log` ③ 检查 4xx/5xx 响应体无 traceback ④ `psql -c "select * from user_sync_status"` 确认无 STS 字段 | 除掩码外零命中；无 traceback/SQL 泄漏；STS 不入库 | TC-H03, TC-H12, TC-A30, TC-L03 |
| SEC-06 资源保护 | ① 101 文件提交（限额 100）② `size=99999` ③ 1001 个子路径 ④ 并发 20 个同步任务 | 超限被拒（`TOO_MANY_FILES`/截断/排队）；`size` 上限 1000；并发 >10 走共享池而非失败 | TC-H08, TC-H09, TC-F07 |
| SEC-07 删除保护 | ① 客户端整体 remove 观察二次确认 ② `delete_files` 空列表 ③ `grep 'delete workspace' ugc_0.log` | 客户端有二次确认；服务端 INFO 审计含操作人/IP/空文件列表 | TC-E01, TC-A41, TC-H10 |
| SEC-08 审计 | ① 触发删工作区/签发 STS/超配额/越权 ② 逐一 grep 审计事件 ③ 校验字段与保留策略 | 四类事件均落审计、字段完整、可检索；保留 ≥90 天（运维配置项登记） | TC-H10, TC-H11, TC-D07, TC-L05 |

---

## 9. 兼容性与升级测试

### 9.1 三种「客户端实际形态」的独立验证

| 形态 | 构造方式 | 断言 | 用例 |
| --- | --- | --- | --- |
| 枚举串查询参数 | `file_type=FileType.WORKSPACE&direction=SyncDirection.PUSH&status=SyncStatus.STAGE1_RUNNING` | HTTP 200 且 `success=1`；落库值与规范形态一致 | TC-A44, TC-A45, TC-A46 |
| `text/plain` Body | `curl -H 'Content-Type: text/plain; charset=utf-8' --data-binary '{"file_list":{"files":[...]}}'` | HTTP 200 且 `success=1`（**不得 422、不得裸 `{"detail":...}`**，ADR-2/F3b） | TC-A46, TC-A50 |
| 包裹外壳 Body | `{"file_list":{"files":[...]}}` / `{"file_infos":{"files":[...]}}` | 与裸体 `{"files":[...]}` 行为一致 | TC-A35, TC-A20, TC-A43 |

### 9.2 开关矩阵

| `legacy_param_compat` | 客户端形态 | 期望 | 用例 |
| --- | --- | --- | --- |
| `true`（默认，灰度期保持 ≥2 个大版本，设计 §13.2-5） | 旧（枚举串 + `text/plain` + 外壳） | ✅ 全链路成功 | TC-J01 |
| `true` | 新（`file_type.value` + `application/json` 裸体） | ✅ 全链路成功 | TC-J04 |
| `false` | 旧 | ❌ `INVALID_PARAM`（用于探测客户端是否升级） | TC-J03 |
| `false` | 新 | ✅ 全链路成功 | TC-J02 |

### 9.3 `cloud-storage` 独立部署回归（COMP-03）

| 检查项 | 步骤 | 期望 | 用例 |
| --- | --- | --- | --- |
| 无前缀路由保持可用 | 对独立服务调用 `GET /get_sts_token`、`POST /list_cluster_files`、`POST /sync_to_cluster`、`POST /sync_from_cluster`、`POST /delete_files`、`GET /sync_to_cluster/status`、`GET /sync_from_cluster/status` | 路径、方法、`dependencies=[Depends(validate_user_token)]`、`Page[FileInfo]` 响应模型**逐项不变**（抽薄只改函数体，设计 §3.2） | TC-J11, TC-I10 |
| 响应模型不变 | 校验 `list_cluster_files` 仍返回 `items/total/page/size/pages` | 字段与分页语义不变 | TC-A16 |
| 服务间鉴权 | 形态 B 转发使用 `cloud_storage/auth.py` JWT（`allowed_users={'multi-server'}`），非用户 token | 内部调用成功且不泄漏用户 token | TC-I10 |
| 双形态等价 | 同组用例在形态 A/B 各跑一次 | 响应与落盘结果等价 | TC-I10 |

### 9.4 DB 与响应字段兼容（COMP-05/06）

| 检查项 | 期望 | 用例 |
| --- | --- | --- |
| 不修改既有列语义、无 DDL（P0） | `db_schemas/010,011` 的列集合与语义不变；MVP 上线无需 DDL（CON-9） | TC-G01, TC-J08 |
| 新增字段只增不减 | 响应追加字段后老客户端仍可渲染（忽略未知字段） | TC-J05 |
| 新增列/表可空且有默认值（P1） | 审计表 DDL 可在线执行、可回滚 | TC-J08 |


---

## 10. 用例优先级与回归矩阵

### 10.1 集合定义

| 集合 | 目的 | 触发时机 | 预计耗时（localfs 路径） |
| --- | --- | --- | --- |
| **P0 冒烟集（SMOKE）** | 每次构建/每次部署后确认主链路可用与无回归 | 每次 CI 构建、每次灰度发布前 | ≤30 min |
| **P1 回归集（REG）** | 覆盖全部功能与兼容分支，发现行为回归 | 每日夜间、每次合并主干前 | ≤4 h |
| **发布前必跑集（RELEASE）** | 上线准入，含性能/安全/故障注入/灰度 | 每个发版候选（RC） | ≤1 天（含真实 OSS 路径） |
| 可选集（OPT） | P1 优先级用例与非阻塞项 | 视排期 | — |

### 10.2 P0 冒烟集（SMOKE）

```
TC-A01, TC-A06, TC-A11, TC-A16, TC-A22, TC-A23, TC-A25, TC-A28, TC-A29, TC-A31, TC-A32,
TC-A33, TC-A39, TC-A41, TC-A44, TC-A46, TC-A47, TC-A50, TC-A54, TC-A55,
TC-B01, TC-B02, TC-B03, TC-B04,
TC-C01, TC-C02, TC-C03, TC-C05, TC-C08, TC-C13, TC-C17, TC-C18, TC-C19, TC-C21, TC-C23,
TC-C25, TC-C26,
TC-D01, TC-D02, TC-D04, TC-D06, TC-D07,
TC-E01, TC-E02, TC-E03,
TC-F01, TC-F02, TC-F04, TC-F06,
TC-G01, TC-G05, TC-G10, TC-G12,
TC-H01, TC-H02, TC-H04, TC-H05, TC-H07, TC-H10, TC-H12,
TC-I01, TC-J01, TC-J03, TC-J06, TC-J09, TC-J12,
TC-K01, TC-K03, TC-K05, TC-K07,
TC-L01, TC-L02, TC-L04,
E2E-01, E2E-02, E2E-05, E2E-06
```

**冒烟通过标准**：以上用例 100% 通过，且 E2E-01/02/05/06 关键断言（集群侧 md5、第二次 push 0 字节、越权 403、重启后 `finished`）全部成立。

### 10.3 P1 回归集（REG）

```
A 组：TC-A02, TC-A03, TC-A04, TC-A05, TC-A07, TC-A08, TC-A09, TC-A10, TC-A12, TC-A13,
      TC-A14, TC-A15, TC-A17, TC-A18, TC-A19, TC-A20, TC-A21, TC-A24, TC-A26, TC-A27,
      TC-A30, TC-A34, TC-A35, TC-A36, TC-A37, TC-A38, TC-A40, TC-A42, TC-A43, TC-A45,
      TC-A48, TC-A49, TC-A51, TC-A52, TC-A53, TC-A56
B 组：TC-B05, TC-B06
C 组：TC-C04, TC-C06, TC-C07, TC-C09, TC-C10, TC-C11, TC-C12, TC-C14, TC-C15, TC-C16,
      TC-C20, TC-C22, TC-C24, TC-C27, TC-C28, TC-C29, TC-C30
D 组：TC-D03, TC-D05, TC-D08, TC-D09, TC-D10
E 组：TC-E04, TC-E05, TC-E06
F 组：TC-F03, TC-F05, TC-F07, TC-F08
G 组：TC-G02, TC-G03, TC-G04, TC-G06, TC-G07, TC-G08, TC-G09, TC-G11
H 组：TC-H03, TC-H06, TC-H08, TC-H09, TC-H11
I 组：TC-I02, TC-I03, TC-I04, TC-I05, TC-I06, TC-I07, TC-I08, TC-I09, TC-I10
J 组：TC-J02, TC-J04, TC-J05, TC-J07, TC-J08, TC-J10, TC-J11, TC-J13, TC-J14
K 组：TC-K02, TC-K04, TC-K06, TC-K08, TC-K09, TC-K10
L 组：TC-L03, TC-L05, TC-L06, TC-L07, TC-L08
E2E：E2E-03, E2E-04, E2E-07, E2E-08
```

> SMOKE ∪ REG 覆盖全部 182 个详细用例与 8 个场景（无遗漏）。

### 10.4 发布前必跑集（RELEASE）

在 REG 基础上追加以下**阻塞项**（任一失败即不可发布）：

| 类别 | 用例 |
| --- | --- |
| 真实 OSS 路径 | TC-A01, TC-A02, TC-A03, TC-C01, TC-C13, TC-C17, TC-C21, TC-C24, TC-I04 |
| 性能与容量 | TC-I01, TC-I02, TC-I03, TC-I04, TC-I05, TC-I06, TC-I08 |
| 稳定性 | §7.3 的 24 h PM-6（≥8 h 可先作为准入，24 h 为发布标准） |
| 安全 | TC-H01~TC-H12 全量（含 TC-H12 泄漏扫描） |
| 故障注入 | FI-01, FI-03, FI-04, FI-06, FI-07, FI-08, FI-09, FI-10（FI-02/FI-05/FI-11/FI-12 归入 REG） |
| 兼容与灰度 | TC-J01, TC-J02, TC-J03, TC-J09, TC-J10, TC-J11 + §9.3 独立部署回归 |
| 任务侧 | TC-K04, TC-K06, TC-K09 |
| 端到端 | E2E-01~E2E-08 全量 |

---

## 11. 缺陷分级标准与准入准出

### 11.1 缺陷分级

| 级别 | 定义 | 典型示例 | 处理时限 |
| --- | --- | --- | --- |
| **阻塞（Blocker）** | 主链路不可用、数据丢失/损坏、越权或凭据泄漏、造成集群不可恢复影响 | push/pull 全链路失败；客户端因响应缺 `success` 抛断言失败；STS 可访问他人前缀；`delete_files` 路径穿越删到 `cluster_base` 之外；任务拿到空工作区导致训练静默跑错；`status='running'` 时 `msg` 不可 `int()` | 立即修复，**发布阻断** |
| **严重（Critical）** | 关键功能可用但结果不正确/不稳定，绕行成本高 | 增量同步失效（第二次 push 仍全量重传）；`cluster_files/list` 返回空列表（F1 复现）导致全量重传；终态 TTL <1800 s 导致客户端 `NOT_FOUND_INDEX`；多 worker 重复恢复导致重复传输；配额未生效；失败状态悬挂在 `running` | 24 h 内修复，**发布阻断**（除非有产品签字的风险接受） |
| **一般（Major）** | 非主链路功能缺陷、体验/可观测缺口、有合理绕行 | `page.size` 截断未生效；`expire_at` 审计未回收（P1）；指标标签缺失；错误 `msg` 不清晰；`local_path` 元数据异常（R4，不影响传输）；P1 接口（API-11/12）缺陷 | 当前迭代修复，不阻塞发布 |
| **轻微（Minor）** | 文档/文案/日志格式问题，无功能影响 | 日志字段顺序不符规范；`msg` 措辞不一致；`index` 日志只截前 10 位与规范不同 | backlog |

### 11.2 准入条件（测试启动前）

1. 代码冻结候选已部署到测试环境，`[cloud.storage]` 配置就绪（TC-A54 前置）；
2. 测试数据已按 §2.5 构造（D1/D2/D3 至少齐备），PG/Redis/localfs 目录已初始化；
3. 需求/设计基线版本与本文件一致（版本表可对齐）；
4. 上一轮遗留 Blocker/Critical 缺陷已关闭或已评估；
5. 客户端版本矩阵明确（旧/新两套二进制可用），`legacy_param_compat` 取值已声明。

### 11.3 准出条件（「可发布」判定）

**必须全部满足**：

| # | 条件 |
| --- | --- |
| 1 | SMOKE 100% 通过；REG 通过率 ≥98%，且未通过项**均非** Blocker/Critical； |
| 2 | RELEASE 集中全部阻塞项通过；未通过项为零（性能项允许 ≤5% 偏差但需书面风险接受）； |
| 3 | Blocker/Critical 缺陷为 **0**（含回归引入的）；Major 缺陷关闭率 ≥90%，未关闭项有排期与绕行方案； |
| 4 | 端到端验收（需求 §10）8 条全部成立：7 子命令全链路、第二次 push 0 字节、三种兼容形态、SEC-01~08 全覆盖、`kill -9` 恢复、多 worker 不重复恢复、指标日志可查且无泄漏、需求 ID 100% 覆盖（本文件 §12 反向表无空洞）； |
| 5 | 灰度可回滚演练完成（TC-J10/TC-J11）：`enabled=false` 秒级生效、回滚无脏数据； |
| 6 | 真实 OSS 路径至少完整跑通一轮 SMOKE + C 组关键用例（TC-C01/13/17/21/24）； |
| 7 | 监控告警接线完成并验证四类告警可触发（TC-J12）； |
| 8 | 测试报告归档：用例执行记录、性能数据、缺陷清单、遗留风险（R1~R9 中未关闭项）与「待确认」清单（§12.4）。 |

---

## 12. 需求追溯反向表

### 12.1 FR / API 反向表

| 需求 | 用例 | 需求 | 用例 |
| --- | --- | --- | --- |
| FR-01 | TC-A01~A05, TC-H02 | API-01 | TC-A01~A05, TC-J01 |
| FR-02 | TC-A06~A10, TC-B01, TC-B05, TC-G01~G04 | API-02 | TC-A06~A10 |
| FR-03 | TC-A11~A15, TC-B02~B04, TC-B06 | API-03 | TC-A11~A15, TC-B02~B04 |
| FR-04 | TC-A16~A21, TC-C02, TC-C03, TC-C05, TC-C19, TC-I02, TC-I06 | API-04 | TC-A16~A21, TC-C05 |
| FR-05 | TC-A22~A27, TC-C01~C12, TC-C25, TC-C29, TC-C30, TC-J09 | API-05 | TC-A22~A27, TC-C01~C12 |
| FR-06 | TC-A28~A32, TC-G05~G09, TC-C26, TC-C28 | API-06 | TC-A28~A32 |
| FR-07 | TC-A33~A38, TC-D01~D06, TC-D09, TC-D10 | API-07 | TC-A33~A38, TC-D01~D10 |
| FR-08 | TC-A39~A43, TC-E01~E06 | API-08 | TC-A28~A32 |
| FR-09 | TC-C13~C16, TC-H06, TC-A34, TC-D03 | API-09 | TC-A39~A43, TC-E01~E06 |
| FR-10 | TC-C17~C20, TC-A19, TC-A24 | API-10 | TC-A51~A53 |
| FR-11 | TC-C21~C24, TC-C15, TC-F05, TC-A38, TC-D10 | API-11 | TC-J14 |
| FR-12 | TC-G01~G12, TC-C26, TC-L04 | API-12 | TC-J13 |
| FR-13 | TC-F01~F04, TC-F07, TC-F08, TC-C12, TC-L02, TC-L07 | — | — |
| FR-14 | TC-A44~A50, TC-J01~J06 | — | — |
| FR-15 | TC-K01~K06 | — | — |
| FR-16 | TC-K07~K10 | — | — |
| FR-17 | TC-D07, TC-D08, TC-H07~H09, TC-C08, TC-C09, TC-D09 | — | — |
| FR-18 | TC-A51~A53 | — | — |
| FR-19 | TC-A54~A56, TC-J07 | — | — |
| FR-20 | TC-G08, TC-G11, TC-C23, TC-C27 | — | — |
| FR-21 | TC-L05~L08 | — | — |

### 12.2 NFR / SEC / OPS / COMP / CON 反向表

| 需求 | 用例 |
| --- | --- |
| NFR-01 | TC-I01, TC-I02, TC-I03 |
| NFR-02 | TC-I02, TC-I04 |
| NFR-03 | TC-I05, TC-I06 |
| NFR-04 | TC-I07, TC-C22 |
| NFR-05 | TC-F06, TC-G04, TC-G12, TC-A08 |
| NFR-06 | TC-L01, TC-L02, TC-L03, TC-L04, TC-G10, TC-L08 |
| NFR-07 | TC-I09, TC-J08 |
| NFR-08 | TC-J01, TC-J02, TC-J04, TC-A48 |
| NFR-09 | TC-I10, TC-I09, TC-A56 |
| NFR-10 | TC-I05, TC-I08, TC-F07 |
| SEC-01 | TC-H01, TC-A05, TC-A10, TC-A27, TC-A47, TC-C29 |
| SEC-02 | TC-H02, TC-A02, TC-A03 |
| SEC-03 | TC-H05, TC-H06, TC-A26, TC-A36, TC-A37, TC-C20, TC-E06 |
| SEC-04 | TC-H04, TC-A14, TC-A32, TC-A42, TC-A52, TC-E05 |
| SEC-05 | TC-H03, TC-H12, TC-A30, TC-L03 |
| SEC-06 | TC-H08, TC-H09, TC-H07 |
| SEC-07 | TC-H10, TC-A41, TC-E01 |
| SEC-08 | TC-H11, TC-H10, TC-D07, TC-L05 |
| OPS-01 | TC-J09, TC-J10 |
| OPS-02 | TC-J07, TC-A55 |
| OPS-03 | TC-J11 |
| OPS-04 | TC-J12, TC-L04, TC-J10 |
| OPS-05 | TC-J12, TC-L03 |
| OPS-06 | TC-J08, TC-C24 |
| OPS-07 | TC-J08 |
| COMP-01 | TC-J01, TC-J02, TC-J03, TC-J11, TC-A44, TC-A56 |
| COMP-02 | TC-J01, TC-J02, TC-A46, TC-A35, TC-A20 |
| COMP-03 | TC-J11, TC-I10, §9.3 |
| COMP-04 | TC-J04, TC-C10 |
| COMP-05 | TC-J05 |
| COMP-06 | TC-J08, TC-G01 |
| CON-1 | TC-J01, TC-J02, TC-J04 |
| CON-2 | 全组 A/B/C/D/E（方法/路径/参数逐项断言） |
| CON-3 | TC-A04, TC-A09, TC-A31, TC-A43, TC-A50, TC-H12 |
| CON-4 | TC-A46, TC-A01（`text/plain` 且 `success=1`） |
| CON-5 | TC-A44, TC-A45 |
| CON-6 | TC-J06, TC-A22 |
| CON-7 | TC-C25, TC-C29 |
| CON-8 | TC-I01, FI-02 |
| CON-9 | TC-G01, TC-J08 |
| CON-10 | TC-F01, TC-F06, TC-L06 |

### 12.3 DB 层用例反向映射（TC-DB-*）

| 需求 / 检查项 | 用例 |
| --- | --- |
| CON-9（不改表结构上线） | TC-DB-01, TC-DB-12 |
| FR-02 写入同步状态（upsert/空值不覆盖/幂等） | TC-DB-02, TC-DB-03, TC-DB-06, TC-DB-09 |
| FR-03 / FR-08（软删不可见、可复活） | TC-DB-11 |
| FR-07 集群→bucket 记账 | TC-DB-04 |
| FR-17 配额口径（G3） | TC-DB-05 |
| SEC-01 身份只来自 token | TC-DB-10 |
| 审计 §4 C1/C2（绑定参数硬约束） | TC-DB-07, TC-DB-08 |
| Checklist DB-01~DB-07 | TC-DB-01, TC-DB-05, TC-DB-07, TC-DB-12 |

### 12.4 与需求文档 §11 追溯矩阵的差异及待确认项

**以覆盖更全的一侧为准**，本文件对需求文档 §11 的差异如下：

| # | 差异 | 需求文档 §11 | 本文件 | 处置 |
| --- | --- | --- | --- | --- |
| D-1 | ~~API-04/FR-08 的用例归属冲突~~ **（复核后撤销）**：经回到需求文档 §11 原文逐行复核，`FR-04` 行写的是 `TC-A16~A21`、`FR-08` 行写的是 `TC-A39~A43`，与 A 组区间定义**完全一致**，不存在冲突（原判来自任务书描述的二义性） | FR-04 → TC-A16~A21；FR-08 → TC-A39~A43 | 同左 | **无需修改**；本行保留以记录复核结论 |
| D-2 | **API-08 无独立用例区间** | API-06/08 合写「FR-06 | TC-A28~A32」 | API-08 由 TC-A28~A32 内显式断言（方向键空间、`is_upload` 不串读） | 采用本文件；建议需求 §11 拆分 API-06/API-08 行 |
| D-3 | **API-11（`update_cluster_venv`）在需求 §11 中无覆盖** | 未列出 | TC-J14（契约级，端到端 `[待确认]`） | 采用本文件，补覆盖 |
| D-4 | **API-12（`cloud_storage/usage`）在需求 §11 中无覆盖** | 未列出 | TC-J13 | 采用本文件，补覆盖 |
| D-5 | **NFR-07 的归属**：需求 §11 将「NFR-07~10」一并映射到 `TC-J08, TC-I09` | NFR-07~10 → TC-J08, TC-I09 | NFR-07 → TC-I09, TC-J08；NFR-08 → TC-J01~J04；NFR-09 → TC-I10, TC-A56；NFR-10 → TC-I05, TC-I08, TC-F07 | 采用本文件（逐条拆解，覆盖更全） |
| D-6 | **NFR-05/06 的归属**：需求 §11 将「NFR-05/06」映射到 `TC-F06, TC-G12, TC-L01~L04` | NFR-05/06 合并 | NFR-05 → TC-F06, TC-G04, TC-G12, TC-A08；NFR-06 → TC-L01~L04, TC-G10, TC-L08 | 采用本文件 |
| D-7 | **FR-13 在需求 §11 的对应行**写 `TC-F01~F04` | FR-13 → TC-F01~F04 | TC-F01~F04 **且** TC-F07/TC-F08/TC-C12/TC-L02/TC-L07（幂等与 gauge 归零属恢复语义） | 采用本文件（并列关系，不冲突） |
| D-8 | **FR-14 的归属**：需求 §11 写 `TC-A44~A50, TC-J01~J06` | 同上 | 一致；本文件额外将 `TC-J08` 纳入 FR-14 的 `legacy_param_compat` 开关维度 | 采用本文件 |
| D-9 | **OPS-06/07 的归属**：需求 §11 将 `TC-J09~J12` 映射到「OPS-01~07」全部 7 项 | OPS-01~07 → TC-J09~J12 | OPS-01 → J09/J10；OPS-02 → J07；OPS-03 → J11；OPS-04 → J12/L04；OPS-05 → J12/L03；**OPS-06/OPS-07 → J08** | 采用本文件（J08 显式覆盖 OPS-06/07，J09~J12 仍覆盖 OPS-01~05） |

**待确认清单（需产品/运维输入，对应设计 §16 与 F 系列）**

| # | 待确认项 | 影响的用例 |
| --- | --- | --- |
| Q-1 | `service.workspace_path` / `env_path` 线上真实取值与挂载方式（R1/R3） | TC-K01, TC-K04, TC-K09, TC-J07（本文件以 `/tmp/hai-test/workspace` 占位，需替换为线上值重跑） |
| Q-2 | `cloud_storage_quota.download` 默认额度与是否区分内部/外部用户 | TC-D07, TC-D08, TC-J13 |
| Q-3 | 是否要求「服务端自动从 bucket 拉取缺失工作区」（FR-15 增强，P1） | TC-K06（当前按 P0 语义：返回明确错误；若采纳 P1，则期望改为「自动拉取一次后成功」） |
| Q-4 | 是否需要跨集群（多 region）同步；若需要，`cloud_base_path` 是否加集群标识 | TC-C13, TC-C14, E2E-01 |
| Q-5 | `fastapi-pagination==0.9.1` 的 `Params(page,size)` 是否可用（R6） | TC-A16 的前置条件，需先有单测确认 |
| Q-6 | 客户端 `--breakpoint_dir`（R5）与 `quote()`（R4）的排期；`legacy_param_compat` 计划关闭的时间点 | TC-J06, TC-J03, TC-C24 |
| Q-7 | 是否启用 P1 审计表 `db_schemas/035`（ADR-9）与 `RUN_AUDIT` 的生产开关 | TC-J08, TC-L05~L08 |
| Q-8 | `provider != 'oss'`（`localfs`/`mock`）是否允许出现在非测试环境；响应 `msg` 的标注文案 | TC-A56 |
| Q-9 | API-11 端到端是否纳入本期验收（客户端 `FileType.ENV` 字符串化缺陷未修复，分析报告 F7） | TC-J14 |

---

*文档结束。用例 ID 与需求 ID 的一一对应关系见 §12；执行时请以「可判定预期结果」为唯一判定依据。*
