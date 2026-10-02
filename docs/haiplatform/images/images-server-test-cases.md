# HAI Platform · `hai-cli images`（用户自定义镜像）服务端功能测试用例集

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

> **本分支文档集状态**：本文件是同批交付物之一；同目录下已有 [hai-cli-images-analysis.md](hai-cli-images-analysis.md)、[images-server-requirements.md](images-server-requirements.md)、[images-server-design.md](images-server-design.md)、[images-server-test-report.md](images-server-test-report.md)、[images-server-task-list.md](images-server-task-list.md)、[images-server-decisions.md](images-server-decisions.md)、[images-server-checklist.md](images-server-checklist.md)，正文中对它们的引用均为**相对链接**（与它们对本文的引用互指）。`docs/haiplatform/scripts/` 的 images 脚本与 `tests/images/` **已并入本分支**（S9-1 并入 P0 资产 + S8 新增上传通道资产），正文中的行内 `code` 即指这些已落地文件。

> **文档定位**:`docs/haiplatform/images/` 四件套之三([分析](hai-cli-images-analysis.md) → 《[需求](images-server-requirements.md)》→《[设计](images-server-design.md)》→ **用例**)。
> **被测对象**:`API-15 /ugc/user/train_image/load`、`API-16 /ugc/user/train_image/update_status`、`API-17 /ugc/user/train_image/list`(修订版)、`API-18 /ugc/user/train_image/delete`;领域层 `server_model/user_impl/user_image/`、`server_model/selector/train_image_selector.py`、`conf/utils.py:get_image_root`、`cloud_storage/utils.py:get_base_path` 的 IMAGE 分支;客户端 `hai-cli images`(4 子命令 + `client/api/image_api.py`,其中 **`images push` 为本分支上传主入口**);**上传链路复用的 `API-01 /ugc/get_sts_token`、`API-05 /ugc/sync_to_cluster`、`API-06 /ugc/sync_to_cluster/status`(`file_type=image`)**;**运行期 `marsv2/scripts/link_hfai_image.sh` 与计算 pod 的 `load-image` initContainer**。
> **不在范围**:镜像**跨集群分发 / 分层去重 / 增量加载 / 镜像市场**(需求 §1.3);P2 的空间回收与 GC(FR-14 / OPS-05 本期只登记);`train_environment` 内建镜像的语义与配额(CMP-03 只做零回归);服务器 pod 内直连节点容器运行时(HC-10)。
> **前提**:接口号沿用跨特性编号空间——`API-15/API-16/API-18` 为新增,`API-17` 为修订;需求 ID 见 [images-server-requirements.md](images-server-requirements.md) §3;验收 ID 为 `AC-01~AC-18`;风险 ID `I1–I19` 见 [hai-cli-images-analysis.md](hai-cli-images-analysis.md) §6;`TC-*` 为本文件用例 ID,**与 Checklist 的 `ACC-*` 是两套编号**,勿混用。

---

## 1. 测试范围与策略

### 1.1 分层模型

| 层 | 名称 | 被测对象 | 依赖与环境 | 通过标准（准出） |
| --- | --- | --- | --- | --- |
| **L1** | 单元测试 | `conf/utils.py` 的 `get_image_root()` / `FileType.IMAGE` / `IMAGE_NAME_RE`、`get_base_path` 的 IMAGE 分支、领域层纯逻辑(命名派生 / 路径校验 / 状态机 / 组校验 / 归一化)、`TrainImageSelector` 的排序与出口类型 | pytest + `pytest-asyncio`;**不启动 FastAPI、不连 PostgreSQL、不起 k8s**(NFR-04);DB 访问以 fake 替身注入;共享根用 `tmp_path` 充当 | 领域层行覆盖率 ≥ 85%;`check_is_subpath` 组合与状态机迁移矩阵 **100% 分支覆盖**;U 组 12 例可在**无 registry、无 k8s**机器上 5 分钟内跑完 |
| **L2** | 接口契约测试 | `api/resource/image/default.py` 的 4 个接口、`api/register/implement.py` 的 3 条新路由、`user_images` 修订后的响应体;**上传通道的接口面**:`file_type=image` 的 `API-01 /ugc/get_sts_token`(**STS 作用域前缀 = `cloud_base_path`**)、`API-05 /ugc/sync_to_cluster`(**stage2 落点**,`no_zip=true`)、`API-06` 轮询,以及 `images push` 落盘后触发的自动登记(API-15) | `ugc-server`(`ONE_SERVER=ugc`,`uvicorn_server.py :8083`);真实 PostgreSQL `mars_db` 的 `public.train_image`;共享盘 `image_path` 可读写;**RustFS/S3 可达(上传链路)**;HTTP 客户端须能伪造 query / `text/plain` JSON / `application/json` 三种入参承载 | 全部响应体含 `success`(§4.5 约定);错误码与设计 §4.5 表**逐项一致**;**只看 200 不算通过**:`psql` 侧副作用(行数 / 状态 / `path` / `updated_at`)必须与响应一致;**上传链路同样「只看 200 不算通过」**——必须核对**共享盘上真的有 md5 一致的文件**、且 `user_sync_status`(stage1/stage2 落点与终态)与 `train_image`(自动登记出的 `loaded` 行)**两套状态各就各位** |
| **L3** | 端到端测试 | **上传主入口** `hai-cli images push <本地 tar>`(本机 tar →(STS 直传)RustFS/S3 →(stage2)`image_path` → 自动登记)+ 真实 `hai-cli images list/load/delete` + 自定义镜像 tar + `ugc-server` + `launcher` + 计算 pod initContainer(`link_hfai_image.sh`)+ 主容器跑探针 | **103 真实环境**(MicroK8s v1.21.13 / containerd 1.4.13 / 4 个 Multipass 节点)+ 共享盘 + **RustFS/S3**;**无内网 registry**(I11) | 一条自定义镜像任务 `succeeded` 且输出**由镜像内容决定**(探针包 / 标记文件,AC-01);initContainer 无 `not found`、exit 0;`/data_local` 前置与 busybox 引用满足(AC-08/AC-09);**上传链路**另需:共享盘落点与本地 tar **md5 一致**、stage1/stage2 终态 `FINISHED`(AC-15) |
| **L4** | 兼容与灰度测试 | 旧客户端 wheel、两种 body 形态、三级灰度开关、私有 `custom.py` 覆盖、`image_path` 覆盖、一级回滚;**上传通道开关**(`[image].upload_enabled` / `max_tar_bytes` / `upload_require_precheck`,S8 已实现)与上传通道的一级回滚 | 两套客户端二进制 + 两组服务端配置 + 一组「私有层已实现同名函数」的模拟部署 | 兼容矩阵(设计 §8)逐行成立;灰度外用户得到 `success=0 + FEATURE_DISABLED`;一级回滚后 `list` 与**已 `loaded` 行上的线上任务不被打断**(OPS-02);上传通道关闭后 `load`(兼容旁路)与**已 `loaded` 行上的任务仍可用**,且共享盘 / 对象存储**零新增写入**(AC-16/AC-18) |
>
> **上传通道（本分支主入口）**：`file_type=image` 的上传用例见 **§4.11 UP 组（TC-UP-01~TC-UP-12）**，
> 端到端见 **E2E-09（上传闭环）/ E2E-10（开关一致性）**，故障注入见 **FI-09~FI-12**。
> 上传通道的判据同样是「副作用双向可判定」：**HTTP 200 不算通过**，必须核对**共享盘上真的出现了 md5 一致的文件**、
> 且 `user_sync_status` / `train_image` 两套状态各就各位（§1.2 策略 3）。

### 1.2 策略要点

1. **「接口 200」与「任务真跑通」必须分开判定**。分析 §5 的 S6 是「管道通了但没有水源」、S10 是「补上水源仍卡 Init」——因此 T 组与 E2E-01 **不可省**,其判据是**任务输出可区分**,不是 HTTP 状态码(AC-01/AC-09)。
2. **以 `register` 后端为主测量路径**。103 **无任何内网 registry**(I11),`register` 后端不访问 registry(ADR-I2),是唯一能端到端验证的后端;`registry` 后端只作对照,其不可验证项必须记为「未验证」而非「通过」。
3. **副作用双向可判定**。每个接口用例同时给出「HTTP 响应」与「`psql` / 共享盘 / 节点侧真实结果」。**只验响应不验副作用**正是 `train_image` 表零行、零写入(I4)却「桩能返回 success=1」的原因。
4. **概念分离是本特性头号风险(I18)**。`image_tar`(tar 包路径)≠ `path`(镜像资产在共享盘上的位置)≠ `image_url`(三段 URL)。P 组独立成组并作为**发布门禁**,含「`register` 后端下两列取值相同但必须分别赋值」这一实现修正。
5. **运行面是最高风险点(I16/I17)**。T 组必须覆盖:`link_hfai_image.sh` **存在 / 被挂载进 pod / 幂等 / 失败可见**、`/data_local` 缺失时的行为、`HFAI_IMAGE_WEKA_PATH` 传递、helper 镜像可配置(AC-08)。
6. **客户端事实优先**。断言以真实 `hai-cli` 行为为准:凡「服务端看起来对但客户端会失败」的形态(裸 `AttributeError`(C-3)、`print(result['msg'])` 永不执行(I10)、DESC 顺序基准(I7))一律判失败。**任务提交路径**(`hai-cli python ... --image ...`)的裸异常(`Exception: 请求失败: ...`)属**范围外但必须记录**(§2.7 + TC-C11)。
7. **状态三方口径不可分裂(HC-03/HC-04)**。服务端是唯一写入方:任务侧**精确**匹配 `loaded`,客户端用**子串** `deleted` 过滤。U/DB/C 三组必须同时断言这两个字面量,任何改动都按致命缺陷处理。

---

## 2. 测试环境与数据准备

### 2.1 环境拓扑（路径 1:103 真实环境,**无内网 registry**,必跑）

| 组件 | 部署 | 关键配置 |
| --- | --- | --- |
| host 103 | `fireflyer@192.168.100.103` | `~/.hfai/conf.yml` 的 `url = http://10.205.52.200`;客户端调用一律 `sudo -u fireflyer hai-cli ...` |
| MicroK8s 集群 | 4 节点:`k8s-master`(invalid)、`k8s-slave01/03`(training)、`k8s-slave02`(jupyter_cpu);containerd 1.4.13 | 节点是 Multipass VM,**节点内探测必须走 `multipass exec k8s-slave01 -- ...`** |
| ugc-server | `hai-platform-0` 1/1 Running,`:8083` | `ONE_SERVER=ugc`;`supervisorctl status` 可见 `ugc_server` |
| PostgreSQL | pod 内 `mars_db` | `sudo kubectl -n hai-platform exec hai-platform-0 -- psql -U root -d mars_db -c "..."` |
| 镜像共享根 | `/nfs-shared/hai-platform/image`(`image_path`) | 需 4 个节点与 `hai-platform-0` 均可见、可读 |
| 内网 registry | **不存在**(无 registry pod / svc) | 只做「不可达」负向验证:`kubectl get svc -A \| grep -i registry` 为空;`getent hosts registry.high-flyer.cn` → `198.18.0.77`(代理段,不可达) |
| 节点本地目录 | `/data_local`(**默认不存在**) | link initContainer 的 hostPath 目标;`V1HostPathVolumeSource` 未指定 `type` → kubelet 不创建(OPS-04 / I17①) |
| helper 镜像 | 节点已有 `docker.io/library/busybox:latest` | `[image].load_helper_image` 指向它,避免 `registry.high-flyer.cn/google_containers/busybox:latest` 拉取失败(I17②) |
| 客户端 | `/usr/local/bin/hai-cli` | 必须 `sudo -u fireflyer`(identity 与 `/home/fireflyer/.hfai/conf.yml` 绑定) |

### 2.2 环境拓扑（路径 2:可选内网 registry,生产对照,条件具备时跑）

在 §2.1 基础上替换「无 registry」前提:集群内提供 `registry.high-flyer.cn`(或本地 `:5000`),把 `[image].loader_backend` 改为 `registry`,验证「预导入 → `push` → 运行期从 registry 拉取 / link」。**103 当前不满足该条件,此路径不构成发布门禁**(AC-09 明确允许),但必须在测试报告中记录为「未验证」;仅 `FI-04` 的负向断言(registry 不可达时 `register` 后端仍成功)必须跑。

### 2.3 测试用户与组

| 标识 | 角色 | `shared_group` | 用途 | 关键属性 |
| --- | --- | --- | --- | --- |
| `T_A` / U-A | 普通用户 | `hfai` | 主路径:load / list / delete / 跑任务 | 103 上 shell 账号为 `fireflyer`,**平台身份为 `haiadmin`**(`sudo -u fireflyer hai-cli whoami` 实测),组 `hfai` |
| `T_B` / U-B | 普通用户 | `hfai` | **组内他人**镜像:验证「组内共享、组内可删」(SEC-05) | 与 T_A 同组,独立 token |
| `T_C` / U-C | 普通用户 | `haigraph` | **跨组**越权:删 A 组镜像、读 A 组列表 | 3 段 URL 里第一段相同、第二段不同 |
| `T_ADMIN` | ops | `hfai` | 灰度名单、`custom.py` 覆盖、回滚演练、节点前置操作 | 允许 `sudo kubectl` / `multipass exec` |

> 103 上只有少量真实用户行,**不得用业务用户做删除类用例**:`T_B`/`T_C` 由 `psql` 直连 `mars_db` 造测试用户与组,或用既有测试账号;所有删除用例的目标行必须由本文件 §2.5 的 fixture 造出。

### 2.4 `[image]` 与共享根配置样例

```toml
# /nfs-shared/hai-platform/override.toml —— 103(无内网 registry)
[image]
enabled = true
enabled_groups = ['hfai']            # 灰度组白名单;跨组用例把 T_C 排除在外
enabled_users = []                       # 三级灰度:总开关 → 组 → 用户(OPS-01)
registry = 'registry.high-flyer.cn'      # 仅作 image_url 第一段,P0 不要求可达(CMP-04)
loader_backend = 'register'              # 不需要 registry 的后端(ADR-I2 / AC-09)
name_regex = '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}(:[A-Za-z0-9._-]{0,127})?$'
load_helper_image = 'docker.io/library/busybox:latest'   # Q-6:103 节点已有
data_local_path = '/data_local'
# —— 上传通道开关（**本分支主入口**，S8 已实现并在 103 实测）
upload_enabled = true                    # 上传通道总开关；与 enabled 同时生效才允许上传（HC-12）
max_tar_bytes = 0                        # 单 tar 上限（字节）；0 = 不限制，超限快速失败 IMAGE_TAR_TOO_LARGE（OPS-07）
upload_require_precheck = false          # 是否强制客户端先走 API-19 预检（灰度期收紧入口）

[cloud.storage.service]
image_path = '/nfs-shared/hai-platform/image'            # 103 覆盖值(设计 §3.3)
```

> **上传开关已实现并实测**：上表 `[image].upload_enabled` / `[image].max_tar_bytes` / `[image].upload_require_precheck`
> 默认值 `true` / `0` / `false` 均已落地（`cloud_storage/service/context.py`）；TC-UP-08（上传开关独立可关）
> 与 TC-UP-10（上限快速失败）已在 103 执行通过（见 [images-server-test-report.md](images-server-test-report.md) §9.2）。

> **基线自检(跑任何用例前)**:
> `sudo kubectl -n hai-platform exec hai-platform-0 -- psql -U root -d mars_db -c "\d train_image"` 应含新增列 `message`(迁移 `035` 已生效);
> `sudo -u fireflyer hai-cli images list` 应能渲染两张表(内建镜像 1 行 + 用户镜像表头)。

### 2.5 镜像 fixture 构造(自带探针,供任务侧可区分断言)

```bash
# ① 造一个「含唯一探针」的自定义镜像并导出 tar —— 探针是 AC-01 判定「用的是自定义镜像」的唯一依据
mkdir -p /tmp/imgbuild && cd /tmp/imgbuild
cat > Dockerfile <<'EOF'
FROM registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:7589fb1
RUN mkdir -p /probe \
 && printf "VALUE = 'images-load-ok'\n" > /probe/image_probe_unique.py \
 && printf "images-load-ok\n"           > /etc/hai-image-mark
EOF
sudo docker build -t registry.high-flyer.cn/hfai/demo:v1 .
sudo docker save -o /tmp/demo.tar registry.high-flyer.cn/hfai/demo:v1
sudo install -m 0644 /tmp/demo.tar /nfs-shared/hai-platform/image/demo.tar

# ② 同名不同内容(v2):用于「同名不同 tar」与排序基准用例(TC-A07 / TC-C07 / E2E-03)
sed 's/images-load-ok/images-load-v2-ok/' Dockerfile > Dockerfile.v2
sudo docker build -f Dockerfile.v2 -t registry.high-flyer.cn/hfai/demo:v1 .
sudo docker save -o /nfs-shared/hai-platform/image/demo_v2.tar registry.high-flyer.cn/hfai/demo:v1

# ③ 任务侧探针脚本(放在登录机 /tmp,由 `hai-cli python` 直接提交,与 §2.7 实测命令同一形态)
cat > /tmp/probe_img.py <<'PY'
import sys
sys.path.insert(0, '/probe')
import image_probe_unique as m          # 只存在于自定义镜像内
print('IMAGE_PROBE=' + m.VALUE)
print('IMAGE_MARK='  + open('/etc/hai-image-mark').read().strip())
PY
```

> **D-1 基线**:共享根已建且可读、`demo.tar` / `demo_v2.tar` 已就位、`/tmp/probe_img.py` 已就位、§2.4 配置已生效、`train_image` 表为空。下文「基线环境」均指此。

### 2.6 对照组:内建镜像必须失败(反向判据)

同一个 `/tmp/probe_img.py` 用内建镜像提交:

```bash
sudo -u fireflyer hai-cli python /tmp/probe_img.py -- -i hai_base -n 1
```

**期望失败**:任务日志出现 `ModuleNotFoundError: No module named 'image_probe_unique'`(或 `/etc/hai-image-mark` 不存在)。
只有**自定义镜像成功 + 内建镜像失败**同时成立,E2E-01 的「输出可区分」才成立——否则说明任务根本没用到自定义镜像。

### 2.7 修复前基线(103 实测,用于回归对照)

```bash
$ sudo -u fireflyer hai-cli python /tmp/probe_img.py -- --image registry.high-flyer.cn/hfai/demo:v1 -n 1
Exception: 请求失败: [exception: ...]         # client/api/api_utils.py:75,裸异常而非可读提示
```

服务端拒绝文案(**逐字**):

`用户所在的组 [hfai] 不存在镜像 [registry.high-flyer.cn/hfai/demo:v1] 或镜像仍在加载, 请使用命令 hfai images list 检查`

三点结论(直接决定 T/C 组的判定方式):

1. 该文案证明**任务侧校验逻辑本身是通的**(K1–K4 全部生效),失败的唯一原因是 `train_image` 里**没有任何 `status='loaded'` 的行**(I4)——这正是「管道通了但没有水源」的实测形态;
2. 文案把 `hfai images list` 写进用户指引(K5),而**修复前 `user_images` 恒为空**(I2)→ 用户按指引自查只会看到「没有镜像」,永远查不出原因。**修复后该指引必须成立**,由 TC-T09 与 E2E-04 判定(AC-03);
3. 裸 `Exception` 呈现只出现在**任务提交路径**(`hai-cli python ... --image ...`),**不在** `images` 子命令范围内(FR-12 只修 `load`/`delete`)→ 记录为**范围外**,由 TC-C11 跟踪,不得当作本特性的准出项。

---

## 3. 用例总览

| 组 | 名称 | 用例数 | 主要覆盖需求 | 层级分布 |
| --- | --- | --- | --- | --- |
| U | 单元（命名派生 / 路径校验 / 状态机迁移 / 组校验 / numpy 归一化） | 12 | FR-04/07/13 · NFR-04 · SEC-01 · HC-03/05 | L1 |
| A | 接口契约（API-15 / API-16 / API-17 / API-18） | 18 | FR-02/03/05/10/11 · SEC-02/04 · CMP-01/02 · AC-03 | L2 |
| P | 路径与概念一致性（`image_tar` / `path` / `image_url`） | 6 | FR-07/13 · HC-02/05/09 · AC-01 | L1/L2/L3 |
| C | 客户端（3 子命令真实 CLI 行为 / `-a` / `msg` / DESC 顺序 / `load -i`） | 11 | FR-01/12 · HC-04 · CMP-01 · AC-02/13 | L2 |
| DB | 数据与迁移（幂等重放 / 唯一索引 / `updated_at` 触发器 / status 字面量） | 8 | FR-04/06 · NFR-01 · OPS-03 · HC-01/06 · AC-12 | L2 |
| S | 安全（路径越界 / 跨组删除 / 伪造回报 / 名字注入） | 9 | SEC-01~SEC-07 · AC-06/07 | L2/L3 |
| F | 并发 / 幂等 / 故障 | 7 | NFR-01/02/06 · FR-06 · AC-05 | L2 |
| O | 兼容 / 配置 / 运维（旧客户端 / 灰度 / `image_path` 覆盖 / 一级回滚） | 8 | CMP-01~06 · OPS-01/02/03 · AC-13 | L2/L4 |
| T | **任务侧与运行面**（自定义镜像跑任务产出可区分输出 / link 脚本 / `/data_local` / K5 闭环） | 9 | FR-08/09/10 · FR-15(K5) · OPS-04 · **AC-01/03/08/09** | L3 |
| L | 可观测（4 个指标 + 日志字段可用 `image_tar` 串联） | 4 | NFR-03 · SEC-06 · AC-11 | L2/L3 |
| **UP**（**本分支主体·上传主入口**） | 上传通道（`images push` / `file_type=image` / STS 作用域 / 落点 / 开关一致性 / 幂等续传） | 12 | FR-16~FR-20 · NFR-07~NFR-10 · SEC-08~SEC-10 · OPS-06~OPS-09 · HC-11~HC-14 | L1/L2/L3 |
| **合计** | | **104** | 见 §9 追溯表 | |
> 另有 §5 的 **10 个端到端场景(E2E-01~E2E-10,其中 E2E-09/E2E-10 属本分支上传主入口交付)** 与 §6 的 **12 条故障注入(FI-01~FI-12,其中 FI-09~FI-12 属本分支上传主入口交付)**,由上述用例组合而成,**不重复计数**。

---

## 4. 详细用例

> 表头说明:`层级` = L1/L2/L3/L4;基础 URL 简写 `$API` = `http://10.205.52.200`;「基线环境」= §2.1 + §2.3 + §2.4 + §2.5-D1;`步骤` 与 `预期结果` 均以 `→` 串联。各组用例表的 `优先级` 列**沿用原有用例取值**(本次改写不改动用例优先级);文首「本分支说明」中**标签约定**的 `P0`/`P1` 是**来源/阶段**标签(§4.11 UP 组按本分支 P0 统一执行),与用例优先级不同义。

### 4.1 U 组 · 单元（TC-U01~TC-U12）

**本组必须能在无 DB、无 k8s、无 registry 的条件下运行(NFR-04)。**

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-U01 | P0 | L1 | `image_path=/nfs-shared/hai-platform/image` | 调 `get_image_root()` → 调 `get_base_path(FileType.IMAGE)` → 三者与配置比对 | → 三者同源且等于配置值;尾斜杠 / 相对路径归一化为绝对路径且不出现 `//`;未配置时取默认 `/nfs_shared/image` | FR-13, OPS-01 |
| TC-U02 | P0 | L1 | 纯函数 | `derive_image_name('/nfs-shared/hai-platform/image/demo.tar', None)` → 再传 `image='demo:v1'` → 再传 `dir/a b.tar`、`x.TAR` | → 缺省派生 `demo:latest`(去 `.tar` 后缀,无 tag 补 `:latest`);显式入参与派生一致时原样返回;非法字符在 U03 拒绝;`.TAR` 不当作 `.tar` 后缀 | FR-07 |
| TC-U03 | P0 | L1 | 纯函数 | 合法集 `['demo:v1','demo','a.b_c-d:v1.2','A1:x']` 与非法集 `['','a/b:v1','../x','a b','a'*65,'a:v1:v2',':v1','a:']` 逐项过 `IMAGE_NAME_RE` | → 合法集原样通过;非法集全部 `INVALID_PARAM`;**`/`、`..`、空白、超长必须拒绝** | FR-07, SEC-03, HC-05 |
| TC-U04 | P0 | L1 | 纯函数 | 对 U02 的所有派生结果断言 `'/' not in image`,并断言 `f'{registry}/{group}/{image}'.split('/')` 长度 == 3 | → 全部成立(恰好 3 段);`image` 自身永不含 `/` | FR-07, HC-05 |
| TC-U05 | P0 | L1 | `tmp_path` 充当 `image_root` | `check_is_subpath(root, ...)` 依次传 `root/a/b.tar`、`root/../etc/passwd`、`/etc/passwd`、`/tmp/fake.tar`、`root` 自身 | → 仅第一个通过;其余抛 `WorkspaceError`（`cloud_storage/service/errors.py:34`）并映射为 `PATH_ESCAPE`;**`root` 自身(目录)也必须拒绝**(必须是文件) | FR-13, SEC-01 |
| TC-U06 | P0 | L1 | `root/link -> /etc` | `check_is_subpath(root, root/link/passwd)` → `realpath` 后再比对 | → 拒绝(`PATH_ESCAPE`);**符号链接不得绕过共享根断言** | SEC-01 |
| TC-U07 | P0 | L1 | 纯函数,状态机常量已导入 | 遍历设计 §7.3 迁移表全部行:`load→processing/loaded`、`processing→loading`、`processing/loading→loaded/failed`、`failed→load`、`loaded→load`、`loaded→delete`、`deleted→load`、`deleted→loaded` | → 表中「✅」全部允许;`loaded → load` 返回现状且**不改行**;`deleted → loaded` 抛 `ILLEGAL_TRANSITION`;无表中未列出的迁移被接受 | FR-04, HC-03 |
| TC-U08 | P0 | L1 | 模块常量已定义 | 断言 `LOADED == 'loaded'`、`'deleted' in DELETED`,并 `grep` 全仓状态字面量 | → 两断言成立;`'loaded'` / `'deleted'` 字面量只出现在常量模块与契约注释中,业务代码无散落字符串 | FR-04, HC-03, HC-04 |
| TC-U09 | P0 | L1 | 用户对象 `shared_group='haigraph'` | `async_delete('registry.high-flyer.cn/hfai/demo:v1')` → 再传 `'demo:v1'`、`'a/b/c/d'` | → 第一个 `FORBIDDEN`(跨组);后两个 `INVALID_PARAM`(非 3 段);**任何方法都不接受调用方传 `shared_group` 参数** | FR-05, SEC-02, SEC-05 |
| TC-U10 | P0 | L1 | 构造含 `np.int64(7)` / `pd.Timestamp` / `np.nan` 的行 | 过 selector 出口归一化 → `json.dumps(结果)` | → `task_id` 为原生 `int` 且 `type(...) is int`;时间列为 ISO 字符串;`json.dumps` 不抛 `TypeError`(修 I8,避免 FastAPI `jsonable_encoder` 500) | FR-11 |
| TC-U11 | P0 | L1 | 同组 3 行,`updated_at` 递增、`image` 相同 | 调 `a_find_user_group_images(group)` | → 首行是 `updated_at` **最大**那行(修 I7);`ascending=False` 生效 | FR-11 |
| TC-U12 | P1 | L1 | 逐项制造异常配置 | 调 `image_self_check()`:`image_root` 不存在 / 不可写、`registry` 为空、`loader_backend='bogus'`、`load_helper_image=''` | → 逐项返回 `ok=false` 与建议值,**只告警不抛异常、不阻断启动**(对齐 `env_registry_self_check`) | OPS-01, OPS-04 |

### 4.2 A 组 · 接口契约（TC-A01~TC-A18）

#### 4.2.1 API-15 `POST /ugc/user/train_image/load`（FR-03）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A01 | P0 | L2 | 基线环境 | `POST $API/ugc/user/train_image/load?token=T_A&image_tar=/nfs-shared/hai-platform/image/demo.tar&image=demo:v1` | → HTTP 200,`success=1`,`msg` 含「已登记」,`status=loaded`,`image=registry.high-flyer.cn/hfai/demo:v1`,`task_id=0`(register 后端同步置 loaded);`psql -c "select image_tar,image,path,status from train_image"` → **1 行**,`path == image_tar` | FR-03, FR-09, API-15 |
| TC-A02 | P0 | L2 | 基线环境 | **旧形态**:只传 `image_tar`(不带 `image`) | → `success=1`;服务端由 basename 派生 `image=demo:latest`;DB 行 `image` 与响应一致(CMP-01 的落地) | FR-03, CMP-01 |
| TC-A03 | P0 | L2 | 基线环境 | 同一入参分别用 ①query string ②`text/plain` body 内 JSON ③`application/json` body 调用 | → 三者响应体**等价**(除 `updated_at`);服务端兼容两种承载(设计 §4.0) | CMP-02 |
| TC-A04 | P0 | L2 | 基线环境 | `image_tar` 依次取 `/etc/passwd`、`/nfs-shared/hai-platform/image/../secret.tar`、`/tmp/fake.tar`、URL 编码的 `%2e%2e` 变体 | → 全部 `success=0` + `PATH_ESCAPE`,HTTP 200;**`train_image` 行数不变(必须为 0 新增)** | FR-13, SEC-01, AC-06 |
| TC-A05 | P0 | L2 | 基线环境 | `image_tar` 依次取不存在的 `.../nope.tar`、目录 `.../image`、0 字节文件 `.../empty.tar` | → 依次 `IMAGE_TAR_NOT_FOUND`、`INVALID_PARAM`(非普通文件)、`INVALID_PARAM`(大小 > 0 校验);三者均无 DB 行 | FR-03 |
| TC-A06 | P0 | L2 | TC-A01 之后 | 同参数连续调用 3 次 | → 3 次响应状态一致(均 `loaded`);`select count(*) from train_image` == **1**;`path` / `task_id` / `updated_at` 与首次**完全一致**(upsert 的 `where status in (failed,deleted)` 生效,`loaded` 行不被覆盖) | FR-06, NFR-01, AC-05 |
| TC-A07 | P0 | L2 | `demo.tar` 已 `loaded` | 用 `demo_v2.tar` + `image=demo:v1`(同名不同 tar)再 load | → 按 Q-3 决策:允许则**保留历史行**(2 行)且 `images list` 行为与文档一致;拒绝则返回 `IMAGE_NAME_CONFLICT` 且不影响已有 `loaded` 行。**两种都必须:任务校验仍只看 `status='loaded'`** | FR-03, FR-15 |

#### 4.2.2 API-16 `POST /ugc/user/train_image/update_status`（FR-10）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A08 | P0 | L2 | `loader_backend='task'`,已 load 出 `status=processing` + `task_id=<id>` 的行 | `POST $API/ugc/user/train_image/update_status?token=T_A`,body `{"image_tar":".../demo.tar","status":"loaded","path":"/nfs-shared/hai-platform/image/demo","task_id":<id>}` | → HTTP 200,`success=1`,`status=loaded`;DB `path` 被**回填为入参值**(而非 image_tar),`message` 为空 | FR-10, FR-03 |
| TC-A09 | P0 | L2 | TC-A08 之前的 `processing` 行 | 用 `task_id=1`(不匹配)与不传 `task_id` 各回报一次 `loaded` | → 两次均 `FORBIDDEN`;行状态仍 `processing`、`path` 仍为空(**`loaded` 不可被伪造回报覆盖**,SEC-04) | FR-10, SEC-04 |
| TC-A10 | P0 | L2 | 一行 `status=deleted`、一行 `status=loaded` | 对 `deleted` 行回报 `loaded` → 对 `loaded` 行回报 `failed` → 对 `loaded` 行重复回报 `loaded` | → ①`ILLEGAL_TRANSITION` ②`ILLEGAL_TRANSITION`(设计 §7.3)③`success=1` 且无副作用(幂等) | FR-04, NFR-01 |
| TC-A11 | P1 | L2 | `processing` 行 | `loaded` 但缺 `path` → `failed` + `message='tar 校验失败'` | → 前者 `INVALID_PARAM`;后者落库 `failed`,且 API-17 响应可见 `message` | FR-10, FR-11 |

#### 4.2.3 API-17 `POST /ugc/user/train_image/list`（**修订版**,FR-02/FR-11）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A12 | P0 | L2 | 基线环境 + TC-A01 | `POST $API/ugc/user/train_image/list?token=T_A` | → `user_images` **非空**;每行含 `registry/shared_group/image/status/image_tar/updated_at` 6 字段;`mars_images` 与改造前**逐字节一致**(字段、值、顺序均不变) | FR-02, FR-11, CMP-03, AC-03 |
| TC-A13 | P0 | L2 | 先 load `demo.tar`(v1)再 load `demo_v2.tar`(v2),`image` 均为 `demo:v1` | 调 list → 检查首行 | → 首行是**后加载(更新)**那行;客户端取「首个」即「以最新为准」的注释意图成立(修 I7,ADR-I5) | FR-11 |
| TC-A14 | P0 | L2 | 已有一行 `task_id` 非 0 | 调 list 并解析 JSON | → HTTP 200(**不得 500**);`task_id` 为 JSON number、`updated_at`/`created_at` 为 ISO 字符串(修 I8) | FR-11 |
| TC-A15 | P1 | L2 | T_A 与 T_C 各有一行 | 用 `T_C` 调 list | → 只返回 `shared_group=haigraph` 的行,看不到 `hfai` 的行;返回**本组全部状态行(含 `deleted`)**,隐藏交由客户端 `-a` 决定 | FR-11, SEC-02, CMP-05 |

#### 4.2.4 API-18 `POST /ugc/user/train_image/delete`（FR-05）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-A16 | P0 | L2 | TC-A01 的 `loaded` 行 | 调 delete 传 `registry.high-flyer.cn/hfai/demo:v1` → 再调一次 | → 第一次 `success=1`,`deleted=1`,DB `status='deleted'`;第二次 `success=1`,`deleted=0`(幂等);**镜像名不回收**,行仍在 | FR-05, NFR-01, AC-04 |
| TC-A17 | P0 | L2 | A 组有一行 `loaded` | 用 `T_C` 调 delete 传 `registry.high-flyer.cn/hfai/demo:v1` | → `FORBIDDEN`;该行 `status` 与 `updated_at` **均不变**(SEC-05:组内可删他人、**跨组一律禁止**) | FR-05, SEC-02, SEC-05, AC-07 |
| TC-A18 | P0 | L2 | 基线环境 | delete 传 `demo:v1`(1 段)、`a/b/c/d`(4 段)、`registry.high-flyer.cn/haigraph/none:v1`(不存在) | → 前两者 `INVALID_PARAM`;第三者 `IMAGE_NOT_FOUND`;三者 HTTP 200 且 `msg` 可读 | FR-05 |

### 4.3 P 组 · 路径与概念一致性（TC-P01~TC-P06）

**本组是 I18 的判定依据,发布门禁必跑。**

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-P01 | P0 | L1/L2 | `loader_backend='register'`,已 load 一行 | 读 DB 的 `image_tar` 与 `path` → 代码评审 `async_load` 中对两列的赋值语句 | → 两列**取值相同**(都指共享盘上的 tar);但实现上是**两次独立赋值**(分别写入),不得写成 `path = image_tar` 的字段复用 | FR-07, AC-01 |
| TC-P02 | P0 | L2 | 已 load 一行 | 拼接 `f'{registry}/{shared_group}/{image}'` → 与 `TrainImageSelector.a_find_user_group_image_urls(shared_group, status='loaded')` 的返回值比对 → 与任务提交时 `resource.image` 比对 | → 三者**逐字节相等**;`split('/')` 长度恰好 3;`image` 自身不含 `/`(K1/K2/HC-02/HC-05/HC-09) | FR-07, FR-15, HC-02, HC-05, HC-09 |
| TC-P03 | P0 | L2 | 切 `loader_backend='task'` | load `demo.tar` → 立即读 DB `path` → 经 API-16 回报 `loaded` 后再读 | → load 阶段 `path` **必须为空**(不得把 `image_tar` 写进 `path`,I18 实现修正);回报后才写入回报值 | FR-07, FR-03 |
| TC-P04 | P0 | L2/L3 | 已 load 一行 | 任务提交后查 `HFAI_IMAGE` 与 `HFAI_IMAGE_WEKA_PATH`(见 TC-T07) | → `HFAI_IMAGE_WEKA_PATH` **逐字节等于 DB `path` 列**;launcher 不读 `image_tar` | FR-07, FR-09 |
| TC-P05 | P0 | L2 | 已 load 一行 | 比对 ①客户端 `images list` 的 `image` 列 ②API-17 的 `registry/shared_group/image` ③DB 三列拼接 | → 三者一致(客户端 `os.path.join(registry, shared_group, image)` 口径);字段名不得改名 | FR-11, CMP-05 |
| TC-P06 | P1 | L3 | 真实部署 | 把 `image_path` 从 `/nfs_shared/image` 改为 `/nfs-shared/hai-platform/image` 并重启 → 重复上述对照 | → 配置 / `get_base_path(FileType.IMAGE)` / API-15 的校验根三方一致;不依赖具体取值 | FR-13, OPS-01 |

### 4.4 C 组 · 客户端（TC-C01~TC-C11）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-C01 | P0 | L2 | 新客户端 wheel | `sudo -u fireflyer hai-cli images --help` → `hai-cli images load --help` | → 3 个子命令 `list/load/delete` 齐全;`load` 出现可选 `-i/--image`;`--help` 自身不报错;帮助文本明确「不回收存储」(FR-14 的「不误导」) | FR-01, FR-14 |
| TC-C02 | P0 | L2 | 基线环境 + 已 load | `sudo -u fireflyer hai-cli images list` | → 渲染两张表;用户表出现该行,列 `image/status/shared_group/image_tar/updated_at` 与 API-17 / DB 一致(不再是空表) | FR-02, AC-03 |
| TC-C03 | P0 | L2 | 基线环境 | `sudo -u fireflyer hai-cli images load /nfs-shared/hai-platform/image/demo.tar --image demo:v1` | → 退出码 0;打印服务端 `msg`(含「已登记」);**输出中不出现 `AttributeError`、不出现 Python 栈**(修 C-3) | FR-01, FR-12, AC-02 |
| TC-C04 | P0 | L2 | 基线环境 | `sudo -u fireflyer hai-cli images load /etc/passwd` | → 退出码 1;**只打印服务端 `msg`(含拒绝原因)**,不出现 `Exception('请求失败: ...')` 或 `AssertionError` 栈(修 I10) | FR-12 |
| TC-C05 | P0 | L2 | 已 load 一行 | `sudo -u fireflyer hai-cli images delete registry.high-flyer.cn/hfai/demo:v1` | → 退出码 0;打印「已删除 N 个镜像记录」;**不出现 `async_delete` 属性错误**(修 C-3) | FR-01, FR-05, AC-02 |
| TC-C06 | P0 | L2 | TC-C05 之后 | `images list` → `images list -a` | → 前者**隐藏**该行(`'deleted' in status` 子串过滤);后者**显示**该行;帮助文本明确「隐藏 status 含 `deleted` 的记录」 | FR-05, HC-04, AC-04 |
| TC-C07 | P0 | L2 | 先 load `demo.tar` 再 load `demo_v2.tar`(同名 `demo:v1`) | `images list` → 观察该行 `status` 的基准取值 | → 基准是**最新那条**(首个出现的即最新);若出现 `deleted by new tar(...)` 这类合成文案,后缀必须是**新的**状态,与注释「以最新的为准」一致(修 I7) | FR-11, HC-04 |
| TC-C08 | P0 | L2 | 基线环境 | `images load /nfs-shared/hai-platform/image/demo.tar`(**不带** `-i`) | → 退出码 0;服务端派生名字;客户端不因缺 `image` 报错(CMP-01) | CMP-01, FR-12 |
| TC-C09 | P1 | L2 | 抓包 / `client/api/image_api.py` 静态核对 | 分别发起 `list`、`load`、`delete` 并统计重试次数 | → `list` 为 `retries=3`;`load` / `delete` 为默认 1 次(**变更型调用不得静默重试**,FR-06) | FR-06, NFR-01 |
| TC-C10 | P1 | L2 | 客户端本机 | `images load /tmp/not-exist.tar` | → 打印「不存在这个镜像包」;**不发起任何 HTTP 请求**(抓包为空),退出码非 0 | FR-12 |
| TC-C11 | P1 | L2/L3 | 修复前基线(§2.7):`train_image` 无该镜像的 `loaded` 行 | `sudo -u fireflyer hai-cli python /tmp/probe_img.py -- --image registry.high-flyer.cn/hfai/demo:v1 -n 1` → 抓取客户端 stderr | → **记录**:客户端把服务端拒绝表现为**裸 `Exception: 请求失败: [exception: ...]`**(`client/api/api_utils.py:75`),不是可读提示。该呈现属**范围外**(FR-12 只覆盖 `images load/delete`),**不得作为本特性准出项**,但必须在测试报告与 Checklist 中显式登记为已知体验缺口(I10 的另一处落点) | FR-12(范围外记录), AC-02 |

### 4.5 DB 组 · 数据与迁移（TC-DB-01~TC-DB-08）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-DB-01 | P0 | L2 | 已部署含 `db_schemas/035.*.sql` 的镜像 | `init_postgresql.sh` 连跑两次 → 每次后执行 `\d train_image` 与 `select count(*) from information_schema.columns where table_name='train_image'` | → 两次均无报错;列只加一次(`add column if not exists`);两次列数相等;`db_schemas/` 无 `drop column` | OPS-03, HC-06, AC-12 |
| TC-DB-02 | P0 | L2 | 空表 | 不走 upsert,直接 `insert` 同 `image_tar` 第二行 → 再看 Q-2 决策后的索引 | → 直接插入报唯一键冲突(证明约束存在);upsert 路径不报错、行数不增;若索引改为 `(shared_group, image_tar)`,则**跨组同 tar 允许**,`on conflict` 目标同步更新 | FR-06, NFR-01 |
| TC-DB-03 | P0 | L2 | 一行 `loaded`、一行 `failed` | 对 `loaded` 行重复 load → 对 `failed` 行重复 load | → 前者 `status/path/task_id/updated_at` **完全不变**;后者被重置为 `processing`(或 register 后端下的 `loaded`),`message` 清空(设计 §5.2 的 `where` 分支) | FR-06, NFR-01, AC-05 |
| TC-DB-04 | P0 | L2 | 一行 `created_at` / `updated_at` 已知 | `update train_image set status='deleted' where ...` | → `updated_at` 严格增大;`created_at` 不变(017 DDL 的 `trigger_update_train_image_updated_at` 生效) | FR-11 |
| TC-DB-05 | P0 | L2 | 走完 load / 回报 / delete 全流程 | `select distinct status from train_image` | → 只出现 `processing/loading/loaded/failed/deleted` 五值,无其他拼写;`deleted` **拼写包含子串 `deleted`**(HC-04);`loaded` 为精确字面量(HC-03) | FR-04, HC-03, HC-04 |
| TC-DB-06 | P0 | L2/L1 | 所有新增写入路径 | 代码评审 + 单测:检索 `%s::`、裸 `%`、非 tuple 参数 → 构造含 `%` 的 `message='100%% done'` 走 API-16 写库 | → 无 `%s::type`(用 `CAST(%s AS ...)`);字面 `%` 写 `%%`;参数为 tuple 且枚举传 `.value`;含 `%` 的 message 写入成功且读回原值 | HC-01 |
| TC-DB-07 | P1 | L2 | 单组造 200 行 | 调 API-17 → 抓 SQL 日志计数并计时 | → 只发 1 条 SQL(无 N+1);P95 < 1s;`explain analyze` 命中 `train_image_image_uindex` 或组索引 | NFR-02 |
| TC-DB-08 | P1 | L2 | 迁移前已存在旧行 | 迁移后 `select message, user_name, count(*) from train_image group by 1,2` | → 旧行 `message` 为 `''`(非 NULL),`user_name` 为 NULL 或 `''`;旧客户端消费字段名不变 | CMP-05 |

### 4.6 S 组 · 安全（TC-S01~TC-S09）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-S01 | P0 | L2 | 基线环境 | 构造 `image_tar` 的 `..` 变体:`.../image/a/../../etc/passwd`、`.../image/%2e%2e/etc/passwd`、`.../image//../etc/passwd` | → 全部 `PATH_ESCAPE`,**`train_image` 行数不变** | SEC-01, AC-06 |
| TC-S02 | P0 | L2 | `.../image/evil -> /etc` | load `.../image/evil/passwd` | → 拒绝(`check_is_subpath` 走 `realpath` 语义);不产生 DB 行 | SEC-01 |
| TC-S03 | P0 | L2 | 客户端本机文件确实存在 | load `/tmp/demo.tar`(本机存在)、`C:\demo.tar`、`~/demo.tar` | → 全部 `PATH_ESCAPE`;**「本机存在」不构成放行理由**(口径必须由服务端统一,分析 §2 的结论) | SEC-01, FR-13 |
| TC-S04 | P0 | L2 | A 组一行 `loaded` | `T_C` 用 3 段 URL 删 A 组镜像 → 再对 A 组镜像调 list | → delete `FORBIDDEN`;list 看不到 A 组行;A 组行 `updated_at` 不变 | SEC-02, SEC-05, AC-07 |
| TC-S05 | P0 | L2 | `T_C` 的 token | 请求中显式带 `shared_group=hfai`、`username=fireflyer` 等字段 | → 服务端**全部忽略**,按服务端解析的 `user.shared_group=haigraph` 处理;`train_image.shared_group` 写入 `haigraph` | SEC-02 |
| TC-S06 | P0 | L2 | 一行 `processing`,登记 `task_id=20261002` | 回报时传 `task_id=0`、`task_id=1`、缺 `task_id` | → 三者均 `FORBIDDEN`;行状态与 `path` 不变(防伪造回报) | SEC-04, FR-10 |
| TC-S07 | P0 | L2 | 基线环境 | `image` 依次取 `demo;rm -rf /`、`x$(id)`、`a';DROP TABLE train_image;--`、`a\nb`、`a b` | → 全部 `INVALID_PARAM`;`train_image` 表结构与行数不变;**link 脚本只读 env、不接受用户可控命令行**(SEC-03/SEC-07) | SEC-03, SEC-07 |
| TC-S08 | P0 | L2 | 基线环境 | 三条新路由分别不带 token / 带过期 token 调用 → 再检索服务端日志 | → 均 `UNAUTHORIZED`(HTTP 403);日志中 `token=` 被掩码,`image_tar` 按需截断,**无完整 token** | SEC-04, SEC-06 |
| TC-S09 | P1 | L2/L3 | 已部署 | 检查 `load-image` initContainer 的 SA / 挂载与命令 | → 只挂 `data-local` 与 `node_schema.mounts`,无宿主任意路径;`command/args` 为固定字面量,无用户可控参数;如需运行时 socket,**限定专用命名空间 / SA** | SEC-07, HC-10 |

### 4.7 F 组 · 并发 / 幂等 / 故障（TC-F01~TC-F07）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-F01 | P0 | L2 | 空表,基线环境 | `seq 5 \| xargs -P5 -I{} curl -s .../load?...&image_tar=.../demo.tar` 并发 5 次 | → 无 5xx;最终 `select count(*)` == **1**;所有响应状态一致(`loaded`) | NFR-01, AC-05 |
| TC-F02 | P0 | L2 | `T_A` / `T_C` 同时 load **同一** `demo.tar` | 并发发起 | → 按 Q-2 决策:索引为 `(shared_group,image_tar)` → 2 行(各组 1 行);否则后者得到明确错误码(不得 500、不得静默覆盖他人行) | FR-06, NFR-01 |
| TC-F03 | P0 | L2 | 已 delete 一行 | 重复 delete 同一 image | → 第二次 `deleted:0` + `success:1`;`updated_at` 不变 | FR-05, NFR-01, AC-05 |
| TC-F04 | P0 | L2 | 一行 `loaded`,`task_id` 已知 | 先回报 `failed` 再回报 `loaded`(顺序颠倒/乱序) | → `failed` 被拒(`ILLEGAL_TRANSITION`),行仍 `loaded`;`path` 不变 | FR-04 |
| TC-F05 | P0 | L2 | 基线环境 | 让 PostgreSQL 不可达(pod 内 `pg_ctl stop` 或断开连接)→ 调 `load` → 恢复后重试 | → 期间返回统一失败体(或 5xx)且 **`msg` 可读**;客户端打印 `msg` 而非裸栈;恢复后重试成功且不产生重复行 | FR-12, NFR-01, OPS-02 |
| TC-F06 | P0 | L2 | 多 worker `ugc=2` | 在 `load` 写库中途 `kill -9` ugc-server → 重启 → 重放同一 `load` | → 表处于一致状态(要么无行、要么一行完整);无半写行;重放幂等成功 | NFR-01, OPS-02 |
| TC-F07 | P1 | L2 | 10 GB 稀疏 tar | 对其调 `load` 并测响应耗时 / 服务端 RSS | → 只读文件元数据(不复制、不读全量);P95 < 1s;服务端内存不因大 tar 明显增长(资源可控) | NFR-02, NFR-06 |

### 4.8 O 组 · 兼容 / 配置 / 运维（TC-O01~TC-O08）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-O01 | P0 | L4 | **旧客户端 wheel**(无 `--image`) | 跑 `images load <tar>` 单参 + `images list` + 用该镜像提交任务 | → 单参可用(服务端派生名字);旧客户端解析 list 正常;任务提交通过(K1–K5 不变) | CMP-01, FR-15, AC-13 |
| TC-O02 | P0 | L4 | 旧客户端 | 对比改造前后 API-17 响应 | → 字段**只增不改名**;`user_images` 由空变有内容属行为修正;`mars_images` 零改动;旧客户端渲染无 `KeyError` | CMP-03, CMP-05, AC-13 |
| TC-O03 | P0 | L4 | 三级灰度配置 | ①`enabled=false` ②`enabled=true` + `enabled_groups=['hfai']` ③`enabled_users=['fireflyer']` | → ①三条写路由全 `FEATURE_DISABLED` 且 `list` 不受影响;②`T_A` 通过、`T_C` 拒绝;③仅名单内用户通过 | OPS-01, AC-10 |
| TC-O04 | P0 | L2 | 已有一行 `loaded` | 置 `[image].enabled=false` → 重启 ugc-server → 调 `load`/`delete` → 再跑 `list` 与**用已 loaded 镜像提交任务** | → 写路由失败关闭并提示 `FEATURE_DISABLED`;`list` 与内建镜像路径正常;**已 loaded 行仍可被任务使用,不打断线上任务**(一级回滚) | OPS-02 |
| TC-O05 | P0 | L2 | 基线环境 | 把 `image_path` 改为一个不含该 tar 的目录并重启 → 调 load;再改回 | → 旧路径的注册被拒(`PATH_ESCAPE`);自检与 `get_image_root()` 同步取新值;改回后恢复正常 | FR-13, OPS-01 |
| TC-O06 | P0 | L2/L3 | 基线环境 | ①对比 `mars_images` 行数与字段 ②用 `-i hai_base` 提交任务 ③检查 `a_find_user_group_image_urls` 签名 | → ①零变化 ②任务成功(内建镜像路径不受影响)③签名与返回语义未改 | CMP-03, FR-15, HC-09, AC-10 |
| TC-O07 | P1 | L4 | 模拟部署:私有 `api/resource/image/custom.py` 定义同名 `hfai_image_load` | 重启并调用 API-15 | → **私有实现生效**,本仓 `default.py` 版本被覆盖;三层 `default/implement/custom` 约定未被破坏 | CMP-06 |
| TC-O08 | P1 | L2 | 备份路由注册代码 | 注释 3 条新路由 → 重启 → 调 `load`/`delete`;随后恢复 | → HTTP 404(`Not Found`),客户端打印 `msg` 而非栈;`train_image` 表数据**保留**;恢复后无需改数据即可用(二级回滚) | OPS-02, HC-07 |

### 4.9 T 组 · 任务侧与运行面（TC-T01~TC-T09）

**本组是 AC-01 的判定依据;只测接口 200 不构成通过。**

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-T01 | P0 | L3 | 新镜像已部署 | ①`ls -l marsv2/scripts/link_hfai_image.sh` ②`grep -n link_hfai_image one/hai-up.sh` ③提交任务后 `kubectl -n hai-platform exec <task-pod> -- ls -l /marsv2/scripts/link_hfai_image.sh` | → ①文件存在且可被 `/bin/sh` 执行 ②`storage` 挂载种子里有该行(`marsv2-scripts-...`)③**pod 内可见**(证明随镜像构建进入 pod,HC-08) | FR-08, HC-08, AC-08 |
| TC-T02 | P0 | L3 | 基线环境 + `demo:v1` 已 `loaded` | `sudo -u fireflyer hai-cli python /tmp/probe_img.py -- --image registry.high-flyer.cn/hfai/demo:v1 -n 1` → `hai-cli status <task_id>` → `hai-cli logs <task_id>` | → 任务 `succeeded`;日志含 `IMAGE_PROBE=images-load-ok`;**同时 §2.6 的内建镜像对照必须失败** → 输出确由自定义镜像内容决定(AC-01 核心判据) | FR-09, AC-01, AC-09 |
| TC-T03 | P0 | L3 | TC-T02 的任务 pod 名已知 | `kubectl -n hai-platform get pod <task-pod> -o jsonpath='{.status.initContainerStatuses[?(@.name=="...-load-image")].state.terminated.exitCode}'` → `kubectl describe pod` | → exitCode == **0**;pod events 中**无** `sh: /marsv2/scripts/link_hfai_image.sh: not found`(I16 回归判据) | FR-08, AC-08 |
| TC-T04 | P0 | L3 | `k8s-slave01` 上已有该镜像 | 手工在节点上带 env 重复执行脚本两次:`multipass exec k8s-slave01 -- sudo sh -c 'HFAI_IMAGE=... HFAI_IMAGE_WEKA_PATH=... /marsv2/scripts/link_hfai_image.sh'` | → 两次 exit 0;**第二次打印「已存在,跳过」且不重复导入**(幂等是 initContainer 重试的前提) | FR-08 |
| TC-T05 | P0 | L3 | 基线环境 | 把 `HFAI_IMAGE_WEKA_PATH` 指向不存在的 `/nfs-shared/hai-platform/image/gone.tar` → 提交任务 → 查 pod 状态与日志 | → 脚本 exit 非 0 并打印 `FAILED: 镜像资产不存在: <路径>`;pod **卡在 Init**、主容器不启动;`kubectl describe pod` 可见失败原因(失败可见,不静默放行) | FR-08, AC-08 |
| TC-T06 | P0 | L3 | `k8s-slave02` 上 `/data_local` 不存在 | `multipass exec k8s-slave02 -- ls -ld /data_local` → 在该节点提交任务 → 按部署动作 `mkdir -p /data_local` → 重试 | → 前置不满足时失败可见(`FailedMount` 或脚本显式报错),**不得静默跳过**;`mkdir` 后重试成功;若改为 `DirectoryOrCreate` 则自动创建。部署自检须覆盖该前置 | OPS-04, FR-08 |
| TC-T07 | P0 | L3 | `demo:v1` 已 `loaded` | 任务 pod 内(或 `single_task_impl` 日志)`echo $HFAI_IMAGE`、`echo $HFAI_IMAGE_WEKA_PATH`;并做「刚 load 完立即提交」的时序测试 | → 两变量分别等于 3 段 URL 与 DB `path` 列;刚 load 完立即提交必须能被 launcher 读到(R-3 缓存失效策略生效,或已按 Checklist 记录「重启 launcher 生效」的已知限制) | FR-07, FR-09, FR-10 |
| TC-T08 | P1 | L3 | 基线环境 | ①`load_helper_image='docker.io/library/busybox:latest'` 提交任务 ②改回 `registry.high-flyer.cn/google_containers/busybox:latest` 再提交 | → ①initContainer 正常起(节点已有该镜像)②`ImagePullBackOff`(该域名解析到 `198.18.0.77` 不可达),且运维提示可读。证明 helper 镜像**可配置**(Q-6/I17②) | FR-08, AC-08 |
| TC-T09 | P0 | L3 | §2.7 修复前基线 + E2E-01 已完成 | ①在 `train_image` **无**该镜像 `loaded` 行的条件下提交 `--image registry.high-flyer.cn/hfai/demo:v1`,记录服务端文案 ②完成 load 后 `images list` ③再用一个**从未登记**的镜像名提交,对比两种情形的文案与自查结果 | → ①服务端**逐字**返回 `用户所在的组 [hfai] 不存在镜像 [registry.high-flyer.cn/hfai/demo:v1] 或镜像仍在加载, 请使用命令 hfai images list 检查`(证明 K1–K4 校验逻辑本身是通的,I4 是唯一失败原因)②`images list` **确实显示**该行的 `loaded` 状态与 `image_tar` → 服务端写在报错里的自查指引**成立**(I2/K5 的可观测断链被闭合,AC-03)③未登记镜像仍被拒且文案一致,但用户按指引自查时能区分「从未登记」与「已登记但未 `loaded`」 | FR-02, FR-15, AC-03 |

### 4.10 L 组 · 可观测（TC-L01~TC-L04）

| ID | 优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-L01 | P0 | L2 | 基线环境 | `sudo kubectl -n hai-platform exec hai-platform-0 -- curl -s localhost:8083/metrics \| grep -E 'image_'` | → 4 个指标均存在:`image_load_total`、`image_load_duration_seconds`、`image_list_rows`、`image_link_failed_total`;标签分别为 `status/code`、`backend`、`shared_group`、`node` | NFR-03, AC-11 |
| TC-L02 | P0 | L2 | 基线环境 | 成功 + 失败各调一次 load;成功一次 list | → `image_load_total{status=loaded,code=...}` 与 `{status=failed,...}` 分别 +1;`image_load_duration_seconds_count` 增长;`image_list_rows{shared_group=hfai}` == 实际行数 | NFR-03 |
| TC-L03 | P0 | L2 | 基线环境 | 检查一次 load 全流程日志字段 | → 含 `user_name/shared_group/image_tar/image/task_id/status/from_status/cost_ms`;**不含 token**;`image_tar` 未超长回显 | NFR-03, SEC-06 |
| TC-L04 | P0 | L2/L3 | 完成一次 E2E-01 | `kubectl logs` + `grep <image_tar>` 串联四段日志 | → 「API-15 登记 → 状态迁移到 `loaded` → 任务提交校验 → pod link」四段均可用同一 `image_tar`(含 `task_id`)串起;link 失败计入 `image_link_failed_total{node=...}` | NFR-03, AC-11 |

### 4.11 UP 组 · 上传通道（**本分支主入口**，TC-UP-01~TC-UP-12）

> **环境前提**：本组必须把「本地」放在**共享盘之外**（例如 `/tmp/hai-image-push`），否则 stage1 会被判成
> 「数据已同步」而完全跳过上传 —— 这与 env 特性踩过的 C-6 是同一类假象（env 用例 §2 的教训）。
>
> **优先级口径**：本组 12 例**全部按「本分支 P0」执行**（上传主入口即本分支发布门禁，不再区分组内 P0/P1）。
> 表中 `本分支优先级` 列的 `P0（上传主入口）` 与文首标签约定的**阶段**标签 `P1`（= 上传通道）不同义，见 §4 表头说明。

| ID | 本分支优先级 | 层级 | 前置条件 | 步骤 | 预期结果 | 覆盖需求 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-UP-01 | **P0**（上传主入口） | L1 | 纯函数 | 调 `get_base_path(..., FileType.IMAGE)` 与 `get_bucket_name(FileType.IMAGE)` | → 同时返回 `cloud_base_path`（非空、形如 `{group}/shared/images/{user}/{name}`）与 `cluster_base_path`；后者 `check_is_subpath(image_path, ...)` 通过；bucket == `private_bucket` | FR-17, HC-11, HC-13, NFR-10 |
| TC-UP-02 | **P0**（上传主入口） | L2 | 基线环境 | ① `POST /ugc/get_sts_token?file_type=image&name=<name>` ② 用返回凭证尝试写「他人前缀」与「同用户其它 name 前缀」 | → ① 200 且授权前缀**恰等于** `cloud_base_path`；② 两种越权写均失败（403/AccessDenied） | SEC-08, FR-17 |
| TC-UP-03 | **P0**（上传主入口） | L2 | 基线环境 | 上传一个小文件后调 `POST /ugc/sync_to_cluster?file_type=image&no_zip=true&files=['demo.tar']`；再调 `GET /ugc/sync_to_cluster/status?index=...` | → 受理返回 `index`/`dst_path`/`accepted`，`dst_path` 落在 `image_path` 之下；轮询到 `FINISHED` 且带可读 `msg` | FR-17, FR-20 |
| TC-UP-04 | **P0**（上传主入口） | L2/L3 | 本地 tar 在共享盘之外 | 完整走 `images push <tar>`，随后 `psql` 查 `user_sync_status(file_type='image')`、`ls -l` 落点、`md5sum` 对比 | → 共享盘出现同名 tar 且 **md5 与本地一致**；`user_sync_status` 一行 `finished`；`train_image` 一行 `loaded`（AC-15 的判定位置） | FR-16, FR-18, AC-15 |
| TC-UP-05 | **P0**（上传主入口） | L2 | 基线环境 | 依次用 `name`/相对路径含 `..`、`/`、绝对路径、符号链接的入参调 API-05 | → 全部 `INVALID_PARAM` / `PATH_ESCAPE`，**共享盘零新增文件**（`ls` 前后对比） | SEC-09, HC-13 |
| TC-UP-06 | **P0**（上传主入口） | L2 | 已完成 TC-UP-04 | 对**同一** tar 连续再 push 2 次 | → 第二次起命中 `index`：`accepted=0`（或 `skipped>0`）且 `msg` 含「已同步」语义；共享盘文件 mtime 不变；`train_image` 仍 **1 行** | FR-18, AC-17 |
| TC-UP-07 | **P0**（上传主入口） | L2 | 基线环境 | 置 `[image].enabled=false` + 重启 ugc_server → 调 API-01 与 API-05 → 恢复 `true` 再调 | → 关闭态两者均 `FEATURE_DISABLED`（HTTP 200），共享盘与 RustFS **零新增写入**；恢复后同参数可用 | FR-19, AC-16, HC-12 |
| TC-UP-08 | **P0**（上传主入口） | L2 | 基线环境 | 置 `[image].upload_enabled=false`（`enabled=true`）→ 走上传与 `load`/`delete`/`list` | → 上传被拒（明确提示），`load`/`delete`/`list` 正常 —— 证明上传开关**独立可关**且不误伤控制面 | OPS-06 |
| TC-UP-09 | **P0**（上传主入口） | L2 | 上传进行中 | 在 stage2 期间 kill 掉平台侧下载（或 `chmod 555` 目标目录）→ 观察 → 恢复后 `--force` 重试 | → 失败态可见（`stage2_failed` + reason，且 `train_image` **无新行**）；恢复后重试/续传成功；不产生重复行 | FR-20, AC-17 |
| TC-UP-10 | **P0**（上传主入口） | L2 | 配置 `[image].max_tar_bytes` 为较小值 | 上传一个超过上限的 tar | → 快速失败并返回可读错误（`IMAGE_TAR_TOO_LARGE`），共享盘零落盘 | OPS-07 |
| TC-UP-11 | **P0**（上传主入口） | L2 | 本地 tar 在共享盘之外 | ① `images push <tar> --image demo:v1` ② `--no-load` 变体 ③ 本地文件不存在的变体 | → ① 上传 + 自动登记，`images list` 可见 `loaded`；② 只有上传、无 `train_image` 行；③ 不发起任何请求并打印「不存在这个镜像包」 | FR-16, FR-18 |
| TC-UP-12 | **P0**（上传主入口） | L2 | 基线环境 | ① 手工把 tar 放到 `image_path` 下再 `images load`（兼容旁路）② 检查 `user_sync_status` 主键与 `workspace`/`env` 的 `file_type` 语义 | → ① 仍可用（CMP-08）；② 主键仍为 `(user_name, file_type, name)`，`workspace`/`env` 行为不变（CMP-07/CMP-09） | CMP-07, CMP-08, CMP-09 |

---

## 5. 端到端场景（E2E）

| ID | 优先级 | 场景 | 步骤 | 通过判据 |
| --- | --- | --- | --- | --- |
| **E2E-01** | P0 | **控制面 → 运行面全链路,103 且在无任何内网 registry 的条件下**(AC-01/AC-09 判定依据) | ①先证明「无 registry」:`kubectl get svc -A \| grep -i registry` 为空 + `getent hosts registry.high-flyer.cn` → `198.18.0.77` ②按 §2.4 配 `loader_backend='register'`、`image_path=/nfs-shared/hai-platform/image`、`load_helper_image=docker.io/library/busybox:latest` ③按 §2.5 造带探针的 `demo:v1` tar ④`sudo -u fireflyer hai-cli images load /nfs-shared/hai-platform/image/demo.tar --image demo:v1` ⑤`images list` 见该行 `loaded` ⑥`sudo -u fireflyer hai-cli python /tmp/probe_img.py -- --image registry.high-flyer.cn/hfai/demo:v1 -n 1` ⑦`hai-cli logs <task_id>` | ④`success=1` 且 DB 有 1 行、`path` 已回填;⑤状态为 `loaded`;⑥任务 `succeeded` 且 initContainer exit 0;⑦日志含 `IMAGE_PROBE=images-load-ok`(**可区分输出**),且 §2.6 的内建镜像对照**失败**;全程未访问任何 registry(AC-01/AC-09) |
| **E2E-02** | P0 | 幂等重放 | 紧接 E2E-01,对**同一** `demo.tar` 再连续 `images load` 2 次 → `psql` 查表 → 再提交一次任务 | 表仍 **1 行**、状态不倒退、`path`/`task_id` 不变;第二次任务仍 `succeeded`(AC-05/NFR-01) |
| **E2E-03** | P0 | 列表可见性与排序基准 | 先 load `demo.tar`(v1) → 再 load `demo_v2.tar`(同名 `demo:v1`) → `images list` → `psql` 对比 | `list` 首行 == `updated_at` 最大那行;客户端显示的 `status` 基准是**最新**(修 I7);`user_images` 6 字段齐全且 JSON 可解析(AC-03/FR-11) |
| **E2E-04** | P0 | 删除闭环与「指引不再误导」 | ①`images delete registry.high-flyer.cn/hfai/demo:v1` ②`images list -a` ③用该镜像提交任务 ④读服务端错误文案与客户端 stderr ⑤与 §2.7 修复前基线逐字对比 | ①`success=1`,`deleted=1` ②该行可见且 `status` 含 `deleted` ③任务提交被拒,服务端逐字返回 `用户所在的组 [hfai] 不存在镜像 [registry.high-flyer.cn/hfai/demo:v1] 或镜像仍在加载, 请使用命令 hfai images list 检查` ④客户端呈现仍为裸 `Exception: 请求失败: ...`(TC-C11,范围外) ⑤此时 `images list -a` **确实能**显示该行的 `deleted` 状态与 `image_tar`,与 §2.7 修复前「永远看到空列表」形成对照 → K5 的可观测断链被修复(AC-04/AC-07) |
| **E2E-05** | P0 | 运行面节点前置回归(I16/I17) | 依次制造:①`marsv2/scripts/link_hfai_image.sh` 不存在 ②`/data_local` 不存在 ③helper 镜像引用为不可达的内网地址 → 各提交一次任务 → 逐项修复后重跑 E2E-01 | ①initContainer `not found`、pod 卡 Init ②`FailedMount` 或脚本显式报错 ③`ImagePullBackOff`;三者**均失败可见且日志可诊断**;修复后 E2E-01 通过(AC-08) |
| **E2E-06** | P0 | 灰度与一级回滚 | ①`enabled=true` + `enabled_groups=['hfai']`:`T_A` 与 `T_C` 各走一遍 load/list/delete ②置 `enabled=false` 重启 → 再走写入路径,并用已 `loaded` 镜像提交任务 | ①`T_A` 全通、`T_C` 得到 `FEATURE_DISABLED` ②写路由失败关闭并提示,`list` 与内建镜像路径正常,**已 `loaded` 行上的任务不受影响**(OPS-01/OPS-02/AC-10) |
| **E2E-07** | P0 | 越权与路径安全 | ①`T_C` 用 3 段跨组 URL 删 A 组镜像 ②`T_C` 调 list ③用 `..`/符号链接/本机路径三种 `image_tar` 调 load | ①`FORBIDDEN` 且行不变 ②看不到 A 组行 ③全部 `PATH_ESCAPE` 且 **DB 新增行数为 0**(AC-06/AC-07) |
| **E2E-08** | 兼容旁路 | 旧客户端兼容 + 零回归 | ①用旧 wheel 跑单参 `images load` + 用该镜像提交任务 ②串跑 §7.2 的 workspace / env 既有脚本 | ①单参可用(名字由服务端派生)、任务成功 ②`smoke_ugc` 8/8、`e2e_workspace` 19/19、`smoke_env` 20/20、`e2e_env` 16/16,内建镜像任务 initContainer 列表无新增项(CMP-01/CMP-03/AC-10/AC-13) |
| **E2E-09** | **P0（本分支第一发布门禁）** | **上传闭环（本分支第一发布门禁）**：本地 tar → RustFS → 共享盘 → `load` → 任务有可区分输出（AC-15） | ①本地目录放在**共享盘之外**（`/tmp/hai-image-push`）②`docker save` 出一个带探针的 tar ③`images push` ④`md5sum` 对比共享盘 ⑤`images list` ⑥提交任务并读日志 | ③成功且 `index` 轮询到 `FINISHED`；④md5 一致；⑤状态 `loaded`；⑥任务 `succeeded` 且日志含镜像内探针内容（**不看 HTTP 200**）；**本门禁不通过则本分支不得发布** |
| **E2E-10** | **P0（本分支上传主入口）** | **上传通道开关一致性 / 一级回滚**（AC-16） | ①`[image].upload_enabled=false` → 走 API-01/API-05 ②`[image].enabled=false` → 再走一次 ③恢复后用同参数重试 | ①上传被拒、控制面正常；②上传与控制面**同时**被拒且共享盘/对象存储零新增；③恢复后可用；全程 workspace/env push 不受影响 |

---

## 6. 异常与故障注入矩阵

> **本分支**：FI-01~FI-08 为 P0 控制面 / 运行面故障注入；**FI-09~FI-12 属本分支上传主入口交付**（原「P1」口径），
> 随上传通道一并作为本分支发布门禁必跑。

| ID | 注入点 | 手法 | 期望行为 | 关联用例 |
| --- | --- | --- | --- | --- |
| FI-01 | tar 被删(R-6) | `loaded` 后 `rm /nfs-shared/hai-platform/image/demo.tar` | 运行期 initContainer link 失败、exit 非 0,pod 卡 Init 并打印**目标路径**;`images list` 状态**不自动回退**(仍 `loaded`),失败由运行面暴露 | TC-T05, TC-F07 |
| FI-02 | `/data_local` 不存在 | `multipass exec k8s-slave02 -- ls -ld /data_local` 确认不存在 → 提交任务 | 挂载失败 / 脚本显式报错,**可见且可诊断**;`mkdir -p /data_local` 后恢复;部署自检(OPS-04)须能提前发现 | TC-T06, TC-U12 |
| FI-03 | base image 引用不可达 | `[image].load_helper_image` 指向 `registry.high-flyer.cn/google_containers/busybox:latest` | `ImagePullBackOff`,pod events 可见;改回节点已有 `docker.io/library/busybox:latest` 即恢复(证明可配置) | TC-T08 |
| FI-04 | `registry.high-flyer.cn` 不可达 | `getent hosts registry.high-flyer.cn` → `198.18.0.77`;`curl` 探测失败 | `register` 后端下 load **仍成功**(不依赖 registry,AC-09);`registry` 后端下回报 `failed` + 可读 `message` | TC-A01, E2E-01 |
| FI-05 | 共享盘不可写 | `chmod 555 /nfs-shared/hai-platform/image` → load 已存在的 tar,再尝试放新 tar | 已存在 tar 仍可登记(只读元数据);新 tar 无法就位 → 运行期 link 失败可见;`image_self_check()` 对不可写根**告警** | TC-U12, TC-A05 |
| FI-06 | DB 唯一键冲突 | 绕过 upsert 直接 `insert` 同 `image_tar`;跨组同 tar 并发 | upsert 路径不产生重复行、不报 5xx;Q-2 改索引后跨组同 tar 各自成行 | TC-DB-02, TC-F02 |
| FI-07 | link 脚本非 0 退出 | 临时把脚本改为 `log ...; exit 7` 部署后提交任务 | initContainer exit 7、主容器**不启动**,`kubectl describe pod` 可见;不得静默放行;`image_link_failed_total{node=...}` +1 | TC-T05, TC-L04 |
| FI-08 | launcher 缓存陈旧(R-3) | `loaded` 后**不做任何刷新**立即提交任务 | 服务端返回「不存在镜像…或镜像仍在加载」;发同步信号 / 重启 launcher 后提交成功;该限制必须写入 Checklist 显式勾验 | TC-T07, E2E-01, TC-F05 |
| FI-09 | 上传中断（stage1 后 / stage2 中） | 上传到一半 kill 客户端；stage2 期间 kill 平台侧下载 | 失败态可见（阶段 + `index` + reason）；重试或 `--force` 后成功；**失败期间 `train_image` 不得新增 `loaded` 行** | TC-UP-09, FR-20 |
| FI-10 | RustFS 不可达 | 停掉 RustFS 容器（或改错 endpoint） | stage1 报错可见（客户端）；stage2 若已受理则记 `stage2_failed`；恢复后重试成功；不影响 `load`（手工放文件的 P0 路径） | TC-UP-03, TC-UP-09 |
| FI-11 | 共享盘只读/写满 | `chmod 555 {image_path}` 或塞满磁盘 | stage2 失败并打印目标路径与 errno；`load`（只读元数据）对已存在 tar 仍可用；`image_self_check()` 对不可写根**告警** | TC-UP-09, TC-U12 |
| FI-12 | `file_type` 枚举未迁移 | 在**未执行 036** 的库上走一次上传 | 明确报错（`invalid input value for enum file_type: "image"`）而不是静默丢状态；执行 036 后恢复 —— 用于验证迁移是硬前提（OPS-08 / HC-06） | TC-UP-04, TC-DB-09 |

---

## 7. 优先级与回归矩阵

### 7.1 集合定义

| 集合 | 内容 | 触发时机 |
| --- | --- | --- |
| **SMOKE** | TC-U01/U03/U07、TC-A01/A02/A04/A12/A16、TC-P01/P02、TC-C02/C03/C06、TC-T01/T02/T09、TC-DB-01 | 每次构建 |
| **SMOKE-UP** | 上述 + TC-UP-01/TC-UP-06/TC-UP-07 | 每次构建（上传通道启用后） |
| **REG(回归)** | SMOKE + U 组全部 + A 组全部 + P 组全部 + C 组全部 + DB 组全部 + S 组全部 | 每个 PR |
| **RELEASE** | REG + F 组 + O 组 + T 组 + L 组 + 全部 E2E + FI 矩阵 + **UP 组全部 + E2E-09/E2E-10 + FI-09~FI-12** + §7.2 的 workspace/env 回归 | 发布前 |

### 7.1.1 执行结果（2026-10-02）

| 集合 | 结果 |
| --- | --- |
| L1（镜像内） | `43 passed, 2 skipped`（新增 `test_image_push.py`；skipped = 客户端两条，需 host 上跑） |
| L1（host 客户端） | `tests/images/test_image_push_client.py` → `2 passed` |
| L2（控制面契约） | `smoke_images.sh` → `PASS=44 FAIL=0` |
| L3（E2E） | `e2e_images.sh`（`E2E_PURGE_IMAGE=1`）→ `PASS=26 FAIL=0`；**`e2e_images_push.sh` → PASS=33 WARN=1 FAIL=0** |
| 回归 | `smoke_ugc 8/8`、`e2e_workspace 19/19`、`smoke_env 20/20`、`e2e_env 16/16` |

> 完整命令、原始输出与缺陷记录见 [images-server-test-report.md](images-server-test-report.md) §6/§9。

### 7.2 workspace 与 env 回归(不可省)

本次改动**同时**落在三处跨特性共用代码与一处跨特性共用部署面:

- `conf/utils.py`(`FileType` / `get_base_path` 调用方)与 `cloud_storage/utils.py:get_base_path` —— **workspace / env 主链路共用**;
- `one/hai-up.sh` 的 `storage` 挂载种子 —— 影响**所有**任务的 initContainer 组装;
- `experiment_manager/manager/init_manager.py` —— 每个计算 pod 的 initContainer;
- 客户端 wheel —— `hai-cli` 全部子命令。

因此 RELEASE 集必须串跑既有回归,并额外核对「其它任务的 initContainer 不得因此多出或缺失」(I16 的修复只在 `train_environment.user_defined` 为真时生效):

```bash
bash docs/haiplatform/scripts/smoke_ugc.sh                          # 期望 PASS=8  FAIL=0
bash docs/haiplatform/scripts/e2e_workspace.sh all                  # 期望 PASS=19 FAIL=0
bash docs/haiplatform/scripts/smoke_env.sh http://10.205.52.200     # 期望 PASS=20 FAIL=0
bash docs/haiplatform/scripts/e2e_env.sh                            # 期望 PASS=16 FAIL=0
bash docs/haiplatform/scripts/smoke_images.sh http://10.205.52.200  # 本特性新增,期望 PASS≥12 FAIL=0
bash docs/haiplatform/scripts/e2e_images.sh                         # 本特性新增,期望 PASS=8  FAIL=0
bash docs/haiplatform/scripts/e2e_images_push.sh                    # 本分支上传主入口新增,期望 PASS≥10 FAIL=0（含开关一致性与 md5 对比）
```

> **并入状态**：本分支**已并入全部 images 脚本与 `tests/images/`**（`docs/haiplatform/scripts/smoke_images.sh` /
> `e2e_images.sh` / `e2e_images_push.sh` 与 §4.11 的 UP 组用例均已落地，其中上传通道脚本 2026-10-02 实测 `PASS=33 WARN=1 FAIL=0`）。
> §7.2 既有回归脚本的期望值不变：
> `smoke_ugc` **8/8**、`e2e_workspace` **19/19**、`smoke_env` **20/20**、`e2e_env` **16/16**。
>
> **口径**：本分支交付 = **S9（P0 资产并入与入口切换，1.0 人日）+ S8（上传通道，2.0 人日）**，
> 本分支新增合计 **3.0 人日**；P0 = **13.0 人日**（旧分支已完成）；特性总计 **16.0 人日**。

> **内建镜像任务回归**:用 `-i hai_base` 提交一条任务,`kubectl get pod -o json` 的 `initContainers` 列表必须与改造前一致(只有既有项),证明挂载种子与 initContainer 改动**零外溢**(OPS-02/TC-O06)。

---

## 8. 缺陷分级

| 级别 | 判定 | 示例 |
| --- | --- | --- |
| **致命(Critical)** | 数据损坏 / 越权 / 不可恢复 / 破坏既有链路 | 跨组删除成功;`..` 或符号链接绕过共享根并写入 DB;伪造 `task_id` 把 `failed` 改成 `loaded`;`one/hai-up.sh` 改动导致内建镜像任务起不来 |
| **严重(Major)** | 主链路不可用或与设计契约不符 | `images load/delete` 仍抛 `AttributeError`(AC-02);`user_images` 仍为空(AC-03);`path != image_tar` 导致 link 指向 tar 文件(I18);`link_hfai_image.sh` 缺失或 `not found`(AC-08);`updated_at DESC` 未生效导致状态基准取最旧(I7);`np.int64` 导致 list 500(I8) |
| **一般(Minor)** | 非主链路、有绕行 | `msg` 文案不准确;`-a` 帮助文本未更新;`message` 列未回显;指标缺一个标签;`images list -a` 的 `deleted` 行排序不理想 |
| **轻微(Trivial)** | 文案/体验/命名 | 错别字、日志级别、指标命名、自检提示措辞 |

**准出**:致命/严重 = **0**;一般 ≤ 2 且均有绕行方案;**E2E-01(AC-01)必须通过**且必须在**无内网 registry** 的条件下通过(AC-09,并记录于测试报告);**E2E-09(AC-15)为本分支第一发布门禁,必须通过**(本地 tar → RustFS → 共享盘 md5 一致 → `loaded` → 任务可区分输出);NFR-02 性能门槛达标;§7.2 的 workspace/env 回归全绿(AC-10);`FR-14`/`OPS-05`(P2 空间回收)按「本期只登记」处理,不计缺陷。

---

## 9. 需求追溯反向表

| 需求 | 用例 |
| --- | --- |
| FR-01（修 C-3:`async_load`/`async_delete`） | TC-A01, TC-C03, TC-C05, TC-O01 |
| FR-02（`user_images` 真实数据源） | TC-A12, TC-A15, TC-C02, TC-T09 |
| FR-03（API-15 加载登记 + 幂等 upsert） | TC-U02, TC-A01~A07, TC-P03 |
| FR-04（状态机落地与三方口径） | TC-U07, TC-U08, TC-A10, TC-DB-05, TC-F04, TC-C06 |
| FR-05（API-18 删除 + 组校验） | TC-U09, TC-A16~A18, TC-C05, TC-S04 |
| FR-06（幂等与重试安全） | TC-A06, TC-C09, TC-DB-02, TC-DB-03, TC-F01, TC-F03 |
| FR-07（概念单点化） | TC-U02~U04, TC-P01~P05, TC-T07 |
| FR-08（运行面脚本 + 挂载种子 + 可配置） | TC-T01, TC-T03, TC-T04, TC-T05, TC-T06, TC-T08 |
| FR-09（数据面执行 / 无 registry 后端） | E2E-01, TC-A01, TC-P04, FI-04 |
| FR-10（API-16 状态回报 + 防伪造） | TC-A08~A11, TC-S06, TC-T07 |
| FR-11（列表输出契约:6 字段 / DESC / 归一化） | TC-U10, TC-U11, TC-A12~A15, TC-C07, TC-DB-04 |
| FR-12（客户端失败提示，修 I10） | TC-C03, TC-C04, TC-C10, TC-F05,TC-C11(任务提交路径,范围外仅记录) |
| FR-13（路径单点与 `check_is_subpath`） | TC-U01, TC-U05, TC-A04, TC-O05, TC-P06 |
| FR-14（P2 空间回收,本期只登记） | 本期不实现:仅 TC-C01 的文案与 TC-C06 的 `-a` 语义保证「不误导」 |
| FR-15（任务侧 K1–K5 不变式） | TC-P02, TC-A12, TC-T02, TC-T09, TC-O01, TC-O06 |
| NFR-01（幂等） | TC-A06, TC-DB-03, TC-F01, TC-F02, TC-F03, TC-F06 |
| NFR-02（性能:200 行 P95 < 1s、无 N+1） | TC-DB-07, TC-F07 |
| NFR-03（可观测:结构化日志 + 4 指标） | TC-L01, TC-L02, TC-L03, TC-L04 |
| NFR-04（无 registry / 无 k8s 可单测） | 全 U 组(TC-U01~U12,L1) |
| NFR-05（兼容:字段只增不改名） | TC-A02, TC-A03, TC-O01, TC-O02 |
| NFR-06（资源可控,大 tar 不打爆服务端） | TC-F07 |
| SEC-01（路径越界防护） | TC-U05, TC-U06, TC-A04, TC-S01, TC-S02, TC-S03 |
| SEC-02（组隔离,只采信服务端解析） | TC-U09, TC-A15, TC-S04, TC-S05 |
| SEC-03（注入防护 / 名字白名单） | TC-U03, TC-S07 |
| SEC-04（鉴权 + `task_id` 归属） | TC-A09, TC-S06, TC-S08 |
| SEC-05（越权:禁跨组、组内可删他人） | TC-A17, TC-S04 |
| SEC-06（日志脱敏:不打印 token） | TC-S08, TC-L03 |
| SEC-07（最小权限 / 不接受用户可控命令） | TC-S07, TC-S09 |
| OPS-01（三级灰度 + 启动自检） | TC-U12, TC-O03, TC-O05, TC-P06 |
| OPS-02（一级回滚） | TC-O04, TC-O08, TC-F05 |
| OPS-03（迁移走 `db_schemas` 重放且幂等） | TC-DB-01 |
| OPS-04（节点 `/data_local` 前置纳入部署自检） | TC-U12, TC-T06, E2E-05 |
| OPS-05（空间回收审计,本期只登记） | 本期不实现:`images list -a` 可见 `deleted` 行(TC-C06) |
| CMP-01（旧客户端单参 `load` 可用） | TC-A02, TC-C08, TC-O01 |
| CMP-02（旧 `delete` 签名 / body 两形态） | TC-A03, TC-A16 |
| CMP-03（`train_environment` 与 `mars_images` 零改动） | TC-A12, TC-O06 |
| CMP-04（`registry` 默认值保留且不依赖可达） | TC-P02, FI-04 |
| CMP-05（`user_images` 字段名不得改名） | TC-A12, TC-A15, TC-DB-08, TC-O02 |
| CMP-06（三层 `custom.py` 覆盖接缝） | TC-O07 |
| HC-01（SQL 三条硬约束） | TC-DB-06 |
| HC-02（三段 URL 逐字节拼接不变） | TC-P02 |
| HC-03（`'loaded'` 字面量不可改） | TC-U08, TC-DB-05 |
| TC-DB-09（本分支：枚举 036 迁移幂等重放） | TC-UP-04, FI-12 |
| FR-16 / FR-18（本分支：`images push` + 自动登记） | TC-UP-04, TC-UP-06, TC-UP-11, E2E-09 |
| FR-17（本分支：复用 API-01/05/06 + key 布局） | TC-UP-01, TC-UP-02, TC-UP-03 |
| FR-19（本分支：数据面开关同源） | TC-UP-07, TC-UP-08, E2E-10 |
| FR-20（本分支：失败可见与可重试） | TC-UP-03, TC-UP-09, TC-UP-10, FI-09~FI-11 |
| NFR-07..10（本分支：大文件/不阻塞/可观测/可测试） | TC-UP-01, TC-UP-03, TC-UP-04, TC-UP-09 |
| SEC-08..10（本分支：授权前缀/落点/凭据不进任务） | TC-UP-02, TC-UP-05, TC-UP-12 |
| OPS-06..09（本分支：上传开关/上限/枚举迁移/排障） | TC-UP-08, TC-UP-10, FI-12 |
| CMP-07..09（本分支：零改动/旧路径可用/不新建表） | TC-UP-12, §7.2 |
| HC-11..14（本分支：三处白名单/开关同源/落点同源/节点无凭据） | TC-UP-01, TC-UP-05, TC-UP-07, TC-UP-12 |
| HC-04（客户端子串 `deleted` 过滤） | TC-U08, TC-C06, TC-C07, TC-DB-05 |
| HC-05（`image` 不含 `/`,恰好 3 段） | TC-U03, TC-U04, TC-P02 |
| HC-06（DDL 幂等并经重放路径生效） | TC-DB-01 |
| HC-07（新能力落在 `ugc-server` 宿主内） | TC-O08(3 条路由同宿主、不新增服务与端口) |
| HC-08（脚本随镜像构建进入任务 pod） | TC-T01 |
| HC-09（`a_find_user_group_image_urls` 签名与语义不变） | TC-P02, TC-O06 |
| HC-10（服务器 pod 不得操作节点容器运行时） | TC-S09, TC-T01 |

---

## 10. 与需求文档 §6 验收标准的对应

| 验收 | 本文件判定位置 |
| --- | --- |
| AC-01 端到端（load → loaded → 任务成功并产出预期输出） | §5 E2E-01 + §4.9 TC-T02（**发布门禁**） |
| AC-02 C-3 闭环（不再抛 `AttributeError`） | TC-C03, TC-C04, TC-C05, TC-A01, TC-A16;任务提交路径的裸异常由 TC-C11 **记录为范围外** |
| AC-03 列表可见（响应与 DB 一致,且服务端自查指引成立） | TC-A12, TC-A13, TC-C02, TC-T09, E2E-03 |
| AC-04 状态机合法 + `-a` 可见性 | TC-U07, TC-U08, TC-A10, TC-A16, TC-C06, TC-DB-05, E2E-04 |
| AC-05 幂等（3 次 load 仍 1 行、重复 delete 返回 0） | TC-A06, TC-F01, TC-F03, TC-DB-03, E2E-02 |
| AC-06 路径安全（越界全部被拒且不产生 DB 行） | TC-U05, TC-U06, TC-A04, TC-S01~S03, E2E-07 |
| AC-07 组隔离（跨组删/看不见） | TC-U09, TC-A15, TC-A17, TC-S04, E2E-07 |
| AC-08 运行面可用（脚本存在 / 被挂载 / 被 pod 成功执行；`/data_local` 纳入自检） | TC-T01, TC-T03, TC-T05, TC-T06, TC-T08, E2E-05 |
| AC-09 无 registry 也能验 | §5 E2E-01 的环境说明 + FI-04 + TC-A01 + TC-P03 |
| AC-10 零回归（workspace / env / `train_environment` 全绿） | §7.2 脚本矩阵 + TC-O06 + TC-O01 + E2E-08 |
| AC-11 可观测（按 `image_tar` 串联 + 4 指标有数据） | TC-L01~L04 |
| AC-12 迁移可重放（重复执行不报错、列只加一次） | TC-DB-01 |
| AC-13 兼容（旧单参 `load`、旧 `list` 字段消费零改动） | TC-A02, TC-O01, TC-O02, TC-C08, E2E-08 |
| AC-14 文档一致（I1–I18 均有处置或显式不处置理由） | §9 追溯表 + Checklist `ACC-*` 阶段 |
| AC-15（本分支）上传闭环（本地 tar → 共享盘 md5 一致 → `loaded` → 任务成功） | §5 E2E-09 + TC-UP-04（**本分支第一发布门禁**） |
| AC-16（本分支）开关一致性（上传与控制面同时失败关闭、零新增写入） | §5 E2E-10 + TC-UP-07, TC-UP-08 |
| AC-17（本分支）幂等与续传（不重复上传、不产生重复行、中断可恢复） | TC-UP-06, TC-UP-09 + FI-09 |
| AC-18（本分支）上传通道零回归（workspace/env push/pull 不变） | §7.2 脚本矩阵 + TC-UP-12 |
