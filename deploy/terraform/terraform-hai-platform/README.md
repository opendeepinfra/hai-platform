# terraform-hai-platform

把 **Hai Platform** 部署到**已经存在**的 MicroK8s 集群上（host `103`，4 节点
`k8s-master` + `k8s-slave01..03`）。逻辑参考
`src/microk8s_up_hai_platfrom.sh`，但针对本环境做了适配。

## 与参考脚本的区别

| 项 | 参考脚本 (microk8s_up_hai_platfrom.sh) | 本目录 (terraform-hai-platform) |
|----|------------------------------------------|--------------------------------|
| 前置 | 假定命令在主机本地、集群与主机同网 | 集群跑在 Multipass VM 里；从 host 103 直接连 apiserver `https://10.205.52.154:16443` |
| kubeconfig | `sudo microk8s config > /root/.kube/config` | 用导出的正确 kubeconfig（apiserver + admin token）**覆盖主机的陈旧错误配置** |
| LoadBalancer | 手工 `kubectl apply` 一个 IPAddressPool（IP 是占位符） | Terraform 部署 **MetalLB + IPAddressPool**（IP 落在 VM 网桥可达网段） |
| 节点名 | `fireflyer-0001/2/3`（本机网卡主机名） | 映射到真实 K8s 节点名 `k8s-master/slave01..03` |
| hai-up | `sudo hai-up up -c config.sh` | 同样，但 config.sh 由 Terraform 生成 |

## 布局

| 文件 | 用途 |
|------|------|
| `main.tf` | Terraform 配置：kubeconfig → MetalLB → 生成 config.sh → `hai-up up` → 校验 |
| `create.sh` | 本地驱动：装 Terraform、staging、`terraform init + apply` |
| `destroy.sh` | 本地驱动：`terraform destroy`（触发 `hai-up down`） |
| `verify.sh` | 只读健康检查：节点 / pods / svc / ingress / hai-cli whoami |
| `.gitignore` | 排除 terraform 缓存与 tfvars |

## 工作方式

Terraform 跑在 host `103`（和 `terraform-k8s-ha` 同一模式），所以
`local-exec` provisioner 直接在 103 上调用 `kubectl` / `hai-up`。host 103 能
直连 VM 网桥子网（只有你的 **Mac** 不能直连），因此这条路无需隧道。

主要步骤：

1. `null_resource.kubeconfig` — 把 `admin_token` + `cluster_ca_b64` 写成
   `/root/.kube/config`（apiserver `https://10.205.52.154:16443`），并校验
   `kubectl get nodes`。覆盖主机上的陈旧配置。
2. `null_resource.metallb` — 安装 MetalLB v0.13.12，建 `IPAddressPool` +
   `L2Advertisement`（默认 `10.205.52.200-10.205.52.210`，可改）。
3. `null_resource.hai_up` — 写 `/nfs-shared/hai-platform/config.sh`（节点分组、
   镜像、DB、账号等），清空旧 db 与 redis 数据，执行 `sudo hai-up up -c ...`，
   随后**加固权限**（`chmod -R a+rwX`）并**确认 pod 内 redis 已启动**。
4. `null_resource.verify` — 打印 pods/svc/ingress，等 `hai-platform-0`
   Running，做 `hai-cli init + whoami + nodes`，并**修正 conf.yml 的 URL 尾斜杠**。
5. `null_resource.nginx_proxy` — 在 host 103 nginx 上写 studio 反代（见下文
   「从浏览器访问 studio」），把入口域名注入 `ingress_host`。
6. `null_resource.pi_task_test` — **任务测试模块**：先在每个 training 节点
   **预拉 worker 镜像**，再以 `fireflyer` 提交一个 Monte-Carlo 求 π 任务，
   **严格等待任务跑完并校验 π 值**，不满足容差就让 `terraform apply` 报错。

## 已加固修复的问题（重跑不再复现）

下面是实际运维中踩过、已固化进 `main.tf` 的两个坑，`terraform apply` 时自动处理：

1. **Redis 起不来 → `/query/node/list` 500**
   `hai-platform` pod 把 `/var/lib/redis` 与 `/var/log/redis` 都挂在共享盘
   (`/nfs-shared/hai-platform/*`) 上。`hai-up up` 生成的这些目录所有权可能落在
   任意 uid，而 pod 内 `redis-server` 会降权到 uid 106，打不开归属为其它 uid、
   权限 640 的日志文件，报 `Can't open the log file: Permission denied`，Redis
   起不来，进而让 `hai-cli nodes` 调 `/query/node/list` 返回 500。
   **修复**：`hai_up` 在 `hai-up up` 后对整个 `hai-platform` 目录 `chmod -R
   a+rwX`（让每个服务角色都能写日志/数据），再检测 pod 内 redis 是否在跑
   （`redis-cli ping`），没跑就 `service redis-server start`。
2. **`hai-cli whoami`/`nodes` 503（URL 双斜杠）**
   `hai-cli init --url http://<LB>` 会把 URL 归一化成**带尾斜杠**写进
   `~/.hfai/conf.yml`（如 `http://10.205.52.200/`）。`hai-cli` 下游把 URL 拼成
   `{url}/query/...`，于是变成 `http://10.205.52.200//query/user/info`
   **双斜杠**，不匹配 haproxy 的 `path_beg /query/` 规则 → 返回 503
   "No server is available"。
   **修复**：`verify` 在 `hai-cli init` 后用 `sed` 把 conf.yml 里的 `url`
   改成**无尾斜杠**（对 `/root/.hfai` 和 `$HOME/.hfai` 都处理），再跑
   `whoami`/`nodes` 验证。

> 说明：这些是**多租户共享文件系统上的 uid 归属**问题，与是否用 NFS 无关
> （sshfs 时代同样会出现）。固定为 `a+rwX` 是最省事的方案；若在意权限收紧，
> 可改为把 db/redis/log 各自 `chown` 到对应的 uid（postgres 111 / redis 106）。

## 任务测试模块 `null_resource.pi_task_test`

参考 `src/submit_hfai_pi_cluster.sh`，把「提交一个求 π 任务并校验」固化进
terraform（第 6 步）。`terraform apply` 结束时它会给出 PASS/FAIL。

要点（都是本环境踩过的坑，已固化）：

1. **Worker 镜像必须在每个 training 节点先就位**（`var.base_image`，
   `opendeepinfra/hai-platform:7589fb1`，约 1.0 GiB）。worker pod 的
   `ImagePullPolicy` 会在每任务启动时重新拉镜像；2-CPU 的 slave 从
   镜像仓库冷拉镜像往往超过调度器给 worker 的启动窗口 → 任务被判成
   `failed_terminating`（无论抽多少任务都一样）。模块会在提交前对
   `var.training_nodes` 逐个 `microk8s.ctr images pull`（幂等，已存在就跳过）。
2. **以 `fireflyer` 运行 hai-cli，勿用 root**。参考脚本用 `sudo /bin/bash -s`
   导致 hai-cli 以 root 跑，而 `/root/.hfai/conf.yml` 不存在 → 报
   `请求集群服务出现连接错误`。这里固定 `sudo -u fireflyer hai-cli`。
3. **严格校验**：轮询 `hai-cli status <id> -j` 直到进入终态，再取
   `hai-cli logs <id>`，grep `PI_RESULT <value>` + `TASK_RUNNER:EXIT_OK`，
   用 python3 计算 `|pi − π| < 0.0005`。任一步失败即 `exit 1` 让 apply 失败。

脚本本体是 `haiadmin` 工作区下的 `hfai_pi_calculation.py`（Monte-Carlo 求
π，torch gloo 后端，CPU 集群也能跑）。改任务名/节点数：
`-- --nodes 1 -g ${training_group} --name pi_test_terraform -f`。

想每次 apply 都重跑一遍测试，把 `triggers.task_test` 的值改一下（默认
`1.0`）即可。

## 从浏览器访问 studio（反代 + 客户端配置）

`hai-studio` 前端把 `window.haiConfig` 注入成两个**浏览器够不到**的地址：
`bffURL/wsURL/jupyterURL = http://<ingress_host>`（cluster 内部 DNS，形如
`hai-platform-svc.hai-platform.svc.cluster.local`）、`clusterServerURL =
http://10.205.52.200`（VM 网桥子网，只有 host 103 可达）。因此登录时前端调
`/proxy/s?...` 会失败（Mac 上 `ERR_EMPTY_RESPONSE` 或 Network Error）。要打通
需要**两边**配合：

### ① Host 103（terraform 已固化为 `null_resource.nginx_proxy`）

部署后自动在 `192.168.100.103` 的 nginx 上写两组反代（幂等，`nginx -t` 通过才
reload），把请求转到 studio 的 LoadBalancer（`http://${var.studio_lb_ip}:${var.studio_port}`，
默认 `10.205.52.200:8080`）：

| 配置文件 | 监听 | server_name | 用途 |
|----------|------|-------------|------|
| `hai-studio-80.conf` | `80` | `${var.ingress_host}` | 承接浏览器按域名访问（配合客户端 hosts） |
| `hai-studio-proxy.conf` | `8090` | `_` | 便捷直连 `http://192.168.100.103:8090/` |

### ② 客户端（Mac）— 手工配置，**不属于** terraform

步骤 1：把集群内部域名解析到 host 103（`/etc/hosts`）：

```bash
sudo tee -a /etc/hosts <<'EOF'
192.168.100.103 hai-platform-svc.hai-platform.svc.cluster.local
EOF
sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder
```

步骤 2：**把这几个地址加进 HTTP/HTTPS/SOCKS 代理的例外**（否则会被本地
Clash/代理劫持成 `ERR_EMPTY_RESPONSE`）。在 Mac「系统设置 → 网络 → 详细信息 →
代理 → 忽略这些主机与域的代理设置」，或：

```bash
# 每个启用的网络服务都要加（此处以 Wi-Fi 为例）
for s in "Wi-Fi" "USB 10/100 LAN"; do
  networksetup -setproxybypassdomains "$s" \
    192.168.0.0/16 10.0.0.0/8 172.16.0.0/12 127.0.0.1 localhost \
    "*.local" "*.cluster.local" \
    "hai-platform-svc.hai-platform.svc.cluster.local" 192.168.100.103
done
```

步骤 3：访问（走 80，不带端口）：`http://hai-platform-svc.hai-platform.svc.cluster.local/`，
用 `haiadmin` / `123456` 登录。若登录后个别数据接口报错，多半是
`clusterServerURL`（`10.205.52.200`，Mac 到不了）或代理仍劫持，把
`10.205.52.200` 一并加入代理例外。

## 用法

```bash
# 1) 准备 terraform.tfvars（凭据不写进代码）
cat > terraform.tfvars <<'EOF'
admin_token     = "<你的 cluster-admin bearer token>"
cluster_ca_b64  = "<从 k8s-master-kubeconfig.yaml 的 certificate-authority-data 复制>"
EOF
#    （或者用环境变量 TF_ADMIN_TOKEN / TF_CLUSTER_CA_B64，二选一）

# 2) 部署
./create.sh

# 3) 查看状态
./verify.sh

# 4) 销毁
./destroy.sh
```

覆盖更多参数示例：

```bash
./create.sh   # 用默认值
# 改节点分组 / IP 池：
terraform -chdir=/opt/terraform/hai-platform apply -auto-approve \
  -var-file=terraform.tfvars \
  -var='training_nodes=k8s-slave01 k8s-slave03' \
  -var='jupyter_nodes=k8s-slave02' \
  -var='manager_nodes=k8s-master' \
  -var='metallb_ip_range=10.205.52.200-10.205.52.210'
```

## 备注 / 限制

- **每个 training 节点至少 ~30G 磁盘**（推荐值，非硬性）。当前 worker 镜像
  `opendeepinfra/hai-platform:7589fb1` 约 1.0 GiB（12 层），containerd
  下载+解包约需 3~4G；镜像已经从旧的 `hfai/hai-platform:latest-202207`
  （5.3 GiB / 24 层）大幅瘦身，磁盘压力显著降低。历史问题：slave 出厂只有
  9.6G 时曾导致任务 `no space left on device` 而失败（根因见上文任务测试模块；
  已用 `multipass set local.k8s-slaveXX.disk=30G` 扩容，需节点短暂停止）。

- **Metallb IP 池必须落在 VM 网桥子网（10.205.52.x）的可达网段**。变量里改。
- `admin_token` 是 cluster-admin，`terraform.tfvars` 已在 `.gitignore` 忽略，
  不要提交到仓库；也不要泄露。
- `hai-cli init` 用固定密码 `123456`（参考脚本做法），部署后可自行 `hai-cli
  users` / 修改。
- 从你的 **Mac** 访问 studio / jupyter 仍需要 SSH 隧道到 host 103（Mac 不能
  直连 VM 子网）。见仓库根目录 `连接K8s集群-OpenLens说明.md` 的隧道做法。
- 本目录只负责在已有集群上部署/卸载 Hai Platform，**不会**重建
  MicroK8s 或 VM（那是 `terraform-k8s-ha` 的职责）。

## 依赖

- 本地: `ssh`, `scp`, `bash`
- host 103: 免密 `sudo`，`hai-up` / `hai-cli`（先跑 `install_hai_cli.sh` /
  `install_hai_cli_direct.sh`），可直连 VM 子网