# Terraform configuration to deploy Hai Platform onto an EXISTING Kubernetes
# (MicroK8s) cluster using the hai-up / hai-cli utilities.
#
# This mirrors the logic of src/microk8s_up_hai_platfrom.sh but adapts it to
# our environment:
#   * The MicroK8s cluster already exists (4 nodes: k8s-master + 3 slaves)
#     running inside Multipass VMs on host 103.
#   * hai-up / hai-cli are already installed on host 103 (/usr/local/bin).
#   * The host 103 CAN reach the master apiserver directly at
#     https://10.205.52.154:16443 (the VM bridge subnet is only unreachable
#     from YOUR Mac, not from host 103).
#   * loads the correctly-scoped kubeconfig into /root/.kube/config
#     (replacing the stale foreign config that points to apiserver.cluster.local).
#   * Deploys MetalLB + an IPAddressPool so the hai-platform-svc LoadBalancer
#     gets a real address, then runs `hai-up up`.
#
# Terraform itself runs on host 103 (like terraform-k8s-ha), so the `null`
# provider's local-exec provisioners execute there via the create.sh driver,
# which stages this file and runs `terraform apply` over SSH.

terraform {
  required_version = ">= 1.5"
  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

locals {
  ns_shared   = "/nfs-shared"
  hai_dir     = "${local.ns_shared}/hai-platform"
  config_path = "${local.hai_dir}/config.sh"
}

# ---------------------------------------------------------------------------
# Deployment knobs (override via -var=... or terraform.tfvars)
# ---------------------------------------------------------------------------

variable "host" {
  type        = string
  default     = "fireflyer@192.168.100.103"
  description = "SSH target of the host that runs hai-up and reaches the cluster."
}

variable "apiserver" {
  type        = string
  default     = "https://10.205.52.154:16443"
  description = "API server base URL reachable from host 103 (no trailing slash)."
}

variable "admin_token" {
  type        = string
  description = "Bearer token (admin, cluster-admin) used by the kubeconfig. Provide via tfvars so it is not hard-coded."
}

variable "cluster_ca_b64" {
  type        = string
  description = "Base64 certificate-authority-data for the cluster. Provide via tfvars."
}

variable "task_namespace" {
  type    = string
  default = "hai-platform"
}

variable "shared_fs_root" {
  type    = string
  default = "/nfs-shared"
}

variable "mars_prefix" {
  type    = string
  default = "hai"
}

variable "training_group" {
  type    = string
  default = "training"
}

variable "jupyter_group" {
  type    = string
  default = "jupyter_cpu"
}

# NOTE: these are K8s NODE NAMES (not hostnames). Label `${mars_prefix}_mars_group`
# is set per node by hai-up to route workloads. Map to your real node names.
variable "training_nodes" {
  type        = string
  default     = "k8s-slave01 k8s-slave03"
  description = "Training compute nodes (space-separated K8s node names)."
}

variable "jupyter_nodes" {
  type        = string
  default     = "k8s-slave02"
  description = "Jupyter compute nodes (space-separated, must differ from training)."
}

variable "manager_nodes" {
  type        = string
  default     = "k8s-master"
  description = "Nodes running the task manager service."
}

variable "ingress_host" {
  type        = string
  default     = "hai-platform-svc.hai-platform.svc.cluster.local"
  description = "Ingress hostname serving studio/jupyter (no http prefix)."
}

variable "user_info" {
  type    = string
  default = "haiadmin:10020:123456"
}

variable "root_user" {
  type    = string
  default = "haiadmin"
}

variable "bff_admin_uid" {
  type    = number
  default = 10000
}

variable "base_image" {
  type        = string
  default     = "registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:7589fb1"
  description = "All-in-one hai-platform image (platform services + task worker)."
}

variable "train_image" {
  type        = string
  default     = "registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:7589fb1"
  description = "Image used for training/jupyter task pods."
}

variable "node_gpus" {
  type        = number
  default     = 0
  description = "GPUs per node (0 for this CPU-only cluster)."
}

variable "has_rdma_hca_resource" {
  type    = number
  default = 0
}

variable "postgres_user" {
  type    = string
  default = "root"
}

variable "postgres_password" {
  type    = string
  default = "root"
}

variable "redis_password" {
  type    = string
  default = "root"
}

variable "ingress_class" {
  type    = string
  default = "nginx"
}

variable "hai_server_addr" {
  type        = string
  default     = ""
  description = "For docker-compose provider; unused for k8s."
}

# MetalLB IP pool. It lives on the VMs' bridge subnet so it is reachable from
# host 103's services / ingress. Adjust to a free range on your VM subnet.
variable "metallb_ip_range" {
  type        = string
  default     = "10.205.52.200-10.205.52.210"
  description = "MetalLB LoadBalancer address pool (must be on the reachable bridge subnet)."
}

# The LoadBalancer IP that hai-platform-svc ends up with. It is the address the
# frontend (hai-studio) and the BFF proxy live on behind haproxy: the studio/UI
# is served on :8080 and the BFF `/proxy/s` endpoint is handled there too. The
# on-host nginx reverse proxies below forward the cluster-internal ingress
# hostname and a convenience port to this upstream.
variable "studio_lb_ip" {
  type        = string
  default     = "10.205.52.200"
  description = "LoadBalancer IP hosting hai-studio (frontend + BFF) on port 8080."
}

# Port on the studio LoadBalancer that serves the SPA so the BFF `/proxy/s`
# (login forward) and the static front-end are reachable from the host.
variable "studio_port" {
  type    = number
  default = 8080
}

# ---------------------------------------------------------------------------
# 1) Prepare a workable kubeconfig on host 103 (replace the stale foreign one).
# ---------------------------------------------------------------------------
resource "null_resource" "kubeconfig" {
  triggers = {
    apiserver = var.apiserver
    token     = var.admin_token
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      sudo mkdir -p /root/.kube
      TMP=$(mktemp)
      cat > "$TMP" <<'KUBE'
apiVersion: v1
kind: Config
clusters:
- name: microk8s-cluster
  cluster:
    server: ${var.apiserver}
    certificate-authority-data: ${var.cluster_ca_b64}
contexts:
- name: microk8s
  context:
    cluster: microk8s-cluster
    user: admin
current-context: microk8s
users:
- name: admin
  user:
    token: ${var.admin_token}
KUBE
      sudo install -o root -g root -m 0600 "$TMP" /root/.kube/config
      rm -f "$TMP"
      echo "==> kubeconfig installed (validates below)"
      sudo kubectl get nodes -o name
    EOT
  }
}

# ---------------------------------------------------------------------------
# 2) Deploy MetalLB + IPAddressPool (LoadBalancer support for hai-platform-svc).
# ---------------------------------------------------------------------------
resource "null_resource" "metallb" {
  triggers = {
    ip_range = var.metallb_ip_range
  }

  depends_on = [null_resource.kubeconfig]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      if ! sudo kubectl get ns metallb-system >/dev/null 2>&1; then
        echo "==> installing MetalLB ..."
        sudo kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.12/config/manifests/metallb-native.yaml
        # the bundled memberlist secret uses a fixed starter key; rotate it.
        sudo kubectl create secret generic -n metallb-system memberlist \
          --from-literal=secretkey="$(openssl rand -base64 128)" --dry-run=client -o yaml | sudo kubectl apply -f - 2>/dev/null || true
      else
        echo "==> metallb-system already present"
      fi
      sudo kubectl -n metallb-system rollout status deployment/controller --timeout=180s
    EOT
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      cat <<'POOL' | sudo kubectl apply -f -
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: hai-platform-pool
  namespace: metallb-system
spec:
  addresses:
  - ${var.metallb_ip_range}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: hai-platform-l2
  namespace: metallb-system
spec:
  ipAddressPools:
  - hai-platform-pool
POOL
      echo "==> MetalLB pool ready:"
      sudo kubectl get ipaddresspools.metallb.io -n metallb-system
    EOT
  }
}

# ---------------------------------------------------------------------------
# 3) Write the hai-platform config script, then run `hai-up up`.
# ---------------------------------------------------------------------------
resource "null_resource" "hai_up" {
  triggers = {
    config_body = sha256(join("", [
      var.task_namespace, var.training_nodes, var.jupyter_nodes,
      var.manager_nodes, var.ingress_host, var.user_info, var.base_image,
    ]))
  }

  depends_on = [null_resource.kubeconfig, null_resource.metallb]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      sudo mkdir -p ${local.hai_dir}
      cat > /tmp/hai_config_terraform.sh <<'CONF'
# generated by terraform-hai-platform
export TASK_NAMESPACE="${var.task_namespace}"
export SHARED_FS_ROOT="${var.shared_fs_root}"
export MARS_PREFIX="${var.mars_prefix}"
export TRAINING_GROUP="${var.training_group}"
export JUPYTER_GROUP="${var.jupyter_group}"
export TRAINING_NODES="${var.training_nodes}"
export JUPYTER_NODES="${var.jupyter_nodes}"
export MANAGER_NODES="${var.manager_nodes}"
export INGRESS_HOST="${var.ingress_host}"
export USER_INFO="${var.user_info}"
export ROOT_USER="${var.root_user}"
export BFF_ADMIN_UID=${var.bff_admin_uid}
export BFF_ADMIN_TOKEN=$(echo $RANDOM | md5sum | head -c 20)
export BASE_IMAGE="${var.base_image}"
export TRAIN_IMAGE="${var.train_image}"
export NODE_GPUS=${var.node_gpus}
export HAS_RDMA_HCA_RESOURCE=${var.has_rdma_hca_resource}
export INGRESS_CLASS="${var.ingress_class}"
export PLATFORM_NAMESPACE="${var.task_namespace}"
export POSTGRES_USER="${var.postgres_user}"
export POSTGRES_PASSWORD="${var.postgres_password}"
export REDIS_PASSWORD="${var.redis_password}"
export DB_PATH="${var.shared_fs_root}/hai-platform/db"
CONF
      sudo install -o root -g root -m 0640 /tmp/hai_config_terraform.sh ${local.config_path}

      echo "==> cleaning old db (mirrors the reference script) ..."
      # The glob MUST be expanded by a ROOT shell. `sudo rm -rf dir/*` expands
      # `*` as the login user (fireflyer) first; the postgres data dir is 0700
      # owned by uid 107 so fireflyer cannot list it, the glob stays literal and
      # the command degenerates to `rm -rf 'dir/*'` -- a silent no-op. The old
      # database then survives, and because the image's init_postgresql.sh skips
      # schema initialisation entirely once `task_ng`/`user` exist, newly added
      # db_schemas/*.sql migrations are never applied (symptom: missing columns
      # like host.flags, and k8swatcher crash-looping on UndefinedColumn).
      sudo sh -c 'rm -rf ${var.shared_fs_root}/hai-platform/db/*'
      # Do NOT chmod the db tree world-writable: postgres refuses to start
      # unless its data directory is 0700/0750 ("has invalid permissions").
      # also clear stale redis AOF/RDB so a previous owner's dump does not
      # trip the redis uid (106) or keep stale users.
      sudo sh -c 'rm -rf ${var.shared_fs_root}/hai-platform/redis/*'
      sudo chmod -R a+rwX ${var.shared_fs_root}/hai-platform/redis 2>/dev/null || true

      echo "==> running hai-up up ..."
      sudo hai-up up -c ${local.config_path}

      # -------------------------------------------------------------------
      # Post-deploy hardening so a re-run does not hit the bugs we fixed:
      #
      # (a) By default hai-up writes every data directory on the shared FS
      #     (db/, redis/, log/, workspace/, ...). Many of those are NFS-backed
      #     hostPath mounts inside the pod that run with a *foreign* uid
      #     (e.g. redis-server drops to uid 106, postgres to uid 111), while
      #     the files may be owned by whatever uid created them first.
      #     The symptom was redis failing to open its logfile with
      #     "Can't open the log file: Permission denied", which in turn broke
      #     /query/node/list with a 500. Make the whole tree world-writable so
      #     every service role can write its log / data regardless of uid.
      # -------------------------------------------------------------------
      echo "==> hardening shared-FS permissions (a+rwX) ..."
      sudo chmod -R a+rwX ${local.hai_dir}

      # -------------------------------------------------------------------
      # (a2) Undo the blanket chmod on the postgres data directory. The a+rwX
      #      above makes db/ 0777, and postgres then refuses to boot with:
      #        FATAL: data directory "/var/lib/postgresql/12/main"
      #               has invalid permissions
      #        DETAIL: Permissions should be u=rwx (0700) or u=rwx,g=rx (0750).
      #      which crash-loops the platform pod. Redis still needs the wide
      #      perms, postgres explicitly must not have them.
      # -------------------------------------------------------------------
      echo "==> restoring postgres data-dir mode (0700) ..."
      sudo sh -c 'test -d ${local.hai_dir}/db && chmod 700 ${local.hai_dir}/db && echo "  db/ -> $(stat -c %a ${local.hai_dir}/db)"'

      # -------------------------------------------------------------------
      # (b) Ensure redis-server is actually running inside the pod. hai-up
      #     starts it via `service redis-server start` in the entrypoint, but
      #     if that failed on boot (e.g. the permission issue above), redis is
      #     down and the query server's /query/node/list 500s. Detect that and
      #     (re)start it in place.
      # -------------------------------------------------------------------
      echo "==> verifying redis inside pod ..."
      POD="hai-platform-0"
      for i in $(seq 1 30); do
        if sudo kubectl -n ${var.task_namespace} get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Running; then
          break
        fi
        echo "  waiting for $POD to run (attempt $i) ..."; sleep 10
      done
      if sudo kubectl -n ${var.task_namespace} exec "$POD" -- \
           sh -c 'redis-cli -a "$${REDIS_PASSWORD:-root}" ping' >/dev/null 2>&1; then
        echo "  redis: OK"
      else
        echo "  redis: NOT running -> starting ..."
        sudo kubectl -n ${var.task_namespace} exec "$POD" -- sh -c \
          'service redis-server start; sleep 2; redis-cli -a "$${REDIS_PASSWORD:-root}" ping' \
          >/dev/null 2>&1 || echo "  WARN: could not (re)start redis; check logs"
      fi

      # -------------------------------------------------------------------
      # (c) Force imagePullPolicy to IfNotPresent. The platform image bakes
      #     `image_pull_policy = 'Always'` into one/one_etc/core.toml, and the
      #     config loader merges core.toml -> scheduler.toml -> extension.toml
      #     -> override.toml, so writing it into override.toml wins.
      #     Without this every task pod re-resolves the tag against the
      #     registry on each start; if the registry is unreachable the pod sits
      #     in ImagePullBackOff even though the image is already cached on the
      #     node. Idempotent, so re-running apply never appends duplicates.
      # -------------------------------------------------------------------
      echo "==> patching override.toml (pull policy + task namespaces) ..."
      OVR="${local.hai_dir}/override.toml"
      if sudo test -f "$${OVR}"; then
        sudo python3 - "$${OVR}" <<'PYEOF'
import sys

path = sys.argv[1]

# Both of these MUST live in override.toml: proj_conf.py merges
# core.toml -> scheduler.toml -> extension.toml -> override.toml, so anything
# written here wins over the values baked into the image.
#
#   launcher/manager.image_pull_policy
#       core.toml ships 'Always'; with a tag reference every task pod then
#       re-resolves against the registry on each start and sits in
#       ImagePullBackOff when the registry is unreachable, even though the
#       image is already cached on the node.
#
#   launcher.task_namespaces_by_role.{internal,external}
#       core.toml ships the vendor placeholder 'poly-hpp'. k8s_watcher.py does
#           namespaces = list(CONF.launcher.task_namespaces_by_role.values())
#           default_namespace = CONF.launcher.task_namespaces_by_role['internal']
#       so with the placeholder the watcher watches a namespace that does not
#       exist: it registers no nodes, and leader election fails with
#       `namespaces "poly-hpp" not found`.
PULL = "image_pull_policy = 'IfNotPresent'"

with open(path, encoding="utf-8") as f:
    lines = f.read().splitlines()

changed = False

if not any(PULL in ln for ln in lines):
    out, done = [], False
    for ln in lines:
        out.append(ln)
        if not done and ln.strip() == "[launcher]":
            out.append(PULL)
            done = True
    if not done:
        out.append("[launcher]")
        out.append(PULL)
    out += ["", "[manager]", PULL]
    lines = out
    changed = True

if not any("task_namespaces_by_role" in ln for ln in lines):
    lines += [
        "",
        "[launcher.task_namespaces_by_role]",
        "internal = 'hai-platform'",
        "external = 'hai-platform'",
    ]
    changed = True

if not changed:
    print("  already set")
    sys.exit(0)

with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print("  patched %s" % path)
PYEOF
      else
        echo "  WARN: $${OVR} not found; skipping override hardening"
      fi

      echo "==> overriding task_namespaces_by_role for the watcher ..."
      echo "==> restarting launcher/k8swatcher to pick up override.toml ..."
      sudo kubectl -n ${var.task_namespace} exec hai-platform-0 -- \
        supervisorctl restart launcher k8swatcher 2>&1 | tail -4 || \
        echo "  WARN: could not restart launcher/k8swatcher; check the pod"

      echo "==> checking /query/node/list ..."
      echo "==> hai-up finished"
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      set +e
      echo "==> tearing down hai platform (down) ..."
      F="/nfs-shared/hai-platform/config.sh"
      test -f "$F" && sudo hai-up down -c "$F" || echo "no config; skipping down"
    EOT
  }
}

# ---------------------------------------------------------------------------
# 4) Verify deployment: pods, service, then hai-cli login / status.
# ---------------------------------------------------------------------------
resource "null_resource" "verify" {
  triggers = {
    run = "always-${null_resource.hai_up.id}"
  }

  depends_on = [null_resource.hai_up]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      NAMESPACE=${var.task_namespace}
      echo "===== pods ====="
      sudo kubectl get pods -n "$NAMESPACE" -o wide
      echo "===== services ====="
      sudo kubectl get svc -n "$NAMESPACE"
      echo "===== ingress ====="
      sudo kubectl get ingress -n "$NAMESPACE" 2>/dev/null || true

      echo "===== wait for hai-platform-0 ready ====="
      for i in $(seq 1 30); do
        if [ "$(sudo kubectl -n "$NAMESPACE" get pod hai-platform-0 -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ]; then
          echo "hai-platform-0 is Running"; break
        fi
        echo "  waiting (attempt $i)..."; sleep 10
      done

      LB=$(sudo kubectl -n "$NAMESPACE" get svc hai-platform-svc -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
      echo "hai-platform-svc LB IP: $${LB:-<none>}"
      if [ -n "$LB" ]; then
        echo "==> login & status"
        sudo hai-cli init 123456 --url "http://$LB" 2>&1 || echo "hai-cli init failed (non-fatal)"

        # -------------------------------------------------------------------
        # hai-cli init normalises the URL to a *trailing-slash* form
        # (http://10.205.52.200/) in ~/.hfai/conf.yml. Every downstream call is
        # built as "{url}/query/...", so this produces a double slash
        # (http://10.205.52.200//query/user/info) which fails to match haproxy's
        # `path_beg /query/` acl -> 503 "No server is available". Normalise the
        # stored URL to remove the trailing slash so whoami / nodes work.
        # hai-cli init above runs as root, so also normalise /root/.hfai.
        # -------------------------------------------------------------------
        for CFG in /root/.hfai/conf.yml "$HOME/.hfai/conf.yml"; do
          if sudo test -f "$CFG"; then
            sudo sed -i -E "s#^([[:space:]]*url:[[:space:]]*).*#\1http://$LB#" "$CFG" 2>/dev/null || true
            echo "==> conf.yml url normalised in $CFG"
          fi
        done

        echo "==> whoami"
        sudo hai-cli whoami 2>&1 || true
        echo "==> nodes"
        sudo hai-cli nodes 2>&1 || true
      fi
    EOT
  }
}

# ---------------------------------------------------------------------------
# 5) Reverse-proxy the studio through nginx on host 103.
#
# The hai-studio frontend injects window.haiConfig with:
#     bffURL / wsURL / jupyterURL = http://${var.ingress_host}   (cluster-internal DNS)
#     clusterServerURL            = http://${var.studio_lb_ip}    (bridge subnet, unreachable from the Mac)
# A browser on the Mac can neither resolve `...cluster.local` nor route to the
# 10.205.52.x bridge subnet, so its login call to `/proxy/s` (BFF) fails. The fix
# is two-fold:
#   * CLIENT (Mac, documented in README, NOT managed here): map
#     ${var.ingress_host} -> 192.168.100.103 in /etc/hosts and add the cluster
#     hostnames + bridge subnet to the HTTP proxy Bypass list so the request
#     reaches host 103 directly instead of your local proxy (7890 ...).
#   * HOST 103 (this resource): nginx listens on :80 for that hostname and on a
#     convenience port, forwarding to the studio LoadBalancer so the BFF can
#     actually serve the login / proxy calls.
# ---------------------------------------------------------------------------
resource "null_resource" "nginx_proxy" {
  triggers = {
    ingress_host = var.ingress_host
    upstream     = "http://${var.studio_lb_ip}:${var.studio_port}"
    studio_port  = var.studio_port
  }

  depends_on = [null_resource.verify]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      # target upstream that serves hai-studio (frontend + BFF /proxy/s)
      UPSTREAM="http://${var.studio_lb_ip}:${var.studio_port}"
      HOSTNAME="${var.ingress_host}"

      write_conf() {
        local path="$1" body="$2"
        if [ ! -f "$path" ] || ! sudo grep -qF "$UPSTREAM" "$path" 2>/dev/null; then
          echo "==> writing $path"
          printf '%s\n' "$body" | sudo tee "$path" > /dev/null
        else
          echo "==> $path already present with same upstream; skipping"
        fi
      }

      # (a) Port 80, keyed to the cluster-internal ingress hostname. This is what
      #     the browser hits after the Mac hosts/>=cluster.local local DNS remap.
      write_conf /etc/nginx/conf.d/hai-studio-80.conf "server {
    listen 80;
    listen [::]:80;
    server_name $HOSTNAME;

    location / {
        proxy_pass $UPSTREAM;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \"upgrade\";
        proxy_read_timeout 600s;
        proxy_send_timeout 600s;
    }
}
"

      # (b) Port 8090, catch-all convenience entry for direct host access
      #     (http://192.168.100.103:8090/).
      write_conf /etc/nginx/conf.d/hai-studio-proxy.conf "server {
    listen 8090;
    listen [::]:8090;
    server_name _;

    location / {
        proxy_pass $UPSTREAM;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \"upgrade\";
        proxy_read_timeout 600s;
    }
}
"

      echo "==> validating & reloading nginx ..."
      sudo nginx -t 2>&1 | grep -E "syntax is ok|successful" || { echo "nginx -t FAILED"; exit 1; }
      sudo nginx -s reload 2>&1 || true

      echo "==> nginx listen check:"
      sudo ss -ltn 2>/dev/null | grep -E ':(80|8090)\b' || true
      echo "==> upstream probe:"
      curl -s -o /dev/null -w "upstream:%%{http_code}\n" --max-time 6 "$UPSTREAM/" || true
    EOT
  }
}

# ---------------------------------------------------------------------------
# 6) Task test: submit a Monte-Carlo PI computation and STRICTLY validate it.
#
# This is the terraform mirror of src/submit_hfai_pi_cluster.sh, hardened for
# this environment and given a strict pass/fail check:
#   * hai-cli is invoked as `fireflyer` (its ~/.hfai/conf.yml exists), never as
#     root (root has no conf -> "request cluster service" connect errors).
#   * Before submitting, the hai-platform worker image is ensured to exist on
#     EVERY training node. The worker pod mounts that 5.3 GiB image and is
#     killed by the scheduler if the pull (ImagePullPolicy: Always) does not
#     finish within its startup window — so a slow cold per-task pull on the
#     2-CPU slaves makes every task end `failed_terminating`. Preloading the
#     image once per node (idempotent) lets the worker start in seconds.
#   * The task is monitored until it reaches a terminal state; then the log is
#     parsed for `PI_RESULT <value>` and validated against pi within a strict
#     tolerance. Any deviation => `terraform apply` fails loudly.
# ---------------------------------------------------------------------------
resource "null_resource" "pi_task_test" {
  triggers = {
    # bump this to force a fresh test run on the next apply
    task_test = "1.0"
  }

  depends_on = [null_resource.verify]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      NAMESPACE=${var.task_namespace}
      LB=$(sudo kubectl -n "$NAMESPACE" get svc hai-platform-svc -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
      LB="$${LB:-${var.studio_lb_ip}}"
      TRAIN_NODES="${var.training_nodes}"
      BASE_IMAGE="${var.base_image}"

      # ------------------------------------------------------------------
      # 6a) Preload the worker image on every training node (idempotent).
      # ------------------------------------------------------------------
      echo "==> ensuring worker image on training nodes"
      for NODE in $TRAIN_NODES; do
        NODE_IP=$(sudo kubectl get node "$NODE" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)
        [ -n "$NODE_IP" ] || { echo "ERROR: no IP for node $NODE"; exit 1; }
        SSH="sudo ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -i /var/snap/multipass/common/data/multipassd/ssh-keys/id_rsa ubuntu@$NODE_IP"
        HAS=$($SSH "sudo microk8s.ctr images ls 2>/dev/null | grep -cF '$${BASE_IMAGE##*/}' || true" 2>/dev/null || true)
        if [ "$${HAS:-0}" -ge 1 ]; then
          echo "  image present on $NODE ($NODE_IP)"
        else
          echo "  pulling $${BASE_IMAGE} on $NODE ($NODE_IP) ..."
          $SSH "sudo microk8s.ctr images pull '$BASE_IMAGE' >/tmp/preload_pi.log 2>&1" \
            || { echo "ERROR: preload pull failed on $NODE"; exit 1; }
          echo "  image loaded on $NODE"
        fi
      done

      # ------------------------------------------------------------------
      # 6b) Write the PI script into the haiadmin workspace (hostPath the
      #     worker mounts) and submit with hai-cli as fireflyer.
      # ------------------------------------------------------------------
      TASK_DIR="/nfs-shared/hai-platform/workspace/haiadmin/jupyter/notebooks/pi_task_terraform_$(date +%s)"
      sudo mkdir -p "$TASK_DIR"
      sudo tee "$TASK_DIR/hfai_pi_calculation.py" >/dev/null <<'PYEOF'
import math, os, sys, time
import numpy as np

# NOTE: the worker image ships numpy but NOT torch. Keep this script numpy-only,
# otherwise the task dies with ModuleNotFoundError and `terraform apply` fails.
TOTAL = int(os.environ.get("PI_TOTAL_SAMPLES", "500000000"))
CHUNK = int(os.environ.get("PI_CHUNK", "5000000"))
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pi_output.txt")

rank = int(os.environ.get("RANK", "0"))
lines = []

def emit(s):
    print(s, flush=True)
    lines.append(s)

emit("PI_INFO rank=%d python=%s numpy=%s total=%d chunk=%d" % (
    rank, ".".join(map(str, sys.version_info[:3])), np.__version__, TOTAL, CHUNK))

inside = 0
done = 0
while done < TOTAL:
    n = min(CHUNK, TOTAL - done)
    x = np.random.random(n)
    y = np.random.random(n)
    inside += int(np.count_nonzero(x * x + y * y <= 1.0))
    done += n

pi = 4.0 * inside / TOTAL
err = abs(pi - math.pi)

if rank == 0:
    emit("PI_SAMPLES %d" % TOTAL)
    emit("PI_RESULT %.10f" % pi)
    emit("PI_ERROR %.10f" % err)
    emit("TASK_RUNNER:EXIT_OK" if err < 0.0005 else "TASK_RUNNER:EXIT_ERR")
emit("PI_DONE")

# The worker pod is deleted the moment the task ends, so persist the verdict on
# the shared workspace instead of racing against log retention.
if rank == 0:
    try:
        with open(OUT, "w") as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        print("PI_INFO could not write %s: %s" % (OUT, e), flush=True)
PYEOF
      sudo chmod -R a+rwX "$TASK_DIR"

      # login as fireflyer (conf.yml lives in its home), normalise trailing-slash
      sudo -u fireflyer hai-cli init 123456 --url "http://$LB" >/dev/null 2>&1 || true
      sudo -u fireflyer hai-cli whoami >/dev/null 2>&1 || true

      echo "==> submitting PI task as fireflyer"
      OUT=$(sudo -u fireflyer hai-cli python "$TASK_DIR/hfai_pi_calculation.py" \
              -- --nodes 1 -g ${var.training_group} --name "pi_test_terraform" -f 2>&1)
      echo "$OUT"
      # Parse the task id from the submission table's first data row:
      #     | 1  | pi_test_t… | 1 | ...
      # Do NOT use a bare `grep -oE '[0-9]{4,}'` here: the first 4+ digit number
      # in the output is the timestamp embedded in TASK_DIR, and task ids are
      # short (they restart at 1 on a fresh database), so that regex silently
      # picks the timestamp and the poll below watches a task that never exists.
      TASK_ID=$(echo "$OUT" | sed -nE 's/^\|[[:space:]]*([0-9]+)[[:space:]]*\|.*/\1/p' | head -1)
      if [ -z "$TASK_ID" ]; then
        echo "ERROR: could not parse task id from submission output"; exit 1
      fi
      echo "==> submitted task id: $TASK_ID"

      # ------------------------------------------------------------------
      # 6c) Strictly wait for the task to finish, then validate PI.
      # ------------------------------------------------------------------
      # Poll the JSON status. `hai-cli status -j` returns:
      #     {"chain_status": "...", "_pods_": [{"status": "...", "exit_code": ...}]}
      # There is no "state" key at all, so the old `grep '"state"'` always came
      # back empty and the loop spun all 120 rounds before failing even when the
      # task had already succeeded. Also note chain_status is only
      # running|finished|failed|stopped -- "finished" is reported for FAILED
      # chains too, so the per-pod status is the authoritative success signal.
      RESULT=""
      JOB=""
      for i in $(seq 1 120); do
        S=$(sudo -u fireflyer hai-cli status "$TASK_ID" -j 2>/dev/null | tr -d '\n' || true)
        CS=$(echo "$S" | grep -oE '"chain_status": *"[^"]*"' | head -1 | sed -E 's/.*"([^"]*)"$/\1/')
        JS=$(echo "$S" | grep -oE '"status": *"[^"]*"'       | head -1 | sed -E 's/.*"([^"]*)"$/\1/')
        echo "  ($${i}/120) task $TASK_ID chain=$CS job=$JS"
        case "$JS" in
          succeeded|failed|stopped) JOB="$JS"; RESULT="$JS"; break ;;
        esac
        case "$CS" in
          finished|failed|stopped) RESULT="$CS"; break ;;
        esac
        sleep 10
      done
      [ -n "$RESULT" ] || { echo "ERROR: task did not reach a terminal state in time"; exit 1; }
      echo "==> task $TASK_ID terminal: chain=$RESULT job=$${JOB:-<none>}"

      echo "==> fetching log for $TASK_ID"
      LOG=$(sudo -u fireflyer hai-cli logs "$TASK_ID" 2>/dev/null || true)
      echo "$LOG" | tail -40

      PI=$(echo "$LOG" | grep -m1 -oE 'PI_RESULT [0-9.]+' | awk '{print $2}')
      CODE=$(echo "$LOG" | grep -m1 -oE 'TASK_RUNNER:EXIT_(OK|ERR)' || true)

      # Fallback: the script also persists its verdict next to itself on the
      # shared workspace, which survives pod deletion and log retention.
      if [ -z "$PI" ] && sudo test -f "$TASK_DIR/pi_output.txt"; then
        echo "==> log carried no PI_RESULT; reading $TASK_DIR/pi_output.txt"
        FILE_OUT=$(sudo cat "$TASK_DIR/pi_output.txt" 2>/dev/null || true)
        echo "$FILE_OUT" | tail -20
        PI=$(echo "$FILE_OUT" | grep -m1 -oE 'PI_RESULT [0-9.]+' | awk '{print $2}')
        CODE=$(echo "$FILE_OUT" | grep -m1 -oE 'TASK_RUNNER:EXIT_(OK|ERR)' || true)
      fi

      # compute |pi - PIREF| using python3 (present on host 103 / VM image)
      ERR=""
      if [ -n "$PI" ]; then
        ERR=$(python3 -c "print(abs(float('$PI')-3.141592653589793))" 2>/dev/null || true)
      fi
      if [ "$JOB" = "succeeded" ] && [ -n "$PI" ] && [ "$CODE" = "TASK_RUNNER:EXIT_OK" ]; then
        OK=$(python3 -c "ok=float('$ERR')<0.0005; print('OK' if ok else 'FAIL')" 2>/dev/null || echo FAIL)
        echo "==> PI RESULT: pi=$PI error=$ERR ($OK)"
        if [ "$OK" = "OK" ]; then
          echo "==> TASK TEST PASSED"
        else
          echo "==> TASK TEST FAILED (pi error out of tolerance)"
          exit 1
        fi
      else
        echo "==> TASK TEST FAILED (chain=$RESULT job=$${JOB:-<none>} pi=$${PI:-<none>} code=$${CODE:-<none>})"
        exit 1
      fi
    EOT
  }
}