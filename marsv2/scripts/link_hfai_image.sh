#!/bin/sh
# ============================================================================
# marsv2/scripts/link_hfai_image.sh
#
# 计算 pod 的 initContainer（load-image）执行：把用户自定义镜像的 tar 导入**本节点**容器运行时，
# 使主容器能用 HFAI_IMAGE 指定的三段镜像名启动。
#
# 背景（分析报告 I16）：`experiment_manager/manager/init_manager.py` 一直引用本脚本，
# 但仓库里从来没有它 —— 结果是 pod 卡在 Init（`not found`），自定义镜像永远跑不起来。
#
# 契约（设计 docs/haiplatform/images/images-server-design.md §5.4）：
#   · 入参只来自**环境变量**，不接受任何命令行参数：
#       HFAI_IMAGE             三段镜像 URL：{registry}/{shared_group}/{image}
#       HFAI_IMAGE_WEKA_PATH   镜像 tar 在共享盘上的绝对路径（= train_image.path）
#       HFAI_CONTAINERD_SOCK   （可选）containerd socket，默认 /run/containerd/containerd.sock
#       HFAI_CONTAINERD_NS     （可选）containerd namespace，默认 k8s.io
#       HFAI_DATA_LOCAL_PATH   （可选）宿主 /data_local 路径，默认 /data_local
#   · 退出码 0 = 镜像已可用（**含「已存在，无需动作」**）；非 0 = 失败，pod 卡 Init 并暴露日志
#   · 幂等：重复执行不报错、不重复导入（initContainer 重试的前提）
#   · 失败可见：打印目标路径与命令输出，不静默跳过
#   · 安全：只读 env，路径与镜像名做白名单/前缀断言（SEC-03 / SEC-07）
# ============================================================================
set -eu

log() { echo "[link_hfai_image] $*"; }

if [ "$#" -ne 0 ]; then
    log "FAILED: 本脚本不接受任何命令行参数（收到 $# 个）"
    exit 2
fi

: "${HFAI_IMAGE:?环境变量 HFAI_IMAGE 未设置}"
: "${HFAI_IMAGE_WEKA_PATH:?环境变量 HFAI_IMAGE_WEKA_PATH 未设置}"

# 镜像名白名单（SEC-03）：只允许三段 URL 需要的字符，杜绝任何 shell 注入面
case "${HFAI_IMAGE}" in
    *[!A-Za-z0-9._:/@-]*) log "FAILED: HFAI_IMAGE 含非法字符: ${HFAI_IMAGE}"; exit 1 ;;
esac
case "${HFAI_IMAGE}" in
    */*/*/*) log "FAILED: HFAI_IMAGE 必须是三段 URL: ${HFAI_IMAGE}"; exit 1 ;;
    */*/*) ;;
    *) log "FAILED: HFAI_IMAGE 必须是三段 URL（registry/shared_group/image）: ${HFAI_IMAGE}"; exit 1 ;;
esac

# tar 路径：必须是绝对路径且不含 ..
case "${HFAI_IMAGE_WEKA_PATH}" in
    /*) ;;
    *) log "FAILED: HFAI_IMAGE_WEKA_PATH 必须是绝对路径: ${HFAI_IMAGE_WEKA_PATH}"; exit 1 ;;
esac
case "${HFAI_IMAGE_WEKA_PATH}" in
    *..*) log "FAILED: HFAI_IMAGE_WEKA_PATH 不允许包含 ..: ${HFAI_IMAGE_WEKA_PATH}"; exit 1 ;;
esac

SOCK="${HFAI_CONTAINERD_SOCK:-/run/containerd/containerd.sock}"
NS="${HFAI_CONTAINERD_NS:-k8s.io}"
DATA_LOCAL="${HFAI_DATA_LOCAL_PATH:-/data_local}"

if [ -d "${DATA_LOCAL}" ]; then
    log "data_local 就绪: ${DATA_LOCAL}"
else
    log "WARN: ${DATA_LOCAL} 不存在（宿主目录未创建？见 OPS-04 / I17①）"
    case "${HFAI_IMAGE_WEKA_PATH}" in
        "${DATA_LOCAL}"/*) log "FAILED: 镜像路径位于 ${DATA_LOCAL} 之下，但该目录不存在"; exit 1 ;;
    esac
fi

if [ ! -e "${HFAI_IMAGE_WEKA_PATH}" ]; then
    log "FAILED: 镜像资产不存在: ${HFAI_IMAGE_WEKA_PATH}"
    exit 1
fi
if [ ! -f "${HFAI_IMAGE_WEKA_PATH}" ]; then
    log "FAILED: 镜像资产不是普通文件: ${HFAI_IMAGE_WEKA_PATH}"
    exit 1
fi
SIZE="$(wc -c < "${HFAI_IMAGE_WEKA_PATH}" 2>/dev/null || echo 0)"
log "镜像资产: ${HFAI_IMAGE_WEKA_PATH} ($((SIZE / 1048576)) MiB)"

CTR=""
if command -v ctr >/dev/null 2>&1; then
    CTR="$(command -v ctr)"
elif [ -x /host-bin/ctr ]; then
    CTR="/host-bin/ctr"
fi

if [ -n "${CTR}" ]; then
    if [ ! -S "${SOCK}" ]; then
        log "FAILED: containerd socket 不可用: ${SOCK}"
        log "提示：需要把节点运行时 socket 与 ctr 挂进 initContainer（[image].containerd_socket / runtime_bin_dir，R-2）"
        exit 1
    fi
    # ① 快速幂等短路：运行时已存在该镜像则直接成功
    if "${CTR}" --address "${SOCK}" -n "${NS}" images ls -q 2>/dev/null | grep -qx "${HFAI_IMAGE}"; then
        log "已存在，跳过: ${HFAI_IMAGE}"
        exit 0
    fi
    log "导入: ${HFAI_IMAGE_WEKA_PATH} -> ${HFAI_IMAGE} (namespace=${NS})"
    IMPORT_OUT="$("${CTR}" --address "${SOCK}" -n "${NS}" images import "${HFAI_IMAGE_WEKA_PATH}" 2>&1)" || {
        log "FAILED: ctr images import 失败"
        echo "${IMPORT_OUT}"
        exit 1
    }
    echo "${IMPORT_OUT}"
    if "${CTR}" --address "${SOCK}" -n "${NS}" images ls -q 2>/dev/null | grep -qx "${HFAI_IMAGE}"; then
        log "OK: ${HFAI_IMAGE}"
        exit 0
    fi
    # ② tar 内没带目标 tag 时，用导入出来的名字补一个 tag
    IMPORTED="$(echo "${IMPORT_OUT}" | awk '/^unpacking /{print $2}' | tail -n 1)"
    if [ -n "${IMPORTED}" ]; then
        log "补 tag: ${IMPORTED} -> ${HFAI_IMAGE}"
        if "${CTR}" --address "${SOCK}" -n "${NS}" images tag "${IMPORTED}" "${HFAI_IMAGE}" >/dev/null 2>&1; then
            log "OK: ${HFAI_IMAGE}"
            exit 0
        fi
        log "FAILED: ctr images tag 失败"
    fi
    log "FAILED: 导入后仍未找到镜像 ${HFAI_IMAGE}"
    "${CTR}" --address "${SOCK}" -n "${NS}" images ls -q 2>/dev/null | tail -n 10
    exit 1
fi

if command -v docker >/dev/null 2>&1; then
    log "导入(docker): ${HFAI_IMAGE_WEKA_PATH}"
    DOCKER_OUT="$(docker load -i "${HFAI_IMAGE_WEKA_PATH}" 2>&1)" || {
        log "FAILED: docker load 失败"
        echo "${DOCKER_OUT}"
        exit 1
    }
    echo "${DOCKER_OUT}"
    log "OK(docker): ${HFAI_IMAGE}"
    exit 0
fi

log "FAILED: 容器运行时不可用（既无 ctr 也无 docker）"
log "提示：需要把节点运行时 socket 挂入 initContainer，或改用自带 ctr 的基础镜像（R-2 / Q-7）"
exit 1
