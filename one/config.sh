    export TASK_NAMESPACE="poly-gwj" # task namespace
    export SHARED_FS_ROOT="/nfs-shared" # shared filesystem root path
    export MARS_PREFIX="hpp-one"
    export TRAINING_GROUP="training" # training compute nodes label will be set to ${MARS_PREFIX}_mars_group=${TRAINING_GROUP}
    export JUPYTER_GROUP="jd_shared_cpu" # jupyter compute nodes label will be set to ${MARS_PREFIX}_mars_group=${JUPYTER_GROUP}
    export TRAINING_NODES="jd-a1007-dl" # training compute nodes list, format: "node1 node2"
    export JUPYTER_NODES="jd-a1006-dl" # jupyter compute nodes list, format: "node1 node2", JUPYTER_NODES should differ from TRAINING_NODES
    export MANAGER_NODES="jd-a1007-dl" # service nodes which running task manager, format: "node1 node2"
    export INGRESS_HOST="nginx-ingress-lb.kube-system.cn-hangzhou.alicontainer.com" # ingress hostname serving studio, jupyter，no http prefix needed
    export USER_INFO="root:10020:xxxxxxxx" # platform user info, format: "user1:uid1:passwd,user2:uid2:passwd"
    export ROOT_USER="root" # username of the root user, must exist in $USER_INFO

    # optional, if not set, following default value will be used
    export BFF_ADMIN_UID=10000 # uid of the reserved admin user for bff
    export BFF_ADMIN_TOKEN=$(echo $RANDOM | md5sum | head -c 20)    # token of the reserved admin user for bff
    export BASE_IMAGE="registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest" # all in one image
    export NODE_GPUS=4 # gpus per node
    export HAS_RDMA_HCA_RESOURCE=0 # ib per node
    export KUBECONFIG="$HOME/.kube/config" # kubeconfig file path
    export POSTGRES_USER="root" # postgres username
    export POSTGRES_PASSWORD="root" # postgres password
    export REDIS_PASSWORD="root" # redis password
    export DB_PATH="${SHARED_FS_ROOT}/hai-platform/db" # postgres database path
    export EXTRA_MOUNTS="src1:dst1:file:ro,src2:dst2:directory" # add extra mounts to hai-platform
    export EXTRA_ENVIRONMENTS="k1:v1,k2:v2" # add extra environments to hai-platform

    # for k8s provider, optional, if not set, following default value will be used
    export INGRESS_CLASS="nginx" # ingress class
    export PLATFORM_NAMESPACE="${TASK_NAMESPACE}" # platform namespace

    # for docker-compose provider
    export HAI_SERVER_ADDR="47.98.195.232" # current server address
