
'''
hai-cli images（用户自定义镜像）指标 —— 设计 docs/haiplatform/images/images-server-design.md §9.4。

范式与 `cloud_storage/metrics.py` 一致：模块级 Counter/Gauge/Histogram，
作为 import 副作用注册到默认 registry（`/metrics` 上可直接抓到）。
'''

from prometheus_client import Counter, Gauge, Histogram


#: 加载请求计数；result=ok/fail，fail 时 code 为业务错误码（FEATURE_DISABLED / PATH_ESCAPE / ...）
image_load_total = Counter(
    "image_load_total",
    "hai-cli images load requests.",
    labelnames=("result", "code",)
)

#: 加载耗时；按数据面后端区分（register / task / registry）
image_load_duration_seconds = Histogram(
    "image_load_duration_seconds",
    "hai-cli images load duration.",
    labelnames=("backend",)
)

#: 列表返回行数（按组），用于观察积压与 cardinality
image_list_rows = Gauge(
    "image_list_rows",
    "Rows returned by train_image list API.",
    labelnames=("shared_group",)
)

#: link 脚本/manager 侧上报的导入失败次数（节点维度）
image_link_failed_total = Counter(
    "image_link_failed_total",
    "hai-cli images link failures on compute nodes.",
    labelnames=("node",)
)

#: 状态回报计数；result=ok/fail，fail 时 code 为业务错误码
image_update_status_total = Counter(
    "image_update_status_total",
    "hai-cli images update_status requests.",
    labelnames=("result", "code",)
)
