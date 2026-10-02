'''
注意（ADR-11）：这里**不能** `from .api import *`。

`cloud_storage.api` 是一个 FastAPI 宿主（会在 `api.app` 上注册无前缀路由并挂
startup 钩子）。ugc-server 复用同一领域层时也会 import `cloud_storage.service.*`，
如果包初始化就把 `.api` 拉进来，就会出现：
  ① ugc-server 意外注册 9 条无前缀路由（JWT 鉴权，语义不同）
  ② 重复注册 on_event('startup') —— 崩溃恢复被执行两次

独立部署仍可通过 `uvicorn_server.py` 的 `cloud_storage.api:app` 加载。
'''

from .auth import *
