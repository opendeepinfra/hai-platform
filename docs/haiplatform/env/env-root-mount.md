# env root 必须挂进任务 pod（否则 `source haienv` 必失败）

> 适用：`hai-cli env`（haienv）功能在**任务侧**的使用。属于部署前置条件，不是客户端行为。

## 现象

任务 yml 里已经写好 `options.py_venv: <env>`，服务端也确实在执行 `source haienv <env>`
（`server_model/task_impl/single_task_impl.py:151`），但任务日志是：

```
File "/home/<user>/.haienv/find_haienv_89.py", line 9, in <module>
  for user in sorted(os.listdir(haienv_root)):
FileNotFoundError: [Errno 2] No such file or directory: '<env_path>/hfai_envs'
no valid env found: env=<name> owner=<user> HAIENV_PATH=<env_path>/hfai_envs/<user>
```

随后 entrypoint 用镜像自带的 python 运行 → `ModuleNotFoundError: No module named 'torch'`
（服务端把 `source haienv` 失败降级为 warning，不改变任务成败语义，所以表现为「跑了但环境没生效」）。

## 原因

任务容器只挂载两类路径：

1. 从 `spec.workspace` 推出的 runtime mount（`server_model/task_impl/runtime_mounts/default.py`）；
2. `mars_db.storage` 表里的挂载行（`server_model/user_impl/user_storage/implement.py:personal_storage`）。

而 `HAIENV_PATH` 由服务端设为 `get_user_env_dir(user) = {env_path}/hfai_envs/<user>`
（`single_task_impl.py:61-62`、`conf/utils.py`），即 **所有用户共享的 env root 的下一级** ——
它既不在 workspace 之下，默认也不在 `storage` 表里。于是容器里这个目录不存在。

> 注：`{env_path}` 来自 `override.toml` 的 `[cloud.storage.service] env_path`；
> 单节点部署里实际数据在 `<共享盘>/hai-single/hai-platform/workspace/...`，
> 但服务端会对配置路径做 realpath，因此 `storage` 行要写**数据真正落盘的那棵树**（见下）。

## 修复

往该部署自己的 Postgres（`mars_db.storage`）里加一行 Directory 挂载，幂等：

```sql
insert into "storage"("host_path","mount_path","owners","conditions","mount_type","read_only","action","active")
select '<env_path>/hfai_envs','<env_path>/hfai_envs','{public}','{}','Directory',false,'add',true
where not exists (select 1 from "storage"
                  where "host_path"='<env_path>/hfai_envs' and "mount_type"='Directory');
```

两种落地方式：

* 手动/联调：`KUBECONFIG=<本集群> bash docs/haiplatform/scripts/mount_env_root.sh <env_root>`
  （脚本支持 `--check`，并会先打印当前 context/节点，避免在多集群主机上打错目标）；
* 部署期：把 `deploy/terraform/terraform-hai-platform-single-node/files/mount_env_root.sql`
  放进部署初始化（`04-hai-up.sh` 已有 `psql -d mars_db -c …` 的钩子，加一行 `-f` 即可），
  这样重建部署后不需要人工补。

改完不需要重启平台：`storage` 行是按任务读取的（本仓库实测：插入后新任务立即生效）。
若某部署缓存了该表，重启一次 `statefulset/hai-platform` 即可。

## 验证

1. 起一个任务，看 pod 的 `MOUNT_LIST`（服务端用 `node_schema.mounts` 生成）里是否出现该路径：

```bash
kubectl -n hai-platform exec <task-pod> -- sh -c 'echo "$MOUNT_LIST" | tr "," "\n" | grep hfai_envs'
```

2. 容器内 `ls <env_root>/<user>` 能看到已 push 的环境目录；
3. 任务日志出现 `found [<name>] from [<user>] in [<env_root>/<user>/<name>_0], start loading...`
   与 `user haienv [<name>] loaded`；
4. 让 entrypoint 打印 `sys.executable` / `torch.__file__`，确认确实来自 env（而不是镜像或共享盘 pylibs）。

## 相关

* `docs/haiplatform/scripts/mount_env_root.sh`（联调脚本，幂等）
* `deploy/terraform/terraform-hai-platform-single-node/files/mount_env_root.sql`（部署期 SQL）
* issue #5 的更正评论（该脚本早期固定 `sudo kubectl`，在多集群主机上会写进另一套集群）
