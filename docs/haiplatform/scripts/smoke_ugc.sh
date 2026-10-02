#!/bin/bash
# /ugc/* 接口冒烟（对应 Checklist 附录 B，用真实 token）
# 用法: bash smoke_ugc.sh [base_url]
set -u
BASE="${1:-http://10.205.52.200}"
TOKEN=$(sudo grep -E '^token:' /home/fireflyer/.hfai/conf.yml | awk '{print $2}')
NAME="${NAME:-smoke_ws}"
FT="file_type=FileType.WORKSPACE"     # 老客户端形态（同时验证兼容层）
PASS=0; FAIL=0
LOG=/tmp/smoke_ugc.log
: > "$LOG"

log() { echo "$*" | tee -a "$LOG"; }
chk() { # chk <描述> <python断言表达式> <文件>
  local desc="$1" expr="$2" file="$3"
  if python3 -c "
import json,sys
d=json.load(open('$file'))
assert $expr, d
print('ok')
" >>"$LOG" 2>&1; then
    log "PASS | $desc"; PASS=$((PASS+1))
  else
    log "FAIL | $desc"; sed -n '$p' "$LOG"; FAIL=$((FAIL+1))
  fi
}

log "=== smoke against $BASE  $(date +%T) ==="

log "--- 1) get_sync_status（无记录应为 success=1 + data:[]）"
curl -s -X POST "$BASE/ugc/get_sync_status?token=$TOKEN&$FT&name=$NAME" -o /tmp/ws1.json -w 'http=%{http_code}\n' | tee -a "$LOG"
cat /tmp/ws1.json >> "$LOG"; echo >> "$LOG"
chk "get_sync_status 含 success 与 data" "d['success']==1 and 'data' in d" /tmp/ws1.json

log "--- 2) set_sync_status（枚举串形态）"
curl -s -X POST "$BASE/ugc/set_sync_status?token=$TOKEN&$FT&name=$NAME&direction=SyncDirection.PUSH&status=SyncStatus.INIT&local_path=/tmp/$NAME&cluster_path=" -o /tmp/ws2.json -w 'http=%{http_code}\n' | tee -a "$LOG"
cat /tmp/ws2.json >> "$LOG"; echo >> "$LOG"
chk "set_sync_status success=1" "d['success']==1" /tmp/ws2.json

log "--- 3) get_sync_status（应出现 1 条记录，7 个字段）"
curl -s -X POST "$BASE/ugc/get_sync_status?token=$TOKEN&$FT&name=$NAME" -o /tmp/ws3.json
cat /tmp/ws3.json >> "$LOG"; echo >> "$LOG"
chk "记录已写入且恰好 7 键" "d['success']==1 and len(d['data'])==1 and set(d['data'][0])=={'name','local_path','cluster_path','push_status','last_push','pull_status','last_pull'}" /tmp/ws3.json

log "--- 4) get_sts_token（应含 s3/oss 五键）"
curl -s -X POST "$BASE/ugc/get_sts_token?token=$TOKEN&$FT&name=$NAME&ttl_seconds=1800" -o /tmp/ws4.json
cat /tmp/ws4.json >> "$LOG"; echo >> "$LOG"
chk "sts 返回五键" "d['success']==1 and any({'endpoint','access_key_id','access_key_secret','security_token','bucket'} <= set(v) for k,v in d.items() if isinstance(v,dict))" /tmp/ws4.json

log "--- 5) cluster_files/list（text/plain + file_list 外壳）"
curl -s -X POST "$BASE/ugc/cloud/cluster_files/list?token=$TOKEN&$FT&name=$NAME&no_checksum=false&no_hfignore=false&recursive=True&page=1&size=100" \
  -H 'Content-Type: text/plain; charset=utf-8' \
  --data '{"file_list": {"files": ["./"]}}' -o /tmp/ws5.json
cat /tmp/ws5.json >> "$LOG"; echo >> "$LOG"
chk "list 返回 items/total 且含 success" "'items' in d and 'total' in d and d.get('success')==1" /tmp/ws5.json

log "--- 6) 不存在的 index（400 且响应体仍含 success）"
code=$(curl -s -o /tmp/ws6.json -w '%{http_code}' "$BASE/ugc/sync_to_cluster/status?token=$TOKEN&index=deadbeef")
log "http=$code"; cat /tmp/ws6.json >> "$LOG"; echo >> "$LOG"
chk "不存在 index 带 success" "'success' in d and d['success']==0" /tmp/ws6.json

log "--- 7) 非法 token（401/403 且带 success）"
curl -s -X POST "$BASE/ugc/get_sync_status?token=bogus&$FT&name=$NAME" -o /tmp/ws7.json -w 'http=%{http_code}\n' | tee -a "$LOG"
cat /tmp/ws7.json >> "$LOG"; echo >> "$LOG"
chk "非法 token 带 success=0" "'success' in d and d['success']==0" /tmp/ws7.json

log "--- 8) 清理 smoke 记录"
curl -s -X POST "$BASE/ugc/delete_files?token=$TOKEN&name=$NAME&$FT" \
  -H 'Content-Type: text/plain; charset=utf-8' --data '{"file_list": {"files": []}}' -o /tmp/ws8.json
cat /tmp/ws8.json >> "$LOG"; echo >> "$LOG"
chk "delete_files success=1" "d['success']==1" /tmp/ws8.json

log "=== SMOKE 结果: PASS=$PASS FAIL=$FAIL ==="
