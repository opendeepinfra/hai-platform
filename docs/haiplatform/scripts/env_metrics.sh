#!/bin/bash
# haienv（`hai-cli env push`）最小看板 —— 抓 ugc-server 的 /metrics，汇总 env 家族指标。
#
# 背景（Checklist OBS-02 / OBS-03）：103 上没有 Prometheus / Grafana，因此「看板」以
# **可复现的命令行汇总**形式交付；告警规则以配置即代码的形式放在 env_alerts.yml，
# 等生产集群有 Prometheus 时直接 apply。两者用的是同一批指标名。
#
# 用法（在 host 103 上，以 fireflyer 身份）：
#   bash env_metrics.sh                                  # 走 kubectl exec 抓 pod 内 8083/metrics
#   bash env_metrics.sh http://127.0.0.1:8083/metrics    # 显式指定 /metrics 地址
#   METRICS_URL=http://x/metrics bash env_metrics.sh
#   WARN_RATE=0.05 bash env_metrics.sh                   # 失败率阈值（默认 5%，对应 OBS-03）
#
# 注意：冒烟 / E2E 脚本会**故意**制造失败（越界 path、非法名、缺 token…），因此刚跑完
# 测试时的失败率不能当线上指标看 —— 判据只在稳态窗口内才有意义。
set -e

NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
PORT="${PORT:-8083}"
WARN_RATE="${WARN_RATE:-0.05}"

SRC="${1:-${METRICS_URL:-}}"
RAW="$(mktemp /tmp/env_metrics.XXXXXX)"
trap 'rm -f "${RAW}"' EXIT

if [ -n "${SRC}" ]; then
  curl -s -m 10 "${SRC}" -o "${RAW}"
else
  SRC="kubectl exec ${POD}:${PORT}/metrics"
  if command -v kubectl >/dev/null 2>&1; then
    sudo kubectl -n "${NS}" exec "${POD}" -- curl -s -m 10 "http://127.0.0.1:${PORT}/metrics" > "${RAW}"
  else
    SRC="http://127.0.0.1:${PORT}/metrics"
    curl -s -m 10 "${SRC}" -o "${RAW}"
  fi
fi

if ! grep -q "env_push_requests_total" "${RAW}"; then
  echo "未在 ${SRC} 抓到 env_push_requests_total —— 检查 ugc-server 是否启动了含 haienv 的版本" >&2
  echo "（前 20 行原始输出）" >&2
  head -20 "${RAW}" >&2
  exit 1
fi

WARN_RATE="${WARN_RATE}" python3 - "${RAW}" "${SRC}" <<'PY'
import os
import re
import sys

path, src = sys.argv[1], sys.argv[2]
warn_rate = float(os.environ.get('WARN_RATE', '0.05'))
text = open(path, encoding='utf-8', errors='replace').read().splitlines()

samples = []          # (name, labels:dict, value)
for line in text:
    if not line or line.startswith('#'):
        continue
    m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+([0-9eE.+-]+|NaN|[+-]Inf)$', line)
    if not m:
        continue
    name, _, label_text, value = m.group(1), m.group(2), m.group(3), m.group(4)
    labels = {}
    if label_text:
        for item in re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"', label_text):
            labels[item[0]] = item[1]
    try:
        samples.append((name, labels, float(value)))
    except ValueError:
        continue


def total(name, **match):
    return sum(v for n, l, v in samples
               if n == name and all(l.get(k) == val for k, val in match.items()))


def series(name):
    return [(l, v) for n, l, v in samples if n == name]


print('=== env 最小看板  source=%s' % src)
print()

print('-- 请求量 env_push_requests_total（api / result / code）')
rows = sorted(series('env_push_requests_total'), key=lambda x: (x[0].get('api', ''), x[0].get('result', '')))
if not rows:
    print('   （无样本：自上一次 ugc-server 重启以来还没有 env 请求）')
for labels, value in rows:
    print('   %-22s %-4s code=%-24s %s' % (labels.get('api', '-'), labels.get('result', '-'),
                                           labels.get('code', '-'), int(value)))
print()

print('-- 成功率（失败率阈值 %.0f%%，OBS-03）' % (warn_rate * 100))
verdict_fail = False
for api in ('update_cluster_venv', 'register_cluster_venv'):
    ok = total('env_push_requests_total', api=api, result='ok')
    fail = total('env_push_requests_total', api=api, result='fail')
    allc = ok + fail
    if allc == 0:
        print('   %-22s 无请求' % api)
        continue
    rate = fail / allc
    flag = 'FAIL' if rate > warn_rate else 'OK'
    verdict_fail = verdict_fail or rate > warn_rate
    print('   %-22s ok=%d fail=%d 失败率=%.1f%%  [%s]' % (api, int(ok), int(fail), rate * 100, flag))
print()

print('-- 注册耗时 env_register_duration_seconds（按 result）')
for result in ('ok', 'fail'):
    buckets = [(float(l['le']), v) for l, v in series('env_register_duration_seconds_bucket')
               if l.get('result') == result and l.get('le') not in (None, '+Inf')]
    count = total('env_register_duration_seconds_count', result=result)
    if not count:
        continue
    buckets.sort()
    quantiles = []
    for q in (0.5, 0.95, 0.99):
        target = q * count
        bound = None
        for le, cumulative in buckets:
            if cumulative >= target:
                bound = le
                break
        quantiles.append('p%d<=%s' % (int(q * 100), ('%.3fs' % bound) if bound is not None else 'n/a'))
    s = total('env_register_duration_seconds_sum', result=result)
    print('   result=%-4s count=%-5d %s  均值=%.3fs' % (result, int(count), '  '.join(quantiles), s / count))
print()

print('-- 失败原因')
for name, title in (('env_registry_write_failures_total', '写失败'),
                    ('env_registry_read_failures_total', '读失败（N3 fail-closed）')):
    rows = sorted(series(name), key=lambda x: x[0].get('reason', ''))
    if not rows:
        print('   %-22s 无样本' % title)
    for labels, value in rows:
        print('   %-22s reason=%-12s %s' % (title, labels.get('reason', '-'), int(value)))
print()

print('-- 结论：%s' % ('存在超过阈值的失败率，需要按 env-server-test-report 的排障章节处理'
                     if verdict_fail else 'env 指标在阈值内'))
PY
