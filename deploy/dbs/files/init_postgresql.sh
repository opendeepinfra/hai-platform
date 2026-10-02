set -e

: ${PGPASSWORD:="root"}
: ${PGUSER:="root"}
export PGPASSWORD

# ---------------------------------------------------------------------------
# Database bootstrap for hai-platform.
#
# Schema handling: every file in db_schemas/ is idempotent -- CREATE TABLE /
# INDEX / UNIQUE INDEX / SCHEMA / MATERIALIZED VIEW all use IF NOT EXISTS,
# ALTER TABLE ADD COLUMN uses IF NOT EXISTS, CREATE TYPE is wrapped in a DO
# block that swallows duplicate_object, and every CREATE TRIGGER is preceded by
# a matching DROP TRIGGER IF EXISTS. The whole directory is therefore replayed
# on every start, which keeps an existing deployment in sync with migrations
# added by newer images.
#
# The previous implementation instead concatenated everything into a single
# transaction and stopped as soon as `task_ng` and `user` existed:
#
#     while [[ task_ng missing ]] || [[ user missing ]]; do psql -f fuse.sql; done
#
# so a database that already had those tables never received later migrations.
# That is how 032.table_host_flags stayed unapplied and `host.flags` ended up
# missing, crash-looping k8s_watcher on `column "flags" does not exist`.
# ---------------------------------------------------------------------------

psql_db() { psql -U "${PGUSER}" -d mars_db "$@"; }

# --- wait for the server to accept connections -----------------------------
while [[ $(psql -U ${PGUSER} mars_db -c "select count(*) from pg_stat_activity" 2>&1 | grep row -c)x != "1"x ]]; do
  echo "数据库还没有启动"
  sleep 1
done

# Decide up front whether this is a brand new database. It has to be sampled
# BEFORE the schema is applied, otherwise task_ng exists by the time we look.
FRESH=0
if ! psql_db -tAc "select 1 from task_ng limit 1" >/dev/null 2>&1; then
  FRESH=1
fi

# --- schema: replay all migrations (idempotent) ----------------------------
apply_schemas() {
  local sql_file
  for sql_file in $(find db_schemas/ -name '*.sql' | sort); do
    if ! psql_db -q -v ON_ERROR_STOP=1 -f "${sql_file}"; then
      echo "应用失败: ${sql_file}" >&2
      return 1
    fi
  done
  return 0
}

attempt=0
until apply_schemas; do
  attempt=$((attempt + 1))
  if [ ${attempt} -ge 12 ]; then
    echo "schema 连续应用失败 ${attempt} 次，放弃" >&2
    exit 1
  fi
  echo "schema 应用未成功，5s 后重试 (${attempt}/12)"
  sleep 5
done
echo "schema 已同步"

# --- seed data -------------------------------------------------------------
# CI data and INIT_SQL are plain INSERTs for default rows, so they are applied
# only when this database was empty; re-running them on a populated database
# would duplicate entries.
if [ "${FRESH}" = "1" ]; then
  fused_sql=/tmp/fuse_sql.sql
  echo "合并初始化数据到 ${fused_sql}"
  if [ -n "${NO_CI_DB_FILE}" ]; then
    db_ci_files=""
  else
    db_ci_files=$(find ci/ci_db_data/ -name '*.sql' 2>/dev/null | sort)
  fi
  {
    echo "BEGIN;"
    for sql_file in ${db_ci_files} ${INIT_SQL:-}; do
      [ -f "${sql_file}" ] || continue
      echo "-- ${sql_file}"
      cat "${sql_file}"
      echo "-- ${sql_file}"
      echo ""
    done
    echo "COMMIT;"
  } > "${fused_sql}"

  if ! psql_db -v ON_ERROR_STOP=1 -f "${fused_sql}" > /dev/null; then
    echo "初始化数据写入失败" >&2
    exit 1
  fi
  echo "初始化数据写入完成"
else
  echo "已初始化的数据库，跳过初始化数据写入"
fi

echo "数据库初始化完成"
