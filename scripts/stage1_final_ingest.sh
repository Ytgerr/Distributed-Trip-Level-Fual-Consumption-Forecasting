#!/bin/bash
set -euo pipefail

TEAM="team15"
DB="team15_projectdb"
PG_HOST="hadoop-04.uni.innopolis.ru"
PG_USER="team15"
WAREHOUSE_HDFS="/user/${TEAM}/project/warehouse"
LOCAL_WAREHOUSE="output/warehouse"
SQOOP_PASSWORD_LOCAL="/tmp/team15_sqoop_psql.pass"
python3 - <<'PY_PASS'
from pathlib import Path
password = Path("secrets/.psql.pass").read_text().strip()
Path("/tmp/team15_sqoop_psql.pass").write_text(password)
PY_PASS
chmod 600 "${SQOOP_PASSWORD_LOCAL}"
PASSWORD_FILE="file://${SQOOP_PASSWORD_LOCAL}"

rm -rf "${LOCAL_WAREHOUSE}/trips" "${LOCAL_WAREHOUSE}/vehicles"

echo "Building PostgreSQL database with bulk COPY ingest..."
python3 scripts/build_projectdb.py

echo "Removing old HDFS warehouse..."
hdfs dfs -rm -r -f "${WAREHOUSE_HDFS}"

echo "Importing vehicles table to HDFS as Parquet + Snappy..."
sqoop import \
  --connect "jdbc:postgresql://${PG_HOST}:5432/${DB}" \
  --username "${PG_USER}" \
  --password-file "${PASSWORD_FILE}" \
  --table vehicles \
  --target-dir "${WAREHOUSE_HDFS}/vehicles" \
  --as-parquetfile \
  --compression-codec snappy \
  --num-mappers 1 \
  --delete-target-dir

echo "Importing trips table to HDFS as Parquet + Snappy with parallel mappers..."
sqoop import \
  --connect "jdbc:postgresql://${PG_HOST}:5432/${DB}" \
  --username "${PG_USER}" \
  --password-file "${PASSWORD_FILE}" \
  --table trips \
  --target-dir "${WAREHOUSE_HDFS}/trips" \
  --as-parquetfile \
  --compression-codec snappy \
  --num-mappers 4 \
  --split-by vehid \
  --delete-target-dir

echo "Copying HDFS warehouse sample to local output..."
rm -rf "${LOCAL_WAREHOUSE}"
mkdir -p output
hdfs dfs -copyToLocal "${WAREHOUSE_HDFS}" "${LOCAL_WAREHOUSE}"

echo "Stage 1 final ingest completed."
