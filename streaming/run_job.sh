#!/usr/bin/env bash
set -uo pipefail

SQL_DIR=/opt/flink/sql

{
  cat "$SQL_DIR/01_source_kafka.sql"
  echo ";"
  cat "$SQL_DIR/02_sink_postgres.sql"
  echo ";"
  cat "$SQL_DIR/03_window_aggregation.sql"
} | /opt/flink/bin/sql-client.sh
