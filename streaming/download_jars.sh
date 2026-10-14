#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p jars

M=https://repo1.maven.org/maven2

fetch() {
  [ -f "jars/$2" ] && { echo "have $2"; return; }
  curl -fL -o "jars/$2" "$M/$1/$2"
}

fetch org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20 flink-sql-connector-kafka-3.3.0-1.20.jar
fetch org/apache/flink/flink-connector-jdbc/3.2.0-1.19 flink-connector-jdbc-3.2.0-1.19.jar
fetch org/postgresql/postgresql/42.7.4 postgresql-42.7.4.jar
