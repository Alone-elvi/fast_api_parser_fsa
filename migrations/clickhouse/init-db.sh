#!/bin/bash
set -e

# Ждем готовности ClickHouse
until clickhouse-client --query "SELECT 1"; do
    echo "Waiting for ClickHouse to start..."
    sleep 2
done

# Применяем миграции
clickhouse-client --multiline --multiquery < /docker-entrypoint-initdb.d/clickhouse_init.sql
