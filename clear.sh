#!/bin/bash

rm -r airflow/config
rm -r airflow/dags/__pycache__
rm -r airflow/data
rm -r airflow/logs
rm -r clickhouse/default
rm -r clickhouse/system
rm -r clickhouse/houses
rm -r postgres
rm -r s3_storage

echo "Удаление папок выполнено успешно"