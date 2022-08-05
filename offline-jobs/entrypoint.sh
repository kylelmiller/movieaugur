#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

export AIRFLOW__CORE__FERNET_KEY=Z6BkzaWcF7r5cC-VMAumjpBpudSyjGskQJHK34KjdfnlhG0=
export AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=false

airflow db init
airflow db upgrade

airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email example@example.com || true

airflow connections add https_tmdb_v3 --conn-type https --conn-host 'api.themoviedb.org/3' || true

# Creates variables that contain the job configuration
SHARED_CONFIG=$(<./configuration/shared_configuration.json)
airflow variables set shared_config "${SHARED_CONFIG}"

COLLECT_METADATA_DAG_CONFIGURATIONS=$(<./configuration/collect_metadata_dag_configuration.json)
airflow variables set collect_metadata_dag_configuration "${COLLECT_METADATA_DAG_CONFIGURATIONS}"

airflow scheduler & airflow webserver