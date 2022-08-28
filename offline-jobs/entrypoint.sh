#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

airflow db init
airflow db upgrade

airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email example@example.com || true
airflow connections add http_tmdb_v3 --conn-type http --conn-host 'api.themoviedb.org/3' --conn-password ${TMDB_API_KEY} || true

airflow scheduler & airflow webserver