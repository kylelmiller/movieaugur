FROM apache/airflow:2.3.4-python3.8

ENV AIRFLOW__CORE__FERNET_KEY=Z6BkzaWcF7r5cC-VMAumjpBpudSyjGskQ0ObquGJhG0= \
    AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=false \
    AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=false \
    AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor

COPY requirements.txt /tmp/
COPY constraints.txt /tmp/
RUN pip install -r /tmp/requirements.txt -c /tmp/constraints.txt

COPY --chown=airflow:root entrypoint.sh /opt/airflow/
COPY --chown=airflow:root src/dags /opt/airflow/dags
