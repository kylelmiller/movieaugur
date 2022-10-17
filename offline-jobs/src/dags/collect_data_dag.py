"""
DAG which runs the collect_data job
"""
from datetime import datetime
from typing import Dict

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from jobs.collectdata.collect_data import (
    collect_full_movielens_data,
    collect_movielens_100k_data,
    collect_popular_tmdb_movie_data,
    collect_popular_tmdb_series_data,
)
from constants import DEFAULT_KAFKA_BROKERS, DEFAULT_SCHEMA_REGISTRY


def create_collect_100k_movielens_data(shared_config: Dict[str, str]) -> DAG:
    """
    Creates a DAG which collects the movie lens data. This is a static file so it's not a schedule job. It's intended
    to be a one time import.

    :param shared_config: Configuration for the DAGs
    :return: The created DAG
    """

    dag = DAG(
        dag_id="collect_movielens_100k_data",
        description="Collect the movielens 100k interaction and movie metadata.",
        default_args=shared_config,
        start_date=datetime(2022, 9, 1),
        schedule_interval=None,
    )
    with dag:
        PythonOperator(
            task_id="collect_movielens_100k_data",
            python_callable=collect_movielens_100k_data,
            op_args=[
                BaseHook.get_connection("http_tmdb_v3").password,
                shared_config.get("kafka_brokers", DEFAULT_KAFKA_BROKERS),
                shared_config.get("schema_registry", DEFAULT_SCHEMA_REGISTRY),
            ],
        )

    return dag


def create_collect_full_movielens_data(shared_config: Dict[str, str]) -> DAG:
    """
    Creates a DAG which collects the movie lens data. This is a static file so it's not a schedule job. It's intended
    to be a one time import.

    :param shared_config: Configuration for the DAGs
    :return: The created DAG
    """

    dag = DAG(
        dag_id="collect_movielens_full_data",
        description="Collect the complete movielens interactions and movie metadata.",
        default_args=shared_config,
        start_date=datetime(2022, 9, 1),
        schedule_interval=None,
    )
    with dag:
        PythonOperator(
            task_id="collect_movielens_full_data",
            python_callable=collect_full_movielens_data,
            op_args=[
                BaseHook.get_connection("http_tmdb_v3").password,
                shared_config.get("kafka_brokers", DEFAULT_KAFKA_BROKERS),
                shared_config.get("schema_registry", DEFAULT_SCHEMA_REGISTRY),
            ],
        )

    return dag


def create_collect_popular_tmdb_series_data(shared_config: Dict[str, str]) -> DAG:
    """
    Creates a DAG which runs the collect popular tmdb series job and writes the results out to Kafka.That endpoint
    changes daily so this job is scheduled to run daily.

    :param shared_config: Configuration for the DAGs
    :return: The created DAG
    """

    dag = DAG(
        dag_id="collect_popular_tmdb_series_data",
        description="Collect the popular tmdb series metadata.",
        default_args=shared_config,
        start_date=datetime(2022, 9, 1),
        schedule_interval=("0 3 * * *"),
    )
    with dag:
        PythonOperator(
            task_id="collect_popular_tmdb_series_data",
            python_callable=collect_popular_tmdb_series_data,
            op_args=[
                BaseHook.get_connection("http_tmdb_v3").password,
                shared_config.get("kafka_brokers", DEFAULT_KAFKA_BROKERS),
                shared_config.get("schema_registry", DEFAULT_SCHEMA_REGISTRY),
            ],
        )

    return dag


def create_collect_popular_tmdb_movie_data(shared_config: Dict[str, str]) -> DAG:
    """
    Creates a DAG which runs the collect popular tmdb movie job and writes the results out to Kafka. That endpoint
    changes daily so this job is scheduled to run daily.

    :param shared_config: Configuration for the DAGs
    :return: The created DAG
    """

    dag = DAG(
        dag_id="collect_popular_tmdb_movie_data",
        description="Collect the popular tmdb movie metadata.",
        default_args=shared_config,
        start_date=datetime(2022, 9, 1),
        schedule_interval=("0 3 * * *"),
    )
    with dag:
        PythonOperator(
            task_id="collect_popular_tmdb_movie_data",
            python_callable=collect_popular_tmdb_movie_data,
            op_args=[
                BaseHook.get_connection("http_tmdb_v3").password,
                shared_config.get("kafka_brokers", DEFAULT_KAFKA_BROKERS),
                shared_config.get("schema_registry", DEFAULT_SCHEMA_REGISTRY),
            ],
        )

    return dag


collect_movielens100k_data = create_collect_movielens_data(
    Variable.get("shared_config", deserialize_json=True, default_var={})
)
collect_popular_tmdb_series_data = create_collect_popular_tmdb_series_data(
    Variable.get("shared_config", deserialize_json=True, default_var={})
)
collect_popular_tmdb_movie_data = create_collect_popular_tmdb_movie_data(
    Variable.get("shared_config", deserialize_json=True, default_var={})
)
