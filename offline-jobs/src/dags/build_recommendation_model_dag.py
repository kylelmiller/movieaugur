"""
DAG which runs the collect_data job
"""
from datetime import datetime
from typing import Dict

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from jobs.recommendationmodel.build_recommendation_model import build_recommendation_model
from constants import DEFAULT_KAFKA_BROKERS, DEFAULT_SCHEMA_REGISTRY


def create_build_recommendation_model_dag(shared_config: Dict[str, str]) -> DAG:
    """
    Creates a DAG which builds a recommendation model. It reads from the user interactions kafka topic and writes
    the user recommendation kafka topic.

    :param shared_config: Configuration for the DAGs
    :return: The created DAG
    """

    dag = DAG(
        dag_id="build_recommendation_model",
        description="Builds a recommendation model.",
        default_args=shared_config,
        start_date=datetime(2022, 9, 1),
        schedule_interval=None,
    )
    with dag:
        PythonOperator(
            task_id="build_recommendation_model",
            python_callable=build_recommendation_model,
            op_args=[shared_config.get("kafka_brokers", DEFAULT_KAFKA_BROKERS)],
        )

    return dag


collect_popular_tmdb_series_data = create_build_recommendation_model_dag(
    Variable.get("shared_config", deserialize_json=True, default_var={})
)
