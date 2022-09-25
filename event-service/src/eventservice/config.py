"""The popularity service configuration"""
import os


def get_schema_registry_url() -> str:
    """
    The url of the confluent schema registry.

    :return:
    """
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8082")


def get_kafka_brokers() -> str:
    """
    The kafka connect url.

    :return:
    """
    return os.environ.get("KAFKA_BROKERS", "kafka:9092")
