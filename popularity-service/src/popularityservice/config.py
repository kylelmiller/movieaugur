"""The popularity service configuration"""
import os


def get_redis_hostname() -> str:
    """
    The Redis connection hostname

    :return:
    """
    return os.environ.get("REDIS_HOST", "redis")


def get_redis_port() -> int:
    """
    The Redis connection port

    :return:
    """
    return int(os.environ.get("REDIS_PORT", 6379))


def get_redis_url() -> str:
    """
    The full Redis url

    :return:
    """
    return f"{get_redis_hostname()}:{get_redis_port()}"


def get_metadata_service_url() -> str:
    """
    The url of the metadata gRPC service

    :return:
    """
    return os.environ.get("METADATA_SERVICE_URL", "metadata-service:5005")


def get_schema_registry_url() -> str:
    """
    The url of the confluent schema registry.

    :return:
    """
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8082")


def get_kafka_connect_url() -> str:
    """
    The kafka connect url.

    :return:
    """
    return os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:18083")
