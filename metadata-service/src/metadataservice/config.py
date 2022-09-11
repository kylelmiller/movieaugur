"""
Contains the services configuration
"""
import multiprocessing
import os


def get_mongodb_db_name() -> str:

    return "metadataservice"


def get_mongodb_uri() -> str:
    """
    The DB connection uri

    :return:
    """
    host = os.environ.get("DB_HOST", "mongodb")
    port = 27017 if host == "localhost" else 27017
    password = os.environ.get("DB_PASSWORD", "root")
    user = "root"
    return f"mongodb://{user}:{password}@{host}:{port}/{get_mongodb_db_name()}?authSource=admin"


def get_schema_registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8082")


def get_kafka_connect_url() -> str:
    return os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:18083")


def get_max_workers() -> int:
    """
    The maximum number of threads the service will use

    :return:
    """
    return int(os.environ.get("MAX_WORKERS", multiprocessing.cpu_count() * 2))


def get_grpc_host() -> str:
    """
    The metadata GRPC server's host

    :return:
    """
    return os.environ.get("GRPC_HOST", "localhost")


def get_grpc_port() -> str:
    """
    The GRPC port that our server will use

    :return:
    """
    return os.environ.get("GRPC_PORT", "5005")
