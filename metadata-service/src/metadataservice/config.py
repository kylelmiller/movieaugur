"""
Contains the services configuration
"""
import multiprocessing
import os


def get_mongodb_uri() -> str:
    """
    The DB connection uri

    :return:
    """
    host = os.environ.get("DB_HOST", "localhost")
    port = 27017 if host == "localhost" else 27017
    password = os.environ.get("DB_PASSWORD", "root")
    user, db_name = "root", "metadataservice"
    return f"mongodb://{user}:{password}@{host}:{port}/{db_name}?authSource=admin"


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
