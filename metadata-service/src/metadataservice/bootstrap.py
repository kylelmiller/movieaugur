"""
Bootstraps the repository
"""
from pymongo import MongoClient

from metadataservice.adapters.repository import AbstractRepository, MongoDBRepository


def bootstrap(config) -> AbstractRepository:
    """
    Bootstraps the repository

    :param config: The configuration
    :return: The configured repository
    """
    return MongoDBRepository(MongoClient(config.get_mongodb_uri(), connectTimeoutMS=100).metadataservice)
