"""
Bootstraps the repository
"""
import asyncio
import json
import requests

from pymongo import MongoClient
from metadataservice.adapters.repository import AbstractRepository, MongoDBRepository


async def configure_kafka_connect(config):
    while True:
        try:
            # configures the kafka connect connector which writes metadata from a kakfa topic to the mongodb database
            response = requests.post(
                f"{config.get_kafka_connect_url()}/connectors",
                data=json.dumps(
                    {
                        "name": "metadataServiceSink",
                        "config": {
                            "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
                            "tasks.max": 4,
                            "connection.uri": config.get_mongodb_uri(),
                            "database": config.get_mongodb_db_name(),
                            "collection": "metadata",
                            "topics": "metadata",
                            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                            "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
                            "value.converter.schema.registry.url": config.get_schema_registry_url(),
                            "transforms": "RenameField",
                            "transforms.RenameField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                            "transforms.RenameField.renames": "id:_id",
                        },
                    }
                ),
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            if response.ok:
                return
            print(f"Request failed: {response.status_code}")
        except Exception as e:
            print(f"Request failed: {e}")
        await asyncio.sleep(15)


def bootstrap(config) -> AbstractRepository:
    """
    Bootstraps the repository

    :param config: The configuration
    :return: The configured repository
    """
    asyncio.run(configure_kafka_connect(config))

    return MongoDBRepository(MongoClient(config.get_mongodb_uri(), connectTimeoutMS=100).metadataservice)
