"""
Bootstraps the repository
"""
import asyncio
import json
import requests

from pymongo import MongoClient
from metadataservice.adapters.repository import AbstractRepository, MongoDBRepository
from requests.exceptions import RequestException


HEADERS = {"Content-Type": "application/json; charset=utf-8"}


def is_connector_configured(connector_name: str, config) -> bool:
    """
    Checks to see if a connector has been configured.

    :param connector_name: The name of the connector
    :param config: The metadata service config
    :return: True of the connector has been configured, else False
    """
    response = requests.get(f"{config.get_kafka_connect_url()}/connectors", headers=HEADERS)
    if not response.ok:
        raise Exception(f"Request failed: {response.status_code}: {response}")
    return connector_name in response.json()


async def configure_kafka_connect(config, object_type: str) -> None:
    """
    Configures kafka connect for a metadata object type

    :param config: The metadata service config
    :param object_type: The object type of the metadata we want to support
    :return:
    """
    connector_name = f"{object_type}MetadataServiceSink"

    while True:
        try:
            if is_connector_configured(connector_name, config):
                return

            # configures the kafka connect connector which writes metadata from a kakfa topic to the mongodb database
            response = requests.post(
                f"{config.get_kafka_connect_url()}/connectors",
                data=json.dumps(
                    {
                        "name": connector_name,
                        "config": {
                            "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
                            "tasks.max": 4,
                            "connection.uri": config.get_mongodb_uri(),
                            "database": config.get_mongodb_db_name(),
                            "collection": f"{object_type}-metadata",
                            "topics": f"{object_type}-metadata",
                            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                            "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
                            "value.converter.schema.registry.url": config.get_schema_registry_url(),
                            "transforms": "RenameField",
                            "transforms.RenameField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                            "transforms.RenameField.renames": "id:_id",
                        },
                    }
                ),
                headers=HEADERS,
            )
            if response.ok:
                return
            print(f"Request failed: {response.status_code}: {response}")
        except RequestException as ex:
            print(f"Request failed: {ex}")
        await asyncio.sleep(15)


def bootstrap(config) -> AbstractRepository:
    """
    Bootstraps the repository

    :param config: The configuration
    :return: The configured repository
    """
    asyncio.run(configure_kafka_connect(config, "movie"))
    asyncio.run(configure_kafka_connect(config, "series"))

    return MongoDBRepository(MongoClient(config.get_mongodb_uri(), connectTimeoutMS=100).metadataservice)
