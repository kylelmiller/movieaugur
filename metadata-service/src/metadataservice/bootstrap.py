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
        raise RequestException(f"Request failed: {response.status_code}: {response}")
    return connector_name in response.json()


async def configure_kafka_connect(config) -> None:
    """
    Configures kafka connect for a metadata object type

    :param config: The metadata service config
    :return:
    """
    connector_name = f"metadataServiceSink"

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
                            "collection": "metadata",
                            "document.id.strategy.overwrite.existing": True,
                            "delete.on.null.values": True,
                            "topics": f"metadata",
                            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                            "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
                            "value.converter.schema.registry.url": config.get_schema_registry_url(),
                            "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
                            "transforms": "hk",
                            "transforms.hk.type": "org.apache.kafka.connect.transforms.HoistField$Key",
                            "transforms.hk.field": "_id",
                        },
                    }
                ),
                headers=HEADERS,
            )
            if response.ok:
                print(f"{connector_name} created")
                return
            print(f"Request failed: {response.status_code}: {response.json()['message']}")
        except RequestException as ex:
            print(f"Request failed: {ex}")
        await asyncio.sleep(15)


def bootstrap(config) -> AbstractRepository:
    """
    Bootstraps the repository

    :param config: The configuration
    :return: The configured repository
    """
    asyncio.run(configure_kafka_connect(config))

    return MongoDBRepository(MongoClient(config.get_mongodb_uri(), connectTimeoutMS=100).metadataservice)
