"""
Bootstraps the repository
"""
import asyncio
import json
import requests

from redis import Redis
from requests.exceptions import RequestException
from popularityservice.adapters.metadata_repository import MetadataServiceMetadataRespository
from popularityservice.adapters.popularity_repository import RedisPopularityRepository
from popularityservice.service_layer.popularity_service import PopularityService


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


async def configure_kafka_connect(config) -> None:
    """
    Configures kafka connect for the popularity service

    :param config: The popularity service config
    :return:
    """
    connector_name = "popularityServiceSink"

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
                            "connector.class": "io.github.jaredpetersen.kafkaconnectredis.sink.RedisSinkConnector",
                            "tasks.max": 4,
                            "redis.hosts": config.get_redis_url(),
                            "topics": "popularity",
                            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                            "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
                            "value.converter.schema.registry.url": config.get_schema_registry_url(),
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


def bootstrap(config) -> PopularityService:
    """
    Bootstraps the repository

    :param config: The configuration
    :return: The configured repository
    """
    asyncio.run(configure_kafka_connect(config))
    return PopularityService(
        RedisPopularityRepository(Redis(host=config.get_redis_hostname(), port=config.get_redis_port())),
        MetadataServiceMetadataRespository(config.get_metadata_service_url()),
    )
