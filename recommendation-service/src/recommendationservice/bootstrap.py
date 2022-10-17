"""
Bootstraps the repository
"""
import asyncio
import json
import requests

from redis import Redis
from requests.exceptions import RequestException
from recommendationservice.adapters.metadata_repository import MetadataServiceMetadataRespository
from recommendationservice.adapters.recommendation_repository import RedisRecommendationRepository
from recommendationservice.service_layer.recommendation_service import RecommendationService


HEADERS = {"Content-Type": "application/json; charset=utf-8"}
REDIS_DB = 1


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
    Configures kafka connect for the recommendation service

    :param config: The popularity service config
    :return:
    """
    connector_name = "recommendationServiceSink"

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
                            "connector.class": "com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector",
                            "tasks.max": 4,
                            "redis.hosts": config.get_redis_url(),
                            "redis.database": REDIS_DB,
                            "topics": "recommendations",
                            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                            "value.converter": "org.apache.kafka.connect.storage.StringConverter",
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


def bootstrap(config) -> RecommendationService:
    """
    Bootstraps the repository

    :param config: The configuration
    :return: The configured repository
    """
    asyncio.run(configure_kafka_connect(config))
    return RecommendationService(
        RedisRecommendationRepository(
            Redis(host=config.get_redis_hostname(), port=config.get_redis_port(), db=REDIS_DB)
        ),
        MetadataServiceMetadataRespository(config.get_metadata_service_url()),
    )
