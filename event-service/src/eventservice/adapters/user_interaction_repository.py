"""The popularity repository"""
# pylint: disable=import-error,no-name-in-module
from abc import ABC
from typing import Dict, Union

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from eventservice.user_interaction_pb2 import UserInteraction


class AbstractUserInteractionRepository(ABC):
    """
    Abstract base class for the repository.
    """

    def __enter__(self):  # -> AbstractUserInteractionRepository
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._commit()

    def _commit(self) -> None:
        """
        Commits the writes.

        :return:
        """
        raise NotImplementedError

    def write(self, user_interaction: Dict[str, Union[str, int]]) -> None:
        """
        Write the user interaction to the repository.

        :param user_interaction: The user interaction that is to be written
        :return:
        """
        raise NotImplementedError


class KafkaUserInteractionRepository(AbstractUserInteractionRepository):
    """
    Implementation of the popularity service which uses Redis to store the popularity rankings.
    """

    TOPIC = "user-interaction"
    PRODUCER_FLUSH_TIMEOUT = 30

    def __init__(self, kafka_brokers: str, schema_registry: str):
        admin_client = AdminClient({"bootstrap.servers": kafka_brokers})
        admin_client.create_topics([NewTopic(self.TOPIC, 1, 1)])

        self.kafka_producer = SerializingProducer(
            {
                "bootstrap.servers": kafka_brokers,
                "key.serializer": StringSerializer(),
                "value.serializer": ProtobufSerializer(
                    UserInteraction,
                    SchemaRegistryClient(
                        {
                            "url": schema_registry,
                        }
                    ),
                    conf={"use.deprecated.format": False},
                ),
            }
        )

    def _commit(self):
        self.kafka_producer.flush(self.PRODUCER_FLUSH_TIMEOUT)

    def write(self, user_interaction: UserInteraction) -> None:
        self.kafka_producer.produce(topic=self.TOPIC, value=user_interaction)
        self.kafka_producer.poll(0)
