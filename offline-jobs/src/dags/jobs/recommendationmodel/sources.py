from abc import ABC
from typing import Generator

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer

from jobs.shared.user_interaction_pb2 import UserInteraction


class AbstractUserInteractionSource(ABC):
    """
    Abstract class source for metadata
    """

    def get_user_interactions(self) -> Generator[UserInteraction, None, None]:
        """
        Given a generator containing user interactions

        :return: Generator of user interactions
        """
        raise NotImplementedError


class KafkaUserInteractionSource(AbstractUserInteractionSource):
    def __init__(self, kafka_brokers: str, model_name: str):
        self.consumer = DeserializingConsumer(
            {
                "group.id": f"{model_name}-recommendations",
                "default.topic.config": {"auto.offset.reset": "smallest"},
                "bootstrap.servers": kafka_brokers,
                "key.deserializer": StringDeserializer(),
                "value.deserializer": ProtobufDeserializer(UserInteraction, conf={"use.deprecated.format": False}),
            }
        )
        self.consumer.subscribe(["user-interaction"])

    def get_user_interactions(self) -> Generator[UserInteraction, None, None]:
        """
        Reads the user interactions from the kafka topic

        :return:
        """

        no_messages = 0
        while no_messages < 20:
            msg = self.consumer.poll(timeout=2)
            if msg is not None:
                yield msg.value()
            else:
                no_messages += 1
