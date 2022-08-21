from abc import ABC, abstractmethod
from typing import Any, Optional

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from metadata_pb2 import ItemMetadata
from user_interaction_pb2 import UserInteraction


PRODUCER_TIMEOUT = 120


class KafkaSink(ABC):
    def __init__(self, kafka_brokers: str, key_serializer, value_serializer, topic: str):
        self.topic = topic
        admin_client = AdminClient({"bootstrap.servers": kafka_brokers})
        admin_client.create_topics([NewTopic(topic, 1, 1)])

        self.kafka_producer = SerializingProducer(
            {
                "bootstrap.servers": kafka_brokers,
                "key.serializer": key_serializer,
                "value.serializer": value_serializer,
            }
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kafka_producer.flush(PRODUCER_TIMEOUT)

    @abstractmethod
    def write(self, value: Any) -> None:
        """
        Writes a key/value to a data source.

        :param key: the key
        :param value: the value
        :return: None
        """
        raise NotImplementedError


class KafkaUserInteractionSink(KafkaSink):
    def __init__(self, kafka_brokers: str, schema_registry: str):
        super().__init__(
            kafka_brokers,
            StringSerializer(),
            ProtobufSerializer(
                UserInteraction,
                SchemaRegistryClient(
                    {
                        "url": schema_registry,
                    }
                ),
                conf={"use.deprecated.format": False},
            ),
            "user-interaction",
        )

    def write(self, user_interaction: UserInteraction) -> None:
        self.kafka_producer.produce(topic=self.topic, value=user_interaction)


class KafkaItemMetadataSink(KafkaSink):
    def __init__(self, kafka_brokers: str, schema_registry: str):
        super().__init__(
            kafka_brokers,
            StringSerializer(),
            ProtobufSerializer(
                ItemMetadata, SchemaRegistryClient({"url": schema_registry}), conf={"use.deprecated.format": False}
            ),
            "metadata",
        )

    def write(self, item_metadata: Optional[ItemMetadata]) -> None:
        self.kafka_producer.produce(topic=self.topic, key=item_metadata.id, value=item_metadata)
