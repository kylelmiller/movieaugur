"""Classes which data can be written to. Provides an abstraction from external systems."""
# pylint: disable=import-error,no-name-in-module
from abc import ABC, abstractmethod
from typing import Any, Optional

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer
from google.protobuf.json_format import MessageToJson

from jobs.collectdata.metadata_pb2 import ItemMetadata
from jobs.collectdata.item_score_pb2 import ItemScores
from jobs.collectdata.user_interaction_pb2 import UserInteraction


PRODUCER_TIMEOUT = 120


class KafkaSink(ABC):
    """
    Abstraction around producing to Kafka
    """

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
    """
    Kafka sink for user interaction data.
    """

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

    def write(self, value: UserInteraction) -> None:
        self.kafka_producer.produce(topic=self.topic, value=value)
        self.kafka_producer.poll(0)


class KafkaItemMetadataSink(KafkaSink):
    """
    Kafka sink for item metadata.
    """

    def __init__(self, kafka_brokers: str, schema_registry: str):
        super().__init__(
            kafka_brokers,
            StringSerializer(),
            ProtobufSerializer(
                ItemMetadata, SchemaRegistryClient({"url": schema_registry}), conf={"use.deprecated.format": False}
            ),
            "metadata",
        )

    def write(self, value: ItemMetadata) -> None:
        self.kafka_producer.produce(topic=self.topic, key=f"#ID#{value.id}#TYPE#{value.object_type}", value=value)
        self.kafka_producer.poll(0)


class KafkaPopularitySink(KafkaSink):
    """
    Kafka sink for object type popularity data.
    """

    def __init__(self, kafka_brokers: str, schema_registry: str, popularity_name: str):
        super().__init__(
            kafka_brokers,
            StringSerializer(),
            StringSerializer(),
            "popularity",
        )
        self.popularity_name = f"popular-{popularity_name}"

    def write(self, value: Optional[ItemScores]) -> None:
        self.kafka_producer.produce(
            topic=self.topic, key=self.popularity_name, value=None if value is None else MessageToJson(value)
        )
        self.kafka_producer.poll(0)
