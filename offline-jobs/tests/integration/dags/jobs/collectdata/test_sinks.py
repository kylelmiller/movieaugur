import os

from typing import Any, Optional
from unittest import TestCase

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer

from metadata_pb2 import ItemMetadata
from sinks import KafkaItemMetadataSink, KafkaUserInteractionSink
from user_interaction_pb2 import UserInteraction


class KafkaSinkTestCase(TestCase):
    @classmethod
    def setUpClass(cls, deserialization_object: Any) -> None:
        cls.kafka_brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
        cls.schema_registry = os.environ.get("SCHEMA_REGISTRY", "http://schema-registry:8082")
        cls.consumer = DeserializingConsumer(
            {
                "group.id": "integration-tests",
                "default.topic.config": {"auto.offset.reset": "smallest"},
                "bootstrap.servers": cls.kafka_brokers,
                "key.deserializer": StringDeserializer(),
                "value.deserializer": ProtobufDeserializer(
                    deserialization_object, conf={"use.deprecated.format": False}
                ),
            }
        )

    def _get_kafka_message(self) -> Optional[Any]:
        for _ in range(10):
            print("Polling for messages")
            msg = self.consumer.poll(timeout=5)
            if msg is not None:
                return msg
        return None


class KafkaUserInteractionSinkTestCase(KafkaSinkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass(UserInteraction)
        cls.consumer.subscribe(["user-interaction"])

    def test_base_case(self):
        user_interaction = UserInteraction(user_id="test_user", item_id="test_item")

        with KafkaUserInteractionSink(self.kafka_brokers, self.schema_registry) as sink:
            sink.write(user_interaction)
        message = self._get_kafka_message()
        self.assertIsNotNone(message)
        self.assertIsNone(message.key())
        self.assertEqual(user_interaction, message.value())


class KafkaItemMetadataSinkTestCase(KafkaSinkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass(ItemMetadata)
        cls.consumer.subscribe(["metadata"])

    def test_base_case(self):
        item_metadata = ItemMetadata(id="test_id")
        with KafkaItemMetadataSink(self.kafka_brokers, self.schema_registry) as sink:
            sink.write(item_metadata)
        message = self._get_kafka_message()
        self.assertIsNotNone(message)
        self.assertEqual(item_metadata.id, message.key())
        self.assertEqual(item_metadata, message.value())
