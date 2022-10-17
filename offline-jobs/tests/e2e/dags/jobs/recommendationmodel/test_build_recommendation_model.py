"""
End to End test cases
"""
import json
import os
from typing import Any, List
from unittest import TestCase

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

from jobs.recommendationmodel.build_recommendation_model import build_recommendation_model
from jobs.shared.user_interaction_pb2 import UserInteraction


KAFKA_CONSUMER = Any
MESSAGE = Any


def get_messages(consumer: KAFKA_CONSUMER) -> List[MESSAGE]:
    """
    Polls a consumer and gets all of the new messages.

    :param consumer: Kafka consumer
    :return: LIst of collected messages
    """
    no_messages = 0
    while no_messages < 12:
        msg = consumer.poll(timeout=5)
        if msg is not None:
            yield msg
        else:
            no_messages += 1


class EndToEndTestCase(TestCase):
    """
    End to End test case for the following collecting data sources:
        * MovieLens 100k
        * TMDB Popular Movies
        * TMDB Popular Series
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        Creates kafka consumers that are used to validate the output
        :return:
        """
        cls.kafka_brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
        cls.schema_registry = os.environ.get("SCHEMA_REGISTRY", "http://schema-registry:8082")
        cls.admin_client = AdminClient({"bootstrap.servers": cls.kafka_brokers})
        cls.admin_client.create_topics([NewTopic("user-interaction", 1, 1)])
        cls.user_interaction_producer = SerializingProducer(
            {
                "bootstrap.servers": cls.kafka_brokers,
                "key.serializer": StringSerializer(),
                "value.serializer": ProtobufSerializer(
                    UserInteraction,
                    SchemaRegistryClient(
                        {
                            "url": cls.schema_registry,
                        }
                    ),
                    conf={"use.deprecated.format": False},
                ),
            }
        )
        cls.item_scores_consumer = DeserializingConsumer(
            {
                "group.id": "recommendation-e2e-tests",
                "default.topic.config": {"auto.offset.reset": "smallest"},
                "bootstrap.servers": cls.kafka_brokers,
                "key.deserializer": StringDeserializer(),
                "value.deserializer": StringDeserializer(),
            }
        )
        cls.item_scores_consumer.subscribe(["recommendations"])

        cls.users = set()
        for i in range(50):
            for j in range(10):
                user_id = f"user_{i}"
                cls.user_interaction_producer.produce(
                    "user-interaction",
                    value=UserInteraction(user_id=user_id, item_id=f"item_{i+j}", object_type="movie"),
                )
                cls.users.add(user_id)

    @classmethod
    def tearDownClass(cls) -> None:
        # Remove all documents from the database
        result = cls.admin_client.delete_topics(["user-interaction", "recommendations"], operation_timeout=30)
        # Wait for operation to finish.
        for topic, f in result.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as ex:
                print("Failed to delete topic {}: {}".format(topic, ex))

    def test_build_recommendation_model(self):
        """
        Tests the tmdb popular movies collection job.

        :return:
        """
        build_recommendation_model(self.kafka_brokers)
        messages = list(get_messages(self.item_scores_consumer))

        self.assertEqual({f"model-default-user-user_{i}" for i in range(50)}, {message.key() for message in messages})
        for message in messages:
            self.assertEqual(20, len(json.loads(message.value())["itemScores"]))
