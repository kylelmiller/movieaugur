import os
import time
from unittest import TestCase

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer
from google.protobuf.json_format import MessageToJson

from recommendationservice import config
from recommendationservice.item_score_pb2 import ItemScore, ItemScores
from recommendationservice.metadata_pb2 import ItemMetadata


class FlaskAppTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recommendation_service_url = os.environ.get("RECOMMENDATION_SERVICE_URL", "http://recommendation-service:5035")
        kafka_brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
        cls.admin_client = AdminClient({"bootstrap.servers": kafka_brokers})

        cls.kafka_metadata_producer = SerializingProducer(
            {
                "bootstrap.servers": kafka_brokers,
                "key.serializer": StringSerializer(),
                "value.serializer": ProtobufSerializer(
                    ItemMetadata,
                    SchemaRegistryClient({"url": config.get_schema_registry_url()}),
                    {"use.deprecated.format": False},
                ),
                "linger.ms": 500,
            }
        )

        cls.kafka_recommendations_producer = SerializingProducer(
            {
                "bootstrap.servers": kafka_brokers,
                "key.serializer": StringSerializer(),
                "value.serializer": StringSerializer(),
                "linger.ms": 500,
            }
        )

    def send_recommendations(self, key, object_type="movie"):
        self.kafka_recommendations_producer.produce(
            "recommendations",
            key=key,
            value=MessageToJson(
                ItemScores(
                    item_scores=[
                        ItemScore(id="0", object_type=object_type, score=1.0),
                        ItemScore(id="1", object_type=object_type, score=0.8),
                        ItemScore(id="2", object_type=object_type, score=0.7),
                    ]
                )
            ),
        )
        self.kafka_recommendations_producer.poll(0)
        self.kafka_recommendations_producer.flush(timeout=5)

    def setUp(self) -> None:
        self.admin_client.create_topics([NewTopic("metadata", 1, 1), NewTopic("recommendations", 1, 1)])

        self.send_recommendations("model-default-user-user_1")
        self.send_recommendations("model-default-user-user_2")

        for id, object_type in (("0", "movie"), ("1", "movie"), ("0", "series"), ("1", "series"), ("2", "series")):
            self.kafka_metadata_producer.produce(
                topic="metadata", key=f"#ID#{id}#TYPE#{object_type}", value=ItemMetadata(id=id, object_type=object_type)
            )
        self.kafka_metadata_producer.poll(0)
        self.kafka_metadata_producer.flush(timeout=5)

    def tearDown(self) -> None:
        # Remove all documents from the database
        result = self.admin_client.delete_topics(["metadata", "recommendations"], operation_timeout=30)
        # Wait for operation to finish.
        for topic, f in result.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as ex:
                print("Failed to delete topic {}: {}".format(topic, ex))

    def test_base_cases(self):
        result = requests.get(f"{self.recommendation_service_url}/recommendations/default/users/user_1", timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual([{"id": "0", "object_type": "movie"}, {"id": "1", "object_type": "movie"}], result.json())

        result = requests.get(f"{self.recommendation_service_url}/recommendations/default/users/user_2", timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual([{"id": "0", "object_type": "movie"}, {"id": "1", "object_type": "movie"}], result.json())

    def test_no_recommendations(self):
        result = requests.get(f"{self.recommendation_service_url}/recommendations/DOES_NOT_EXIST/users/user_1", timeout=5)
        self.assertFalse(result.ok)

        result = requests.get(f"{self.recommendation_service_url}/recommendations/default/users/DOES_NOT_EXIST", timeout=5)
        self.assertFalse(result.ok)

    def test_switch_recommendations(self):
        result = requests.get(f"{self.recommendation_service_url}/recommendations/default/users/user_1", timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual([{"id": "0", "object_type": "movie"}, {"id": "1", "object_type": "movie"}], result.json())

        self.send_recommendations("model-default-user-user_1", object_type="series")
        time.sleep(10)

        result = requests.get(f"{self.recommendation_service_url}/recommendations/default/users/user_1", timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual([{"id": "0", "object_type": "series"}, {"id": "1", "object_type": "series"}, {"id": "2", "object_type": "series"}], result.json())
