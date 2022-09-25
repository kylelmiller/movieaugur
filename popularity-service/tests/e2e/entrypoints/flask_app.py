import os
from unittest import TestCase

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer
from google.protobuf.json_format import MessageToJson

from popularityservice import config
from popularityservice.item_score_pb2 import ItemScore, ItemScores
from popularityservice.metadata_pb2 import ItemMetadata


class FlaskAppTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.popularity_service_url = os.environ.get("POPULARITY_SERVICE_URL", "http://popularity-service:5015")
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
            }
        )

        cls.kafka_popularity_producer = SerializingProducer(
            {
                "bootstrap.servers": kafka_brokers,
                "key.serializer": StringSerializer(),
                "value.serializer": StringSerializer(),
            }
        )

    def setUp(self) -> None:
        self.admin_client.create_topics([NewTopic("metadata", 1, 1), NewTopic("popularity", 1, 1)])

        self.kafka_popularity_producer.produce(
            "popularity",
            key="popular-movie",
            value=MessageToJson(
                ItemScores(
                    item_scores=[
                        ItemScore(id="0", object_type="movie"),
                        ItemScore(id="1", object_type="movie"),
                        ItemScore(id="2", object_type="movie"),
                    ]
                )
            ),
        )

        self.kafka_popularity_producer.produce(
            "popularity",
            key="popular-series",
            value=MessageToJson(
                ItemScores(
                    item_scores=[
                        ItemScore(id="1", object_type="series"),
                        ItemScore(id="2", object_type="series"),
                    ]
                )
            ),
        )
        self.kafka_popularity_producer.poll(0)
        self.kafka_popularity_producer.flush(timeout=5)

        for id, object_type in (("0", "movie"), ("1", "movie"), ("0", "series"), ("1", "series"), ("2", "series")):
            self.kafka_metadata_producer.produce(
                topic="metadata", key=f"#ID#{id}#TYPE#{object_type}", value=ItemMetadata(id=id, object_type=object_type)
            )
        self.kafka_metadata_producer.poll(0)
        self.kafka_metadata_producer.flush(timeout=5)

    def tearDown(self) -> None:
        # Remove all documents from the database
        result = self.admin_client.delete_topics(["metadata", "popularity"], operation_timeout=30)
        # Wait for operation to finish.
        for topic, f in result.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as ex:
                print("Failed to delete topic {}: {}".format(topic, ex))

    def test_base_cases(self):
        result = requests.get(f"{self.popularity_service_url}/popularity/movie", timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual([{"id": "0", "object_type": "movie"}, {"id": "1", "object_type": "movie"}], result.json())

        result = requests.get(f"{self.popularity_service_url}/popularity/series", timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual([{"id": "1", "object_type": "series"}, {"id": "2", "object_type": "series"}], result.json())

    def test_no_object_type(self):
        result = requests.get(f"{self.popularity_service_url}/popularity/DOES_NOT_EXIST", timeout=5)
        self.assertFalse(result.ok)
        self.assertEqual(404, result.status_code)
