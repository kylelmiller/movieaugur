"""
End to End test cases
"""
import os
from argparse import Namespace
from typing import Any, List
from unittest import TestCase

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer

from collect_data import main
from metadata_pb2 import ItemMetadata
from user_interaction_pb2 import UserInteraction


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
        cls.api_key = os.environ["TMDB_API_KEY"]
        cls.kafka_brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
        cls.schema_registry = os.environ.get("SCHEMA_REGISTRY", "http://schema-registry:8082")
        cls.user_interaction_consumer = DeserializingConsumer(
            {
                "group.id": "e2e-tests",
                "default.topic.config": {"auto.offset.reset": "smallest"},
                "bootstrap.servers": cls.kafka_brokers,
                "key.deserializer": StringDeserializer(),
                "value.deserializer": ProtobufDeserializer(UserInteraction, conf={"use.deprecated.format": False}),
            }
        )
        cls.item_metadata_consumer = DeserializingConsumer(
            {
                "group.id": "e2e-tests",
                "default.topic.config": {"auto.offset.reset": "smallest"},
                "bootstrap.servers": cls.kafka_brokers,
                "key.deserializer": StringDeserializer(),
                "value.deserializer": ProtobufDeserializer(ItemMetadata, conf={"use.deprecated.format": False}),
            }
        )

    def test_collect_tmdb_popular_movies(self):
        """
        Tests the tmdb popular movies collection job.

        :return:
        """
        args = Namespace()
        args.collection_type = "tmdb-popular-movies"
        args.tmdb_api_key = self.api_key
        args.kafka_brokers = self.kafka_brokers
        args.schema_registry = self.schema_registry

        response_code = main(args)
        self.item_metadata_consumer.subscribe(["metadata"])
        self.assertEqual(os.EX_OK, response_code)

        item_metadata_messages = list(get_messages(self.item_metadata_consumer))
        self.assertEqual(20, len(item_metadata_messages))

    def test_collect_tmdb_popular_series(self):
        """
        Tests the tmdb popular series collection job.

        :return:
        """
        args = Namespace()
        args.collection_type = "tmdb-popular-series"
        args.tmdb_api_key = self.api_key
        args.kafka_brokers = self.kafka_brokers
        args.schema_registry = self.schema_registry

        response_code = main(args)
        self.item_metadata_consumer.subscribe(["metadata"])
        self.assertEqual(os.EX_OK, response_code)

        item_metadata_messages = list(get_messages(self.item_metadata_consumer))
        self.assertEqual(20, len(item_metadata_messages))

    def test_collect_movielens_100k(self):
        """
        Tests the movielens 100k collection job.

        :return:
        """
        args = Namespace()
        args.collection_type = "movielens-100k"
        args.tmdb_api_key = self.api_key
        args.kafka_brokers = self.kafka_brokers
        args.schema_registry = self.schema_registry

        response_code = main(args)
        self.user_interaction_consumer.subscribe(["user-interaction"])
        self.item_metadata_consumer.subscribe(["metadata"])
        self.assertEqual(os.EX_OK, response_code)

        user_interaction_messages = list(get_messages(self.user_interaction_consumer))
        item_metadata_messages = list(get_messages(self.item_metadata_consumer))
        self.assertGreater(len(list(item_metadata_messages)), 8_000)
        self.assertGreater(len(list(user_interaction_messages)), 50_000)
        self.assertEqual(
            {item_metadata_message.key() for item_metadata_message in item_metadata_messages},
            {user_interaction_message.value().item_id for user_interaction_message in user_interaction_messages},
        )
