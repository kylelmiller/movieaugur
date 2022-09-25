import json
import os
import sys
from http import HTTPStatus
from typing import Any, List
from unittest import TestCase

import requests
from confluent_kafka import DeserializingConsumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
from google.protobuf.json_format import ParseDict

from eventservice.entrypoints.flask_app import MAX_ITEMS_PER_POST
from eventservice.user_interaction_pb2 import UserInteraction
from eventservice.adapters.user_interaction_repository import KafkaUserInteractionRepository


KAFKA_CONSUMER = Any
MESSAGE = Any
HEADER = {"Content-Type": "application/json; charset=utf-8"}


class FlaskAppTestCase(TestCase):
    """
    Tests the event service flask api
    """

    @classmethod
    def get_messages(cls) -> List[MESSAGE]:
        """
        Polls the consumer and gets all the new messages.

        :param consumer: Kafka consumer
        :return: LIst of collected messages
        """
        no_messages = 0
        while no_messages < 20:
            msg = cls.user_interaction_consumer.poll(timeout=2)
            if msg is not None:
                yield msg
            else:
                no_messages += 1

    @classmethod
    def setUpClass(cls) -> None:
        cls.event_service_url = os.environ.get("EVENT_SERVICE_URL", "http://event-service:5025")
        cls.kafka_brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")

        cls.admin_client = AdminClient({"bootstrap.servers": cls.kafka_brokers})
        cls.user_interaction_consumer = DeserializingConsumer(
            {
                "group.id": "e2e-tests",
                "default.topic.config": {"auto.offset.reset": "smallest"},
                "bootstrap.servers": cls.kafka_brokers,
                "key.deserializer": StringDeserializer(),
                "value.deserializer": ProtobufDeserializer(UserInteraction, conf={"use.deprecated.format": False}),
            }
        )
        cls.user_interaction_consumer.subscribe([KafkaUserInteractionRepository.TOPIC])

    def request(self, data):
        return requests.post(f"{self.event_service_url}/events", timeout=5, data=json.dumps(data), headers=HEADER)

    def test_base_cases(self):
        # toss all of the pre-existing messages
        self.get_messages()
        user_interaction_data = [
            {"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": 0},
            {"user_id": "2", "item_id": "2", "object_type": "series", "timestamp": 1},
        ]
        result = self.request(user_interaction_data)
        self.assertTrue(result.ok)
        messages = list(self.get_messages())
        self.assertEqual(len(user_interaction_data), len(messages))
        for user_interaction, message in zip(user_interaction_data, messages):
            self.assertIsNone(message.key())
            self.assertEqual(ParseDict(user_interaction, UserInteraction()), message.value())

    def test_too_few_events(self):
        result = self.request([])
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

    def test_too_many_events(self):
        result = self.request(
            [{"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": 0}] * (MAX_ITEMS_PER_POST + 1)
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

    def test_invalid_timestamp(self):
        result = self.request([{"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": sys.maxsize}])
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

    def test_invalid_field_type(self):
        result = self.request([{"user_id": "1", "item_id": "1", "object_type": "not_valid", "timestamp": 0}])
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

        result = self.request([{"user_id": 1, "item_id": "1", "object_type": "movie", "timestamp": 0}])
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

        result = self.request([{"user_id": "1", "item_id": 1, "object_type": "series", "timestamp": 0}])
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

        result = self.request([{"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": "0"}])
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

    def test_missing_field(self):
        valid_user_interaction = {"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": 0}
        for field in ("user_id", "item_id", "object_type", "timestamp"):
            result = self.request([{key: value for key, value in valid_user_interaction.items() if key != field}])
            self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

    def test_field_that_does_not_exist(self):
        result = self.request(
            [{"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": 0, "field_does_not_exist": True}]
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)
