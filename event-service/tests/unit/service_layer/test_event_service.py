"""Unit tests for the event service layer"""
# pylint: disable=import-error,no-name-in-module,unused-argument
import sys
from unittest import TestCase
from typing import Dict, Union

from google.protobuf.json_format import ParseError

from eventservice.user_interaction_pb2 import UserInteraction
from eventservice.adapters.user_interaction_repository import AbstractUserInteractionRepository
from eventservice.service_layer.event_service import EventService


class TestUserInteractionRespository(AbstractUserInteractionRepository):
    """
    Test implementation of the user interaction repository
    """

    def __init__(self):
        self.committed = False
        self.user_interactions = []

    def write(self, user_interaction: Dict[str, Union[str, int]]) -> None:
        self.user_interactions.append(user_interaction)

    def _commit(self) -> None:
        self.committed = True


class EventServiceTestCase(TestCase):
    """
    Tests the event service layer.
    """

    def test_multiple_user_interactions(self):
        """
        Tests when there are multiple user interactions.

        :return:
        """
        repository = TestUserInteractionRespository()
        event_service = EventService(repository)
        user_interaction_data = [
            {"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": 0},
            {"user_id": "2", "item_id": "2", "object_type": "series", "timestamp": 1},
        ]
        event_service.write_user_interactions(user_interaction_data)
        self.assertEqual([UserInteraction(**kwargs) for kwargs in user_interaction_data], repository.user_interactions)
        self.assertTrue(repository.committed)

    def test_invalid_timestamp(self):
        """
        Tests when the timestamp fails validation

        :return:
        """
        repository = TestUserInteractionRespository()
        event_service = EventService(repository)
        self.assertRaises(
            ValueError,
            lambda: event_service.write_user_interactions(
                [{"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": sys.maxsize}]
            ),
        )
        self.assertTrue(repository.committed)

    def test_invalid_dictionary_field(self):
        """
        Tests when there is an extra field.

        :return:
        """
        repository = TestUserInteractionRespository()
        event_service = EventService(repository)
        self.assertRaises(
            ParseError,
            lambda: event_service.write_user_interactions(
                [{"user_id": "1", "item_id": "1", "object_type": "movie", "timestamp": 0, "not_a_field": 0}]
            ),
        )
        self.assertTrue(repository.committed)
