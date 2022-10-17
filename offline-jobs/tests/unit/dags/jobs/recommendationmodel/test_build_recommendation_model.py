"""
Unit tests for data sources
"""
# pylint: disable=import-error,no-name-in-module
import json
from unittest import TestCase
from typing import Any, Generator, List

from jobs.recommendationmodel.build_recommendation_model import build_recommendation_model_with_source_and_sink
from jobs.recommendationmodel.sources import AbstractUserInteractionSource
from jobs.shared.sinks import Sink, UserRecommendations
from jobs.shared.user_interaction_pb2 import UserInteraction


class TestUserInteractionSource(AbstractUserInteractionSource):
    def __init__(self, user_interactions: List[UserInteraction]):
        self.user_interactions = user_interactions

    def get_user_interactions(self) -> Generator[UserInteraction, None, None]:
        return self.user_interactions


class TestSink(Sink):
    def __init__(self):
        self.recommendations = []

    def write(self, value: Any) -> None:
        self.recommendations.append(value)

    def get_results(self):
        return self.recommendations


class TestBuildRecommendationModelWithSources(TestCase):
    """Tests building recommendation model"""

    def test_no_user_interactions(self):
        sink = TestSink()
        self.assertIsNone(build_recommendation_model_with_source_and_sink(TestUserInteractionSource(None), sink, []))
        self.assertEqual(0, len(sink.get_results()))
        self.assertIsNone(build_recommendation_model_with_source_and_sink(TestUserInteractionSource([]), sink, []))
        self.assertEqual(0, len(sink.get_results()))

    def test_build_recommendation_model_with_sources(self):
        """
        Tests creating a recommendation model

        :return:
        """
        user_interactions = []
        users = set()
        for i in range(50):
            for j in range(10):
                user_id = f"user_{i}"
                user_interactions.append(UserInteraction(user_id=user_id, item_id=f"item_{i+j}", object_type="movie"))
                users.add(user_id)
        sink = TestSink()
        build_recommendation_model_with_source_and_sink(TestUserInteractionSource(user_interactions), sink)
        self.assertEqual(users, {result.user_id for result in sink.get_results()})
