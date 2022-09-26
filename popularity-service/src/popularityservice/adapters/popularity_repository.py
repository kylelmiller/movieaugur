"""The popularity repository"""
# pylint: disable=import-error,no-name-in-module
from abc import ABC
from typing import Optional

from google.protobuf.json_format import Parse

from popularityservice.item_score_pb2 import ItemScores


class AbstractPopularityRepository(ABC):
    """
    Abstract base class for the repository.
    """

    def get(self, object_type: str) -> Optional[ItemScores]:
        """
        Given an object type get the popular items.

        :param object_type: List of ids
        :return: Item scores of the most popular content
        """
        raise NotImplementedError


class RedisPopularityRepository(AbstractPopularityRepository):
    """
    Implementation of the popularity service which uses Redis to store the popularity rankings.
    """

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def get(self, object_type: str) -> Optional[ItemScores]:
        popular_item_scores = self.redis_client.get(f"popular-{object_type}")
        if popular_item_scores is None:
            return None
        return Parse(popular_item_scores, ItemScores())
