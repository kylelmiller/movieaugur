"""The popularity repository"""
# pylint: disable=import-error,no-name-in-module
from abc import ABC
from typing import Optional

from google.protobuf.json_format import Parse

from popularityservice.item_score_pb2 import ItemScores


class AbstractRecommendationRepository(ABC):
    """
    Abstract base class for the repository.
    """

    def get(self, model_name: str, user_id: str) -> Optional[ItemScores]:
        """
        Given a model name and user id return the recommended item scores..

        :param model_name: The name of the recommendation model
        :param user_id: The id of the user we want the recommendation from
        :return: Item scores of the most popular content
        """
        raise NotImplementedError


class RedisRecommendationRepository(AbstractRecommendationRepository):
    """
    Implementation of the recommendation service which uses Redis to store the recommendation model results.
    """

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def get(self, model_name: str, user_id: str) -> Optional[ItemScores]:
        recommended_item_scores = self.redis_client.get("-".join(["model", model_name, "user", user_id]))
        if recommended_item_scores is None:
            return None
        return Parse(recommended_item_scores, ItemScores())
