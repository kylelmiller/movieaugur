"""Service for the popularity business logic"""
# pylint: disable=import-error,no-name-in-module
from typing import Optional

from recommendationservice.adapters.metadata_repository import AbstractMetadataRepository
from recommendationservice.adapters.recommendation_repository import AbstractRecommendationRepository
from recommendationservice.metadata_pb2 import ItemsMetadata


class RecommendationService:
    """
    Service which provides recommendations models results for a given user
    """

    def __init__(
        self,
        recommendation_repository: AbstractRecommendationRepository,
        metadata_repository: AbstractMetadataRepository,
    ):
        self.recommendation_repository = recommendation_repository
        self.metadata_repository = metadata_repository

    def get_recommendations(self, model_name: str, user_id: str) -> Optional[ItemsMetadata]:
        """
        Given the name of the recommendation model and a user id, return the metadata for the recommended items.

        :param model_name:
        :param user_id:
        :return: Item metadata if the recommendations exist
        """
        recommended_item_scores = self.recommendation_repository.get(model_name, user_id)
        if recommended_item_scores is None:
            return None
        return self.metadata_repository.get(recommended_item_scores.item_scores)
