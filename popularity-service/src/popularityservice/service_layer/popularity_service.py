"""Service for the popularity business logic"""
# pylint: disable=import-error,no-name-in-module
from typing import Optional

from popularityservice.adapters.metadata_repository import AbstractMetadataRepository
from popularityservice.adapters.popularity_repository import AbstractPopularityRepository
from popularityservice.metadata_pb2 import ItemsMetadata


class PopularityService:
    """
    Service which provides a given type of contents currently popular items metadata
    """

    def __init__(
        self, popularity_repository: AbstractPopularityRepository, metadata_repository: AbstractMetadataRepository
    ):
        self.popularity_repository = popularity_repository
        self.metadata_repository = metadata_repository

    def get_popularity(self, object_type: str) -> Optional[ItemsMetadata]:
        """
        Given an object type get a list of the popular ids. Then collect the metadata for those ids.

        :param object_type: The object type for the requested popularity
        :return: Item metadata if the popularity exists
        """
        popular_item_scores = self.popularity_repository.get(object_type)
        if popular_item_scores is None:
            return None
        return self.metadata_repository.get(popular_item_scores.item_scores)
