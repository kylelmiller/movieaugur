"""Unit tests for the popularity service layer"""
# pylint: disable=import-error,no-name-in-module,unused-argument
from unittest import TestCase
from typing import Dict, List, Optional

from popularityservice.item_score_pb2 import ItemScore, ItemScores
from popularityservice.metadata_pb2 import ItemMetadata, ItemsMetadata
from popularityservice.adapters.metadata_repository import AbstractMetadataRepository
from popularityservice.adapters.popularity_repository import AbstractPopularityRepository
from popularityservice.service_layer.popularity_service import PopularityService


class TestMetadataRespository(AbstractMetadataRepository):
    """
    Test implementation of the metadata repository
    """

    def __init__(self, metadata: Dict[str, ItemsMetadata] = None):
        self.metadata = metadata or {}

    def get(self, item_ids: List[str], object_type: str) -> Optional[ItemsMetadata]:
        """
        Gets the item metadata from the local metadata store

        :param item_ids: Unused in the test implementation
        :param object_type: The key of the metadata dictionary
        :return: The items metadata
        """
        return self.metadata.get(object_type)


class TestPopularityRepository(AbstractPopularityRepository):
    """
    Test implementation of the popularity repository
    """

    def __init__(self, popularity: Dict[str, ItemScores] = None):
        self.popularity = popularity or {}

    def get(self, object_type: str) -> Optional[ItemScores]:
        """
        Given the key returns the item scores.

        :param object_type: Key of the popularity store
        :return:
        """
        return self.popularity.get(object_type)


class PopularityServiceTestCase(TestCase):
    """
    Tests the popularity service layer.
    """

    def test_no_popularity(self):
        """
        Tests when there is no popularity for a given object type.

        :return:
        """
        popularity_service = PopularityService(TestPopularityRepository(), TestMetadataRespository())
        self.assertIsNone(popularity_service.get_popularity("movie"))

    def test_no_metadata(self):
        """
        Tests when there is popularity for a given object type, but no metadata.

        :return:
        """
        popularity_service = PopularityService(
            TestPopularityRepository(
                popularity={"movie": ItemScores(item_scores=[ItemScore(id="1", object_type="movie")])}
            ),
            TestMetadataRespository(),
        )
        self.assertIsNone(popularity_service.get_popularity("movie"))

    def test_popularity_and_metadata(self):
        """
        Tests when there is popularity and metadata for a given object type.

        :return:
        """
        metadata = ItemsMetadata(metadata=[ItemMetadata(id="1", object_type="movie")])
        popularity_service = PopularityService(
            TestPopularityRepository(
                popularity={"movie": ItemScores(item_scores=[ItemScore(id="1", object_type="movie")])}
            ),
            TestMetadataRespository(metadata={"movie": metadata}),
        )
        self.assertEqual(metadata, popularity_service.get_popularity("movie"))
