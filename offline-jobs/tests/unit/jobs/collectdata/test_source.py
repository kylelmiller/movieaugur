"""
Unit tests for data sources
"""
from unittest import TestCase

from pandas import DataFrame
from user_interaction_pb2 import UserInteraction
from metadata_pb2 import ItemMetadata
from metadata_extractor import AbstractMetadataExtractor
from source import MovieLens100kInteractionSource


class TestAbstractMetadataExtractor(AbstractMetadataExtractor):
    def _get_item_metadata(self, id):
        return ItemMetadata(
            id=id, title="test", objectType="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
        )


class TestMovieLensInteractionSource(MovieLens100kInteractionSource):
    def __init__(
        self, metadata_extractor=TestAbstractMetadataExtractor(), ratings_data=None, movie_data=None, links_data=None
    ):
        if ratings_data == None:
            ratings_data = [
                {"userId": 1, "movieId": 1, "rating": self.RATING_INTERACTION_THRESHOLD, "timestamp": 100},
                {"userId": 1, "movieId": 2, "rating": self.RATING_INTERACTION_THRESHOLD - 0.001, "timestamp": 101},
                {"userId": 2, "movieId": 3, "rating": self.RATING_INTERACTION_THRESHOLD, "timestamp": 102},
            ]

        if movie_data == None:
            movie_data = [{"movieId": 1}, {"movieId": 2}, {"movieId": 3}]

        if links_data == None:
            links_data = [
                {
                    "movieId": 1,
                    "tmdbId": 4,
                },
                {
                    "movieId": 2,
                    "tmdbId": 5,
                },
                {
                    "movieId": 3,
                    "tmdbId": 6,
                },
            ]
        self.data_sets = (ratings_data, movie_data, links_data)
        super().__init__(metadata_extractor)

    def _get_data(self):
        return (DataFrame(data_set) for data_set in (self.data_sets))


class MovieLens100kInteractionSourceTests(TestCase):
    def test_get_interactions_base_case(self):
        source = TestMovieLensInteractionSource()
        self.assertEqual(
            [
                UserInteraction(userId="1", itemId="4", type="Movie", timestamp=100),
                UserInteraction(userId="2", itemId="6", type="Movie", timestamp=102),
            ],
            list(source.get_interactions()),
        )

    def test_get_metadata_base_case(self):
        source = TestMovieLensInteractionSource()
        self.assertCountEqual(
            [
                ItemMetadata(
                    id="4", title="test", objectType="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="5", title="test", objectType="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="6", title="test", objectType="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
            ],
            list(source.get_items_metadata()),
        )
