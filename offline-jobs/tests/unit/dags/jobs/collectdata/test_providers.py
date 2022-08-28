"""
Unit tests for data sources
"""
import json
from unittest import TestCase
from typing import Any

from pandas import DataFrame
from user_interaction_pb2 import UserInteraction
from metadata_pb2 import ItemMetadata
from providers import MovieLens100kProvider, TMDBPopularMovieProvider, TMDBPopularSeriesProvider
from sources import AbstractMetadataSource


class TestMetadataSource(AbstractMetadataSource):
    def _get_item_metadata(self, id):
        return ItemMetadata(
            id=id, title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
        )


class MockResponse:
    """
    Mock requests response
    """

    def __init__(self, content: str, ok: bool = True):
        self.content = content
        self.ok = ok

    def json(self) -> Any:
        return json.loads(self.content)


def get_ids(url):
    return MockResponse(json.dumps({"results": [{"id": i} for i in range(2)]}))


class MovieLens100kProviderTests(TestCase):
    class TestMovieLensProvider(MovieLens100kProvider):
        """
        Test MovieLens Interaction source which overrides calls to external dependencies
        """

        def __init__(
            self,
            metadata_source=TestMetadataSource(),
            ratings_data=None,
            movie_data=None,
            links_data=None,
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
            super().__init__(metadata_source)

        def _get_data(self):
            return (DataFrame(data_set) for data_set in (self.data_sets))

    def test_get_interactions_base_case(self):
        source = MovieLens100kProviderTests.TestMovieLensProvider()
        self.assertEqual(
            [
                UserInteraction(user_id="1", item_id="4", type="Movie", timestamp=100),
                UserInteraction(user_id="2", item_id="6", type="Movie", timestamp=102),
            ],
            list(source.get_interactions()),
        )

    def test_get_metadata_base_case(self):
        source = MovieLens100kProviderTests.TestMovieLensProvider()
        self.assertCountEqual(
            [
                ItemMetadata(
                    id="4", title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="5", title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="6", title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
            ],
            list(source.get_items_metadata()),
        )


class TMDBPopularMovieMetadataProviderTests(TestCase):
    def test_get_items_metadata(self):
        provider = TMDBPopularMovieProvider("", TestMetadataSource(), get_ids)
        self.assertEqual(
            [
                ItemMetadata(
                    id="0", title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="1", title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
            ],
            list(provider.get_items_metadata()),
        )


class TMDBPopularSeriesMetadataProviderTests(TestCase):
    def test_get_items_metadata(self):
        provider = TMDBPopularSeriesProvider("", TestMetadataSource(), get_ids)
        self.assertEqual(
            [
                ItemMetadata(
                    id="0", title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="1", title="test", object_type="Movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
            ],
            list(provider.get_items_metadata()),
        )
