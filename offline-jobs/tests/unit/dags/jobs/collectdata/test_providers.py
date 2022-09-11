"""
Unit tests for provider data sources
"""
#pylint: disable=import-error,no-name-in-module
import json
from unittest import TestCase
from typing import Any

from jobs.collectdata.user_interaction_pb2 import UserInteraction
from jobs.collectdata.metadata_pb2 import ItemMetadata
from jobs.collectdata.providers import MovieLens100kProvider, TMDBPopularMovieProvider, TMDBPopularSeriesProvider
from jobs.collectdata.sources import AbstractMetadataSource
from pandas import DataFrame


class TestMetadataSource(AbstractMetadataSource):
    def _get_item_metadata(self, item_id):
        return ItemMetadata(
            id=item_id, title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
        )


class MockResponse:
    """
    Mock requests response
    """

    #pylint: disable=invalid-name
    def __init__(self, content: str, ok: bool = True):
        self.content = content
        self.ok = ok

    def json(self) -> Any:
        """
        Returns the json parsed content

        :return:
        """
        return json.loads(self.content)


#pylint: disable=unused-argument
def get_ids(url):
    """
    Test http request mock

    :param url: argument that isn't used, but will be passed to this function
    :return: The mock reponse
    """
    return MockResponse(json.dumps({"results": [{"id": i} for i in range(2)]}))


class MovieLens100kProviderTests(TestCase):
    """
    Test case for the movie lens 100k provider
    """
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
            if ratings_data is None:
                ratings_data = [
                    {"userId": 1, "movieId": 1, "rating": self.RATING_INTERACTION_THRESHOLD, "timestamp": 100},
                    {"userId": 1, "movieId": 2, "rating": self.RATING_INTERACTION_THRESHOLD - 0.001, "timestamp": 101},
                    {"userId": 2, "movieId": 3, "rating": self.RATING_INTERACTION_THRESHOLD, "timestamp": 102},
                ]

            if movie_data is None:
                movie_data = [{"movieId": 1}, {"movieId": 2}, {"movieId": 3}]

            if links_data is None:
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
                UserInteraction(user_id="1", item_id="4", type="movie", timestamp=100),
                UserInteraction(user_id="2", item_id="6", type="movie", timestamp=102),
            ],
            list(source.get_interactions()),
        )

    def test_get_metadata_base_case(self):
        source = MovieLens100kProviderTests.TestMovieLensProvider()
        self.assertCountEqual(
            [
                ItemMetadata(
                    id="4", title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="5", title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="6", title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
            ],
            list(source.get_items_metadata()),
        )


class TMDBPopularMovieMetadataProviderTests(TestCase):
    def test_get_items_metadata(self):
        provider = TMDBPopularMovieProvider("api_key", TestMetadataSource(), get_ids)
        self.assertEqual(
            [
                ItemMetadata(
                    id="0", title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="1", title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
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
                    id="0", title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
                ItemMetadata(
                    id="1", title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
                ),
            ],
            list(provider.get_items_metadata()),
        )
