import os
from unittest import TestCase

from providers import MovieLens100kProvider, TMDBPopularMovieProvider, TMDBPopularSeriesProvider
from sources import AbstractMetadataSource

from metadata_pb2 import ItemMetadata


class TestMetadataSource(AbstractMetadataSource):
    def _get_item_metadata(self, id):
        return ItemMetadata(
            id=id, title="test", object_type="movie", categories=["Drama", "Comedy"], creators=["Test", "Test"]
        )


class MovieLens100kProviderTestCase(TestCase):
    def test_base_case(self):
        items_metadata = MovieLens100kProvider(TestMetadataSource()).get_items_metadata()
        user_interactions = MovieLens100kProvider(TestMetadataSource()).get_interactions()
        self.assertGreater(len(list(items_metadata)), 8_000)
        self.assertGreater(len(list(user_interactions)), 50_000)
        self.assertCountEqual(items_metadata, {user_interaction.item_id for user_interaction in user_interactions})


class TMDBProviderTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.api_key = os.environ.get("TMDB_API_KEY")


class TMDBPopularMovieProviderTestCase(TMDBProviderTestCase):
    def test_get_items_metadata(self):
        self.assertIsNotNone(self.api_key, msg="TMDB_API_KEY environment variable must be set")
        self.assertEqual(
            20, len(list(TMDBPopularMovieProvider(self.api_key, TestMetadataSource()).get_items_metadata()))
        )

    def test_invalid_api_key(self):
        self.assertEqual(
            [], list(TMDBPopularSeriesProvider("DOES_NOT_EXIST", TestMetadataSource()).get_items_metadata())
        )


class TMDBPopularSeriesProviderTestCase(TMDBProviderTestCase):
    def test_get_items_metadata(self):
        self.assertIsNotNone(self.api_key, msg="TMDB_API_KEY environment variable must be set")
        self.assertEqual(
            20, len(list(TMDBPopularSeriesProvider(self.api_key, TestMetadataSource()).get_items_metadata()))
        )

    def test_invalid_api_key(self):
        self.assertEqual(
            [], list(TMDBPopularSeriesProvider("DOES_NOT_EXIST", TestMetadataSource()).get_items_metadata())
        )
