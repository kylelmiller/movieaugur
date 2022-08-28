import os
from unittest import TestCase

from sources import TMDBSeriesMetadataSource, TMDBMovieMetadataSource


class TMDBMetadataSourceTestCases(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.api_key = os.environ.get("TMDB_API_KEY")


class TMDBMovieMetadataSourceTestCases(TMDBMetadataSourceTestCases):
    def test_base_case(self):
        self.assertIsNotNone(self.api_key, msg="TMDB_API_KEY environment variable must be set")
        items_metadata = list(TMDBMovieMetadataSource(self.api_key).get_items_metadata(["361743", "335983"]))
        self.assertEqual(
            ["361743", "Top Gun: Maverick", "Movie", "2022-05-24"],
            [
                items_metadata[0].id,
                items_metadata[0].title,
                items_metadata[0].object_type,
                items_metadata[0].release_date,
            ],
        )
        self.assertEqual(
            ["335983", "Venom", "Movie", "2018-09-28"],
            [
                items_metadata[1].id,
                items_metadata[1].title,
                items_metadata[1].object_type,
                items_metadata[1].release_date,
            ],
        )

    def test_does_not_exist(self):
        self.assertIsNotNone(self.api_key, msg="TMDB_API_KEY environment variable must be set")
        self.assertCountEqual([], list(TMDBMovieMetadataSource(self.api_key).get_items_metadata(["-1"])))

    def test_invalid_api_key(self):
        self.assertCountEqual([], list(TMDBMovieMetadataSource("DOES_NOT_EXIST").get_items_metadata(["335983"])))


class TMDBSeriesMetadataSourceTestCases(TMDBMetadataSourceTestCases):
    def test_base_case(self):
        self.assertIsNotNone(self.api_key, msg="TMDB_API_KEY environment variable must be set")
        items_metadata = list(TMDBSeriesMetadataSource(self.api_key).get_items_metadata(["1399", "1398"]))
        self.assertEqual(
            ["1399", "Game of Thrones", "Series", "2011-04-17"],
            [
                items_metadata[0].id,
                items_metadata[0].title,
                items_metadata[0].object_type,
                items_metadata[0].release_date,
            ],
        )
        self.assertEqual(
            ["1398", "The Sopranos", "Series", "1999-01-10"],
            [
                items_metadata[1].id,
                items_metadata[1].title,
                items_metadata[1].object_type,
                items_metadata[1].release_date,
            ],
        )

    def test_does_not_exist(self):
        self.assertIsNotNone(self.api_key, msg="TMDB_API_KEY environment variable must be set")
        self.assertEqual([], list(TMDBSeriesMetadataSource(self.api_key).get_items_metadata(["-1"])))

    def test_invalid_api_key(self):
        self.assertEqual([], list(TMDBSeriesMetadataSource("DOES_NOT_EXIST").get_items_metadata(["1399"])))
