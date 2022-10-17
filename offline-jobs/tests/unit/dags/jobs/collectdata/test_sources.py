"""
Unit tests for data sources
"""
# pylint: disable=import-error,no-name-in-module
import json
from unittest import TestCase
from typing import Any

from jobs.collectdata.sources import TMDBMovieMetadataSource
from jobs.shared.metadata_pb2 import ItemMetadata


# pylint: disable=invalid-name
class MockResponse:
    """
    Mock requests response
    """

    def __init__(self, content: str, ok: bool = True):
        self.content = content
        self.ok = ok

    def json(self) -> Any:
        """
        Returns the json parsed content

        :return:
        """
        return json.loads(self.content)


class TestTMDBMovieMetadataSource(TestCase):
    """Tests the tmdb movie metadata source"""

    @staticmethod
    def get_metadata(url):
        """
        Method for overridding the external metadata request and replaces the response with a mock reponse.

        :param url:
        :return:
        """
        if "keywords" in url:
            return MockResponse(json.dumps({"keywords": [{"name": "keyword_1"}, {"name": "keyword_2"}]}))
        elif "credits" in url:
            return MockResponse(json.dumps({"cast": [{"name": "cast_1"}, {"name": "cast_2"}]}))

        return MockResponse(
            json.dumps({"id": 1, "title": "test", "genres": [{"name": "genre_1"}, {"name": "genre_2"}]})
        )

    def test_get_item_metadata(self):
        """
        Tests getting all of an items metadata.

        :return:
        """
        source = TMDBMovieMetadataSource("api_key", self.get_metadata)
        self.assertEqual(
            ItemMetadata(
                id="1",
                title="test",
                object_type="movie",
                categories=["genre_1", "genre_2"],
                keywords=["keyword_1", "keyword_2"],
                creators=["cast_1", "cast_2"],
            ),
            source._get_item_metadata("1"),
        )

    def test_get_names(self):
        """
        Tests extracting the names from a specific metadata calls reponse.

        :return:
        """
        self.assertEqual(
            [f"genre_{i}" for i in range(5)],
            TMDBMovieMetadataSource._get_names([{"name": f"genre_{i}"} for i in range(5)]),
        )

    def test_get_names_with_limit(self):
        """
        Tests extracting the names from a specific metadata calls reponse with a limit.

        :return:
        """
        self.assertEqual(
            [f"genre_{i}" for i in range(5)],
            TMDBMovieMetadataSource._get_names([{"name": f"genre_{i}"} for i in range(6)], limit=5),
        )
