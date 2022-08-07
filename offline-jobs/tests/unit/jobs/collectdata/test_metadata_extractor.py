import json
from unittest import TestCase
from typing import Any

from metadata_extractor import TMDBMovieMetadataExtractor
from metadata_pb2 import ItemMetadata


class MockResponse:
    """
    Mock requests repsonse
    """

    def __init__(self, content: str, ok: bool = True):
        self.content = content
        self.ok = ok

    def json(self) -> Any:
        return json.loads(self.content)


class TestTMDBMovieMetadataExtractor(TestCase):
    """ """

    @staticmethod
    def get_metadata(url):
        if "keywords" in url:
            return MockResponse(json.dumps({"keywords": [{"name": "keyword_1"}, {"name": "keyword_2"}]}))
        elif "credits" in url:
            return MockResponse(json.dumps({"cast": [{"name": "cast_1"}, {"name": "cast_2"}]}))

        return MockResponse(
            json.dumps({"id": 1, "title": "test", "genres": [{"name": "genre_1"}, {"name": "genre_2"}]})
        )

    def test_get_item_metadata(self):
        extractor = TMDBMovieMetadataExtractor("", self.get_metadata)
        self.assertEqual(
            ItemMetadata(
                id="1",
                title="test",
                objectType="Movie",
                categories=["genre_1", "genre_2"],
                keywords=["keyword_1", "keyword_2"],
                creators=["cast_1", "cast_2"],
            ),
            extractor._get_item_metadata("1"),
        )

    def test_get_names(self):
        self.assertEqual(
            [f"genre_{i}" for i in range(5)],
            TMDBMovieMetadataExtractor._get_names([{"name": f"genre_{i}"} for i in range(5)]),
        )

    def test_get_names_with_limit(self):
        self.assertEqual(
            [f"genre_{i}" for i in range(5)],
            TMDBMovieMetadataExtractor._get_names([{"name": f"genre_{i}"} for i in range(6)], limit=5),
        )
