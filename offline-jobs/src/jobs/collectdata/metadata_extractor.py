"""

"""
import logging
import multiprocessing
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional

import requests
from metadata_pb2 import ItemMetadata


class AbstractMetadataExtractor(ABC):
    """
    Abstract class source for metadata
    """

    MAXIMUM_CREATORS = 10
    MAXIMUM_CATEGORIES = 10

    def get_items_metadata(self, ids: Iterable[str]) -> Generator[Dict[str, Any], None, None]:
        """
        Given a list of ids get the metadata for those ids

        :param ids: List of ids
        :return: Generator of item metadata
        """
        for id in ids:
            item_metadata = self._get_item_metadata(id)
            if item_metadata:
                yield item_metadata

    @abstractmethod
    def _get_item_metadata(self, id: str) -> Optional[ItemMetadata]:
        """
        Given an id, get the item metadata from the underlying source.

        :param id:
        :return:
        """
        raise NotImplementedError


class TMDBMovieMetadataExtractor(AbstractMetadataExtractor):
    """
    Class that implements a TMDB movie metadata source
    """

    TMDB_KEYWORDS_URL = "https://api.themoviedb.org/3/movie/%s/keywords?api_key=%s"
    TMDB_MOVIE_URL = "https://api.themoviedb.org/3/movie/%s?api_key=%s"
    TMDB_CAST_URL = "https://api.themoviedb.org/3/movie/%s/credits?api_key=%s"

    def __init__(self, api_key: str, http_request: Callable = requests.get):
        self.api_key = api_key
        self.http_request = http_request

    @staticmethod
    def _get_names(data: List[Dict[str, str]], limit: Optional[int] = None) -> List[str]:
        """
        Gets the keyword from a portion of the TMDB keyword API response.

        :param keyword_data: Keyword portion of the Movie keyword API call
        :return: List of keyword names
        """
        names = [k["name"] for k in data]
        return names if limit is None else names[:limit]

    def _get_item_metadata(self, id: str) -> Optional[ItemMetadata]:
        """
        Given an item id, return the TMDB movie metadata associated with that id.

        :param id: A tmdb movie id
        :return: The items metadata
        """
        with multiprocessing.Pool(3) as pool:
            movie_response, keyword_response, cast_response = pool.map_async(
                self.http_request,
                [url % (id, self.api_key) for url in (self.TMDB_MOVIE_URL, self.TMDB_KEYWORDS_URL, self.TMDB_CAST_URL)],
            ).get()

        if not all(response.ok for response in (movie_response, keyword_response, cast_response)):
            logging.warning("API call for TMDB movie id %s was not successful", id)
            return None

        movie_data = movie_response.json()

        return ItemMetadata(
            id=str(id),
            title=movie_data["title"],
            description=movie_data.get("overview", ""),
            objectType="Movie",
            releaseDate=movie_data.get("release_date", ""),
            categories=self._get_names(movie_data.get("genres", []), limit=self.MAXIMUM_CATEGORIES),
            keywords=self._get_names(keyword_response.json().get("keywords", [])),
            creators=self._get_names(cast_response.json().get("cast", []), limit=self.MAXIMUM_CREATORS),
        )
