"""
Classes for extracting metadata from an external source.
"""
# pylint: disable=import-error,no-name-in-module
import logging
import multiprocessing
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional

import requests
from jobs.shared.metadata_pb2 import ItemMetadata


TIMEOUT_DURATION = 5


class AbstractMetadataSource(ABC):
    """
    Abstract class source for metadata
    """

    MAXIMUM_CREATORS = 10
    MAXIMUM_CATEGORIES = 10

    def get_items_metadata(self, item_ids: Iterable[str]) -> Generator[ItemMetadata, None, None]:
        """
        Given a list of ids get the metadata for those ids

        :param item_ids: List of ids
        :return: Generator of item metadata
        """
        for item_id in item_ids:
            item_metadata = self._get_item_metadata(item_id)
            if item_metadata:
                yield item_metadata

    @abstractmethod
    def _get_item_metadata(self, item_id: str) -> Optional[ItemMetadata]:
        """
        Given an id, get the item metadata from the underlying source.

        :param item_id: The id of the item we need metadata for
        :return: Returns the item metadata
        """
        raise NotImplementedError


class TMDBMetadataSource(AbstractMetadataSource):
    """
    Class that extracts TMDB metadata
    """

    def __init__(self, api_key: str, asset_url, keyword_url, cast_url, http_request: Callable):
        if not api_key:
            raise ValueError("api_key is required.")
        self.api_key = api_key
        self.asset_url = asset_url
        self.keyword_url = keyword_url
        self.cast_url = cast_url
        self.http_request = http_request

    @staticmethod
    def _get_names(data: List[Dict[str, str]], limit: Optional[int] = None) -> List[str]:
        """
        Gets the keyword from a portion of the TMDB keyword API response.

        :param data: Portion of the Movie keyword API call that requires its names to be extracted
        :return: List of names
        """
        names = [k["name"] for k in data]
        return names if limit is None else names[:limit]

    @abstractmethod
    def _construct_item_metadata(
        self, asset_data: Dict[str, Any], keyword_data: Dict[str, Any], credits_data: Dict[str, Any]
    ) -> ItemMetadata:
        """
        Taking asset, keyword and credits data construct a complete item metadata object

        :param asset_data: TMDB asset data
        :param keyword_data: TMDB Keyword data
        :param credits_data: TMDB Credits data
        :return: Item metadata object
        """
        raise NotImplementedError

    def _get_item_metadata(self, item_id: str) -> Optional[ItemMetadata]:
        """
        Given an item id, return the TMDB movie metadata associated with that id.

        :param item_id: A tmdb movie id
        :return: The items metadata
        """
        try:
            with multiprocessing.Pool(3) as pool:
                asset_response, keyword_response, cast_response = pool.map_async(
                    self.http_request,
                    [url % (item_id, self.api_key) for url in (self.asset_url, self.keyword_url, self.cast_url)],
                ).get(TIMEOUT_DURATION)
        except Exception as ex:
            logging.warning("API call for TMDB asset id %s was not successful: %s", item_id, ex)
            return None

        if not all(response.ok for response in (asset_response, keyword_response, cast_response)):
            logging.warning("API call for TMDB asset id %s was not successful", item_id)
            return None

        return self._construct_item_metadata(asset_response.json(), keyword_response.json(), cast_response.json())


class TMDBMovieMetadataSource(TMDBMetadataSource):
    """
    Class that implements a TMDB movie metadata source
    """

    def __init__(self, api_key: str, http_request: Callable = requests.get):
        super().__init__(
            api_key,
            "https://api.themoviedb.org/3/movie/%s?api_key=%s",
            "https://api.themoviedb.org/3/movie/%s/keywords?api_key=%s",
            "https://api.themoviedb.org/3/movie/%s/credits?api_key=%s",
            http_request,
        )

    def _construct_item_metadata(
        self, asset_data: Dict[str, Any], keyword_data: Dict[str, Any], credits_data: Dict[str, Any]
    ) -> ItemMetadata:
        return ItemMetadata(
            id=str(asset_data["id"]),
            title=asset_data["title"],
            description=asset_data.get("overview", ""),
            object_type="movie",
            release_date=asset_data.get("release_date", ""),
            categories=self._get_names(asset_data.get("genres", []), limit=self.MAXIMUM_CATEGORIES),
            keywords=self._get_names(keyword_data.get("keywords", [])),
            creators=self._get_names(credits_data.get("cast", []), limit=self.MAXIMUM_CREATORS),
        )


class TMDBSeriesMetadataSource(TMDBMetadataSource):
    """
    Class that implements a TMDB series metadata source
    """

    def __init__(self, api_key: str, http_request: Callable = requests.get):
        super().__init__(
            api_key,
            "https://api.themoviedb.org/3/tv/%s?api_key=%s",
            "https://api.themoviedb.org/3/tv/%s/keywords?api_key=%s",
            "https://api.themoviedb.org/3/tv/%s/credits?api_key=%s",
            http_request,
        )

    def _construct_item_metadata(
        self, asset_data: Dict[str, Any], keyword_data: Dict[str, Any], credits_data: Dict[str, Any]
    ) -> ItemMetadata:
        return ItemMetadata(
            id=str(asset_data["id"]),
            title=asset_data["name"],
            description=asset_data.get("overview", ""),
            object_type="series",
            release_date=asset_data.get("first_air_date", ""),
            categories=self._get_names(asset_data.get("genres", []), limit=self.MAXIMUM_CATEGORIES),
            keywords=self._get_names(keyword_data.get("results", [])),
            creators=self._get_names(credits_data.get("cast", []), limit=self.MAXIMUM_CREATORS),
        )
