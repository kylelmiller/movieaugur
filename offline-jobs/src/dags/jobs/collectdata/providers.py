"""
Provides user interaction data and item ids from various data sources. The specific sources are tightly coupled to
specific provider by id space, but by separating them we ensure they are independently testable. Providers have ids
but need to be able to collect the metadata which is what the metadata sources provide.
"""
# pylint: disable=import-error,no-name-in-module
import os
import tempfile
import zipfile
from abc import ABC, abstractmethod
from typing import Callable, Generator, Iterable

import pandas as pd
import requests
from pandas import DataFrame
from jobs.collectdata.item_score_pb2 import ItemScore, ItemScores
from jobs.collectdata.metadata_pb2 import ItemMetadata
from jobs.collectdata.user_interaction_pb2 import UserInteraction
from jobs.collectdata.sources import AbstractMetadataSource


class AbstractMetadataProvider(ABC):
    """
    Manages collecting ids and then returning the metadata for those ids using the metadata source.
    """

    def __init__(self, metadata_source: AbstractMetadataSource = None):
        self.ids = None
        self.metadata_source = metadata_source

    @abstractmethod
    def get_items_metadata(self) -> Generator[ItemMetadata, None, None]:
        """
        Gets a stream of item metadata from the source of ids

        :return: Generator of item metadata
        """
        raise NotImplementedError


class AbstractInteractionProvider(AbstractMetadataProvider):
    """
    Manages collecting ids/user interactions and then returning the metadata for those ids using the metadata source.
    """

    @abstractmethod
    def get_interactions(self) -> Generator[UserInteraction, None, None]:
        """
        Gets the user interactions from the source.

        :return: Generator of user interactions
        """
        raise NotImplementedError


class MovieLensProvider(AbstractInteractionProvider, ABC):
    """
    Converts MovieLens data using MovieLens ids to TMDB ids
    """

    RATING_INTERACTION_THRESHOLD = 3.5

    def __init__(self, metadata_source: AbstractMetadataSource):
        super().__init__(metadata_source)
        self.ratings_df, self.movies_df, self.links_df = self._get_data()
        self.ratings_df = (
            self.ratings_df[self.ratings_df["rating"] >= self.RATING_INTERACTION_THRESHOLD]
            .set_index("movieId")
            .astype({"userId": "str"})
        )
        self.movies_df = self.movies_df.set_index("movieId")
        self.links_df = (
            self.links_df[~self.links_df["tmdbId"].isna()]
            .set_index("movieId")
            .astype({"tmdbId": "int"})
            .astype({"tmdbId": "str"})
        )

    @staticmethod
    @abstractmethod
    def _get_url():
        """
        Gets the url required to collect the id source data

        :return: The url string
        """
        raise NotImplementedError

    def _get_data(self) -> Generator[DataFrame, None, None]:
        """
        Extracts the data from the url payload which in the movielens interactions case is a zip payload with csv files.

        :return: Dataframes containing the interaction, movie and movie id translation data
        """
        filename = self._get_url().split("/")[-1]
        extracted_directory_name = filename[: -len(".zip")]
        base_directory = tempfile.gettempdir()
        full_downloaded_path = os.path.join(base_directory, filename)
        full_extracted_path = os.path.join(base_directory, extracted_directory_name)

        # Download the movielens data if it doesn't exist
        if not os.path.exists(full_extracted_path):
            response = requests.get(self._get_url())

            with open(full_downloaded_path, "wb") as file_out:
                file_out.write(response.content)
            with zipfile.ZipFile(full_downloaded_path, "r") as zip_reference:
                zip_reference.extractall(base_directory)

        for filename in ("ratings.csv", "movies.csv", "links.csv"):
            yield pd.read_csv(os.path.join(full_extracted_path, filename))

    def get_items_metadata(self) -> Generator[ItemMetadata, None, None]:
        df = self.movies_df.join(self.links_df, how="inner").astype({"tmdbId": "str"})
        return self.metadata_source.get_items_metadata(set(df["tmdbId"]))

    def get_interactions(self) -> Generator[UserInteraction, None, None]:
        for _, row in (self.ratings_df.join(self.links_df, how="inner")).iterrows():
            yield UserInteraction(
                user_id=row["userId"], item_id=row["tmdbId"], type="movie", timestamp=row["timestamp"]
            )


class MovieLens100kProvider(MovieLensProvider):
    """
    Provides the MovieLens 100k dataset
    """

    @staticmethod
    def _get_url():
        return "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"


class TMDBPopularContentProvider(AbstractMetadataProvider, ABC):
    """
    Base class for providing tmdb popularity data.
    """

    def __init__(self, api_key, metadata_source: AbstractMetadataSource, http_request: Callable = requests.get):
        super().__init__(metadata_source)
        self.data = http_request(self._get_url() % api_key).json()

    @staticmethod
    @abstractmethod
    def _get_url():
        """
        Gets the url required to collect the id source data.

        :return: The url string
        """
        raise NotImplementedError

    def _get_item_ids(self) -> Iterable[str]:
        """
        Gets the item ids for all the popular items.

        :return: List of item ids
        """
        if self.ids is None:
            self.ids = [str(result["id"]) for result in self.data.get("results", [])]
        return self.ids

    @abstractmethod
    def get_item_scores(self) -> ItemScores:
        """
        Returns the item scores for the popularity.

        :return:
        """
        raise NotImplementedError()

    def get_items_metadata(self) -> Generator[ItemMetadata, None, None]:
        return self.metadata_source.get_items_metadata(self._get_item_ids())


class TMDBPopularMovieProvider(TMDBPopularContentProvider):
    """
    Provides popular movie ids in the tmdb id space.
    """

    def get_item_scores(self) -> ItemScores:
        return ItemScores(item_scores=[ItemScore(id=id, object_type="movie") for id in self._get_item_ids()])

    @staticmethod
    def _get_url():
        return "https://api.themoviedb.org/3/movie/popular?api_key=%s"


class TMDBPopularSeriesProvider(TMDBPopularContentProvider):
    """
    Provides popular series ids in the tmdb id space.
    """

    def get_item_scores(self) -> ItemScores:
        return ItemScores(item_scores=[ItemScore(id=id, object_type="series") for id in self._get_item_ids()])

    @staticmethod
    def _get_url():
        return "https://api.themoviedb.org/3/tv/popular?api_key=%s"
