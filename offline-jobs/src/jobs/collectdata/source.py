"""
Abstract class sources of data
"""
import os
import tempfile
import zipfile
from abc import ABC, abstractmethod
from pandas import DataFrame
from typing import Callable, Generator

import pandas as pd
import requests
from metadata_pb2 import ItemMetadata
from user_interaction_pb2 import UserInteraction
from metadata_extractor import AbstractMetadataExtractor


class AbstractMetadataSource(ABC):
    """
    Manages collecting ids and then returning the metadata for those ids using the metadata extractor.
    """

    def __init__(self, metadata_extractor: AbstractMetadataExtractor = None):
        self.metadata_extractor = metadata_extractor

    @abstractmethod
    def get_items_metadata(self) -> Generator[ItemMetadata, None, None]:
        """
        Gets a stream of item metadata from the source of ids

        :return: Generator of item metadata
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _get_url():
        """
        Gets the url required to collect the id source data

        :return: The url string
        """
        raise NotImplementedError


class AbstractInteractionSource(AbstractMetadataSource):
    """
    Manages collecting ids/user interactions and then returning the metadata for those ids using the metadata extractor.
    """

    @abstractmethod
    def get_interactions(self) -> Generator[UserInteraction, None, None]:
        """
        Gets the user interactions from the source.

        :return: Generator of user interactions
        """
        raise NotImplementedError


class MovieLensInteractionSource(AbstractInteractionSource, ABC):
    """
    Converts MovieLens data using MovieLens ids to TMDB ids
    """

    RATING_INTERACTION_THRESHOLD = 3.5

    def __init__(self, metadata_extractor: AbstractMetadataExtractor):
        super().__init__(metadata_extractor)
        self.ratings_df, self.movies_df, self.links_df = self._get_data()

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

            with open(full_downloaded_path, "wb") as fo:
                fo.write(response.content)
            with zipfile.ZipFile(full_downloaded_path, "r") as zip_reference:
                zip_reference.extractall(base_directory)

        for filename in ("ratings.csv", "movies.csv", "links.csv"):
            yield pd.read_csv(os.path.join(full_extracted_path, filename))

    def get_items_metadata(self) -> Generator[ItemMetadata, None, None]:
        df = (
            self.movies_df.set_index("movieId")
            .join(self.links_df[~self.links_df["tmdbId"].isna()].set_index("movieId"), how="inner")
            .astype({"tmdbId": "str"})
        )
        return self.metadata_extractor.get_items_metadata(set(df["tmdbId"]))

    def get_interactions(self) -> Generator[UserInteraction, None, None]:
        for index, row in (
            self.ratings_df[self.ratings_df["rating"] >= self.RATING_INTERACTION_THRESHOLD]
            .set_index("movieId")
            .join(self.links_df[~self.links_df["tmdbId"].isna()].set_index("movieId"), how="inner")
            .astype({"userId": "str", "tmdbId": "str"})
        ).iterrows():
            yield UserInteraction(userId=row["userId"], itemId=row["tmdbId"], type="Movie", timestamp=row["timestamp"])


class MovieLens100kInteractionSource(MovieLensInteractionSource):
    @staticmethod
    def _get_url():
        return "https://files.grouplens.org/datasets/movielens/ml-100k.zip"


class TMDBPopularMetadataSource(AbstractMetadataSource, ABC):
    def __init__(self, api_key, metadata_extractor: AbstractMetadataExtractor, http_request: Callable = requests.get):
        super().__init__(metadata_extractor)
        self.data = http_request.get(self._get_url() % api_key).json()

    def get_items_metadata(self) -> Generator[ItemMetadata, None, None]:
        return self.metadata_extractor.get_items_metadata([str(result["id"]) for result in self.data["results"]])


class TMDBPopularMovieMetadataSource(TMDBPopularMetadataSource):
    @staticmethod
    def _get_url():
        return "https://api.themoviedb.org/3/movie/popular?api_key=%s"


class TMDBPopularSeriesMetadataSource(TMDBPopularMetadataSource):
    @staticmethod
    def _get_url():
        return "https://api.themoviedb.org/3/tv/popular?api_key=%s"
