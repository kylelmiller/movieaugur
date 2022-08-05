"""
Abstract class sources of data
"""
import os
import tempfile
import zipfile
from abc import ABC, abstractmethod
from typing import Generator

import pandas as pd
import requests
from metadata_pb2 import ItemMetadata
from user_interaction_pb2 import UserInteraction
from metadata_extractor import AbstractMetadataExtractor


class AbstractMetadataSource(ABC):
    """ """

    @abstractmethod
    def get_items_metadata(self) -> Generator[ItemMetadata, None, None]:
        """


        :return:
        """
        raise NotImplementedError


class AbstractInteractionSource(AbstractMetadataSource):
    """
    Abstract class source for interactions
    """

    @abstractmethod
    def get_interactions(self) -> UserInteraction:
        """


        :return:
        """
        raise NotImplementedError


class MovieLensInteractionSource(AbstractInteractionSource):
    """
    Converts MovieLens data using MovieLens ids to TMDB ids
    """

    RATING_INTERACTION_THRESHOLD = 3.5

    def __init__(self, metadata_extractor: AbstractMetadataExtractor):
        self.metadata_extractor = metadata_extractor
        self.ratings_df, self.movies_df, self.links_df = self._get_data()

    @abstractmethod
    def _get_url(self):
        raise NotImplementedError

    def _get_data(self):
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
        """
        Gets all the metadata associated with the movies in the provided dataset

        :return:
        """
        df = (
            self.movies_df.set_index("movieId")
            .join(self.links_df[~self.links_df["tmdbId"].isna()].set_index("movieId"), how="inner")
            .astype({"tmdbId": "str"})
        )
        return self.metadata_extractor.get_items_metadata(set(df["tmdbId"]))

    def get_interactions(self) -> Generator[UserInteraction, None, None]:
        """
        Returns the user/item interactions for the given dataset

        :return:
        """
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
