"""
Repository pattern to abstract the database implementation
"""
# pylint: disable=too-few-public-methods
import abc
from typing import Any, Dict, List

from pymongo.database import Database


class AbstractRepository(abc.ABC):
    """
    Abstract base class for the repository
    """

    def get(self, item_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Given a list of item ids get the metadata associated with those ids

        :param item_ids: List of ids
        :return: List of dictionaries where the dictionary contains the items metadata
        """
        return self._get(item_ids)

    def _get(self, item_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Private get that must be overridden

        :param item_ids:
        :return:
        """
        raise NotImplementedError


class MongoDBRepository(AbstractRepository):
    """
    MongoDB implementation of the metadata repository
    """

    def __init__(self, database: Database):
        """
        Sets member variables

        :param database: Takes the MongoDB database connection
        """
        super().__init__()
        self.database = database

    def _get(self, item_ids: List[str]) -> List[Dict[str, Any]]:
        items = []
        for item in self.database.metadata.find({"_id": {"$in": item_ids}}):
            item["id"] = item.pop("_id")
            items.append(item)
        return items