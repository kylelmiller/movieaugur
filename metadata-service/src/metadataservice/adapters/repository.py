"""
Repository pattern to abstract the database implementation
"""
# pylint: disable=too-few-public-methods
import abc
from operator import itemgetter
from typing import Any, Dict, List

import numpy as np
from pymongo.database import Database


class AbstractRepository(abc.ABC):
    """
    Abstract base class for the repository
    """

    def get(self, items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Given a list of item ids get the metadata associated with those ids

        :param item_ids: List of ids
        :param object_type: The object type of the ids
        :return: List of dictionaries where the dictionary contains the items metadata
        """
        return self._get(items)

    def _get(self, items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
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

    def _get(self, items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        def get_key(item):
            return (item["id"], item["object_type"])

        index_map = {get_key(value): index for index, value in enumerate(items)}
        items_metadata = []
        items_index = []
        for item_metadata in self.database["metadata"].find(
            {"$or": [{"$and": [{key: value for key, value in item.items()}]} for item in items]}
        ):
            del item_metadata["_id"]
            items_metadata.append(item_metadata)
            items_index.append(index_map[get_key(item_metadata)])

        # we need to guarantee the order that the items were received and there could be items that were requested
        # that we don't have the metadata for
        return list(itemgetter(*np.argsort(items_index))(items_metadata))
