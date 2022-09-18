"""The metadata repository"""
# pylint: disable=import-error,no-name-in-module
from abc import ABC
from typing import List, Optional

from grpc import insecure_channel
from popularityservice.metadata_pb2 import ItemsMetadata, MetadataRequest
from popularityservice.metadata_pb2_grpc import MetadataStub


class AbstractMetadataRepository(ABC):
    """
    Abstract base class for the repository
    """

    def get(self, item_ids: List[str], object_type: str) -> Optional[ItemsMetadata]:
        """
        Given a list of item ids get the metadata associated with those ids

        :param item_ids: List of ids
        :param object_type: The object type we want to get the metadata for
        :return: List of dictionaries where the dictionary contains the items metadata
        """
        raise NotImplementedError


class MetadataServiceMetadataRespository(AbstractMetadataRepository):
    """
    Metadata service implementation of the metadata repository.
    """

    def __init__(self, hostname: str):
        self.grpc_stub = MetadataStub(insecure_channel(hostname))

    def get(self, item_ids: List[str], object_type: str) -> Optional[ItemsMetadata]:
        return self.grpc_stub.GetMetadata(MetadataRequest(ids=item_ids, object_type=object_type), timeout=1.0)
