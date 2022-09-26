"""The metadata repository"""
# pylint: disable=import-error,no-name-in-module
from abc import ABC
from typing import List, Optional

from grpc import insecure_channel
from popularityservice.item_score_pb2 import ItemScore
from popularityservice.metadata_pb2 import ItemsMetadata, ItemMetadataRequest
from popularityservice.metadata_pb2_grpc import MetadataStub


class AbstractMetadataRepository(ABC):
    """
    Abstract base class for the repository
    """

    def get(self, item_scores: List[ItemScore]) -> Optional[ItemsMetadata]:
        """
        Given a list of item ids get the metadata associated with those ids

        :param item_ids: List of ids
        :param object_type: The object type we want to get the metadata for
        :return: The items metadata
        """
        raise NotImplementedError


class MetadataServiceMetadataRespository(AbstractMetadataRepository):
    """
    Metadata service implementation of the metadata repository.
    """

    def __init__(self, hostname: str):
        self.grpc_stub = MetadataStub(insecure_channel(hostname))

    def get(self, item_scores: List[ItemScore]) -> Optional[ItemsMetadata]:
        return self.grpc_stub.GetMetadata(
            ItemsMetadata(
                item_requests=[
                    ItemMetadataRequest(id=item_score.id, object_type=item_score.object_type) for item_score in item_scores
                ]
            ),
            timeout=1.0,
        )
