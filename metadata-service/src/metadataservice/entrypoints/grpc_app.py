"""
GRPC metadata service implementations
"""
import grpc
from grpc import ServicerContext
from concurrent import futures

from metadataservice import config
from metadataservice.bootstrap import bootstrap
from metadataservice.adapters.repository import AbstractRepository
from metadata_pb2 import ItemMetadata, ItemsMetadata, MetadataRequest
from metadata_pb2_grpc import MetadataServicer, add_MetadataServicer_to_server


class MetadataService(MetadataServicer):
    """
    Makes metadata service functions available
    """

    def __init__(self, repository: AbstractRepository):
        self.repository = repository

    def GetMetadata(self, request: MetadataRequest, context: ServicerContext) -> ItemsMetadata:
        """
        Takes a metadata request of ids and returns the metadata for that request

        :param MetadataRequest: the metadata request which contains a list of ids
        :param context: The request context
        :return: The metadata for the found ids
        """
        if len(request.ids) == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Ids are required.")

        return ItemsMetadata(
            metadata=[
                ItemMetadata(**item_data) for item_data in self.repository.get(list(request.ids), request.object_type)
            ]
        )


def main():
    """
    Sets up the GRPC service
    :return:
    """
    repostory = bootstrap(config)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=config.get_max_workers()), options=(("grpc.so_reuseport", 1),)
    )
    add_MetadataServicer_to_server(MetadataService(repostory), server)
    server.add_insecure_port(f"[::]:{config.get_grpc_port()}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
