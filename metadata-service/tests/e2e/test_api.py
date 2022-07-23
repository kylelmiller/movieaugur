import unittest
from typing import Dict, List

import grpc
from pymongo import MongoClient

from metadataservice import config
from metadata_pb2 import MetadataRequest
from metadata_pb2_grpc import MetadataStub


class TestGetMetadata(unittest.TestCase):
    def __init__(self, method_name="runTest"):
        super().__init__(methodName=method_name)
        self.mongo_database = MongoClient(config.get_mongodb_uri(), connectTimeoutMS=500).metadataservice
        self.grpc_stub = MetadataStub(grpc.insecure_channel(f"{config.get_grpc_host()}:{config.get_grpc_port()}"))

    def tearDown(self) -> None:
        # Remove all documents from the database
        self.mongo_database.metadata.delete_many({})

    def add_document_to_mongo(self, document: Dict) -> None:
        self.mongo_database.metadata.insert_one(document)

    def get_metadata_from_grpc_service(self, ids: List[str]):
        return self.grpc_stub.GetMetadata(MetadataRequest(ids=ids), timeout=0.5).metadata

    def test_happy_path(self):
        self.add_document_to_mongo(
            {
                "_id": "found",
                "title": "title",
                "description": "description",
                "objectType": "Movie",
                "categories": ["action", "comedy"],
                "creators": ["creator_1", "creator_2"],
            }
        )

        self.assertEqual(1, len(self.get_metadata_from_grpc_service(["found"])))

    def test_missing_metadata(self):
        self.assertEqual(0, len(self.get_metadata_from_grpc_service(["not_found"])))

    def test_two_exists_one_missing(self):
        self.add_document_to_mongo(
            {
                "_id": "found_1",
                "title": "title",
                "description": "description",
                "objectType": "Movie",
                "categories": ["action", "comedy"],
                "creators": ["creator_1", "creator_2"],
            }
        )
        self.add_document_to_mongo(
            {
                "_id": "found_2",
                "title": "title",
                "description": "description",
                "objectType": "Movie",
                "categories": ["action", "comedy"],
                "creators": ["creator_1", "creator_2"],
            }
        )
        self.assertEqual(2, len(self.get_metadata_from_grpc_service(["found_1", "found_2", "not_found"])))


if __name__ == "__main__":
    unittest.main()
