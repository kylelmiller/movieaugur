import os
import time
import unittest
from typing import Dict, List

import grpc
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer
from pymongo import MongoClient

from metadataservice import config
from metadata_pb2 import ItemMetadata, MetadataRequest
from metadata_pb2_grpc import MetadataStub


class TestGetMetadata(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mongo_database = MongoClient(config.get_mongodb_uri(), connectTimeoutMS=500).metadataservice
        cls.grpc_stub = MetadataStub(grpc.insecure_channel(f"{config.get_grpc_host()}:{config.get_grpc_port()}"))

        kafka_brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
        admin_client = AdminClient({"bootstrap.servers": kafka_brokers})
        admin_client.create_topics([NewTopic("metadata", 1, 1)])

        cls.kafka_producer = SerializingProducer(
            {
                "bootstrap.servers": kafka_brokers,
                "key.serializer": StringSerializer(),
                "value.serializer": ProtobufSerializer(
                    ItemMetadata,
                    SchemaRegistryClient({"url": config.get_schema_registry_url()}),
                    {"use.deprecated.format": False},
                ),
            }
        )

    def tearDown(self) -> None:
        # Remove all documents from the database
        self.mongo_database.metadata.delete_many({})

    def add_document_to_mongo(self, document: Dict) -> None:
        self.mongo_database.metadata.insert_one(document)

    def add_item_metadata(self, item_metadata: ItemMetadata) -> None:
        self.kafka_producer.produce(topic="metadata", value=item_metadata)
        self.kafka_producer.flush()

    def get_metadata_from_grpc_service(self, ids: List[str]):
        return self.grpc_stub.GetMetadata(MetadataRequest(ids=ids), timeout=0.5).metadata

    def test_happy_path(self):
        # self.add_document_to_mongo(
        #     {
        #         "_id": "found",
        #         "title": "title",
        #         "description": "description",
        #         "object_type": "Movie",
        #         "categories": ["action", "comedy"],
        #         "creators": ["creator_1", "creator_2"],
        #     }
        # )
        self.add_item_metadata(
            ItemMetadata(
                id="found",
                title="title",
                description="description",
                object_type="Movie",
                categories=["action", "comedy"],
                creators=["creator_1", "creator_2"],
            )
        )
        time.sleep(5)
        self.assertEqual(1, len(self.get_metadata_from_grpc_service(["found"])))

    def test_missing_metadata(self):
        self.assertEqual(0, len(self.get_metadata_from_grpc_service(["not_found"])))

    def test_two_exists_one_missing(self):
        # self.add_document_to_mongo(
        #     {
        #         "_id": "found_1",
        #         "title": "title",
        #         "description": "description",
        #         "object_type": "Movie",
        #         "categories": ["action", "comedy"],
        #         "creators": ["creator_1", "creator_2"],
        #     }
        # )
        # self.add_document_to_mongo(
        #     {
        #         "_id": "found_2",
        #         "title": "title",
        #         "description": "description",
        #         "object_type": "Movie",
        #         "categories": ["action", "comedy"],
        #         "creators": ["creator_1", "creator_2"],
        #     }
        # )
        self.add_item_metadata(
            ItemMetadata(
                id="found_1",
                title="title",
                description="description",
                object_type="Movie",
                categories=["action", "comedy"],
                creators=["creator_1", "creator_2"],
            )
        )
        self.add_item_metadata(
            ItemMetadata(
                id="found_2",
                title="title",
                description="description",
                object_type="Movie",
                categories=["action", "comedy"],
                creators=["creator_1", "creator_2"],
            )
        )
        time.sleep(5)
        self.assertEqual(2, len(self.get_metadata_from_grpc_service(["found_1", "found_2", "not_found"])))


if __name__ == "__main__":
    unittest.main()
