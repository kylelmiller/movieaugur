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
from metadata_pb2 import ItemMetadata, ItemMetadataRequest, ItemsMetadataRequest
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
                "linger.ms": 500,
            }
        )

    def tearDown(self) -> None:
        # Remove all documents from the database
        for collection_name in self.mongo_database.list_collection_names():
            self.mongo_database[collection_name].delete_many({})

    def add_item_metadata(self, item_metadata: ItemMetadata) -> None:
        self.kafka_producer.produce(
            topic=f"metadata",
            key="-".join(["id", item_metadata.id, "type", item_metadata.object_type]),
            value=item_metadata,
        )
        self.kafka_producer.poll(0)
        self.kafka_producer.flush()

    def get_metadata_from_grpc_service(self, item_requests: List[Dict[str, str]]):
        return self.grpc_stub.GetMetadata(
            ItemsMetadataRequest(item_requests=[ItemMetadataRequest(**item_request) for item_request in item_requests]),
            timeout=1.0,
        ).metadata

    def test_happy_path(self):
        movie_item_metadata = ItemMetadata(
            id="found",
            title="title",
            description="description",
            object_type="movie",
            categories=["action", "comedy"],
            creators=["creator_1", "creator_2"],
        )
        self.add_item_metadata(movie_item_metadata)

        series_item_metadata = ItemMetadata(
            id="found",
            title="title",
            description="description",
            object_type="series",
            categories=["action", "comedy"],
            creators=["creator_1", "creator_2"],
        )
        self.add_item_metadata(series_item_metadata)

        time.sleep(5)
        self.assertEqual(
            [movie_item_metadata, series_item_metadata],
            list(
                self.get_metadata_from_grpc_service(
                    [{"id": "found", "object_type": "movie"}, {"id": "found", "object_type": "series"}]
                )
            ),
        )
        self.assertEqual(
            [series_item_metadata, movie_item_metadata],
            list(
                self.get_metadata_from_grpc_service(
                    [{"id": "found", "object_type": "series"}, {"id": "found", "object_type": "movie"}]
                )
            ),
        )

    def test_object_type_must_match(self):
        item_metadata = ItemMetadata(
            id="movie_item",
            title="title",
            description="description",
            object_type="movie",
            categories=["action", "comedy"],
            creators=["creator_1", "creator_2"],
        )
        self.add_item_metadata(item_metadata)
        time.sleep(5)
        self.assertEqual(0, len(self.get_metadata_from_grpc_service([{"id": "movie_item", "object_type": "series"}])))

    def test_missing_metadata(self):
        self.assertEqual(0, len(self.get_metadata_from_grpc_service([{"id": "not_found", "object_type": "movie"}])))

    def test_two_exists_one_missing(self):
        items_metadata = [
            ItemMetadata(
                id="found_1",
                title="title",
                description="description",
                object_type="movie",
                categories=["action", "comedy"],
                creators=["creator_1", "creator_2"],
            ),
            ItemMetadata(
                id="found_2",
                title="title",
                description="description",
                object_type="movie",
                categories=["action", "comedy"],
                creators=["creator_1", "creator_2"],
            ),
        ]
        for item_metadata in items_metadata:
            self.add_item_metadata(item_metadata)
        time.sleep(5)
        self.assertEqual(
            items_metadata,
            list(
                self.get_metadata_from_grpc_service(
                    [
                        {"id": "found_1", "object_type": "movie"},
                        {"id": "found_2", "object_type": "movie"},
                        {"id": "not_found", "object_type": "movie"},
                    ]
                )
            ),
        )


if __name__ == "__main__":
    unittest.main()
