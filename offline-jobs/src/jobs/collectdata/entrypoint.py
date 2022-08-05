"""
Entry point for jobs with collect data and then passes that data into the system
"""
from argparse import ArgumentParser, Namespace
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

import config
from metadata_pb2 import ItemMetadata
from user_interaction_pb2 import UserInteraction
from metadata_extractor import TMDBMovieMetadataExtractor
from source import AbstractInteractionSource, AbstractMetadataSource, MovieLens100kInteractionSource


def parse_arguments() -> Namespace:
    """
    Parses the arguments that are passed the script.

    :return:
    """
    parser = ArgumentParser()
    parser.add_argument(
        "collection_type", required=True, help="The type of data that should be collected.", choices=("movielens-100k",)
    )
    return parser.parse_args()


def run_interaction_job(interaction_source: AbstractInteractionSource):
    kafka_producer = SerializingProducer(
        {
            "bootstrap.servers": config.get_kafka_brokers(),
            "key.serializer": StringSerializer(),
            "value.serializer": ProtobufSerializer(
                UserInteraction,
                SchemaRegistryClient({"url": config.get_schema_registry_url()}),
                {"use.deprecated.format": False},
            ),
        }
    )

    for user_interaction in interaction_source.get_interactions():
        kafka_producer.produce(topic="user-interaction", value=user_interaction)


def run_metadata_job(metadata_source: AbstractMetadataSource):

    kafka_producer = SerializingProducer(
        {
            "bootstrap.servers": config.get_kafka_brokers(),
            "key.serializer": StringSerializer(),
            "value.serializer": ProtobufSerializer(
                ItemMetadata,
                SchemaRegistryClient({"url": config.get_schema_registry_url()}),
                {"use.deprecated.format": False},
            ),
        }
    )

    for item_metadata in metadata_source.get_items_metadata():
        kafka_producer.produce(topic="metadata", value=item_metadata)


def main():
    args = parse_arguments()

    if args.collection_type == "movielens-100k":
        source = MovieLens100kInteractionSource(TMDBMovieMetadataExtractor(config.get_tmdb_api_key()))
        run_metadata_job(source)
        run_interaction_job(source)

    raise ValueError(f"collection_type {args.collection_type} is not supported")


if __name__ == "__main__":
    main()
