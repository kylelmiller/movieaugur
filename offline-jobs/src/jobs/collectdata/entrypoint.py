"""
Entry point for jobs with collect data and then passes that data into the system
"""
import os
import re
import sys
from argparse import ArgumentParser, ArgumentTypeError, Namespace
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from metadata_pb2 import ItemMetadata
from user_interaction_pb2 import UserInteraction
from metadata_extractor import TMDBMovieMetadataExtractor, TMDBSeriesMetadataExtractor
from source import (
    AbstractInteractionSource,
    AbstractMetadataSource,
    TMDBPopularMovieMetadataSource,
    TMDBPopularSeriesMetadataSource,
    MovieLens100kInteractionSource,
)

URL_PORT_REGEX_PATTERN = re.compile(r"^((\d|\w|\.|-)+):\d+$")


def validate_url_port_argument(argument: str) -> str:
    """
    Takes a string arguments and validates it using a re Pattern object for an url and port.

    :param argument: The passed argument
    :return: The passed argument if it passes validation
    """
    if not URL_PORT_REGEX_PATTERN.match(argument):
        raise ArgumentTypeError(f"{argument} is a misformed argument")
    return argument


def parse_arguments() -> Namespace:
    """
    Parses the arguments that are passed the script.

    :return: The parsed arguments namespace
    """
    parser = ArgumentParser()
    parser.add_argument(
        "collection_type",
        required=True,
        help="The type of data that should be collected.",
        choices=("movielens-100k", "tmdb-popular-movies", "tmdb-popular-series"),
    )

    parser.add_argument(
        "tmdb_api_key",
        required=True,
        help="The tmdb_api_key required to collect the movie and series metadata.",
    )

    parser.add_argument(
        "kafka_broker",
        default="kafka:9092",
        type=validate_url_port_argument,
        help="Kafka broker where the incoming data will be published to.",
    )

    parser.add_argument(
        "schema_registry",
        default="schema-registry:8081",
        type=lambda x: f"http://{validate_url_port_argument(x)}",
        help="The confluent schema registry url and port where the schemas for the published data will be stored.",
    )

    return parser.parse_args()


def run_interaction_job(interaction_source: AbstractInteractionSource, args: Namespace):
    """
    Given an interaction source get the list of interactions and produce to Kafka

    :param interaction_source:
    :param args:
    :return:
    """
    kafka_producer = SerializingProducer(
        {
            "bootstrap.servers": args.kafka_brokers,
            "key.serializer": StringSerializer(),
            "value.serializer": ProtobufSerializer(
                UserInteraction,
                SchemaRegistryClient({"url": args.schema_registry}),
                {"use.deprecated.format": False},
            ),
        }
    )

    for user_interaction in interaction_source.get_interactions():
        kafka_producer.produce(topic="user-interaction", value=user_interaction)


def run_metadata_job(metadata_source: AbstractMetadataSource, args: Namespace):
    """


    :param metadata_source:
    :param args:
    :return:
    """

    kafka_producer = SerializingProducer(
        {
            "bootstrap.servers": args.kafka_brokers,
            "key.serializer": StringSerializer(),
            "value.serializer": ProtobufSerializer(
                ItemMetadata,
                SchemaRegistryClient({"url": args.schema_registry}),
                {"use.deprecated.format": False},
            ),
        }
    )

    for item_metadata in metadata_source.get_items_metadata():
        kafka_producer.produce(topic="metadata", value=item_metadata)


def main():
    """


    :return:
    """
    args = parse_arguments()

    if args.collection_type == "movielens-100k":
        source = MovieLens100kInteractionSource(TMDBMovieMetadataExtractor(args.tmdb_api_key))
        run_interaction_job(source, args)
    elif args.collection_type == "tmdb-popular-movies":
        source = TMDBPopularMovieMetadataSource(args.tmdb_api_key, TMDBMovieMetadataExtractor(args.tmdb_api_key))
    elif args.collection_type == "tmdb-popular-series":
        source = TMDBPopularSeriesMetadataSource(args.tmdb_api_key, TMDBSeriesMetadataExtractor(args.tmdb_api_key))

    run_metadata_job(source, args)
    return os.EX_OK


if __name__ == "__main__":
    sys.exit(main())
