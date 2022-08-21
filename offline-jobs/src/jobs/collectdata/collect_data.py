"""
Entry point for jobs with collect data and then passes that data into the system
"""
import os
import re
import sys
from argparse import ArgumentParser, ArgumentTypeError, Namespace

from providers import (
    AbstractInteractionProvider,
    AbstractMetadataProvider,
    TMDBPopularMovieProvider,
    TMDBPopularSeriesProvider,
    MovieLens100kProvider,
)
from sinks import KafkaItemMetadataSink, KafkaUserInteractionSink, KafkaSink
from sources import TMDBMovieMetadataSource, TMDBSeriesMetadataSource


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
        help="The type of data that should be collected.",
        choices=("movielens-100k", "tmdb-popular-movies", "tmdb-popular-series"),
    )

    parser.add_argument(
        "--tmdb_api_key",
        required=True,
        help="The tmdb_api_key required to collect the movie and series metadata.",
    )

    parser.add_argument(
        "--kafka_brokers",
        default="kafka:9092",
        type=validate_url_port_argument,
        help="Kafka broker where the incoming data will be published to.",
    )

    parser.add_argument(
        "--schema_registry",
        default="schema-registry:8082",
        type=lambda x: f"http://{validate_url_port_argument(x)}",
        help="The confluent schema registry url and port where the schemas for the published data will be stored.",
    )

    return parser.parse_args()


def run_interaction_job(interaction_provider: AbstractInteractionProvider, sink: KafkaSink) -> None:
    """
    Given an interaction provider get the list of interactions and writes to a new data source

    :param interaction_provider: Source of interaction data
    :param sink: Destination for the interaction data
    :return: None
    """
    for user_interaction in interaction_provider.get_interactions():
        sink.write(user_interaction)


def run_metadata_job(metadata_provider: AbstractMetadataProvider, sink: KafkaSink) -> None:
    """
    Gets the item metadata from a metadata provider and writes to a new data source

    :param metadata_provider: Source of metadata
    :param sink: Destination for the item metadata
    :return: None
    """
    for item_metadata in metadata_provider.get_items_metadata():
        sink.write(item_metadata)


def main(args: Namespace) -> int:
    """
    Parses the pass arguments and runs the specified data collection job.

    :return: Exit code
    """

    if args.collection_type == "movielens-100k":
        # Gets the MovieLens100k data set with tmdb ids
        provider = MovieLens100kProvider(TMDBMovieMetadataSource(args.tmdb_api_key))
        # write the interaction data to kafka
        with KafkaUserInteractionSink(args.kafka_brokers, args.schema_registry) as sink:
            run_interaction_job(provider, sink)
    elif args.collection_type == "tmdb-popular-movies":
        provider = TMDBPopularMovieProvider(args.tmdb_api_key, TMDBMovieMetadataSource(args.tmdb_api_key))
    elif args.collection_type == "tmdb-popular-series":
        provider = TMDBPopularSeriesProvider(args.tmdb_api_key, TMDBSeriesMetadataSource(args.tmdb_api_key))
    else:
        raise ValueError(f"{args.collection_type} is an invalid collection_type.")

    # Write the collect item metadata to kafka
    with KafkaItemMetadataSink(args.kafka_brokers, args.schema_registry) as sink:
        run_metadata_job(provider, sink)

    return os.EX_OK


if __name__ == "__main__":
    sys.exit(main(parse_arguments()))
