"""
Entry point for jobs with collect data and then passes that data into the system
"""
import logging

from jobs.collectdata.providers import (
    AbstractInteractionProvider,
    AbstractMetadataProvider,
    TMDBPopularContentProvider,
    TMDBPopularMovieProvider,
    TMDBPopularSeriesProvider,
    MovieLensLatestFullProvider,
    MovieLens100kProvider,
)
from jobs.collectdata.sources import TMDBMovieMetadataSource, TMDBSeriesMetadataSource
from jobs.shared.sinks import KafkaItemMetadataSink, KafkaPopularitySink, KafkaUserInteractionSink, KafkaSink


def run_interaction_job(interaction_provider: AbstractInteractionProvider, sink: KafkaSink) -> None:
    """
    Given an interaction provider get the list of interactions and writes to a new data source

    :param interaction_provider: Source of interaction data
    :param sink: Destination for the interaction data
    :return: None
    """
    logging.info("Writing user interaction data to sink")
    for user_interaction in interaction_provider.get_interactions():
        sink.write(user_interaction)
    logging.info("Done writing user interaction data to sink")


def run_metadata_job(metadata_provider: AbstractMetadataProvider, sink: KafkaSink) -> None:
    """
    Gets the item metadata from a metadata provider and writes to a new data source

    :param metadata_provider: Source of metadata
    :param sink: Destination for the item metadata
    :return: None
    """
    logging.info("Writing metadata to sink")
    for item_metadata in metadata_provider.get_items_metadata():
        sink.write(item_metadata)
    logging.info("Done writing metadata to sink")


def run_popularity_job(metadata_provider: TMDBPopularContentProvider, sink: KafkaSink) -> None:
    """
    Gets the item ids from a popularity provider and writes item scores to a new data source

    :param metadata_provider: Source of metadata
    :param sink: Destination for the item scores
    :return: None
    """
    logging.info("Writing item scores to sink")
    sink.write(metadata_provider.get_item_scores())
    logging.info("Done writing item scores to sink")


def collect_movielens_100k_data(tmdb_api_key: str, kafka_brokers: str, schema_registry: str) -> None:
    """
    Parses the pass arguments and runs the specified data collection job.

    :param tmdb_api_key: tmdb developer api key
    :param kafka_brokers: kafka brokers
    :param schema_registry: schema registry
    :return:
    """
    provider = MovieLens100kProvider(TMDBMovieMetadataSource(tmdb_api_key))
    # Write the collect item metadata to kafka
    with KafkaItemMetadataSink(kafka_brokers, schema_registry) as sink:
        run_metadata_job(provider, sink)
    # write the interaction data to kafka
    with KafkaUserInteractionSink(kafka_brokers, schema_registry) as sink:
        run_interaction_job(provider, sink)


def collect_full_movielens_data(tmdb_api_key: str, kafka_brokers: str, schema_registry: str) -> None:
    """
    Parses the pass arguments and runs the specified data collection job.

    :param tmdb_api_key: tmdb developer api key
    :param kafka_brokers: kafka brokers
    :param schema_registry: schema registry
    :return:
    """
    provider = MovieLensLatestFullProvider(TMDBMovieMetadataSource(tmdb_api_key))
    # Write the collect item metadata to kafka
    with KafkaItemMetadataSink(kafka_brokers, schema_registry) as sink:
        run_metadata_job(provider, sink)
    # write the interaction data to kafka
    with KafkaUserInteractionSink(kafka_brokers, schema_registry) as sink:
        run_interaction_job(provider, sink)


def collect_popular_tmdb_movie_data(tmdb_api_key: str, kafka_brokers: str, schema_registry: str) -> None:
    """
    Collects popular movies in the tmdb id space and writes the result to the movie metadata topic and the popularity
    topic.

    :param tmdb_api_key: tmdb developer api key
    :param kafka_brokers: kafka brokers
    :param schema_registry: schema registry
    :return:
    """
    provider = TMDBPopularMovieProvider(tmdb_api_key, TMDBMovieMetadataSource(tmdb_api_key))
    # Write the collect item metadata to kafka
    with KafkaItemMetadataSink(kafka_brokers, schema_registry) as sink:
        run_metadata_job(provider, sink)

    with KafkaPopularitySink(kafka_brokers, schema_registry, "movie") as sink:
        run_popularity_job(provider, sink)


def collect_popular_tmdb_series_data(tmdb_api_key: str, kafka_brokers: str, schema_registry: str) -> None:
    """
    Collects popular series in the tmdb id space and writes the result to the series metadata topic and the popularity
    topic.

    :param tmdb_api_key: tmdb developer api key
    :param kafka_brokers: kafka brokers
    :param schema_registry: schema registry
    :return:
    """
    provider = TMDBPopularSeriesProvider(tmdb_api_key, TMDBSeriesMetadataSource(tmdb_api_key))
    # Write the collect item metadata to kafka
    with KafkaItemMetadataSink(kafka_brokers, schema_registry) as sink:
        run_metadata_job(provider, sink)

    with KafkaPopularitySink(kafka_brokers, schema_registry, "series") as sink:
        run_popularity_job(provider, sink)
