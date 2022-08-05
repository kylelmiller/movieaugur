"""
Contains the data collection configuration
"""
import os


def get_tmdb_api_key():
    tmdb_api_key = os.environ.get("TMDB_API_KEY")
    if not tmdb_api_key:
        raise ValueError("TMDB_API_KEY environment variable is required to be set.")
    return tmdb_api_key


def get_kafka_brokers():
    return os.environ.get("KAFKA_BROKERS", "kafka:9092")


def get_schema_registry_url():
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
