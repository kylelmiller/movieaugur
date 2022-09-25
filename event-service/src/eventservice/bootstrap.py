"""
Bootstraps the repository
"""

from eventservice.adapters.user_interaction_repository import KafkaUserInteractionRepository
from eventservice.service_layer.event_service import EventService


def bootstrap(config) -> EventService:
    """
    Bootstraps the repository

    :param config: The configuration
    :return: The configured repository
    """
    return EventService(
        KafkaUserInteractionRepository(config.get_kafka_brokers(), config.get_schema_registry_url()),
    )
