"""Service for the event business logic"""
# pylint: disable=import-error,no-name-in-module
import time
from typing import Dict, List, Optional, Union

from google.protobuf.json_format import ParseDict

from eventservice.adapters.user_interaction_repository import AbstractUserInteractionRepository
from eventservice.user_interaction_pb2 import UserInteraction


class EventService:
    """
    Service which validates and writes incoming dictionaries as user interaction events.
    """

    def __init__(self, user_interaction_repository: AbstractUserInteractionRepository):
        self.user_interaction_repository = user_interaction_repository

    def write_user_interactions(self, user_interactions: List[Dict[str, Union[str, int]]]) -> None:
        """
        Validates and writes incoming dictionaries as user interaction events..

        :param user_interactions:
        :return: Item metadata if the popularity exists
        """

        current_timestamp = int(time.time())

        with self.user_interaction_repository as repository:
            for user_interaction_data in user_interactions:
                user_interaction = ParseDict(user_interaction_data, UserInteraction())

                if user_interaction.timestamp > current_timestamp:
                    raise ValueError(
                        f"{user_interaction.timestamp} is greater than the current timestamp of "
                        f"{current_timestamp}. You cannot pass future events. Event: {user_interaction_data}"
                    )

                repository.write(user_interaction)
