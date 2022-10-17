"""
Builds a recommendation model.

Reads the user interaction data from Kafka, creates the recommendation model, calculates the top 20 recommendations
for each user and then writes recommendations to Kafka.

Please, note: The recommendation removes any recommended items that the user has already interacted with. The goal of
the recommendation model is to introduce the user to new content.
"""
from typing import Optional, Set

import numpy as np
from implicit.als import AlternatingLeastSquares
from pandas import DataFrame
from scipy.sparse import coo_matrix, csr_matrix

from jobs.recommendationmodel.sources import AbstractUserInteractionSource, KafkaUserInteractionSource
from jobs.shared.item_score_pb2 import ItemScore, ItemScores
from jobs.shared.sinks import KafkaRecommendationSink, Sink, UserRecommendations


def get_dataframe(
    user_interactions_source: AbstractUserInteractionSource, object_types: Optional[Set[str]]
) -> Optional[DataFrame]:
    user_interactions = user_interactions_source.get_user_interactions()
    if user_interactions is None:
        return None
    df = DataFrame(
        [
            [user_interaction.user_id, user_interaction.item_id, user_interaction.object_type]
            for user_interaction in user_interactions
        ],
        columns=["user_id", "item_id", "object_type"],
    ).drop_duplicates()

    return df if object_types is None else df[df["object_type"].isin(object_types)]


def get_sparse_matrix(df: DataFrame) -> (DataFrame, DataFrame, csr_matrix):

    # Create indices for users and movies
    unique_users_df = df[["user_id"]].drop_duplicates().reset_index(drop=True)
    unique_users_df["user_index"] = unique_users_df.index
    unique_items_df = df[["item_id", "object_type"]].drop_duplicates().reset_index(drop=True)
    unique_items_df["item_index"] = unique_items_df.index

    df = df.merge(unique_users_df, how="inner", on="user_id").merge(
        unique_items_df, how="inner", on=["item_id", "object_type"]
    )
    return (
        unique_users_df,
        unique_items_df,
        coo_matrix(
            (np.ones(len(df)), (df["user_index"], df["item_index"])), shape=(len(unique_users_df), len(unique_items_df))
        ).tocsr(),
    )


def build_recommendation_model_with_source_and_sink(
    user_interaction_source: AbstractUserInteractionSource,
    recommendation_sink: Sink,
    object_types: Optional[Set[str]] = None,
) -> None:
    """
    Reads user interactions from a source, trains a recommendation model and writes the result to a sink.

    :param user_interaction_source: The source of the user interactions
    :param recommendation_sink: The recommendation sink
    :param object_types: List of object types that will be included in the model
    :return:
    """

    df = get_dataframe(user_interaction_source, object_types)
    if df is None or len(df) == 0:
        print("No user interaction events")
        return

    unique_users_df, unique_items_df, user_interaction_matrix = get_sparse_matrix(df)

    als = AlternatingLeastSquares(factors=1024, iterations=30, alpha=1.0)
    als.fit(user_interaction_matrix)

    recommendation_item_indices, recommendation_item_scores = als.recommend(
        list(unique_users_df["user_index"]), user_items=user_interaction_matrix, filter_already_liked_items=True, N=20
    )

    for user_id, user_recommendation_item_indices, user_recommendation_item_scores in zip(
        list(unique_users_df["user_id"]), recommendation_item_indices, recommendation_item_scores
    ):
        recommendation_sink.write(
            UserRecommendations(
                user_id,
                ItemScores(
                    item_scores=[
                        ItemScore(
                            id=recommendations_item_id,
                            object_type=recommendations_item_object_type,
                            score=score,
                        )
                        for recommendations_item_id, recommendations_item_object_type, score in zip(
                            list(unique_items_df.iloc[user_recommendation_item_indices]["item_id"]),
                            list(unique_items_df.iloc[user_recommendation_item_indices]["object_type"]),
                            user_recommendation_item_scores,
                        )
                    ]
                ),
            )
        )


def build_recommendation_model(
    kafka_brokers: str, model_name: str = "default", object_types: Optional[Set[str]] = None
) -> None:
    """
    Reads from the interaction kafka topic and writes the recommendations to another Kafka topic.

    :param kafka_brokers: kafka brokers
    :param model_name: the name of the model that is being built
    :param object_types: The list of object types that will be included in the model
    :return:
    """
    with KafkaRecommendationSink(kafka_brokers, model_name) as sink:
        build_recommendation_model_with_source_and_sink(
            KafkaUserInteractionSource(kafka_brokers, model_name), sink, object_types
        )
