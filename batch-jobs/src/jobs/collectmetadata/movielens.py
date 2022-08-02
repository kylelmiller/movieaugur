import os
import multiprocessing

from typing import List

import pandas as pd
import requests
import zipfile
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from metadata_pb2 import ItemMetadata

class MovieLens100kSource():
    url = "https://files.grouplens.org/datasets/movielens/ml-100k.zip"
    local_directory_name = "movielens_100k"
    BASE_DIRECTORY = "/tmp"

    def _open(self) -> None:
        if os.path.exists(os.path.join(self.BASE_DIRECTORY, self.local_directory_name)):
            return

        response = requests.get(self.url)

        with open(os.path.join(self.BASE_DIRECTORY, "ml-100k.zip"), "wb") as fo:
            fo.write(response.content)



url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
# url = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"
BASE_DIRECTORY = "/tmp"
TMDB_API_KEY = "91bb601e056735bc9f50c23f5d59e4d4"
TMDB_KEYWORDS_URL = f"https://api.themoviedb.org/3/movie/%s/keywords?api_key={TMDB_API_KEY}"
TMDB_MOVIE_URL = f"https://api.themoviedb.org/3/movie/%s?api_key={TMDB_API_KEY}"
TMDB_CAST_URL = f"https://api.themoviedb.org/3/movie/%s/credits?api_key={TMDB_API_KEY}"
MAXIMUM_CAST_MEMBERS = 10
MAXIMUM_GENRES = 10
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
protobuf_serializer = ProtobufSerializer(ItemMetadata, schema_registry_client, {'use.deprecated.format': False})
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
producer_conf = {'bootstrap.servers': KAFKA_BROKERS,
                 'key.serializer': StringSerializer(),
                 'value.serializer': protobuf_serializer}
kafka_producer = SerializingProducer(producer_conf)

filename = url.split("/")[-1]
extracted_directory_name = filename[:-len(".zip")]

if not os.path.exists(os.path.join(BASE_DIRECTORY, extracted_directory_name)):
    response = requests.get(url)

    with open(os.path.join(BASE_DIRECTORY, filename), "wb") as fo:
        fo.write(response.content)
    with zipfile.ZipFile(os.path.join(BASE_DIRECTORY, filename), 'r') as zip_reference:
        zip_reference.extractall(BASE_DIRECTORY)

links_df = pd.read_csv(os.path.join(BASE_DIRECTORY, extracted_directory_name, "links.csv"))
movies_df = pd.read_csv(os.path.join(BASE_DIRECTORY, extracted_directory_name, "movies.csv"))
#ratings_df = movies_df = pd.read_csv(os.path.join(BASE_DIRECTORY, extracted_directory_name, "ratings.csv"))

def get_keywords(keyword_data) -> List[str]:
    return [k["name"] for k in keyword_data.get("keywords", [])]

def get_cast(cast_data) -> List[str]:
    return [c["name"] for c in cast_data.get("cast", [])[:MAXIMUM_CAST_MEMBERS]]

def get_genres(movie_data) -> List[str]:
    return [g["name"] for g in movie_data.get("genres", [])[:MAXIMUM_GENRES]]

df = movies_df.set_index("movieId").join(links_df[~links_df["tmdbId"].isna()].set_index("movieId"), how="inner").astype({"tmdbId": "int32"})
for tmdb_id in df["tmdbId"]:
    with multiprocessing.Pool(3) as pool:
        movie_response, keyword_response, cast_response = pool.map_async(requests.get, [url % tmdb_id for url in (TMDB_MOVIE_URL, TMDB_KEYWORDS_URL, TMDB_CAST_URL)]).get()
    i = 3
    movie_data = movie_response.json()

    item_metadata = ItemMetadata(
        id=str(tmdb_id),
        title=movie_data["title"],
        description=movie_data.get("overview", ""),
        objectType="Movie",
        releaseDate=movie_data.get("release_date", ""),
        categories=get_genres(movie_data),
        keywords=get_keywords(keyword_response.json()),
        creators=get_cast(cast_response.json())
    )
    kafka_producer.produce(topic="movie-metadata", key=str(tmdb_id), value=item_metadata)
