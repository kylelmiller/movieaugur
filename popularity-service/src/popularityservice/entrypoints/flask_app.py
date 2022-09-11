"""The Flask entrypoint"""
from flask import Flask
from google.protobuf.json_format import MessageToJson

from popularityservice import bootstrap, config

app = Flask(__name__)
popularity_service = bootstrap.bootstrap(config)


@app.route("/popularity/<object_type>", methods=["GET"])
def get_popularity_endpoint(object_type: str):
    """
    Given an object type, return the most popular content with associated metadata

    :param object_type: The object type we want the popular data for
    :return:
    """
    items_metadata = popularity_service.get_popularity(object_type)
    if not items_metadata:
        return "not found", 404
    return MessageToJson(items_metadata.metadata), 200
