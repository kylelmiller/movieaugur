"""The Flask entrypoint"""
import logging
from http import HTTPStatus

from flask import Flask
from google.protobuf.json_format import MessageToDict

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
        return "not found", HTTPStatus.NOT_FOUND
    return [
        MessageToDict(metadata, preserving_proto_field_name=True) for metadata in items_metadata.metadata
    ], HTTPStatus.OK


if __name__ == "__main__":
    # run directly as a flask application
    app.run(debug=True, port=5015)
else:
    # run through gunicorn
    GUNICORN_LOGGER = logging.getLogger("gunicorn.error")
    app.logger.handlers = GUNICORN_LOGGER.handlers
    # pylint: disable=no-member
    app.logger.setLevel(GUNICORN_LOGGER.level)
