"""The Flask entrypoint"""
import logging
from http import HTTPStatus

from flask import Flask
from google.protobuf.json_format import MessageToDict

from recommendationservice import bootstrap, config

app = Flask(__name__)
recommendation_service = bootstrap.bootstrap(config)


@app.route("/recommendations/<model_name>/users/<user_id>", methods=["GET"])
def get_recommendations(model_name: str, user_id: str):
    """
    Given the recommendation model name and user id get the recommended content.

    :param model_name:
    :param user_id:
    :return:
    """
    items_metadata = recommendation_service.get_recommendations(model_name, user_id)
    if not items_metadata:
        return "not found", HTTPStatus.NOT_FOUND
    return [
        MessageToDict(metadata, preserving_proto_field_name=True) for metadata in items_metadata.metadata
    ], HTTPStatus.OK


if __name__ == "__main__":
    # run directly as a flask application
    app.run(debug=True, port=5035)
else:
    # run through gunicorn
    GUNICORN_LOGGER = logging.getLogger("gunicorn.error")
    app.logger.handlers = GUNICORN_LOGGER.handlers
    # pylint: disable=no-member
    app.logger.setLevel(GUNICORN_LOGGER.level)
