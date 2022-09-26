"""The Flask entrypoint"""
import logging
from http import HTTPStatus

from flask import Flask, request
from google.protobuf.json_format import ParseError
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from eventservice import bootstrap, config


MAX_ITEMS_PER_POST = 20_000

event_service = bootstrap.bootstrap(config)
app = Flask(__name__)

request_schema = {
    "id": "Request",
    "type": "array",
    "maxItems": MAX_ITEMS_PER_POST,
    "minItems": 1,
    "items": {
        "id": "UserInteraction",
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "item_id": {"type": "string"},
            "object_type": {"type": "string", "enum": ["movie", "series"]},
            "timestamp": {"type": "integer"},
        },
        "required": ["user_id", "item_id", "object_type", "timestamp"],
    },
}


@app.route("/events", methods=["POST"])
def post_events():
    post_data = request.get_json()
    try:
        validate(request.get_json(), request_schema)
    except ValidationError as ex:
        return ex.message, HTTPStatus.BAD_REQUEST
    try:
        event_service.write_user_interactions(post_data)
    except ValueError as ex:
        return str(ex), HTTPStatus.BAD_REQUEST
    except ParseError as ex:
        return "Unable to parse json object.", HTTPStatus.BAD_REQUEST
    return "success", HTTPStatus.OK


if __name__ == "__main__":
    # run directly as a flask application
    app.run(debug=True, port=5025)
else:
    # run through gunicorn
    GUNICORN_LOGGER = logging.getLogger("gunicorn.error")
    app.logger.handlers = GUNICORN_LOGGER.handlers
    # pylint: disable=no-member
    app.logger.setLevel(GUNICORN_LOGGER.level)
