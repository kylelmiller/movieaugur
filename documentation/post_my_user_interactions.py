import json
import requests

with open("my_user_interaction_events.json", "r") as file_in:
    my_user_interaction_events = json.load(file_in)
    requests.post(
        "movie-augur:8080/events",
        data=json.dumps(my_user_interaction_events),
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=10,
    )
