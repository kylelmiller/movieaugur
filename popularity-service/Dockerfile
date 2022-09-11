FROM python:3.9-slim-buster

COPY requirements.txt /tmp/
COPY constraints.txt /tmp/
RUN pip install -r /tmp/requirements.txt -c /tmp/constraints.txt

RUN mkdir -p /src
COPY src/ /src/
RUN pip install -e /src
COPY tests/ /tests/

WORKDIR /src
