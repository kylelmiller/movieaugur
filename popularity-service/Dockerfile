FROM python:3.9-slim-buster

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt /tmp/
COPY constraints.txt /tmp/
RUN mkdir -p /src
COPY src/ /src/
RUN pip install -r /tmp/requirements.txt -c /tmp/constraints.txt && pip install -e /src
COPY tests/ /tests/

WORKDIR /src