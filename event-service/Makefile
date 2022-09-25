# these will speed up builds, for docker-compose >= 1.25
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: clean build up test

build: grpc
	docker-compose build

grpc:
	python -m grpc_tools.protoc -I../proto --python_out=./src/eventservice ../proto/user_interaction.proto

up:
	docker-compose up -d

down:
	docker-compose down --remove-orphans

test: unit-tests e2e-tests

unit-tests: grpc
	PYTHONPATH=./src:./src/eventservice python -m unittest discover tests/unit/

e2e-tests: up
	docker-compose run --rm --no-deps -e PYTHONPATH=/tests/e2e --entrypoint="python -m unittest flask_app" event-service
	docker-compose down --remove-orphans

logs:
	docker-compose logs --tail=25 event-service

black:
	black .

pylint: clean
	pylint src/eventservice/
	pylint tests/unit
	pylint tests/e2e

clean: down
	find . -name *_pb2*.py -exec rm -f {} \;
