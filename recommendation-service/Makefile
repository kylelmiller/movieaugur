# these will speed up builds, for docker-compose >= 1.25
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: clean build up test

build: grpc
	docker-compose build

grpc:
	python -m grpc_tools.protoc -I../proto --python_out=./src/recommendationservice --grpc_python_out=./src/recommendationservice ../proto/metadata.proto
	# The import isn't based on the module and will fail to import because it doesn't search the relative location
	sed -i 's/metadata_pb2/recommendationservice.metadata_pb2/' src/recommendationservice/metadata_pb2_grpc.py
	python -m grpc_tools.protoc -I../proto --python_out=./src/recommendationservice ../proto/item_score.proto

up:
	docker-compose up -d

down:
	docker-compose down --remove-orphans

test: unit-tests e2e-tests

unit-tests: grpc
	PYTHONPATH=./src:./src/recommendationservice python -m unittest discover tests/unit/

e2e-tests: up
	docker-compose run --rm --no-deps -e PYTHONPATH=/tests/e2e/entrypoints --entrypoint="python -m unittest flask_app" recommendation-service
	docker-compose down --remove-orphans

logs:
	docker-compose logs --tail=25 recommendation-service

black:
	black .

pylint: clean
	pylint src/recommendationservice/
	pylint tests/unit
	pylint tests/e2e

clean: down
	find . -name *_pb2*.py -exec rm -f {} \;
