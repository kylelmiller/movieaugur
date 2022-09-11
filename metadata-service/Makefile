# these will speed up builds, for docker-compose >= 1.25
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: clean build up test clean

build: grpc
	docker-compose build

grpc:
	python -m grpc_tools.protoc -I../proto --python_out=./src/metadataservice/entrypoints --grpc_python_out=./src/metadataservice/entrypoints ../proto/metadata.proto
	python -m grpc_tools.protoc -I../proto --python_out=./tests/e2e --grpc_python_out=./tests/e2e ../proto/metadata.proto

up:
	docker-compose up -d

down:
	docker-compose down --remove-orphans

test: e2e-tests

e2e-tests: up
	docker-compose run --rm --no-deps -e PYTHONPATH=/tests/e2e -e GRPC_HOST=metadata-service --entrypoint="python -m unittest test_api" metadata-service
	docker-compose down --remove-orphans

logs:
	docker-compose logs --tail=25 metadata-service

black:
	black .

pylint: clean
	pylint src/metadataservice/
	pylint tests/e2e

clean: down
	find . -name *_pb2*.py -exec rm -f {} \;
