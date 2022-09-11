# these will speed up builds, for docker-compose >= 1.25
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: clean build up test

build: grpc
	docker-compose build

grpc:
	python -m grpc_tools.protoc -I../proto --python_out=./src/dags/jobs/collectdata ../proto/*.proto
	python -m grpc_tools.protoc -I../proto --python_out=./tests/integration/dags/jobs/collectdata ../proto/*.proto

up:
	docker-compose up -d

down:
	docker-compose down --remove-orphans

test: unit integration e2e-tests

unit-tests:
	PYTHONPATH=./src/dags python -m unittest discover tests/unit/

integration-tests: up
	docker-compose run --rm --no-deps -e PYTHONPATH=/tests/integration -e GRPC_HOST=metadata-service --entrypoint="python -m unittest test_sink test_source" metadata-service
	docker-compose down --remove-orphans

e2e-tests: up
	docker-compose run --rm --no-deps -e PYTHONPATH=/tests/e2e -e GRPC_HOST=metadata-service --entrypoint="python -m unittest test_api" metadata-service
	docker-compose down --remove-orphans

logs:
	docker-compose logs --tail=25 metadata-service

black:
	black .

pylint: clean
	PYTHONPATH=./src/dags pylint src/dags/
	PYTHONPATH=./src/dags pylint tests/unit/dags
	PYTHONPATH=./src/dags pylint tests/integration/dags
	PYTHONPATH=./src/dags pylint tests/e2e/dags

clean: down
	find . -name *_pb2*.py -exec rm -f {} \;
