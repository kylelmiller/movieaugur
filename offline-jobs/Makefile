# these will speed up builds, for docker-compose >= 1.25
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: clean build up test

build: grpc
	docker-compose build

grpc:
	python -m grpc_tools.protoc -I../proto --python_out=./src/jobs/collectdata ../proto/metadata.proto
	python -m grpc_tools.protoc -I../proto --python_out=./src/jobs/collectdata ../proto/user_interaction.proto

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
	black -l 120 $$(find * -name '*.py')

clean: down
	rm src/metadataservice/entrypoints/metadata*.py
	rm tests/e2e/metadata*.py
