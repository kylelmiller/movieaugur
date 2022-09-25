# these will speed up builds, for docker-compose >= 1.25
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: down build up test

build: grpc
	docker-compose build

grpc:
	cd metadata-service && make grpc
	cd offline-jobs && make grpc
	cd popularity-service && make grpc
	cd event-service && make grpc

up:
	docker-compose up -d

down:
	docker-compose down --remove-orphans

test: e2e-tests

e2e-tests: up
	docker-compose run --rm --no-deps --entrypoint="python -m unittest api /tests/e2e"
	docker-compose down --remove-orphans

black:
	black .
