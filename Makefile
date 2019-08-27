# General

.PHONY: all list

all: list

list:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'


# Python

.PHONY: install lint release test version notebooks

PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d "'" -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/VERSION)

all: list

install:
	pip install --upgrade -e .[develop]

lint:
	pylama $(PACKAGE)

release:
	bash -c '[[ -z `git status -s` ]]'
	git tag -a -m release $(VERSION)
	git push --tags

test:
	tox

version:
	@echo $(VERSION)


# Docker

.PHONY: ci-build ci-run ci-test ci-remove ci-push-tag ci-push-latest ci-login

NAME   := os-dgp-backend
ORG    := openspending
REPO   := ${ORG}/${NAME}
TAG    := $(shell git log -1 --pretty=format:"%h")
IMG    := ${REPO}:${TAG}
LATEST := ${REPO}:latest

ci-build:
	docker pull ${LATEST}
	docker build --cache-from ${LATEST} -t ${IMG} -t ${LATEST} .

ci-run:
	docker run ${RUN_ARGS} --name ${NAME} -d ${LATEST}

ci-test:
	docker ps | grep latest

ci-remove:
	docker rm -f ${NAME}

ci-push: ci-login
	docker push ${IMG}
	docker push ${LATEST}

ci-push-tag: ci-login
	docker build -t ${REPO}:${TAG} .
	docker push ${REPO}:${TAG}

ci-login:
	docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}
