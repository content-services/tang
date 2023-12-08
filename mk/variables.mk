## Project
COMPOSE_PROJECT_NAME ?= tang
export COMPOSE_PROJECT_NAME

PROJECT_DIR := $(shell dirname $(abspath $(firstword $(MAKEFILE_LIST))))

## Docker/Podman command
ifneq (,$(shell command podman -v 2>/dev/null))
DOCKER ?= podman
else
ifneq (,$(shell command docker -v 2>/dev/null))
DOCKER ?= docker
else
DOCKER ?= false
endif
endif

## Docker Compose
PULP_COMPOSE_FILES ?= "compose_files/pulp/docker-compose.yml"
PULP_COMPOSE_OPTIONS=PULP_POSTGRES_PATH="pulp_db" PULP_STORAGE_PATH="pulp_storage"

PULP_COMPOSE_COMMAND=$(PULP_COMPOSE_OPTIONS) $(DOCKER)-compose --project-name=$(COMPOSE_PROJECT_NAME)  -f $(PULP_COMPOSE_FILES) up --detach
PULP_COMPOSE_DOWN_COMMAND=$(PULP_COMPOSE_OPTIONS) $(DOCKER)-compose --project-name=$(COMPOSE_PROJECT_NAME) -f $(PULP_COMPOSE_FILES) down

# Tests
ifeq (,$(shell ls -1d vendor 2>/dev/null))
MOD_VENDOR :=
else
MOD_VENDOR ?= -mod vendor
endif
