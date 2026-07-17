##
# Set of rules to manage podman-compose
#
# Requires 'mk/variables.mk'
##

.PHONY: compose-up
compose-up: ## Start up service dependencies using podman(docker)-compose
	PULP_DATABASE_PORT=5434 PULP_API_PORT=8087 PULP_CONTENT_PORT=8088 $(PULP_COMPOSE_COMMAND)

.PHONY: compose-down
compose-down: ## Shut down service  dependencies using podman(docker)-compose
	$(PULP_COMPOSE_DOWN_COMMAND)

MAVEN_FIXTURE_PORT ?= 8089
MAVEN_FIXTURE_NAME ?= tang_maven_fixture

.PHONY: maven-fixture-up
maven-fixture-up: ## Start the maven fixture server
	$(DOCKER) run -d --name $(MAVEN_FIXTURE_NAME) -p $(MAVEN_FIXTURE_PORT):80 -v $(PROJECT_DIR)/compose_files/pulp/assets/maven-fixture:/usr/share/nginx/html:ro,z docker.io/library/nginx:alpine

.PHONY: maven-fixture-down
maven-fixture-down: ## Stop the maven fixture server
	$(DOCKER) rm -f $(MAVEN_FIXTURE_NAME)

.PHONY: compose-clean ## Clear out data (dbs, files) for service dependencies
compose-clean: compose-down
	$(DOCKER) volume prune --force
