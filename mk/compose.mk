##
# Set of rules to manage podman-compose
#
# Requires 'mk/variables.mk'
##

.PHONY: compose-up
compose-up: ## Start up service dependencies using podman(docker)-compose
	$(PULP_COMPOSE_COMMAND)

.PHONY: compose-down
compose-down: ## Shut down service  dependencies using podman(docker)-compose
	$(PULP_COMPOSE_DOWN_COMMAND)

.PHONY: compose-clean ## Clear out data (dbs, files) for service dependencies
compose-clean: compose-down
	$(DOCKER) volume prune --force