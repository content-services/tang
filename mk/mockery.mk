##
# Set of rules to manage podman-compose
#
# Requires 'mk/variables.mk'
##

MOCKERY_VERSION := $(shell curl -L https://api.github.com/repos/vektra/mockery/releases/latest | jq --raw-output .tag_name | sed 's/^v//')

GO_OUTPUT ?= $(PROJECT_DIR)/bin

$(GO_OUTPUT)/mockery: ## Install mockery locally on your GO_OUTPUT (./release) directory
	mkdir -p $(GO_OUTPUT) && \
	curl -sSfL https://github.com/vektra/mockery/releases/download/v$(MOCKERY_VERSION)/mockery_$(MOCKERY_VERSION)_$(shell uname -s)_$(shell uname -m).tar.gz | tar -xz -C $(GO_OUTPUT) mockery

.PHONY: mock ## Run mockery
mock: $(GO_OUTPUT)/mockery ## Install mockery if it isn't already in ./release directory and regenerate mocks
	$(GO_OUTPUT)/mockery
