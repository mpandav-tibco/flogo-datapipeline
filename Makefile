# flogo-datapipeline — build & test helpers.
#
# Live integration tests are gated on the DP_TEST_* / KAFKA_TEST_BROKER env vars
# below and skip cleanly when unset. `make test-integration` starts the
# docker-compose.test.yml stack and exports the matching vars so those tests
# actually run.
#
# ★ These vars are deliberately TARGET-SPECIFIC (`LIVE_TARGETS: export VAR ?=
# ...` below), not globally exported at file scope. A plain `export VAR ?= ...`
# at the top of this file would apply to EVERY target, including `build`/
# `test`/`test-unit` — meaning a fresh machine with no backends running would
# still see e.g. KAFKA_TEST_BROKER=localhost:9092 (just pointing at nothing),
# which is NOT the same as unset: live tests check "is this var EMPTY", so
# they would try to connect and FAIL ("connection refused"), not skip. Verified
# this exact failure mode when wiring up CI (`KAFKA_TEST_BROKER=localhost:1 go
# test` fails with a dial error; a truly-unset var skips cleanly). Scoping the
# exports to only the targets that bring up real backends keeps `make test` /
# `make test-unit` genuinely backend-free on any machine, CI included.

GOFLAGS ?= -buildvcs=false
export GOFLAGS

COMPOSE := docker compose -f docker-compose.test.yml

# Targets that need a live backend — env vars below are exported ONLY for
# these (and only take effect for their own recipes + prerequisites' recipes
# within the same `make` invocation).
LIVE_TARGETS := test-integration-up test-integration test-integration-race

# Gating env vars for the live tests (ports/creds match docker-compose.test.yml).
# `?=` lets callers override from their own environment.
$(LIVE_TARGETS): export DP_TEST_PG_DSN      ?= postgres://postgres:FlogoPass123@127.0.0.1:15432/dp?sslmode=disable
$(LIVE_TARGETS): export DP_TEST_MSSQL_DSN    ?= sqlserver://sa:Flogo-Pass-123@localhost:11433?database=master
$(LIVE_TARGETS): export DP_TEST_ORACLE_DSN   ?= oracle://dpuser:FlogoPass123@localhost:11521/FREEPDB1
$(LIVE_TARGETS): export DP_TEST_S3_ENDPOINT  ?= http://localhost:9000
$(LIVE_TARGETS): export DP_TEST_REDIS_ADDR   ?= 127.0.0.1:16379
$(LIVE_TARGETS): export KAFKA_TEST_BROKER    ?= localhost:9092
$(LIVE_TARGETS): export KAFKA_TEST_MTLS_BROKER ?= localhost:9094
$(LIVE_TARGETS): export KAFKA_TEST_SASL_BROKER ?= localhost:9095
$(LIVE_TARGETS): export KAFKA_TEST_SASLSSL_BROKER ?= localhost:9096
$(LIVE_TARGETS): export KAFKA_TEST_CERTS_DIR ?= $(CURDIR)/.kafka-certs
$(LIVE_TARGETS): export KAFKA_TEST_SCRAM_USER ?= testuser256
$(LIVE_TARGETS): export KAFKA_TEST_SCRAM_PASSWORD ?= testpass256
$(LIVE_TARGETS): export KAFKA_TEST_SCRAM512_USER ?= testuser512
$(LIVE_TARGETS): export KAFKA_TEST_SCRAM512_PASSWORD ?= testpass512
$(LIVE_TARGETS): export DP_TEST_GCS_ENDPOINT ?= http://127.0.0.1:4443/storage/v1/
$(LIVE_TARGETS): export DP_TEST_AZURITE_URL  ?= http://127.0.0.1:10000/devstoreaccount1

.PHONY: help build vet test test-unit test-extension-example test-integration test-integration-up test-integration-down test-integration-race

help: ## List available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

build: ## Compile the whole module
	go build ./...

vet: ## Run go vet across the module
	go vet ./...

test: ## Run the whole suite (live tests skip without backends)
	go test ./...

test-unit: ## Run the fast unit tests only (excludes the integration package)
	go test $$(go list ./... | grep -v /integration)

test-extension-example: ## Build & test the out-of-tree example plugin module
	cd examples/extension-plugin && GOWORK=off go test ./...

test-integration-up: ## Start the live backends and block until they are ready
	@[ -f .kafka-certs/ca.crt ] || ./scripts/gen-kafka-test-certs.sh
	$(COMPOSE) up -d
	@echo ">> waiting for Postgres..."
	@i=0; until $(COMPOSE) exec -T postgres pg_isready -U postgres -d dp -q 2>/dev/null; do \
		i=$$((i+1)); [ $$i -gt 60 ] && echo "!! Postgres not ready" && exit 1; sleep 1; done
	@echo ">> waiting for SQL Server..."
	@i=0; until $(COMPOSE) exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Flogo-Pass-123" -C -Q "SELECT 1" >/dev/null 2>&1; do \
		i=$$((i+1)); [ $$i -gt 90 ] && echo "!! SQL Server not ready" && exit 1; sleep 2; done
	@echo ">> waiting for Oracle (first run can take ~1 min)..."
	@i=0; until $(COMPOSE) exec -T oracle healthcheck.sh >/dev/null 2>&1; do \
		i=$$((i+1)); [ $$i -gt 100 ] && echo "!! Oracle not ready" && exit 1; sleep 3; done
	@echo ">> waiting for MinIO/Kafka/emulators to settle..."
	@sleep 8
	@echo ">> provisioning SCRAM-SHA-256 and SCRAM-SHA-512 test users..."
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
		--alter --add-config 'SCRAM-SHA-256=[password=$(KAFKA_TEST_SCRAM_PASSWORD)]' \
		--entity-type users --entity-name $(KAFKA_TEST_SCRAM_USER) >/dev/null
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
		--alter --add-config 'SCRAM-SHA-512=[password=$(KAFKA_TEST_SCRAM512_PASSWORD)]' \
		--entity-type users --entity-name $(KAFKA_TEST_SCRAM512_USER) >/dev/null
	@echo ">> backends ready."

test-integration: test-integration-up ## Start backends and run the full suite against them
	go test ./...

test-integration-race: test-integration-up ## Start backends and run the full suite with -race (used by CI)
	go test -race ./...

test-integration-down: ## Stop the backends and remove their volumes
	$(COMPOSE) down -v
