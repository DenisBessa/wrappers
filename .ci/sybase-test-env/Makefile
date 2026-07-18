# Atalhos para operar o ambiente de testes do sybase_fdw.
#
# Fluxo típico:
#   make bootstrap   # sobe Sybase com schema, builda imagem do Postgres,
#                    # sobe Postgres, aplica setup do FDW.
#   make test        # roda todos os testes SQL em fixtures/
#   make repro-bug2  # roda só o teste do bug #2
#   make shell-pg    # psql interativo no Postgres
#   make shell-sa    # dbisql interativo no Sybase
#   make rebuild     # rebuilda o FDW com mudanças no código fonte
#   make clean       # derruba e remove volumes (perde dados)

COMPOSE := docker compose
PG := docker exec -i sybase-fdw-test-postgres
SA := docker exec -i sybase-fdw-test-sybase
PSQL := $(PG) psql -h localhost -U postgres -X -v ON_ERROR_STOP=1

.PHONY: help bootstrap setup-sa setup-pg up down restart test test-connection-lifetime test-bug1 test-bug2 \
        rebuild shell-pg shell-sa logs logs-pg logs-sa clean ps

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/:	/'

bootstrap: setup-sa-volume up setup-sa-extra setup-pg ## Setup completo desde zero.
	@echo
	@echo "==> Ambiente pronto. Rode 'make test' para validar."

# Cria o volume sybase-test-db com o schema bethadba populado. Idempotente:
# detecta se o volume já existe e tem dados.
setup-sa-volume:
	@if ! docker volume inspect sybase-test-db >/dev/null 2>&1; then \
	  echo "==> Inicializando volume sybase-test-db..."; \
	  docker volume create sybase-test-db; \
	  docker run --rm --platform linux/amd64 \
	    --entrypoint sh -v sybase-test-db:/db \
	    brenoandrader/sqlanywhere17:latest \
	    -c "dbinit -dba 'externo,externo123' -p 4096 -ze UTF-8 -z UCA /db/bethadba.db"; \
	  $(MAKE) -s up; \
	  $(MAKE) -s setup-sa; \
	else \
	  echo "==> Volume sybase-test-db já existe — skip dbinit."; \
	  $(MAKE) -s up; \
	fi

# Carrega o schema bethadba no Sybase. Pode ser re-rodado quando schema muda.
setup-sa:
	@echo "==> Aplicando schema no Sybase..."
	@docker cp sql/00_users.sql sybase-fdw-test-sybase:/tmp/00_users.sql
	@docker cp sql/01_schema.sql sybase-fdw-test-sybase:/tmp/01_schema.sql
	@docker cp sql/02_data.sql sybase-fdw-test-sybase:/tmp/02_data.sql
	@$(SA) sh -c "for f in 00_users 01_schema 02_data; do \
	  echo '--- $$f ---'; \
	  dbisql -onerror exit -c 'UID=externo;PWD=externo123;HOST=localhost:2638;DBN=bethadba;ENG=bethadba' -nogui /tmp/$$f.sql || \
	    { [ $$f = 00_users ] && echo '(users já existem, ok)' || exit 1; }; \
	done"

# Aplica tabelas/dados EXTRAS (03/04) das 12 famílias de queries de prod.
# Idempotente (DROP TABLE IF EXISTS). Roda depois que o volume base já existe.
setup-sa-extra: ## Aplica 03_prod_query_tables.sql + 04_prod_query_data.sql no Sybase.
	@echo "==> Aplicando schema/dados extras (prod queries) no Sybase..."
	@docker cp sql/03_prod_query_tables.sql sybase-fdw-test-sybase:/tmp/03_prod_query_tables.sql
	@docker cp sql/04_prod_query_data.sql sybase-fdw-test-sybase:/tmp/04_prod_query_data.sql
	@$(SA) sh -c "for f in 03_prod_query_tables 04_prod_query_data; do \
	  echo '--- $$f ---'; \
	  dbisql -onerror exit -c 'UID=externo;PWD=externo123;HOST=localhost:2638;DBN=bethadba;ENG=bethadba' -nogui /tmp/$$f.sql || exit 1; \
	done"

setup-pg: ## Aplica setup-pg.sql (extension + FT + locals).
	@echo "==> Aplicando setup do Postgres..."
	@$(PSQL) < scripts/setup-pg.sql

up: ## Sobe containers.
	@$(COMPOSE) up -d
	@echo "==> Aguardando healthcheck dos containers..."
	@for i in $$(seq 1 60); do \
	  s=$$(docker inspect --format '{{.State.Health.Status}}' sybase-fdw-test-postgres 2>/dev/null || echo notfound); \
	  if [ "$$s" = healthy ]; then break; fi; \
	  sleep 2; \
	done
	@echo "==> Postgres healthy."

down: ## Para containers (mantém volumes).
	@$(COMPOSE) down --remove-orphans

restart: down up ## Reinicia.

test: ## Roda as fixtures SQL e a regressão persistente de conexões/FDs.
	@./scripts/run-tests.sh
	@./scripts/test-connection-lifetime.sh

test-connection-lifetime: ## Valida 1 sessão × 150 tx × 5 SELECTs e FDs limitados.
	@./scripts/test-connection-lifetime.sh

test-bug1: ## Roda repro do bug #1 (SIGSEGV).
	@./scripts/run-tests.sh fixtures/bug1_segfault_correlated_subq.sql

test-bug2: ## Roda repro do bug #2 (unrecognized node type: 0).
	@./scripts/run-tests.sh fixtures/bug2_unrecognized_node_type.sql

rebuild: ## Rebuilda o FDW dentro do container (após editar código).
	@$(PG) /usr/local/bin/build-fdw.sh
	@$(PG) psql -U postgres -c "DROP EXTENSION IF EXISTS wrappers CASCADE;"
	@$(MAKE) -s setup-pg

shell-pg: ## psql interativo.
	@docker exec -it sybase-fdw-test-postgres psql -U postgres

shell-sa: ## dbisql interativo no Sybase.
	@docker exec -it sybase-fdw-test-sybase dbisql -nogui \
	  -c 'UID=externo;PWD=externo123;HOST=localhost:2638;DBN=bethadba;ENG=bethadba'

logs: ## Logs dos dois.
	@$(COMPOSE) logs --tail=50 --follow

logs-pg:
	@$(COMPOSE) logs --tail=200 --follow postgres

logs-sa:
	@$(COMPOSE) logs --tail=50 --follow sybase

ps:
	@$(COMPOSE) ps

clean: ## Apaga TUDO incluindo dados (não recuperável).
	@$(COMPOSE) down -v --remove-orphans
	@docker volume rm sybase-test-db 2>/dev/null || true
