# Ambiente de testes do `sybase_fdw`

Stack local em Docker que sobe um SQL Anywhere 17 com schema espelhando o
ambiente de produção da Domínio Contábil (`bethadba`), um Postgres 17 com o
`sybase_fdw` compilado em cima do código atual, e um runner de fixtures SQL
para reproduzir bugs e prevenir regressões.

Objetivo: dar um loop apertado de **edita → rebuilda → reproduz** que não
depende da Sybase de produção.

## Pré-requisitos

- Docker com suporte a `linux/amd64` (OrbStack ou Docker Desktop com
  emulation funcionando — a imagem do SQL Anywhere só tem build amd64).
- ~5 GB de espaço em disco para o cache do cargo + imagens.

## Setup inicial

```bash
cd .ci/sybase-test-env
make bootstrap
```

O `bootstrap` é idempotente:

1. Cria o volume `sybase-test-db` rodando `dbinit` se ainda não existir.
2. Builda a imagem do Postgres (Debian + PGDG + Rust + cargo-pgrx + FreeTDS).
3. Sobe ambos os containers e aguarda o healthcheck.
4. No primeiro boot do container do Postgres, o `entrypoint.sh` builda o FDW
   (3–8 minutos dependendo do hardware) e instala em `pg17/lib/wrappers.so`.
5. Aplica `setup-pg.sql`: cria extensão, server, FTs `_slow` (mapeando
   `bethadba.*`), MVs `dominio.<x>` (cacheando as FTs), uma FT direta
   `dominio.efsaidas` espelhando a topologia atual de prod (FT renomeada),
   tabelas locais (`nfe`, `customers_tax_numbers`) e popula com 5000 linhas
   em `nfe` para exercitar o caminho do bug #1 (nested-loop parametrizado).

A partir daí, o ciclo é:

```bash
# Roda toda a suíte
make test

# Roda só um teste
make test-bug2

# Editou wrappers/src/fdw/sybase_fdw/*.rs? Rebuilda o FDW:
make rebuild

# Acesso interativo:
make shell-pg     # psql no Postgres
make shell-sa     # dbisql no Sybase
```

## Estrutura

```
.ci/sybase-test-env/
├── docker-compose.yaml          # 2 services: sybase + postgres
├── Makefile                     # atalhos
├── README.md                    # este arquivo
├── fdw/
│   ├── Dockerfile               # postgres17 + freetds + pgrx
│   ├── build-fdw.sh             # rebuild incremental do FDW
│   └── entrypoint.sh            # initdb + build (uma vez) + postgres
├── sql/
│   ├── 00_users.sql             # cria user bethadba
│   ├── 01_schema.sql            # tabelas bethadba.efsaidas, etc.
│   └── 02_data.sql              # popula com dados sintéticos
├── scripts/
│   ├── setup-pg.sql             # CREATE EXTENSION/SERVER/FT no Postgres
│   └── run-tests.sh             # runner de fixtures
├── fixtures/
│   ├── 000_smoke.sql            # FT funciona
│   ├── 010_pushdown_indexed.sql # filtros PK pushdown
│   ├── 020_join_ft_ft.sql       # join pushdown FT-FT
│   ├── 030_parameterized_join.sql
│   ├── 040_aggregate_count.sql
│   ├── 050_in_subquery_local_only.sql
│   ├── 060_in_subquery_correlated_no_join.sql
│   ├── 070_now_pushdown.sql
│   ├── 080_null_handling.sql
│   ├── bug1_segfault_correlated_subq.sql   # repro original do bug #1
│   ├── bug1b_segfault_high_iterations.sql  # alta cardinalidade
│   ├── bug1c_stress_repeat.sql             # stress de cached_conn
│   ├── bug2_unrecognized_node_type.sql     # repro original do bug #2
│   └── bug2b_generic_plan.sql              # PREPARE+7×EXECUTE — regressão
└── results/                     # gerado pelo runner: stdout/stderr de cada fixture
```

## Anatomia de uma fixture

Cada arquivo em `fixtures/*.sql` começa com metadata em comentários:

```sql
-- @desc: <descrição curta>
-- @expect: <ok|crash|error|error XX000|ok-or-error XX000>
```

O runner classifica:

- `ok` — `psql` sai 0 e o backend não morre.
- `crash` — backend morre por sinal (esperado durante repro do bug #1).
- `error <SQLSTATE>` — sai com erro do SQLSTATE específico.
- `ok-or-error XX000` — aceita ambos. Usado em reproduções enquanto o fix
  ainda não está no main: antes do fix dá XX000, depois dá ok. Quando o fix
  estabilizar, vira `ok`.

## Decisões deliberadas

**Por que SQL Anywhere 17 e não Sybase ASE?** A produção é ASA (SQL Anywhere)
no porto 2638. ASE tem dialeto diferente (T-SQL clássico vs Watcom-SQL), e os
bugs investigados estão no caminho FreeTDS+odbc-api+SQL Anywhere — não em
ASE.

**Por que `brenoandrader/sqlanywhere17` e não a imagem oficial?** SAP não
distribui SQL Anywhere developer edition como imagem pública. A
`brenoandrader/sqlanywhere17` é o `dbsrv17` standalone da Developer Edition,
binário oficial empacotado em uma imagem amd64. Suficiente para 3 conexões
simultâneas (limite da Developer Edition) — mais que o que o pgrx test
precisa.

**Por que dados sintéticos e não dump de produção?** Privacidade + tamanho.
500 linhas em `efsaidas` + 5000 em `nfe` é o bastante para forçar nested-loop
parametrizado em vez de hash-join, que é o caminho que aciona os bugs.

**Por que existe `bug2b_generic_plan.sql`?** Bug #1 (SIGSEGV) e bug #2
(`unrecognized node type: 0`) têm o mesmo root cause: use-after-free de
`FdwState` quando o Postgres troca para generic plan na 6ª–7ª execução de uma
prepared statement. Em produção (PG Supabase fork) o crash aparece como XX000;
no nosso ambiente local aparece como SIGSEGV. A fixture `bug2b` executa
`PREPARE ... AS ... ; EXECUTE × 7` e é a regressão direta dos dois bugs —
antes do fix em `supabase-wrappers/src/scan.rs` e `qual.rs`, ela crasha; depois,
todos os 7 executes retornam dados.

**Por que `log_statement = 'all'` e `log_min_duration_statement = 0`?** Para
capturar o SQL do statement que crashou no log do container — sem isso o
`log_min_duration_statement = 5000` da produção não loga statement que morre
antes do statement terminar.

**Por que dropei `ON_ERROR_STOP=1` no entrypoint?** Não dropei — o runner
usa, mas o `setup-pg.sql` precisa ser idempotente porque `DROP FOREIGN TABLE
IF EXISTS ... CASCADE` falha graciosamente.

## Limitações conhecidas

- O healthcheck do Sybase espera `dbisql` retornar — em hardware lento na
  primeira subida, pode timeout. Aumente `start_period` no compose se
  acontecer.
- `make rebuild` recompila incrementalmente (cache via volume
  `pg-cargo-target`), mas se houver mudança em `Cargo.toml` ou
  feature-flags, pode rebuildar tudo de novo (~3–8 min).
- Core dumps caem em `/workspace/cores/` dentro do container. Mapeado pelo
  volume `pg-cores`. Para inspecionar com gdb:
  `docker exec -it sybase-fdw-test-postgres gdb /usr/lib/postgresql/17/bin/postgres /workspace/cores/core.X.postgres`.
