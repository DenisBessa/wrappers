#!/usr/bin/env bash
set -euo pipefail

# Habilita core dumps (best-effort).
ulimit -c unlimited || true

# Garante permissão de escrita para o user postgres nos volumes montados.
chown -R postgres:postgres /workspace/cores /workspace/wrappers/target 2>/dev/null || true

# PGDATA é um volume nomeado pelo compose. Na primeira subida o volume vem
# herdado da imagem (apt-installed pg17 escreveu PG_VERSION e estrutura
# básica em /var/lib/postgresql/17/main). Mas os arquivos de config canônicos
# do Debian ficam em /etc/postgresql/17/main, NÃO em PGDATA. Então:
#   - se postgresql.conf não existir em PGDATA, copia do /etc;
#   - se PG_VERSION não existir, faz initdb completo;
#   - sempre re-aplica nosso pg_hba.conf customizado.
PGDATA=/var/lib/postgresql/17/main

if [ ! -f "${PGDATA}/PG_VERSION" ]; then
  echo "==> initdb (volume vazio)..."
  sudo -u postgres /usr/lib/postgresql/17/bin/initdb -D "${PGDATA}" \
    --encoding=UTF8 --locale=C.UTF-8 >/dev/null
fi

if [ ! -f "${PGDATA}/postgresql.conf" ]; then
  echo "==> copiando postgresql.conf do /etc..."
  # Strip diretivas Debian-specific (include_dir, ssl_*) que apontam para
  # paths que não existem no nosso layout.
  grep -vE '^include_dir|^ssl(_|=)|^ssl ' /etc/postgresql/17/main/postgresql.conf \
    > "${PGDATA}/postgresql.conf"
  chown postgres:postgres "${PGDATA}/postgresql.conf"
fi

# pg_hba customizado (trust local + host) sempre re-aplicado.
cp /etc/postgresql/17/main/pg_hba.conf "${PGDATA}/pg_hba.conf"
chown postgres:postgres "${PGDATA}/pg_hba.conf"

# Builda o FDW se ainda não houver wrappers.so instalado.
if [ ! -f /usr/lib/postgresql/17/lib/wrappers.so ]; then
  echo "==> Building sybase_fdw (primeiro boot, demora alguns minutos)..."
  /usr/local/bin/build-fdw.sh
fi

# Sobe o Postgres em foreground como user postgres.
echo "==> starting postgres..."
exec sudo -u postgres /usr/lib/postgresql/17/bin/postgres \
  -D "${PGDATA}" \
  -c config_file="${PGDATA}/postgresql.conf" \
  -c hba_file="${PGDATA}/pg_hba.conf"
