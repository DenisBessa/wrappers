#!/usr/bin/env bash
# Builda e instala o sybase_fdw na instalação do pg17 do sistema.
# Rodável em runtime pra rebuild rápido após edits no código.
#
# /workspace/wrappers é a raiz do repo (bind-mount). O crate pgrx que vira a
# extensão fica em /workspace/wrappers/wrappers/. cargo-pgrx exige rodar
# dentro do crate que tem #[pg_extern].
set -euo pipefail

sudo -u postgres -E \
    PATH="/var/lib/postgresql/.cargo/bin:/usr/local/bin:/usr/bin:/bin" \
    CARGO_HOME="/var/lib/postgresql/.cargo" \
    RUSTUP_HOME="/var/lib/postgresql/.rustup" \
    CARGO_TARGET_DIR="/workspace/wrappers/target" \
    HOME="/var/lib/postgresql" \
    PGRX_HOME="/var/lib/postgresql/.pgrx" \
    bash -c "
  cd /workspace/wrappers/wrappers && \
  cargo pgrx install \
    --pg-config /usr/lib/postgresql/17/bin/pg_config \
    --features 'pg17 sybase_fdw' \
    --release
"

# cargo-pgrx 0.16 instala como wrappers-0.6.1.so (com versão). PG carrega via
# shared_preload_libraries='wrappers' que procura wrappers.so. Symlink resolve.
LIBDIR=/usr/lib/postgresql/17/lib
SO=$(ls "$LIBDIR"/wrappers-*.so 2>/dev/null | head -1)
if [ -n "$SO" ] && [ ! -e "$LIBDIR/wrappers.so" ]; then
    ln -s "$(basename "$SO")" "$LIBDIR/wrappers.so"
fi

echo "+ sybase_fdw instalado em $LIBDIR/wrappers.so -> $(readlink "$LIBDIR/wrappers.so")"
