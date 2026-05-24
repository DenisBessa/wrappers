# Container que builda o sybase_fdw com pgrx e roda Postgres 17 com a extensão
# instalada. Inclui FreeTDS+unixODBC pra falar com SQL Anywhere via TCP/IP.
FROM rust:1.88-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# Adiciona o repositório oficial do PostgreSQL (PGDG) — Debian bookworm só
# tem PG 15 no main. PGDG fornece 17.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg lsb-release \
 && install -d /usr/share/postgresql-common/pgdg \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
 && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config git \
    libssl-dev libreadline-dev libz-dev libicu-dev \
    bison flex \
    freetds-dev freetds-bin tdsodbc \
    unixodbc unixodbc-dev odbcinst \
    sudo locales gdb procps lsof netcat-openbsd \
    postgresql-server-dev-17 postgresql-17 postgresql-client-17 \
 && rm -rf /var/lib/apt/lists/*

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8 || true
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# Postgres roda como user 'postgres' (criado pelo pacote). Damos a ele sudo
# sem senha pra facilitar manipulação de pg_hba/permissões durante testes.
RUN echo "postgres ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Wrapper de cargo-pgrx pegamos da mesma versão do projeto.
RUN cargo install --locked cargo-pgrx --version 0.16.1

# HOME do user postgres é /var/lib/postgresql (pacote do Debian). Cria
# CARGO_HOME/RUSTUP_HOME ali; copia toolchain + cargo-pgrx instalados na
# imagem base.
RUN mkdir -p /var/lib/postgresql/.cargo /var/lib/postgresql/.rustup \
 && cp -r /usr/local/cargo/. /var/lib/postgresql/.cargo/ \
 && cp -r /usr/local/rustup/. /var/lib/postgresql/.rustup/ \
 && chown -R postgres:postgres /var/lib/postgresql/.cargo /var/lib/postgresql/.rustup

USER postgres
WORKDIR /var/lib/postgresql
ENV CARGO_HOME=/var/lib/postgresql/.cargo \
    RUSTUP_HOME=/var/lib/postgresql/.rustup \
    PGRX_HOME=/var/lib/postgresql/.pgrx \
    PATH=/var/lib/postgresql/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Inicializa pgrx apontando para o pg17 do sistema (apt). Não usamos o pg17
# baixado por pgrx porque o sistema já tem um, e queremos o backend gerenciado
# pelo systemd-init normal do Debian (mais próximo da produção Supabase).
RUN cargo pgrx init --pg17 /usr/lib/postgresql/17/bin/pg_config

USER root
WORKDIR /workspace

# pgrx install copia .so/.control/.sql para os dirs do pacote PG (root-owned).
# Damos write ao postgres pra evitar precisar de --sudo no cargo pgrx install.
RUN chown -R postgres:postgres \
        /usr/share/postgresql/17/extension \
        /usr/lib/postgresql/17/lib

# ODBC: deixa o template do FreeTDS pronto. O DSN concreto é injetado em
# runtime pela configuração do servidor (CREATE SERVER ... OPTIONS).
# Resolve o multiarch triplet (x86_64-linux-gnu, aarch64-linux-gnu, …) na
# build pra suportar tanto PG amd64 emulado quanto arm64 nativo no Apple
# Silicon — `dpkg-architecture -qDEB_HOST_MULTIARCH` é a fonte canônica.
RUN MULTIARCH="$(dpkg-architecture -qDEB_HOST_MULTIARCH)" \
 && printf '[FreeTDS]\nDescription = FreeTDS Driver\nDriver = /usr/lib/%s/odbc/libtdsodbc.so\nSetup = /usr/lib/%s/odbc/libtdsS.so\nFileUsage = 1\n' "$MULTIARCH" "$MULTIARCH" > /etc/odbcinst.ini

# Postgres config: relax shared_preload, ativar logging detalhado pro repro.
RUN echo "shared_preload_libraries = 'wrappers'" >> /etc/postgresql/17/main/postgresql.conf \
 && echo "log_min_messages = info" >> /etc/postgresql/17/main/postgresql.conf \
 && echo "log_min_error_statement = error" >> /etc/postgresql/17/main/postgresql.conf \
 && echo "log_statement = 'all'" >> /etc/postgresql/17/main/postgresql.conf \
 && echo "log_destination = 'stderr'" >> /etc/postgresql/17/main/postgresql.conf \
 && echo "logging_collector = off" >> /etc/postgresql/17/main/postgresql.conf \
 && echo "log_min_duration_statement = 0" >> /etc/postgresql/17/main/postgresql.conf \
 && sed -i 's/^#listen_addresses.*/listen_addresses = '"'"'*'"'"'/' /etc/postgresql/17/main/postgresql.conf \
 && sed -i 's/^#port.*/port = 5432/' /etc/postgresql/17/main/postgresql.conf \
 && { echo "host all all 0.0.0.0/0 trust"; \
      echo "host all all ::/0 trust"; \
      echo "local all all trust"; \
      cat /etc/postgresql/17/main/pg_hba.conf; } > /tmp/pg_hba.new \
 && mv /tmp/pg_hba.new /etc/postgresql/17/main/pg_hba.conf

# Enables core dumps (pra capturar stack do bug #1)
RUN echo "kernel.core_pattern = /workspace/cores/core.%p.%e" > /etc/sysctl.d/99-cores.conf
ENV RUST_BACKTRACE=full

# Build script: rodado pelo entrypoint na primeira subida (ou re-build manual).
COPY build-fdw.sh /usr/local/bin/build-fdw.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/build-fdw.sh /usr/local/bin/entrypoint.sh \
 && mkdir -p /workspace/cores && chmod 1777 /workspace/cores

EXPOSE 5432

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
