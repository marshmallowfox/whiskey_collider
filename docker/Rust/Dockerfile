FROM rust:1.88-slim AS builder

RUN apt-get update  \
    && apt-get install -y --no-install-recommends \
    libpq-dev \
    pkg-config \
    build-essential \
    git \
    postgresql-server-dev-all \
    libpam0g-dev \
    libselinux1-dev \
    liblz4-dev \
    libzstd-dev \
    libkrb5-dev \
    zlib1g-dev \
    libreadline-dev \
 && rm -rf /var/lib/apt/lists/*

RUN cargo install sqlx-cli

RUN git clone https://github.com/ossc-db/pg_bulkload.git /tmp/pg_bulkload && \
    cd /tmp/pg_bulkload && \
    make USE_PGXS=1 && \
    make USE_PGXS=1 install BINDIR=/usr/local/bin && \
    cp "$(pg_config --bindir)/pg_bulkload" /usr/local/bin/pg_bulkload && \
    chmod +x /usr/local/bin/pg_bulkload && \
    rm -rf /tmp/pg_bulkload

WORKDIR /usr/src/app

COPY . .

CMD ["bash"]