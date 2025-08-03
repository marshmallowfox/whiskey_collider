FROM postgres:15-alpine

RUN apk update && apk add --no-cache \
      build-base \
      git \
      postgresql15-dev \
      linux-pam-dev \
      readline-dev \
      libedit-dev \
      openssl-dev \
      zlib-dev \
      krb5-dev \
      lz4-dev \
      zstd-dev \
      libxml2-dev \
      clang llvm llvm-dev lld

RUN git clone https://github.com/ossc-db/pg_bulkload.git /tmp/pg_bulkload \
 && cd /tmp/pg_bulkload \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install \
 && rm -rf /tmp/pg_bulkload

COPY postgresql.conf /etc/postgresql/postgresql.conf
COPY pg_hba.conf /etc/postgresql/pg_hba.conf
COPY init-pg-bulkload.sql /docker-entrypoint-initdb.d/init-pg-bulkload.sql

EXPOSE 5432

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]