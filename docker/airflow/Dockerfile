# Image Airflow + PostgreSQL client 16 (psql, pg_dump, pg_restore) + library ETL
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

USER root
# postgresql-client-16 dari repository resmi PostgreSQL (Debian bawaan hanya versi 15,
# sedangkan pg_dump harus >= versi server)
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates gnupg lsb-release \
 && install -d /usr/share/postgresql-common/pgdg \
 && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc \
 && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client-16 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
# Image Airflow sudah membawa postgresql-client 17. pg_dump 17 menulis perintah SET transaction_timeout
# yang tidak dikenal server 16, sehingga pg_restore gagal. Utamakan klien versi 16 (= versi server).
ENV PATH="/usr/lib/postgresql/16/bin:${PATH}"

USER airflow
ARG AIRFLOW_VERSION
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pandas==2.2.3" "openpyxl>=3.1" "pyarrow>=14"
