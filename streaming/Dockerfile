# Build context is the repo root (see docker-compose.yml).
# The base image provides Spark 3.5.1 + PySpark; we add DuckDB/pandas/
# pyarrow for the foreachBatch DuckDB writer.
FROM apache/spark:3.5.1

USER root

COPY streaming/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common /app/common
COPY streaming /app/streaming

WORKDIR /app
