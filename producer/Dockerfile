# Build context is the repo root (see docker-compose.yml) so the shared
# `common` package can be copied in alongside the producer.
FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt ./producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

COPY common ./common
COPY producer ./producer

ENV PYTHONPATH=/app
CMD ["python", "producer/producer.py"]
