# Minimalny obraz Pythona dla procesu ingestion.
FROM python:3.12-slim

WORKDIR /app

# Instalacja zależności osobno poprawia wykorzystanie cache warstw.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=10001:10001 ingest.py .
COPY --chown=10001:10001 check_ingestion_health.py .

USER 10001:10001
CMD ["python", "ingest.py"]
