# Pinned Apache Airflow image
FROM apache/airflow:3.2.1

ARG AIRFLOW_VERSION=3.2.1
ARG PYTHON_VERSION=3.13

# Extra libraries needed by our scripts, resolved against Airflow's official
# constraints so they cannot silently upgrade Airflow's own dependencies.
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY ingestion_contract.py /opt/airflow/src/ingestion_contract.py
