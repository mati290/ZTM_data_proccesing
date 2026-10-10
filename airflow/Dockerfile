# # Pinned Apache Airflow image
FROM apache/airflow:3.2.1

# doinstalowujemy biblioteki potrzebne naszym skryptom
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt