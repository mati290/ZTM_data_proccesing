# obraz bazowy: lekki Linux z Pythonem
FROM python:3.12-slim

# folder roboczy w kontenerze
WORKDIR /app

# najpierw same zależności (wyjaśnienie niżej)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# potem kod
COPY ingest.py .

# komenda startowa kontenera
CMD ["python", "ingest.py"]