import requests 
import json
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import boto3
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()                          
api_key = os.getenv("ZTM_API_KEY")

if not api_key:
    raise ValueError("API key not found. Please set ZTM_API_KEY in your .env file.")


url = "https://api.um.warszawa.pl/api/action/busestrams_get/"

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),   # adres MinIO; na AWS ta linia zniknie
    aws_access_key_id=os.getenv("MINIO_USER"),
    aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
)


def fetch_positions(vehicle_type, max_attempts=3):
    params = {
        "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey": api_key,
        "type": vehicle_type
    }
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            result = data["result"]
            if not isinstance(result, list):
                raise ValueError(f"API zwróciło błąd: {result}")
            return result
        except (requests.RequestException, ValueError) as e:
            if attempt == max_attempts:
                logger.error(f"All {max_attempts} attempts failed for vehicle type {vehicle_type}: {e}")
                raise
            logger.warning(f"Attempt {attempt}/{max_attempts} failed for vehicle type {vehicle_type}: {e}")
            time.sleep(2** attempt)  
    raise RuntimeError(f"Failed to fetch positions for vehicle type {vehicle_type}")

def save_to_bronze(records, vehicle_label):
    if not records:
        logger.warning(f"No records fetched for vehicle type {vehicle_label}")
        return
    
    ingested_at = datetime.now(timezone.utc)

    
    file_name = f"positions_{ingested_at.strftime('%Y%m%dT%H%M%S')}.jsonl"
    
    key = (f"vehicle_type={vehicle_label}/"
           f"date={ingested_at.strftime('%Y-%m-%d')}/"
           f"hour={ingested_at.strftime('%H')}/"
           f"{file_name}")
    
    
    lines = []
    for record in records:
        record["_ingested_at"] = ingested_at.isoformat()
        lines.append(json.dumps(record, ensure_ascii=False))
        
    body = "\n".join(lines)

    s3.put_object(Bucket="bronze", Key=key, Body=body.encode("utf-8"))

    
    logger.info(f"Zapisano {len(records)} rekordów do s3://bronze/{key}")


def run_once():
    buses = fetch_positions(1)
    trams = fetch_positions(2)
    logger.info(f"Fetched {len(buses)} buses and {len(trams)} trams")
    
    save_to_bronze(buses, "bus")
    save_to_bronze(trams, "tram")
 
    
def main():
    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    logger.info(f"Starting main loop with interval {interval} seconds")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.exception(f"Ingestion cycle failed, retrying in next cycle:")
        time.sleep(interval)


if __name__ == "__main__":
    main()
