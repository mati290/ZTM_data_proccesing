import requests 
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import boto3

load_dotenv()                          
api_key = os.getenv("ZTM_API_KEY")

if not api_key:
    raise ValueError("API key not found. Please set ZTM_API_KEY in your .env file.")


url = "https://api.um.warszawa.pl/api/action/busestrams_get/"

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",   # adres MinIO; na AWS ta linia zniknie
    aws_access_key_id=os.getenv("MINIO_USER"),
    aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
)


def fetch_positions(vehicle_type):
    params = {
        "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey": api_key,
        "type": vehicle_type
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data["result"]

def save_to_bronze(records, vehicle_label):
    
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

    
    print(f"Zapisano {len(records)} rekordów do s3://bronze/{key}")

buses = fetch_positions(1)
trams = fetch_positions(2)
print(len(buses), len(trams))
  
save_to_bronze(buses, "bus")
save_to_bronze(trams, "tram")

