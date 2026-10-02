import requests 
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path

load_dotenv()                          
api_key = os.getenv("ZTM_API_KEY")

if not api_key:
    raise ValueError("API key not found. Please set ZTM_API_KEY in your .env file.")


url = "https://api.um.warszawa.pl/api/action/busestrams_get/"


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
    #moment pobrania danych 
    ingested_at = datetime.now(timezone.utc)

    # sciezka folderu  
    folder = (
        Path("data") / "bronze"
        / f"vehicle_type={vehicle_label}"                  # tekst "bus" / "tram", NIE lista
        / f"date={ingested_at.strftime('%Y-%m-%d')}"       # np. 2026-10-02
        / f"hour={ingested_at.strftime('%H')}"             # np. 08
    )

    # Nazwa pliku z pełnym czasem 
    file_name = f"positions_{ingested_at.strftime('%Y%m%dT%H%M%S')}.jsonl"

    # Tworzymy foldery
    folder.mkdir(parents=True, exist_ok=True)

    # Zapisuuje rekordy jeden na linię
    with open(folder / file_name, "w", encoding="utf-8") as f:
        for record in records:
            record["_ingested_at"] = ingested_at.isoformat()
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    #informacja czy pliki zostaly pobrane 
    print(f"Zapisano {len(records)} rekordów do {folder / file_name}")

buses = fetch_positions(1)
trams = fetch_positions(2)
print(len(buses), len(trams))
  
save_to_bronze(buses, "bus")
save_to_bronze(trams, "tram")

