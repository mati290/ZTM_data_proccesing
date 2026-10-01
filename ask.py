import requests 
import json
import os
from dotenv import load_dotenv

load_dotenv()                          
api_key = os.getenv("ZTM_API_KEY")

if not api_key:
    raise ValueError("API key not found. Please set ZTM_API_KEY in your .env file.")

url = "https://api.um.warszawa.pl/api/action/busestrams_get/"
params = {
    "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
    "apikey": api_key,
    "type": "1"
}
response = requests.get(url, params = params)

print(response.status_code)


data = response.json()
print(len(data["result"]))

print(json.dumps(data["result"][0], indent=4, ensure_ascii=False))
