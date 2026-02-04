import json
import time
import requests
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
POLL_INTERVAL = 20  # seconds between requests (6 calls/min, safe for free tier)

while True:
    try:
        response = requests.get(API_URL, timeout=10)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
            
        response.raise_for_status()
        data = response.json()
        
        # Check if response has expected structure
        if "bitcoin" not in data:
            print(f"Unexpected response: {data}")
            time.sleep(POLL_INTERVAL)
            continue

        event = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "usd_price": float(data["bitcoin"]["usd"])
        }

        producer.send("prices", event)
        print("sent:", event)
        
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except (KeyError, ValueError) as e:
        print(f"Parse error: {e}")

    time.sleep(POLL_INTERVAL)
