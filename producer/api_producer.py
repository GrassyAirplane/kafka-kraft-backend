import json
import time
import requests
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

while True:
    try:
        data = requests.get(API_URL, timeout=10).json()

        event = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "usd_price": float(data["bitcoin"]["usd"])
        }

        producer.send("prices", event)
        print("sent:", event)
    except Exception as e:
        print(f"Error: {e}")

    time.sleep(5)
