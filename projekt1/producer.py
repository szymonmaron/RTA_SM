
import json
import time
from datetime import datetime
from kafka import KafkaProducer

from config import KAFKA_SERVER, TOPIC, EVENTS
from weather_gen import generate_weather

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

event_index = 0

print("Producer started...")

while True:

    event = EVENTS[event_index % len(EVENTS)]
    event_index += 1

    weather_event = generate_weather(event)

    payload = {
        **weather_event,
        "ingested_at": datetime.utcnow().isoformat()
    }

    producer.send(TOPIC, payload)

    print(
        f"{payload['event_id']} | "
        f"city={payload['city']} | "
        f"wind={payload['wind_kmh']} km/h | "
        f"gust={payload['gust_kmh']} km/h | "
        f"rain={payload['rain_intensity']} mm | "
        f"visibility={payload['visibility_m']} m"
    )

    time.sleep(1.2)
