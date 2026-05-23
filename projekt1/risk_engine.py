
import json
from kafka import KafkaConsumer, KafkaProducer

from config import KAFKA_SERVER, TOPIC

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id="risk-engine-v2"
)


alert_producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def compute_risk(event):

    score = 0
    reasons = []

    wind = event["wind_kmh"]
    gust = event["gust_kmh"]
    rain = event["rain_intensity"]
    visibility = event["visibility_m"]

    # wind risk
    if wind > 60:
        score += 3
        reasons.append("Strong wind > 60 km/h")

    if wind > 90:
        score += 2
        reasons.append("Extreme wind > 90 km/h")

    # gust risk
    if gust > 80:
        score += 2
        reasons.append("High wind gust > 80 km/h")

    # rain risk
    if rain > 10:
        score += 2
        reasons.append("Heavy rain > 10 mm")

    # visibility risk
    if visibility < 2000:
        score += 2
        reasons.append("Low visibility < 2000m")

    # final decision
    if score >= 7:
        level = "CRITICAL"
    elif score >= 4:
        level = "HIGH"
    elif score >= 2:
        level = "MEDIUM"
    else:
        level = "LOW"

    return score, level, reasons


print("Risk Engine started...")

for msg in consumer:

    event = msg.value

    score, level, reasons = compute_risk(event)

    decision = {
        "event_id": event["event_id"],
        "city": event["city"],
        "venue": event["venue"],
        "risk_score": score,
        "risk_level": level,
        "reasons": reasons,
        "wind": event["wind_kmh"],
        "gust": event["gust_kmh"],
        "rain": event["rain_intensity"],
        "visibility": event["visibility_m"],
        "timestamp": event["timestamp"],
        "ingested_at": event["ingested_at"]
    }

    print(
        f"{decision['event_id']} | "
        f"RISK={level} ({score}) | "
        f"wind={event['wind_kmh']} gust={event['gust_kmh']}"
    )

    if level in ["HIGH", "CRITICAL"]:

        alert_producer.send("event-alerts", decision)

        print("ALERT SENT ->", level)
