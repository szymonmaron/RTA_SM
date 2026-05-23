
import json
from kafka import KafkaConsumer

from config import KAFKA_SERVER

consumer = KafkaConsumer(
    "event-alerts",
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id="alert-dashboard"
)

alert_count = 0
critical_count = 0
high_count = 0

latest_alerts = [] 
MAX_BUFFER = 5


def print_dashboard():
    print("\n" + "=" * 70)
    print("EVENT WEATHER LIVE DASHBOARD")
    print("=" * 70)

    print(f"Total alerts   : {alert_count}")
    print(f"HIGH alerts    : {high_count}")
    print(f"CRITICAL alerts: {critical_count}")

    print("\nLatest alerts:")

    for a in latest_alerts[-MAX_BUFFER:]:
        print(
            f"- {a['event_id']} | {a['city']} | "
            f"RISK={a['risk_level']} ({a['risk_score']}) | "
            f"wind={a['wind']} gust={a['gust']} rain={a['rain']} visibility={a['visibility']}"
        )

    print("=" * 70 + "\n")


print("Weather Alert Dashboard started... waiting for events\n")

for msg in consumer:

    alert = msg.value

    alert_count += 1
    latest_alerts.append(alert)

    if alert["risk_level"] == "CRITICAL":
        critical_count += 1
    elif alert["risk_level"] == "HIGH":
        high_count += 1

    # live refresh dashboard
    print_dashboard()
