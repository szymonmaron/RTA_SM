
import json
from kafka import KafkaConsumer
from collections import deque

from config import KAFKA_SERVER


consumer = KafkaConsumer(
    "event-alerts",
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id="alert-dashboard-v2"
)

# ----------------------------
# STATE
# ----------------------------
alert_count = 0
critical_count = 0
high_count = 0

WINDOW_SIZE = 20
window = deque(maxlen=WINDOW_SIZE)


# ----------------------------
# STATS HELPERS
# ----------------------------
def calc_avg(field):
    if not window:
        return 0
    return sum(a[field] for a in window) / len(window)


def print_dashboard():
    print("\n" + "=" * 80)
    print("EVENT WEATHER LIVE DASHBOARD")
    print("=" * 80)

    print(f"Total alerts   : {alert_count}")
    print(f"HIGH alerts    : {high_count}")
    print(f"CRITICAL alerts: {critical_count}")

    print("\nLATEST ALERTS:")

    for a in list(window)[-5:]:
        print("\n----------------------------------------")
        print(f"Event      : {a['event_id']}")
        print(f"City       : {a['city']}")
        print(f"Venue      : {a['venue']}")
        print(f"Risk level : {a['risk_level']} ({a['risk_score']})")
        print(f"Time       : {a['timestamp']}")
        print(f"Ingested   : {a['ingested_at']}")

        print("\nWeather snapshot:")
        print(f"  wind       : {a['wind']} km/h")
        print(f"  gust       : {a['gust']} km/h")
        print(f"  rain       : {a['rain']} mm")
        print(f"  visibility : {a['visibility']} m")

        print("\nReasons:")
        for r in a.get("reasons", []):
            print(f"  - {r}")

    print("\n" + "=" * 80 + "\n")


print("Weather Alert Dashboard started...\n")

for msg in consumer:

    alert = msg.value

    alert_count += 1
    window.append(alert)

    if alert["risk_level"] == "CRITICAL":
        critical_count += 1
    elif alert["risk_level"] == "HIGH":
        high_count += 1

    print_dashboard()
