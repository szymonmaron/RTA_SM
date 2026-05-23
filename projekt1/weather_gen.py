
import random
from datetime import datetime

def generate_weather(event):

    wind = random.randint(10, 55)
    gust = wind + random.randint(0, 25)
    rain = round(random.uniform(0, 6), 2)
    visibility = random.randint(3000, 10000)

    # extreme weather injection (5%)
    if random.random() < 0.05:
        wind = random.randint(70, 130)
        gust = wind + random.randint(10, 40)
        rain = round(random.uniform(8, 25), 2)
        visibility = random.randint(500, 3000)

    return {
        "event_id": event["event_id"],
        "city": event["city"],
        "venue": event["venue"],
        "wind_kmh": wind,
        "gust_kmh": gust,
        "rain_intensity": rain,
        "visibility_m": visibility,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "event_weather_simulator"
    }
