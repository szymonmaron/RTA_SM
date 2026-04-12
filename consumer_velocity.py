from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Dla każdego usera przechowujemy czasy jego transakcji
user_events = defaultdict(deque)

WINDOW = timedelta(seconds=60)

print("Monitoruję anomalie prędkości (więcej niż 3 transakcje / 60s)...")

for message in consumer:
    tx = message.value
    user_id = tx["user_id"]
    
    # Parsowanie czasu (ISO → datetime)
    event_time = datetime.fromisoformat(tx["timestamp"])
    
    events = user_events[user_id]
    events.append(event_time)
    
    # Usuwamy stare zdarzenia spoza okna 60s
    while events and (event_time - events[0]) > WINDOW:
        events.popleft()
    
    # Sprawdzenie warunku (więcej niż 3 w oknie)
    if len(events) > 3:
        print(f"ALERT: {user_id} wykonał {len(events)} transakcji w ciągu 60s!")
        print(f"  Ostatnia: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
