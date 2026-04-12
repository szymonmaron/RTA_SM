from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    "count": 0,
    "sum": 0.0,
    "min": float("inf"),
    "max": float("-inf")
})

msg_count = 0

print("Zbieram statystyki per kategoria...")

for message in consumer:
    tx = message.value
    category = tx["category"]
    amount = tx["amount"]
    
    stats[category]["count"] += 1
    stats[category]["sum"] += amount
    stats[category]["min"] = min(stats[category]["min"], amount)
    stats[category]["max"] = max(stats[category]["max"], amount)
    
    msg_count += 1
    
    if msg_count % 10 == 0:
        print("\n=== STATYSTYKI KATEGORII ===")
        print(f"{'Kategoria':<15} | {'Liczba':<6} | {'Suma':<10} | {'Min':<10} | {'Max':<10}")
        print("-" * 65)
        
        for cat, data in stats.items():
            print(f"{cat:<15} | {data['count']:<6} | {data['sum']:<10.2f} | {data['min']:<10.2f} | {data['max']:<10.2f}")
        
        print("=" * 65)