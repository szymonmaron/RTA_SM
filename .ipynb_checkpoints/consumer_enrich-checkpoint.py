from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='enrich-group',  # inny group_id niż poprzednio
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję i wzbogacam transakcje o risk_level...")

for message in consumer:
    tx = message.value
    
    # Dodanie pola risk_level
    amount = tx["amount"]
    if amount > 3000:
        tx["risk_level"] = "HIGH"
    elif amount > 1000:
        tx["risk_level"] = "MEDIUM"
    else:
        tx["risk_level"] = "LOW"
    
    print(tx)
    
# TWÓJ KOD
# Czytaj z 'transactions' (użyj INNEGO group_id!)
# Dodaj pole risk_level na podstawie amount
# Wypisz wzbogaconą transakcję