from kafka import KafkaConsumer
import json
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group', #INNYgroup_idniżfilter-group!
    value_deserializer=lambda x:json.loads(x.decode('utf-8'))
)

def get_risk_level(amount):
    if amount >3000:
        return 'HIGH'
    elifamount > 1000:
        return 'MEDIUM'
    else:
        return 'LOW'

print("Konsument wzbogacający — dodaje risk_level...\n")

for message in consumer:
    tx =message.value
    tx['risk_level'] =get_risk_level(tx['amount'])
    print(
        f"[{tx['risk_level']:6s}]{tx['tx_id']} |"
        f"{tx['amount']:8.2f} PLN | {tx['store']}"
        )
