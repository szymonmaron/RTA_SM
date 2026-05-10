
from kafka import KafkaProducer
import json,random,time
from datetime import datetime
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v:json.dumps(v).encode('utf-8')
)
stores =['Warszawa', 'Kraków','Gdańsk','Wrocław']
categories=['elektronika', 'odzież', 'żywność','książki']
def generate_transaction():
    tx_num =random.randint(1,9999)
    return {
    'tx_id': f'TX{tx_num:04d}',
    'user_id': f'u{random.randint(1, 20):02d}',
    'amount': round(random.uniform(5.0, 5000.0), 2),
    'store': random.choice(stores),
    'category': random.choice(categories),
    'timestamp': datetime.now().isoformat(),
    }
for i in range(50):
    tx = generate_transaction()
    producer.send('transactions',value=tx)
    print(f"[{i+1:02d}]{tx['tx_id']}| {tx['amount']:8.2f} PLN| {tx['store']}")
    time.sleep(1)
producer.flush()
producer.close()
print("Gotowe.")
