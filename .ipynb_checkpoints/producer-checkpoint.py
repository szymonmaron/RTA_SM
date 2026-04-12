from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tx_counter = 1

def generate_transaction():
    global tx_counter
    
    transaction = {
        "tx_id": f"TX{tx_counter:04d}",
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warszawa", "Kraków", "Gdańsk", "Wrocław"]),
        "category": random.choice(["elektronika", "odzież", "żywność", "książki"]),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    tx_counter += 1
    return transaction
    pass

# TWÓJ KOD
if __name__ == "__main__":
    while True:
        tx = generate_transaction()
        producer.send("transactions", tx)
        print(tx)
        time.sleep(1)
# Pętla: generuj transakcję, wyślij do tematu 'transactions', wypisz, sleep 1s