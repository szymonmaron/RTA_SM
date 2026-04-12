from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0



print("Zliczam transakcje per sklep...")

for message in consumer:
    tx = message.value
    store = tx["store"]
    amount = tx["amount"]
    
    # 1. Zwiększ licznik
    store_counts[store] += 1
    
    # 2. Dodaj do sumy
    total_amount[store] = total_amount.get(store, 0) + amount
    
    # 3. Zwiększ licznik wiadomości
    msg_count += 1
    
    # Co 10 wiadomości wypisz podsumowanie
    if msg_count % 10 == 0:
        print("\n=== PODSUMOWANIE ===")
        print(f"{'Sklep':<10} | {'Liczba':<6} | {'Suma':<10} | {'Średnia':<10}")
        print("-" * 50)
        
        for store in store_counts:
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count
            
            print(f"{store:<10} | {count:<6} | {total:<10.2f} | {avg:<10.2f}")
        
        print("=" * 50)

        
# TWÓJ KOD
# Dla każdej wiadomości:
#   1. Zwiększ store_counts[store]
#   2. Dodaj amount do total_amount[store]
#   3. Co 10 wiadomości wypisz tabelę:
#      Sklep | Liczba | Suma | Średnia