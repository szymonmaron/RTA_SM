from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Stan — żyje pomiędzy iteracjami pętli
store_counts = Counter() # {sklep: liczba_tx}
total_amount = defaultdict(float) # {sklep: suma_PLN}
msg_count = 0

print("Konsument zliczający — podsumowanie co 10 wiadomości...\n")

for message in consumer:
    tx = message.value
    store = tx['store']
    store_counts[store] += 1
    total_amount[store] += tx['amount']
    msg_count += 1

    if msg_count % 10 == 0: # co 10 wiadomości
        print(f"\n{'='*55}")
        print(f"{'Sklep':<12} {'Liczba':>8} {'Suma PLN':>12} {'Śr. PLN':>10}")
        print(f"{'-'*55}")
        for sklep in sorted(store_counts):
            n = store_counts[sklep]
            s = total_amount[sklep]
            avg = s / n
            print(f"{sklep:<12} {n:>8} {s:>12.2f} {avg:>10.2f}")
        print(f"{'='*55}")
        print(f"Łącznie przetworzono: {msg_count} wiadomości\n")
