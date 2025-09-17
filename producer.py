# producer.py
import time
import requests
import json
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, API_URL

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_produce_data(coin_symbols):
    try:
        symbols_str = ",".join(coin_symbols)
        response = requests.get(API_URL.format(symbols_str))
        response.raise_for_status()
        data = response.json()

        for symbol in coin_symbols:
            if symbol in data:
                price_data = {
                    "symbol": symbol,
                    "price": data[symbol]['usd'],
                    "timestamp": int(time.time())
                }
                producer.send(KAFKA_TOPIC, value=price_data)
                print(f"Produced: {price_data}")
            else:
                print(f"Symbol {symbol} not found in API response.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    tracked_cryptos = ['bitcoin', 'ethereum']
    while True:
        fetch_and_produce_data(tracked_cryptos)
        time.sleep(30)