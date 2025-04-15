import json
import time
import requests
from confluent_kafka import Producer
from utils.config import (
    COINGECKO_API_KEY,
    COINGECKO_API_URL,
    COINGECKO_RATE_LIMIT,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    TRACKED_COINS
)

class CryptoDataProducer:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
        })
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.request_count = 0
        self.last_reset = time.time()

    def _check_rate_limit(self):
        current_time = time.time()
        if current_time - self.last_reset >= 60:  # Reset counter every minute
            self.request_count = 0
            self.last_reset = current_time
        
        if self.request_count >= COINGECKO_RATE_LIMIT:
            time.sleep(60 - (current_time - self.last_reset))
            self.request_count = 0
            self.last_reset = time.time()

    def _delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def fetch_prices(self):
        """Fetch current prices for tracked coins."""
        self._check_rate_limit()
        try:
            # Format the request URL with proper parameters
            url = f"{COINGECKO_API_URL}/simple/price"
            params = {
                'ids': ','.join(TRACKED_COINS),
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true'
            }
            
            print(f"Request URL: {url}")
            print(f"Request params: {params}")
            
            response = requests.get(url, params=params, headers=self.headers)
            self.request_count += 1
            
            print(f"Response status: {response.status_code}")
            print(f"Response text: {response.text}")
            
            if response.status_code == 200:
                data = response.json()
                for coin_id, values in data.items():
                    message = {
                        'coin_id': coin_id,
                        'price_usd': values['usd'],
                        'market_cap': values.get('usd_market_cap', 0),
                        'volume_24h': values.get('usd_24h_vol', 0),
                        'timestamp': int(time.time() * 1000)
                    }
                    self.producer.produce(
                        KAFKA_TOPICS['PRICES'],
                        json.dumps(message).encode('utf-8'),
                        callback=self._delivery_report
                    )
                    self.producer.poll(0)
            else:
                print(f"Error fetching prices: {response.status_code}")
        except Exception as e:
            print(f"Exception in fetch_prices: {str(e)}")

    def fetch_market_data(self):
        """Fetch detailed market data for tracked coins."""
        self._check_rate_limit()
        try:
            # Format the request URL with proper parameters
            url = f"{COINGECKO_API_URL}/coins/markets"
            params = {
                'vs_currency': 'usd',
                'ids': ','.join(TRACKED_COINS),
                'order': 'market_cap_desc',
                'per_page': len(TRACKED_COINS),
                'sparkline': 'false'
            }
            
            print(f"Request URL: {url}")
            print(f"Request params: {params}")
            
            response = requests.get(url, params=params, headers=self.headers)
            self.request_count += 1
            
            print(f"Response status: {response.status_code}")
            print(f"Response text: {response.text}")
            
            if response.status_code == 200:
                for coin_data in response.json():
                    message = {
                        'coin_id': coin_data['id'],
                        'price_change_24h': coin_data['price_change_percentage_24h'],
                        'market_cap_change_24h': coin_data['market_cap_change_percentage_24h'],
                        'volume_change_24h': coin_data['total_volume'],
                        'timestamp': int(time.time() * 1000)
                    }
                    self.producer.produce(
                        KAFKA_TOPICS['MARKET_DATA'],
                        json.dumps(message).encode('utf-8'),
                        callback=self._delivery_report
                    )
                    self.producer.poll(0)
            else:
                print(f"Error fetching market data: {response.status_code}")
        except Exception as e:
            print(f"Exception in fetch_market_data: {str(e)}")

    def run(self):
        """Main loop to continuously fetch and send data."""
        while True:
            self.fetch_prices()
            self.fetch_market_data()
            self.producer.flush()  # Ensure all messages are sent
            time.sleep(60)  # Wait for 1 minute before next fetch

if __name__ == "__main__":
    producer = CryptoDataProducer()
    producer.run() 
