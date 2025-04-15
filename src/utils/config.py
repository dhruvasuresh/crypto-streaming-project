import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY', 'CG-np3iHysTiMv5gabCMAwmzpR7')
COINGECKO_API_URL = 'https://api.coingecko.com/api/v3'
COINGECKO_RATE_LIMIT = 30  # requests per minute

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPICS = {
    'PRICES': 'crypto_prices',
    'MARKET_DATA': 'crypto_market_data',
    'HISTORICAL': 'crypto_historical'
}

# Spark Configuration
SPARK_APP_NAME = 'CryptoStreaming'
SPARK_MASTER = 'local[*]'
SPARK_BATCH_DURATION = 60  # seconds

# Database Configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'database': os.getenv('POSTGRES_DB', 'crypto_db'),
    'user': os.getenv('POSTGRES_USER', 'dhruva'),
    'password': os.getenv('POSTGRES_PASSWORD', 'Dhruva@2004'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}

# Window Configuration
WINDOW_DURATION = 15  # minutes
SLIDE_DURATION = 5    # minutes

# Tracked Cryptocurrencies
TRACKED_COINS = [
    'bitcoin',
    'ethereum',
    'binancecoin',
    'ripple',
    'cardano',
    'solana',
    'polkadot',
    'dogecoin'
] 
