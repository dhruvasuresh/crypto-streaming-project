import psycopg2
from psycopg2.extras import RealDictCursor
from .config import POSTGRES_CONFIG

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**POSTGRES_CONFIG)

def init_database():
    """Initialize the database with required tables."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(50),
            price_usd DECIMAL(30, 8),
            market_cap DECIMAL(30, 2),
            volume_24h DECIMAL(30, 2),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS market_metrics (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(50),
            price_change_24h DECIMAL(20, 8),
            market_cap_change_24h DECIMAL(20, 8),
            volume_change_24h DECIMAL(20, 8),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS window_aggregations (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            coin_id VARCHAR(50),
            avg_price DECIMAL(20, 8),
            min_price DECIMAL(20, 8),
            max_price DECIMAL(20, 8),
            total_volume DECIMAL(20, 2)
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def insert_price_data(coin_id, price_usd, market_cap, volume_24h):
    """Insert price data into the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO crypto_prices (coin_id, price_usd, market_cap, volume_24h)
        VALUES (%s, %s, %s, %s)
    """, (coin_id, price_usd, market_cap, volume_24h))
    
    conn.commit()
    cur.close()
    conn.close()

def insert_market_metrics(coin_id, price_change, market_cap_change, volume_change):
    """Insert market metrics into the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO market_metrics (coin_id, price_change_24h, market_cap_change_24h, volume_change_24h)
        VALUES (%s, %s, %s, %s)
    """, (coin_id, price_change, market_cap_change, volume_change))
    
    conn.commit()
    cur.close()
    conn.close()

def insert_window_aggregation(window_start, window_end, coin_id, avg_price, min_price, max_price, total_volume):
    """Insert window aggregation data into the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO window_aggregations (window_start, window_end, coin_id, avg_price, min_price, max_price, total_volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (window_start, window_end, coin_id, avg_price, min_price, max_price, total_volume))
    
    conn.commit()
    cur.close()
    conn.close() 
