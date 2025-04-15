# Crypto Streaming Project

This project implements a real-time cryptocurrency data streaming system using CoinGecko API, Apache Kafka, Apache Spark, and PostgreSQL. It provides both streaming and batch processing capabilities for cryptocurrency data analysis.

## Project Components

1. **Data Collection**: Fetches cryptocurrency data from CoinGecko API
2. **Stream Processing**: Uses Apache Kafka for message queuing and Apache Spark for stream processing
3. **Data Storage**: PostgreSQL database for storing processed data
4. **Batch Processing**: Identical queries run in batch mode for comparison

## Prerequisites

### System Requirements
- Python 3.8 or higher
- Java 8 or higher (required for Apache Spark)
- At least 8GB RAM (16GB recommended)
- 20GB free disk space

### Required Software
1. **Apache Kafka**
   - Download from: https://kafka.apache.org/downloads
   - Version: 3.5.0 or higher
   - Requires Zookeeper

2. **Apache Spark**
   - Download from: https://spark.apache.org/downloads.html
   - Version: 3.5.0
   - Set SPARK_HOME environment variable

3. **PostgreSQL**
   - Download from: https://www.postgresql.org/download/
   - Version: 14 or higher

4. **Docker** (optional)
   - Download from: https://www.docker.com/products/docker-desktop
   - For containerized setup

## Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd dbtmini
```

2. **Create and activate virtual environment**
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python -m venv venv
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Set up environment variables**
Create a `.env` file in the project root with the following variables:
```env
COINGECKO_API_KEY=your_api_key
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=crypto_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
SPARK_MASTER=local[*]
```

5. **Database Setup**
```sql
CREATE DATABASE crypto_db;
CREATE TABLE crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50),
    price_usd DECIMAL(20,8),
    market_cap DECIMAL(20,8),
    volume_24h DECIMAL(20,8),
    timestamp TIMESTAMP
);
CREATE TABLE market_metrics (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50),
    price_change_24h DECIMAL(20,8),
    market_cap_change_24h DECIMAL(20,8),
    volume_change_24h DECIMAL(20,8),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE window_aggregations (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    coin_id VARCHAR(50),
    avg_price DECIMAL(20,8),
    min_price DECIMAL(20,8),
    max_price DECIMAL(20,8),
    total_volume DECIMAL(20,8)
);
```

## Running the Project

1. **Start Kafka and Zookeeper**
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties
```

2. **Start PostgreSQL**
```bash
# Windows
pg_ctl -D "C:\Program Files\PostgreSQL\14\data" start

# Linux
sudo service postgresql start
```

3. **Run the Data Producer**
```bash
python src/producer.py
```

4. **Run the Stream Processor**
```bash
python src/stream_processor.py
```

5. **Run the Batch Processor**
```bash
python src/batch_processor.py
```

## Project Structure

```
.
├── src/
│   ├── producer.py           # CoinGecko API data producer
│   ├── stream_processor.py   # Spark streaming processor
│   ├── batch_processor.py    # Batch processing queries
│   └── utils/
│       ├── database.py       # Database utilities
│       └── config.py         # Configuration settings
├── data/                     # Sample data and schemas
├── tests/                    # Test files
├── requirements.txt          # Project dependencies
└── README.md                 # Project documentation
```

## Features

- Real-time cryptocurrency price tracking
- Window-based aggregations (15-minute windows)
- Hashtag analysis and trending coins
- Batch processing for historical data analysis
- Performance comparison between streaming and batch modes
- Volume analysis and market trend detection

## API Usage

The project uses the CoinGecko API with the following endpoints:
- `/simple/price` for current prices
- `/coins/markets` for market data
- `/coins/{id}/market_chart` for historical data

## Rate Limiting

The CoinGecko API has a rate limit of 30 requests per minute. The project implements efficient caching and request scheduling to stay within these limits.

## Troubleshooting

1. **Kafka Connection Issues**
   - Ensure Kafka and Zookeeper are running
   - Check KAFKA_BOOTSTRAP_SERVERS in .env file
   - Verify network connectivity

2. **PostgreSQL Connection Issues**
   - Verify database credentials in .env file
   - Check if PostgreSQL service is running
   - Ensure database and tables are created

3. **Spark Memory Issues**
   - Adjust memory settings in config.py
   - Increase system memory allocation
   - Check Spark logs for detailed errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
