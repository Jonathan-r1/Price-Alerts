import os


#  CoinGecko API 
API_URL = os.getenv('coin_api')

# The Kafka broker's bootstrap servers
KAFKA_BOOTSTRAP_SERVERS = os.getenv('localhost:9092')


KAFKA_TOPIC = os.getenv('crypto_prices_topic')


# PostgreSQL Database Settings

DB_NAME = os.getenv('crypto_prices_db')
DB_USER = os.getenv('user')
DB_PASSWORD = os.getenv('password')
DB_HOST = os.getenv('localhost')
DB_PORT = os.getenv('port')


# Email Settings

EMAIL_SENDER = os.getenv('email')

# The sender's email password.

EMAIL_PASSWORD = os.getenv('app_password')

# The recipient's email address to receive alerts.
EMAIL_RECEIVER = os.getenv('send_email')


# Alert Thresholds: the price thresholds that will trigger an alert.

ALERT_THRESHOLDS = {
    "bitcoin": {"high": 75000, "low": 65000},
    "ethereum": {"high": 4500, "low": 3500},
    "litecoin": {"high": 100, "low": 75},
    "dogecoin": {"high": 0.15, "low": 0.10}
}