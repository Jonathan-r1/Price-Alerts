
import json
import psycopg2
import os
import smtplib, ssl
from email.message import EmailMessage

from kafka import KafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, ALERT_THRESHOLDS, EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def send_email_alert(symbol, price, alert_type):
    #Sends an email alert for a price threshold.
    msg = EmailMessage()
    subject = f"Crypto Price Alert: {symbol.upper()}"

    if alert_type == "high":
        body = f"🚀 The price of {symbol.upper()} has reached a new high of ${price:,.2f}!"
    else:
        body = f"📉 The price of {symbol.upper()} has dropped to a low of ${price:,.2f}!"

    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER

    # Use a secure SSL connection
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"Sent email alert to {EMAIL_RECEIVER} for {symbol}.")
    except Exception as e:
        print(f"Failed to send email: {e}")

try:
    conn = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}")
    cur = conn.cursor()
    print("PostgreSQL connection successful.")

    for message in consumer:
        price_data = message.value
        symbol = price_data['symbol']
        price = price_data['price']
        timestamp = price_data['timestamp']

        # Store data in PostgreSQL 
        try:
            cur.execute(
                "INSERT INTO crypto_prices (symbol, price_usd, timestamp) VALUES (%s, %s, to_timestamp(%s)) ON CONFLICT DO NOTHING",
                (symbol, price, timestamp)
            )
            conn.commit()
            print(f"Saved to DB: {symbol} at ${price}")
        except psycopg2.Error as e:
            print(f"Database error: {e}")
            conn.rollback()

        # Check for price alerts and send email
        if symbol in ALERT_THRESHOLDS:
            if price >= ALERT_THRESHOLDS[symbol]['high']:
                send_email_alert(symbol, price, "high")
            elif price <= ALERT_THRESHOLDS[symbol]['low']:
                send_email_alert(symbol, price, "low")

except (psycopg2.OperationalError, Exception) as e:
    print(f"An error occurred: {e}")
finally:
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()
