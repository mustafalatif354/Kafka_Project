from kafka import KafkaConsumer
import psycopg2
import json
import time

# wachten tot services klaar zijn
time.sleep(10)

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

conn = psycopg2.connect(
    dbname="orders",
    user="user",
    password="pass",
    host="postgres"
)

cursor = conn.cursor()

print("Consumer started...")

for message in consumer:
    order = message.value
    print("Processing:", order)

    cursor.execute(
        "INSERT INTO orders (order_id, amount) VALUES (%s, %s)",
        (order["order_id"], order["amount"])
    )

    conn.commit()