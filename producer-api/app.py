from fastapi import FastAPI
from kafka import KafkaProducer
import json

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.get("/")
def root():
    return {"status": "running"}

@app.post("/order")
def create_order(order_id: int, amount: int):

    event = {
        "order_id": order_id,
        "amount": amount
    }

    producer.send("orders", event)

    return {"status": "sent", "data": event}