from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

log_levels = ['INFO', 'DEBUG', 'WARNING', 'ERROR']

while True:
    log = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "level": random.choice(log_levels),
        "message": "Service X log entry"
    }
    producer.send('logs', log)
    print("Sent:", log)
    time.sleep(1)
