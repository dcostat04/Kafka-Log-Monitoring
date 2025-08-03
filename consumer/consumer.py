from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='log-monitor-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for messages...")
for msg in consumer:
    log = msg.value
    if log['level'] == 'ERROR':
        print("⚠️ ERROR:", log)
    else:
        print("Log:", log)
