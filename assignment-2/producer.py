from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    return {
        'sensor_id': fake.uuid4(),
        'timestamp': fake.iso8601(),
        'temperature': round(random.uniform(15, 35), 2),
        'humidity': round(random.uniform(30, 90), 2),
        'status': random.choice(['OK', 'FAIL', 'WARN'])
    }

if __name__ == '__main__':
    while True:
        data = generate_sensor_data()
        producer.send('iot_sensors', value=data)
        print(f"Sent: {data}")
        time.sleep(1)
