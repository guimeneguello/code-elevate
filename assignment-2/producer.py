###Producer Kafka para simular sensores IoT enviando dados.
from faker import Faker
from kafka import KafkaProducer
import json
import time
import random

fake = Faker() # criando instância do Faker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # serializa os dados em json e encode utf-8
)

topic = 'iot-sensors'

while True: # o loop infinito serve para simular um envio contínuo
    data = {
        'sensor_id': fake.uuid4(),
        'timestamp': fake.iso8601(), #formato ISO 8601
        'temperature': round(random.uniform(15, 35), 2),
        'humidity': round(random.uniform(30, 90), 2),
        'location': fake.city()
    }
    producer.send(topic, value=data) #envia os dados para o tópico kafka
    print(f"Enviado: {data}") # exibe no console
    time.sleep(1)
