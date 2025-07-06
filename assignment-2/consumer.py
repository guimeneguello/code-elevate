###Consumer Kafka para processar e armazenar dados de sensores IoT.
from kafka import KafkaConsumer 
import json 
import sqlite3


conn = sqlite3.connect('iot_data.db')  # Conecta (ou cria) o banco de dados SQLite chamado 'iot_data.db'
cursor = conn.cursor() 
cursor.execute('''CREATE TABLE IF NOT EXISTS sensor_data (
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    location TEXT
)''') 
conn.commit() 

consumer = KafkaConsumer(
    'iot-sensors',  # Define o tópico Kafka a ser consumido
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Desserializa os dados recebidos do Kafka
)

for message in consumer:  # Itera sobre as mensagens recebidas do Kafka
    data = message.value  # Obtém o valor da mensagem (dados do sensor)
    cursor.execute('''INSERT INTO sensor_data VALUES (?, ?, ?, ?, ?)''',
                   (data['sensor_id'], data['timestamp'], data['temperature'], data['humidity'], data['location']))
    conn.commit()
    print(f"Armazenado: {data}")  # Exibe os dados armazenados no console
