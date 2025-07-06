from kafka import KafkaConsumer
import mysql.connector
import json
import time

consumer = KafkaConsumer(
    'iot_sensors',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='iot-group'
)

def connect_db():
    return mysql.connector.connect(
        host='db', user='iotuser', password='iotpass', database='iotdb', port=3306
    )

def create_table(conn):
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id VARCHAR(50),
            timestamp VARCHAR(50),
            temperature FLOAT,
            humidity FLOAT,
            status VARCHAR(10)
        )
    ''')
    conn.commit()
    cur.close()

def insert_data(conn, data):
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO sensor_data (sensor_id, timestamp, temperature, humidity, status)
        VALUES (%s, %s, %s, %s, %s)
    ''', (data['sensor_id'], data['timestamp'], data['temperature'], data['humidity'], data['status']))
    conn.commit()
    cur.close()

if __name__ == '__main__':
    conn = None
    while True:
        try:
            if conn is None:
                conn = connect_db()
                create_table(conn)
            for msg in consumer:
                insert_data(conn, msg.value)
                print(f"Inserted: {msg.value}")
        except Exception as e:
            print(f"DB error: {e}")
            time.sleep(5)
            conn = None
