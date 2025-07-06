import unittest
from unittest.mock import patch, MagicMock

class TestConsumer(unittest.TestCase):
    @patch('consumer.sqlite3.connect')
    @patch('consumer.KafkaConsumer')
    def test_consumer_inserts_data(self, mock_kafka_consumer, mock_sqlite_connect):
        # Setup mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_sqlite_connect.return_value = mock_conn

        # Setup mock Consumer
        mock_message = MagicMock()
        mock_message.value = {
            'sensor_id': 'test-uuid',
            'timestamp': '2025-07-04T12:00:00Z',
            'temperature': 25.5,
            'humidity': 60.0,
            'location': 'TestCity'
        }
        mock_kafka_consumer.return_value = [mock_message]

        # Simular uma iteração do loop Consumidor
        for message in mock_kafka_consumer.return_value:
            data = message.value
            mock_cursor.execute.assert_any_call(
                '''INSERT INTO sensor_data VALUES (?, ?, ?, ?, ?)''',
                (data['sensor_id'], data['timestamp'], data['temperature'], data['humidity'], data['location'])
            )
            mock_conn.commit.assert_called()

if __name__ == '__main__':
    unittest.main()
