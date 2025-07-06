import unittest
from unittest.mock import patch, MagicMock

class TestProducer(unittest.TestCase):
    @patch('producer.KafkaProducer')
    @patch('producer.Faker')
    @patch('producer.random')
    def test_producer_sends_data(self, mock_random, mock_faker, mock_kafka_producer):
        # Setup mocks
        fake_instance = MagicMock()
        fake_instance.uuid4.return_value = 'test-uuid'
        fake_instance.iso8601.return_value = '2025-07-04T12:00:00Z'
        fake_instance.city.return_value = 'TestCity'
        mock_faker.return_value = fake_instance
        mock_random.uniform.side_effect = [20.5, 50.5]
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        # Simular uma iteração do loop do produtor
        data = {
            'sensor_id': fake_instance.uuid4(),
            'timestamp': fake_instance.iso8601(),
            'temperature': round(mock_random.uniform(15, 35), 2),
            'humidity': round(mock_random.uniform(30, 90), 2),
            'location': fake_instance.city()
        }
        mock_producer_instance.send('iot-sensors', value=data)
        mock_producer_instance.send.assert_called_with('iot-sensors', value=data)

if __name__ == '__main__':
    unittest.main()
