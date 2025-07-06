# Dockerfile for assignment-2 (producer and consumer)
FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x wait-for-kafka.sh

# Default command (override in docker-compose)
CMD ["python", "producer.py"]
