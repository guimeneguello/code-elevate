#!/bin/sh
# Wait for Kafka to be available before running the app
set -e

host="$1"
shift

until nc -z "$host" 9092; do
  echo "Waiting for Kafka at $host:9092..."
  sleep 2
done

exec "$@"
