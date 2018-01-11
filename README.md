# Kafka Stack Docker Compose

Replicates typical Kafka stack using docker compose.

## Stack version

- Zookeeper version: 3.4.9
- Kafka version: 1.0.0 (Confluent 4.0.0)
- Kafka Schema Registry: Confluent 4.0.0
- Kafka Schema Registry UI: 0.9.3
- Kafka Rest Proxy: Confluent 4.0.0
- Kafka Topics UI: 0.9.3

## Running

```bash
docker-compose up
```

## Viewing topics and schema

- Kafka Topics UI: http://localhost:8000
- Kafka Schema Registry UI: http://localhost:8001

## Confluent Kafka REST Proxy

### Producing messages

```bash
pip install -r requirements.txt

curl -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" \
  --data "{
    \"key_schema_id\": 1,
    \"value_schema_id\": 2,
    \"records\":
        [
            {
              \"key\": $(python serialize.py ./avro/event.avsc examples/event.json | jq .event_id),
              \"value\": $(python serialize.py ./avro/event.avsc examples/event.json)
            }
        ]
  }" \
  "http://localhost:8082/topics/event"
```

### Consuming messages

```bash
# Create an instance in a new consumer group
curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" \
  --data '{"id": "my_avro_consumer_instance_1", "format": "avro", "auto.offset.reset": "smallest"}' \
  http://localhost:8082/consumers/my_avro_consumer

# consume messages with created consumer instance
curl -X GET -H "Accept: application/vnd.kafka.avro.v1+json" \
  http://localhost:8082/consumers/my_avro_consumer/instances/my_avro_consumer_instance_1/topics/event 2>/dev/null | jq .
```

## License

MIT
