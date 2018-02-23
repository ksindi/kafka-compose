# Kafka PySpark Demo

Demo of Python Spark Streaming and Kafka

## Usage

Make sure you run sudo:
```
docker-compose create
docker-compose start
```

To stop:
```
docker-compose stop
```

To destroy:
```
docker-compose down
```

Adding topics:

```
# create twitter Kafka topic if none exist
docker run \
  --net=host \
  --rm confluentinc/cp-kafka:3.1.1 \
  kafka-topics --create --topic twitter --partitions 1 --replication-factor 1 \
    --if-not-exists --zookeeper localhost:32181

# Describe topic
docker run \
  --net=host \
  --rm confluentinc/cp-kafka:3.1.1 \
  kafka-topics --describe --topic twitter --zookeeper localhost:32181

# Create consumer that reads from beginning
docker run \
  --net=host \
  --rm \
  confluentinc/cp-kafka:3.1.1 \
  kafka-console-consumer --bootstrap-server localhost:29092 --topic twitter \
    --new-consumer --from-beginning --max-messages 42
```

## License

MIT
