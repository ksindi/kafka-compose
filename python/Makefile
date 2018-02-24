include ../Makefile

app_name:=consumer

run: command=python3 consumer.py
run: extra_run_opts=--net=host \
	-e KAFKA_BROKERS=0.0.0.0:9092 \
	-e KAFKA_CONSUMER_GROUP=python_consumer_group \
	-e KAFKA_CLIENT_ID=client_1 \
	-e KAFKA_TOPICS=event \
	-e KAFKA_SCHEMA_REGISTRY_URL=http://0.0.0.0:8081/
