app_name:=consumer
container_name:=${app_name}

build:
	docker build ${extra_build_opts} --tag ${app_name} .

run-without-build:
	docker run -it --init --rm --name ${container_name} ${extra_run_opts} ${app_name} ${command} ${extra_args}

.PHONY: run
run: build run-without-build
run: command=python3 consumer.py
run: extra_run_opts=--net=host \
	-e KAFKA_BROKERS=0.0.0.0:9092 \
	-e KAFKA_CONSUMER_GROUP=python_consumer_group \
	-e KAFKA_CLIENT_ID=client_1 \
	-e KAFKA_TOPICS=event \
	-e KAFKA_SCHEMA_REGISTRY_URL=http://0.0.0.0:8081/
