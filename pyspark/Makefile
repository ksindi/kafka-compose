include ../Makefile

app_name:=pyspark

run: command=python3 producer.py
run: extra_run_opts=--net=host \
	-e KAFKA_BROKERS=0.0.0.0:9092 \
	-e KAFKA_TOPICS=twitter
