import os

from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


config = {
    k: os.getenv(k, default)
    for k, default in [
        ('KAFKA_BROKERS', '0.0.0.0:9092'),
        ('KAFKA_CONSUMER_GROUP', 'python_consumer'),
        ('KAFKA_CLIENT_ID', 'client_1'),
        ('KAFKA_TOPICS', 'event'),
        ('KAFKA_SCHEMA_REGISTRY_URL', 'http://0.0.0.0:8081'),
        ('CONSUMER_BATCH_SIZE', 100),
    ]
}

settings = {
    'bootstrap.servers': config['KAFKA_BROKERS'],
    'group.id': config['KAFKA_CONSUMER_GROUP'],
    'client.id': config['KAFKA_CLIENT_ID'],
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'schema.registry.url': config['KAFKA_SCHEMA_REGISTRY_URL']
}

consumer = AvroConsumer(settings)
consumer.subscribe(config['KAFKA_TOPICS'].split(','))


def process_message(batch, msg, batch_size):
    print('{key}: {val}'.format(key=msg.key(), val=msg.value()))
    if len(batch) < batch_size:
        batch.add(msg)

    elif len(batch) == batch_size:
        print(batch)
        persist_messages(batch)
        # purge the batch
        return set()

    else:
        # unexpected happening
        raise Exception("Unexpected batch size")

    return batch


def persist_messages(batch):
    if len(batch) == 0:
        return

    for msg in batch:
        print(msg.value())

    print('Committing to ZK the offset for this client_id and group_id')
    consumer.commit()


def main():
    batch = set()
    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg:
                batch = process_message(msg, config['CONSUMER_BATCH_SIZE'])
            elif msg is None:
                print('No message')
            elif not msg.error():
                print('Received message: {}'.format(msg.value()))
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {}, {}'.format(msg.topic(), msg.partition()))
            else:
                print('Error occurred: {}'.format(msg.error().str()))
    except KeyboardInterrupt:
        pass
    except SerializerError as e:
        print('Message deserialization failed for {msg}: {e}'.format(msg=msg, e=e))
    finally:
        persist_messages(batch)
        consumer.close()


if __name__ == '__main__':
    main()
