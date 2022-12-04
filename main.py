from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps
from json import loads


def producer():
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    for i in range(10):
        data = {'counter': i}
        kafka_producer.send('topic_test', value=data)


def consumer():
    kafka_consumer = KafkaConsumer(
        'topic_test',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='version-1',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    for event in kafka_consumer:
        event_data = event.value
        # manipulation of data
        print(event_data)


if __name__ == '__main__':
    # producer()
    consumer()
