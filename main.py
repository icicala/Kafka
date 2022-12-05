"""
Produce and Consumer for Kafka Broker
"""
from json import dumps
from json import loads
from csv import reader
from kafka import KafkaProducer
from kafka import KafkaConsumer


def producer():
    """
    Producer for Kafka Broker
    """
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    with open('C:/Users/IonCicala/bikecsv/DimCurrency.csv', mode='rt', encoding='utf-8') \
            as csv_data:
        file_read = reader(csv_data, delimiter='|')
        for row in file_read:
            kafka_producer.send('DimCurrency', value=row)


def consumer():
    """
    Consumer from Kafka Broker
    :return: None
    """
    kafka_consumer = KafkaConsumer(
        'DimCurrency',
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
