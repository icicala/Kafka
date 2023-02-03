"""
Produce and Consumer for Kafka Broker
"""
import json
from json import dumps
from json import loads
from csv import reader
from kafka import KafkaProducer
from kafka import KafkaConsumer
import mysql.connector


#
# def producer():
#     """
#     Producer for Kafka Broker
#     """
#     kafka_producer = KafkaProducer(
#         bootstrap_servers=['localhost:9092'],
#         value_serializer=lambda x: dumps(x).encode('utf-8')
#     )
#     with open('C:/Users/IonCicala/bikecsv/DimAccount.csv', mode='rt', encoding='utf-8') as csv_data:
#         file_read = reader(csv_data, delimiter='|')
#         for row in file_read:
#             kafka_producer.send('DimCurrency1', value=row)
#

# def consumer():
#     """
#     Consumer from Kafka Broker
#     :return: None
#     """
#     kafka_consumer = KafkaConsumer(
#         'DimCurrency1',
#         bootstrap_servers=['localhost:9092'],
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         group_id='version-1',
#         value_deserializer=lambda x: loads(x.decode('utf-8'))
#     )
#     for event in kafka_consumer:
#         event_data = event.value
#         # manipulation of data
#         print(event_data)

# Create the connection

def producer(connection_config):
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x, default=str).encode('utf-8')
    )
    connection = mysql.connector.connect(**connection_config)

    # Create the cursor object
    cursor = connection.cursor(dictionary=True)
    try:
        test = 'show tables'
        # Sales_Currency Person_Password
        select_query = "select * from Sales_Currency;"
        cursor.execute(select_query)
        result = cursor.fetchall()
        for row in result:
            # print(row)
            kafka_producer.send('PersonTable', value=row)
    except mysql.connector.Error as error:
        print('Connection failed: {}'.format(error))
    connection.close()

def consumer():
    kafka_consumer = KafkaConsumer(
        'PersonTable',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='version-1',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
       )
    for event in kafka_consumer:
        event_data = event.value
        # manipulation of data
        print(event_data)
if __name__ == '__main__':
    # create connection object
    connection_config = {
        'user': 'dbuser',
        'password': 'Password123#@!',
        'host': 'localhost',
        'port': '3306',
        'database': 'ebike_db',
        'raise_on_warnings': True,
        'use_pure': False
    }
    # producer(connection_config)
    # producer()
    consumer()
