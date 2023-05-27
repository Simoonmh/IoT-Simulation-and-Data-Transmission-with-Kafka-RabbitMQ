from kafka import KafkaConsumer
from kafka import KafkaConsumer
import json
import sys
import os
import argparse
import threading

#Configuración de Kafka
bootstrap_servers = 'kafka:9092'
topic = 'iot_topic'

def process_message(message):
    data = json.loads(message.value.decode('utf-8'))
    device_id = message.key.decode('utf-8')
    category = data['category']
    print(f"Dispositivo {device_id} (Categoría: {category}) recibió datos: {data}")

def main(m):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=bootstrap_servers,
                             group_id='iot_group')

    #Procesar los mensajes recibidos
    for message in consumer:
        process_message(message)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', type=int, help='Número de consumidores')
    args = parser.parse_args()

    try:
        main(args.m)
    except KeyboardInterrupt:
        print('Interrupted')
