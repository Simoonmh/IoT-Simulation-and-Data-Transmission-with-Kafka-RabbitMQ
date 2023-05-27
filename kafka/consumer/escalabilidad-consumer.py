from kafka import KafkaConsumer
import json
import sys
import os
import argparse
import threading

def process_message(group_id, device_id, message):
    data = message.value
    print(f"Grupo {group_id} - Dispositivo {device_id} recibió datos: {data}")

def consume_data(group_id, device_id):
    consumer = KafkaConsumer(f'mi_tema_{device_id}', bootstrap_servers='kafka:9092',
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')), auto_offset_reset='latest')
    for message in consumer:
        process_message(group_id, device_id, message)

def main(groups, consumers_per_group):
    threads = []
    for group_id in range(groups):
        for consumer_id in range(consumers_per_group):
            device_id = group_id * consumers_per_group + consumer_id
            thread = threading.Thread(target=consume_data, args=(group_id, device_id,))
            thread.daemon = True
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', type=int, help='Número de grupos')
    parser.add_argument('-c', type=int, help='Consumidores por grupo')
    args = parser.parse_args()

    try:
        main(args.g, args.c)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
