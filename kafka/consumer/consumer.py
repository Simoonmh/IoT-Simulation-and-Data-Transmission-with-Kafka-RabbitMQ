from kafka import KafkaConsumer
import json
import sys
import os
import argparse
import threading

def process_message(device_id, message):
    data = message.value
    print(f"Dispositivo {device_id} recibió datos: {data}")

def consume_data(device_id):
    consumer = KafkaConsumer(f'mi_tema_{device_id}', bootstrap_servers='kafka:9092',
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')), auto_offset_reset='latest')
    for message in consumer:
        process_message(device_id, message)

def main(m):
    threads = []
    for device_id in range(m):
        thread = threading.Thread(target=consume_data, args=(device_id,))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', type=int, help='Número de consumidores')
    args = parser.parse_args()

    try:
        main(args.m)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
