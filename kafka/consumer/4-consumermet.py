import json
import argparse
import time
from kafka import KafkaConsumer

def process_message(message):
    data = json.loads(message.value.decode('utf-8'))
    category = data['category']
    print(f"Categoría: {category} - Recibió datos: {data}")

    return data['timestamp']  # Devuelve el timestamp para calcular la latencia

def main(m):
    consumer = KafkaConsumer(
        'iot_topic',
        bootstrap_servers='kafka:9092',
        group_id='iot_consumer_group',
        enable_auto_commit=True
    )

    total_latency = 0
    message_count = 0

    for message in consumer:
        receive_time = time.time()
        timestamp = process_message(message)
        latency = receive_time - timestamp

        total_latency += latency
        message_count += 1

        if message_count % 100 == 0:
            average_latency = total_latency / message_count
            print(f"Latencia promedio de {message_count} mensajes: {average_latency}s")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', type=int, help='Número de consumidores')
    args = parser.parse_args()

    try:
        main(args.m)
    except KeyboardInterrupt:
        print('Interrupted')
