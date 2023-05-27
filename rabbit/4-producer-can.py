import pika
import json
import threading
import random
import time
import argparse

def send_data(device_id, interval):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='iot_topic_exchange', exchange_type='topic')
    channel.queue_declare(queue='iot_data')
    channel.queue_bind(queue='iot_data', exchange='iot_topic_exchange', routing_key='iot_data')

    categories = ['A', 'B', 'C', 'D', 'E']
    category = categories[device_id % len(categories)]  # Asignar la categoría de forma secuencial

    while True:
        try:
            timestamp = int(time.time())
            values = random.choices(range(100), k=random.randint(1, 10))
            data = json.dumps({"timestamp": timestamp, "values": values, "category": category})
            channel.basic_publish(exchange='iot_topic_exchange', routing_key='category.' + category, body=data)
            print(f"Dispositivo {device_id} (Categoría: {category}) envió datos: {data}")
        except pika.exceptions.AMQPError as e:
            print(f"Error al enviar datos: {str(e)}")

        time.sleep(interval)

def main(n, t):
    threads = []

    for device_id in range(n):
        thread = threading.Thread(target=send_data, args=(device_id, t))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, help='Número de dispositivos')
    parser.add_argument('-t', type=int, help='Intervalo de tiempo en segundos')
    args = parser.parse_args()

    try:
        main(args.n, args.t)
    except KeyboardInterrupt:
        print('Interrupted')
