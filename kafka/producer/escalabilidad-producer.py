from kafka import KafkaProducer
from json import dumps
import random
import time
import argparse
import threading

def send_data(device_id, interval):
    servidores_bootstrap = 'kafka:9092'
    topic = f'mi_tema_{device_id}'
    producer = KafkaProducer(bootstrap_servers=[servidores_bootstrap], value_serializer=lambda x: dumps(x).encode('utf-8'))

    while True:
        try:
            timestamp = int(time.time())
            values = random.choices(range(100), k=random.randint(1, 10))
            data = {"timestamp": timestamp, "values": values}
            producer.send(topic, value=data)
            producer.flush()
            print(f"Dispositivo {device_id} envió datos: {data}")
        except Exception as e:
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
