import pika
import json
import threading
import random
import time
import argparse

class ThroughputCounter:
    def __init__(self):
        self.message_count = 0
        self.start_time = time.time()

    def increment(self):
        self.message_count += 1

    def get_throughput(self):
        elapsed_time = time.time() - self.start_time
        return self.message_count / elapsed_time

def send_data(device_id, interval, throughput_counter):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='iot_data')

    while True:
        try:
            timestamp = int(time.time())
            values = random.choices(range(100), k=random.randint(1, 10))
            data = json.dumps({"timestamp": timestamp, "values": values})
            channel.basic_publish(exchange='', routing_key='iot_data', body=data)
            print(f"Dispositivo {device_id} envió datos: {data}")

            throughput_counter.increment()

            if device_id == 0:  # Solo el primer dispositivo imprime el promedio del throughput
                throughput = throughput_counter.get_throughput()
                print(f"Throughput Promedio: {throughput} mensajes/segundo")

        except pika.exceptions.AMQPError as e:
            print(f"Error al enviar datos: {str(e)}")

        time.sleep(interval)

def main(n, t):
    throughput_counter = ThroughputCounter()

    threads = []
    for device_id in range(n):
        thread = threading.Thread(target=send_data, args=(device_id, t, throughput_counter))
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
