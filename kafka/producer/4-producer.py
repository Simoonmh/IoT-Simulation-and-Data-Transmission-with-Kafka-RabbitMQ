from kafka import KafkaProducer
import json
import threading
import random
import time
import argparse

# Configuración de Kafka
bootstrap_servers = 'kafka:9092'
topic = 'iot_topic'

def send_data(device_id, interval):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    categories = ['A', 'B', 'C', 'D', 'E']
    category = categories[device_id % len(categories)]  # Asignar la categoría de forma secuencial

    while True:
        try:
            timestamp = int(time.time())
            values = random.choices(range(100), k=random.randint(1, 10))
            data = {"timestamp": timestamp, "values": values, "category": category}
            producer.send(topic, key=str(device_id).encode('utf-8'), value=data)
            producer.flush()
            print(f"Dispositivo {device_id} (Categoría: {category}) envió datos: {data}")
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
