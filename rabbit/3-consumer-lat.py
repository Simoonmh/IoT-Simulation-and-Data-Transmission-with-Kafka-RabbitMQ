import pika
import json
import sys
import os
import argparse
import time

# Variables para calcular la latencia y el throughput
message_count = 0
latency_sum = 0

def main(m):
    # Configuración de RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declarar la cola en RabbitMQ
    channel.queue_declare(queue='iot_data')

    # Parámetros del sistema
    for i in range(m):
        queue_name = f'iot_consumer_{i}'
        channel.queue_declare(queue=queue_name)

    # Función para procesar los mensajes recibidos
    def process_message(ch, method, properties, body):
        global message_count, latency_sum
        data = json.loads(body)
        device_id = method.consumer_tag
        print(f"Dispositivo {device_id} recibió datos: {data}")

        # Cálculo de la latencia y el promedio cada 100 mensajes
        timestamp = data['timestamp']
        latency = time.time() - timestamp
        latency_sum += latency
        message_count += 1

        if message_count % 100 == 0:
            average_latency = latency_sum / message_count
            print(f"Latencia promedio de {message_count} mensajes: {average_latency} segundos")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Configurar el número máximo de mensajes en paralelo por consumidor
    channel.basic_qos(prefetch_count=1)

    # Registrar consumidores
    for i in range(m):
        queue_name = f'iot_consumer_{i}'
        channel.basic_consume(queue='iot_data', on_message_callback=process_message, consumer_tag=str(i))

    # Esperar por nuevos mensajes
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


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
