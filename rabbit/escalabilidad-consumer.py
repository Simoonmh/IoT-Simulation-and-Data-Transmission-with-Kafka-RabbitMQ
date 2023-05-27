import pika
import json
import sys
import os
import argparse

def main(m):
    # Configuración de RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='iot_direct_exchange', exchange_type='direct')

    # Declarar la cola en RabbitMQ
    channel.queue_declare(queue='iot_data')

    # Vincular la cola al exchange directo
    channel.queue_bind(queue='iot_data', exchange='iot_direct_exchange', routing_key='iot_data')

    # Parámetros del sistema
    for i in range(m):
        queue_name = f'iot_consumer_{i}'
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(queue=queue_name, exchange='iot_direct_exchange', routing_key='iot_data')

    # Función para procesar los mensajes recibidos
    def process_message(ch, method, properties, body):
        data = json.loads(body)
        device_id = method.consumer_tag
        print(f"Dispositivo {device_id} recibió datos: {data}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Configurar el número máximo de mensajes en paralelo por consumidor
    channel.basic_qos(prefetch_count=1)

    # Registrar consumidores
    for i in range(m):
        queue_name = f'iot_consumer_{i}'
        channel.basic_consume(queue=queue_name, on_message_callback=process_message, consumer_tag=str(i))

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
