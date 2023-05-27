import pika
import json
import time
import argparse

# Configuración de RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='iot_topic_exchange', exchange_type='topic')

def process_message(ch, method, properties, body):
    data = json.loads(body)
    device_id = method.consumer_tag.split('.')[1]
    category = method.routing_key.split('.')[1]  # Obtener la categoría del dispositivo a partir de la clave de enrutamiento
    print(f"Dispositivo {device_id} (Categoría: {category}) recibió datos: {data}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main(m):
    # Declarar la cola en RabbitMQ para cada categoría
    categories = ['A', 'B', 'C', 'D', 'E']
    queues = []

    for category in categories:
        queue_name = f'iot_consumer_{category}'
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(queue=queue_name, exchange='iot_topic_exchange', routing_key=f'category.{category}')
        queues.append(queue_name)

    # Configurar el número máximo de mensajes en paralelo por consumidor
    channel.basic_qos(prefetch_count=1)

    # Variables para calcular la latencia promedio
    total_latency = 0
    message_count = 0

    # Función para procesar los mensajes recibidos
    def process_message(ch, method, properties, body):
        nonlocal total_latency, message_count

        data = json.loads(body)
        device_id = method.consumer_tag
        category = method.routing_key.split('.')[1]  # Obtener la categoría del dispositivo a partir de la clave de enrutamiento
        print(f"Dispositivo {device_id} (Categoría: {category}) recibió datos: {data}")

        # Calcular la latencia
        timestamp = data['timestamp']
        latency = time.time() - timestamp

        total_latency += latency
        message_count += 1

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if message_count % 10 == 0:
            average_latency = total_latency / message_count
            print(f"Latencia promedio de {message_count} mensajes: {average_latency}s")

    # Configurar el consumidor para cada categoría
    for queue_name in queues:
        channel.basic_consume(queue=queue_name, on_message_callback=process_message)

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
