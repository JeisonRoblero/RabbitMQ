import pika
import threading

def receive_messages_from_queue_1(queue_name_1, queue_name_2):
    # Conexión con RabbitMQ usando el puerto para AMQP
    connection = pika.BlockingConnection(pika.ConnectionParameters('161.97.146.0', 5672, '/', pika.PlainCredentials('tele', 'tele')))
    channel = connection.channel()

    # Asegura que las colas existen y son durables
    channel.queue_declare(queue=queue_name_1, durable=True)
    channel.queue_declare(queue=queue_name_2, durable=True)

    def callback(ch, method, properties, body):
        print(f"[Receiver] Recibido de la cola {queue_name_1}: {body.decode()}")
        # Genera 8 nuevos mensajes para la cola 2
        for i in range(8):
            new_message = f"{body.decode()} - generado por hilo 1 - mensaje {i + 1}"
            channel.basic_publish(exchange='',
                                  routing_key=queue_name_2,
                                  body=new_message,
                                  properties=pika.BasicProperties(delivery_mode=2))  # Mensaje durable
            print(f"[Remitente] Enviado: {new_message} a la cola {queue_name_2}")

    # Suscribe a los mensajes de la cola 1
    channel.basic_consume(queue=queue_name_1, on_message_callback=callback, auto_ack=True)

    print(f"Esperando mensajes de {queue_name_1}...")
    channel.start_consuming()

def receive_messages_from_queue_2(queue_name):
    # Conexión con RabbitMQ usando el puerto para AMQP
    connection = pika.BlockingConnection(pika.ConnectionParameters('161.97.146.0', 5672, '/', pika.PlainCredentials('tele', 'tele')))
    channel = connection.channel()

    # Asegura que la cola existe y es durable
    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):
        print(f"[Receiver] Recibido de la cola {queue_name}: {body.decode()}")

    # Suscribe a los mensajes de la cola 2
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print(f"Esperando mensajes de {queue_name}...")
    channel.start_consuming()

if __name__ == "__main__":
    cola_1 = "prueba_cola"
    cola_2 = "prueba_cola2"

    # Se crean dos hilos para manejar cada cola
    hilo_1 = threading.Thread(target=receive_messages_from_queue_1, args=(cola_1, cola_2))
    hilo_2 = threading.Thread(target=receive_messages_from_queue_2, args=(cola_2,))

    hilo_1.start()
    hilo_2.start()

    # Espera a que los hilos terminen
    hilo_1.join()
    hilo_2.join()
