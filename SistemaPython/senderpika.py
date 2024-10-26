import pika

def send_message(queue_name, message):
    # Establece la conexión con RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('161.97.146.0', 5672, '/', pika.PlainCredentials('tele', 'tele')))
    channel = connection.channel()

    # Asegura que la cola existe y es durable
    channel.queue_declare(queue=queue_name, durable=True)

    # Publica el mensaje en la cola
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=message)

    print(f"[Remitente] Enviado: {message} a la cola: {queue_name}")

    # Cierra la conexión
    connection.close()

def send_to_two_queues(queue_name1, queue_name2, message):
    send_message(queue_name1, message)
    send_message(queue_name2, message)

def send_multiple_messages(queue_name, num_messages):
    for i in range(num_messages):
        message = f"Mensaje {i + 1}"
        send_message(queue_name, message)

if __name__ == "__main__":
    print("Selecciona una opción:")
    print("1. Enviar un solo mensaje a una cola")
    print("2. Enviar múltiples mensajes a una cola")
    print("3. Enviar un mensaje a dos colas")

    option = input("Opción: ")

    if option == "1":
        queue_name = input("Introduce el nombre de la cola (routing key): ")
        message = input("Introduce el mensaje (payload): ")
        send_message(queue_name, message)

    elif option == "2":
        queue_name = input("Introduce el nombre de la cola (routing key): ")
        num_messages = int(input("Introduce la cantidad de mensajes a enviar: "))
        send_multiple_messages(queue_name, num_messages)

    elif option == "3":
        queue_name1 = input("Introduce el nombre de la primera cola (routing key): ")
        queue_name2 = input("Introduce el nombre de la segunda cola (routing key): ")
        message = input("Introduce el mensaje (payload): ")
        send_to_two_queues(queue_name1, queue_name2, message)

    else:
        print("Opción no válida. Por favor, selecciona 1, 2 o 3.")
