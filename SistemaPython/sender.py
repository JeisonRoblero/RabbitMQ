import requests
import base64

def send_message(queue_name, message):
    url = 'http://161.97.146.0:15672/api/exchanges/%2F/amq.default/publish'

    # Autorización básica en Base64
    auth = base64.b64encode(b'tele:tele').decode('utf-8')
    
    headers = {
        'content-type': 'application/json',
        'Authorization': f'Basic {auth}'
    }

    data = {
        "properties": {},
        "routing_key": queue_name,
        "payload": message,
        "payload_encoding": "string"
    }

    # Hace la petición POST
    response = requests.post(url, json=data, headers=headers)

    # Muestra el estado de la respuesta
    if response.status_code == 200:
        print(f"[Remitente] Enviado: {message} a la cola: {queue_name}")
    else:
        print(f"Error al enviar el mensaje. Código de estado: {response.status_code}, Respuesta: {response.text}")


def send_multiple_messages(queue_name, num_messages):
    for i in range(num_messages):
        message = f"Mensaje {i + 1}"
        send_message(queue_name, message)


if __name__ == "__main__":
    print("Selecciona una opción:")
    print("1. Enviar un solo mensaje a la cola")
    print("2. Enviar múltiples mensajes a la cola")
    
    option = input("Opción: ")

    if option == "1":
        # Opción 1: Envia un solo mensaje
        queue_name = input("Introduce el nombre de la cola (routing key): ")
        message = input("Introduce el mensaje (payload): ")
        send_message(queue_name, message)

    elif option == "2":
        # Opción 2: Envia múltiples mensajes
        queue_name = input("Introduce el nombre de la cola (routing key): ")
        num_messages = int(input("Introduce la cantidad de mensajes a enviar: "))
        send_multiple_messages(queue_name, num_messages)

    else:
        print("Opción no válida. Por favor, selecciona 1 o 2.")
