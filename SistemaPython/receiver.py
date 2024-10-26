import threading
import requests
import base64
import time

def receive_message(queue_name):
    url = f'http://161.97.146.0:15672/api/queues/%2F/{queue_name}/get'

    # Autorización básica en Base64
    auth = base64.b64encode(b'tele:tele').decode('utf-8')
    
    headers = {
        'content-type': 'application/json',
        'Authorization': f'Basic {auth}'
    }

    # Payload para obtener los mensajes de la cola
    data = {
        "count": 1,            # Obtener un mensaje a la vez
        "ackmode": "ack_requeue_false", 
        "encoding": "auto",
        "truncate": 50000
    }

    # Hace la petición POST para obtener el mensaje
    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 200:
        messages = response.json()
        if len(messages) > 0:
            for message in messages:
                payload = message['payload']
                print(f"[Receiver] Recibido de la cola {queue_name}: {payload}")
                return payload
        else:
            print(f"No hay mensajes en la cola {queue_name}.")
            return None
    else:
        print(f"Error al recibir mensajes. Código de estado: {response.status_code}, Respuesta: {response.text}")
        return None


def send_generated_messages(queue_name, message, count):
    url = 'http://161.97.146.0:15672/api/exchanges/%2F/amq.default/publish'

    # Autorización básica en Base64
    auth = base64.b64encode(b'tele:tele').decode('utf-8')
    
    headers = {
        'content-type': 'application/json',
        'Authorization': f'Basic {auth}'
    }

    for i in range(count):
        new_message = f"{message} - generado por hilo 1 - mensaje {i + 1}"
        data = {
            "properties": {},
            "routing_key": queue_name,
            "payload": new_message,
            "payload_encoding": "string"
        }

        # Hace la petición POST
        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 200:
            print(f"[Remitente] Enviado: {new_message} a la cola {queue_name}")
        else:
            print(f"Error al enviar el mensaje. Código de estado: {response.status_code}, Respuesta: {response.text}")


def thread_1(cola_1, cola_2):
    while True:
        # Lee mensaje de la cola 1
        message = receive_message(cola_1)
        
        if message:
            # Si se recibe un mensaje de la cola 1, generar 8 nuevos mensajes en la cola 2
            send_generated_messages(cola_2, message, 8)
        
        # Pausa
        time.sleep(2)


def thread_2(cola_2):
    while True:
        # Lee mensajes de la cola 2
        print(f"Escuchando mensajes de la cola {cola_2}...")
        receive_message(cola_2)
        
        # Pausa antes de la siguiente iteración
        time.sleep(2)


if __name__ == "__main__":
    cola_1 = "prueba_cola"
    cola_2 = "prueba_cola2"

    # Crea hilos para las dos colas
    hilo1 = threading.Thread(target=thread_1, args=(cola_1, cola_2))
    hilo2 = threading.Thread(target=thread_2, args=(cola_2,))

    # Inicia los hilos
    hilo1.start()
    hilo2.start()

    # Espera a que los hilos terminen
    hilo1.join()
    hilo2.join()
