import json
import os

from dotenv import load_dotenv
from confluent_kafka import Consumer

def get_consumer():
    conf_broker = {
        'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVERS"),
        'group.id': os.environ.get("CONSUMER_GROUP"),
        'session.timeout.ms': os.environ.get("SESSION_TIMEOUT"),
        'auto.offset.reset': 'earliest',
        'security.protocol': os.environ.get("SECURITY_PROTOCOL"),
        'ssl.ca.location': os.environ.get("CA_FILE"),
        'ssl.certificate.location': os.environ.get("CERT_FILE"),
        'ssl.key.location': os.environ.get("KEY_FILE")
    }

    return Consumer(**conf_broker)


consumer = get_consumer()
consumer.subscribe(os.environ.get("TOPIC"))


def process_msg(msg):
    if msg is None:
        return None
    if msg.error():
        print(f'Consumer error: {msg.error()}')
        return None

    data = json.loads(msg.value().decode('utf-8'))
    print(f'Received message: {data}')


def start():
    while True:
        try:
            msg = consumer.poll(1.0)
            process_msg(msg)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    start()
    consumer.close()
