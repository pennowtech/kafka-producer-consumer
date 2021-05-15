import json
import os

from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

def get_producer():
    conf_broker = {
        'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVERS"),
        'security.protocol': os.environ.get("SECURITY_PROTOCOL"),
        'ssl.ca.location': os.environ.get("CA_FILE"),
        'ssl.certificate.location': os.environ.get("CERT_FILE"),
        'ssl.key.location': os.environ.get("KEY_FILE")
    }

    return Producer(**conf_broker)
    

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

producer = get_producer()
topic = os.environ.get("TOPIC")
data = {"text": "Hello"}

try:
    producer.produce(
    topic,
    value=json.dumps(data).encode('utf-8'),
    callback=delivery_callback)

except BufferError as err:
    print(f'Producer queue is full with {len(producer)} messages waiting to be delivered to broker.')


producer.poll(0)

producer.flush()