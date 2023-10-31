from datetime import datetime, timezone
import json
import logging

from confluent_kafka import Producer
from confluent_kafka import Consumer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.devel('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce(bootstrap_servers: str, topic: str):
    p = Producer({'bootstrap.servers': bootstrap_servers})

    count = 0
    while True:
		# Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        time = datetime.now(timezone.utc)
        msg = {'key': 'test', 'value': count, 'timestamp': time.isoformat()}

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        p.produce(topic, json.dumps(msg).encode('utf-8'), callback=delivery_report)

        count += 1
    
    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()

def consume(bootstrap_servers: str, group_id: str, topic: str):
    c = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    
    c.subscribe([topic])
    
    while True:
        msg = c.poll(1.0)
    
        if msg is None:
            continue
        if msg.error():
            logging.error("Consumer error: {}".format(msg.error()))
            continue

        print(msg.value().decode('utf-8'))
    
    # If comsuming is stopped, close the consumer
    c.close()
