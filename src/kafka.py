from datetime import datetime, timezone
import argparse
import json
import logging
import time

from confluent_kafka import Producer
from confluent_kafka import Consumer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.devel('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce(bootstrap_servers: str, topic: str, interval: float):
    p = Producer({'bootstrap.servers': bootstrap_servers})

    count = 0
    while True:
		# Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        now = datetime.now(timezone.utc)
        msg = {'key': 'test', 'value': count, 'timestamp': now.isoformat()}

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        p.produce(topic, json.dumps(msg).encode('utf-8'), callback=delivery_report)

        count += 1
        time.sleep(interval)
    
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
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka clients')
    parser.add_argument('-b', '--bootstrap-servers', type=str, required=True, help='Kafka bootstrap servers')
    parser.add_argument('-t', '--topic', type=str, required=True, help='Kafka topic')
    parser.add_argument('-g', '--group-id', type=str, required=False, help='Kafka consumer group id')
    parser.add_argument('-v', '--verbose', action='count', required=False, default=0, help='Verbose logging')
    parser.add_argument('-i', '--interval', type=float, required=False, default=1.0, help='Interval between messages')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('-p', '--produce', action='store_true', help='Produce messages')
    mode.add_argument('-c', '--consume', action='store_true', help='Consume messages')
    args = parser.parse_args()

    if args.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    elif args.verbose > 1:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    
    if args.produce:
        produce(args.bootstrap_servers, args.topic, args.interval)
    elif args.consume:
        consume(args.bootstrap_servers, args.group_id, args.topic)
