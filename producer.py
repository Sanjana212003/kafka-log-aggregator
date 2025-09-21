# producer.py
import time, json, random, signal, sys, logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

LOG = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO)

TOPIC = "app-logs"

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def create_topic_if_missing(bootstrap_servers):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin.list_topics()
    if TOPIC not in topics:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)])
        LOG.info("Created topic %s", TOPIC)

def main():
    bootstrap = ["127.0.0.1:9092"]
    create_topic_if_missing(bootstrap)
    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=json_serializer)
    running = True

    def shutdown(sig, frame):
        nonlocal running
        running = False
        LOG.info("Shutting down producer...")
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
    services = ['payment-service', 'order-service', 'inventory-service']

    while running:
        msg = {
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'level': random.choice(log_levels),
            'service': random.choice(services),
            'message': 'This is a sample log message.'
        }
        key = msg['service'].encode('utf-8')
        producer.send(TOPIC, value=msg, key=key)
        LOG.info("Sent: %s", msg)
        time.sleep(random.uniform(0.5,3.0))

if __name__ == "__main__":
    main()
