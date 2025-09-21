# consumer.py
import json, time, logging, requests, os
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv  # to read .env

# Load environment variables from .env
load_dotenv()

LOG = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_TOPIC = "app-logs"
ES_INDEX = "app-logs-index"
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")  # now reads from .env

# Throttle alerts: { service_name: datetime_of_last_alert }
last_alert_time = {}
ALERT_COOLDOWN = timedelta(minutes=5)

# Batch indexing settings
BATCH_SIZE = 100
BATCH_TIMEOUT = 5  # seconds

# Kafka consumer setup
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='log-consumers',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Elasticsearch setup
es = Elasticsearch([{'host':'localhost','port':9200,'scheme':'http'}])

def send_slack_alert(service, level, message, timestamp):
    """Send an alert to Slack or print to console if webhook not configured."""
    alert_text = (
        f"🚨 *ERROR ALERT* 🚨\n"
        f"*Time:* {timestamp}\n"
        f"*Service:* {service}\n"
        f"*Level:* {level}\n"
        f"*Message:* {message}"
    )
    if SLACK_WEBHOOK:
        try:
            response = requests.post(SLACK_WEBHOOK, json={"text": alert_text}, timeout=5)
            if response.status_code != 200:
                LOG.warning("Slack alert failed: HTTP %s", response.status_code)
            else:
                LOG.info("✅ Slack alert sent for service: %s", service)
        except Exception as e:
            LOG.exception("Slack webhook failed: %s", e)
    else:
        LOG.info("Slack webhook not configured, printing alert:\n%s", alert_text)

def process_batch(records):
    """Process a batch of Kafka messages."""
    actions = []
    for rec in records:
        val = rec.value
        doc_id = f"{rec.topic}-{rec.partition}-{rec.offset}"
        action = {
            "_index": ES_INDEX,
            "_id": doc_id,
            "_source": val
        }
        actions.append(action)

        # Alert on ERROR logs with throttling
        if val.get('level') == 'ERROR':
            svc = val.get('service', 'unknown')
            now = datetime.utcnow()
            last = last_alert_time.get(svc)
            if (not last) or (now - last >= ALERT_COOLDOWN):
                ts = val.get('timestamp', now.strftime("%Y-%m-%dT%H:%M:%S"))
                send_slack_alert(svc, val.get('level'), val.get('message'), ts)
                last_alert_time[svc] = now

    # Bulk index logs in Elasticsearch
    if actions:
        try:
            helpers.bulk(es, actions, request_timeout=30)
            LOG.info("Indexed %d logs into Elasticsearch", len(actions))
        except Exception:
            LOG.exception("Failed to index logs to Elasticsearch")

def main():
    buffer = []
    last_flush = time.time()
    LOG.info("Starting Kafka consumer...")
    for msg in consumer:
        buffer.append(msg)
        if len(buffer) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_TIMEOUT:
            try:
                process_batch(buffer)
                consumer.commit()
                buffer = []
                last_flush = time.time()
            except Exception:
                LOG.exception("Error processing batch - leaving offsets uncommitted for retry")

if __name__ == "__main__":
    main()
