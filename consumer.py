# consumer.py
import json, time, logging, requests, os
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv  # to read .env

# Load environment variables from .env
load_dotenv()

LOG = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_TOPIC = "app-logs"
DLQ_TOPIC = "app-logs-dlq" # DLQ - Dead Letter Queue
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

# Kafka producer for sending invalid messages to the Dead-Letter Queue
dlq_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Elasticsearch setup
es = Elasticsearch([{'host':'localhost','port':9200,'scheme':'http'}])

# Validation of fields in the logs which are in JSON format
def validate_log(log):
    required_fields = ["timestamp", "level", "service", "message"]

    missing_fields = [
        field for field in required_fields
        if field not in log
    ]

    if missing_fields:
        return False, f"Missing required fields: {missing_fields}"

    return True, None

# Sends invalid messages and  also the Failure Reason along with its Timestamp to the Dead-Letter Queue 
def send_to_dlq(log, error_reason):
    """Send an invalid log message to the Kafka Dead-Letter Queue."""
    dlq_message = {
        "original_message": log,
        "error": error_reason,
        "failed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    }

    try:
        dlq_producer.send(DLQ_TOPIC, value=dlq_message).get(timeout=10)
        LOG.warning("Message sent to DLQ: %s", error_reason)
        return True
    except Exception:
        LOG.exception("Failed to send message to DLQ")
        return False

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

        # Validate the log before sending it to Elasticsearch
        valid, error_reason = validate_log(val)

        if not valid:
            if not send_to_dlq(val, error_reason):
                raise RuntimeError("Failed to send invalid message to DLQ")
            continue

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

    # Bulk index valid logs in Elasticsearch
    if actions:
        try:
            helpers.bulk(es, actions, request_timeout=30)
            LOG.info("Indexed %d valid logs into Elasticsearch", len(actions))
        except Exception:
            LOG.exception("Failed to index logs to Elasticsearch")
            raise
        
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
