**Kafka Log Aggregator & Slack Alerting System**

A real-time log aggregation system using **Kafka**, **Elasticsearch**, **Kibana**, and **Python**, with **Slack alerting for ERROR logs**.

---

## **Prerequisites**

1. **Install Docker & Docker Compose**

   * [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows)
   * Ensure `docker` and `docker-compose` are in your PATH.

2. **Install Python 3.10+**

   * [Python](https://www.python.org/downloads/)
   * Add Python to PATH.

3. **Install VS Code or any code editor** (optional but recommended).

---

## **Step 1: Clone or Create Project Folder**

```powershell
mkdir kafka-log-aggregator
cd kafka-log-aggregator
```

---

## **Step 2: Docker Setup for Kafka + Elasticsearch + Kibana**

1. Create `docker-compose.yml` in the project folder.

2. Start Docker services:

```powershell
docker-compose up -d
```

* Wait a few minutes. Services will be running in the background.

---

## **Step 3: Python Environment Setup**

1. Create a virtual environment (optional but recommended):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install Python dependencies:

```powershell
pip install -r requirements.txt
pip install python-dotenv requests kafka-python elasticsearch
```

---

## **Step 4: Configure Slack Alerts**

1. Create a Slack App: [https://api.slack.com/apps](https://api.slack.com/apps)

2. Enable **Incoming Webhooks** and add a webhook to a channel.

3. Copy the **Webhook URL**.

4. Create a `.env` file in the project folder:

```env
SLACK_WEBHOOK=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

> Replace with your real Slack webhook URL.

---

## **Step 5: Run the Log Producer**

* The producer generates logs and sends them to Kafka.

```powershell
python producer.py
```

* Logs will start printing in the console.

---

## **Step 6: Run the Log Consumer with Slack Alerts**

* Open a **new PowerShell window** in the same folder:

```powershell
python consumer.py
```

* Consumer will:

  * Index logs into Elasticsearch.
  * Print alerts to console if Slack is not configured.
  * Send ERROR alerts to Slack if webhook is configured.

---

## **Step 7: Verify Slack Alerts**

1. Send an ERROR log from the producer:

* Optionally, modify `producer.py` to send an ERROR:

```python
log_levels = ['ERROR']
```

2. Check the Slack channel you configured in Step 4.

* You should see messages like:

```
🚨 ERROR ALERT 🚨
Time: 2025-09-21T15:30:00
Service: payment-service
Level: ERROR
Message: This is a sample log message.
```

---

## **Step 8: Verify Logs in Elasticsearch**

* Open browser: `http://localhost:9200/_cat/indices?v`

* You should see `app-logs-index` listed.

* Query logs:

```powershell
curl http://localhost:9200/app-logs-index/_search?q=*&pretty
```

---

## **Step 9: View Logs in Kibana**

1. Open Kibana: `http://localhost:5601`
2. Go to **Management → Stack Management → Index Patterns → Create index pattern**
3. Type `app-logs-index*`, choose `timestamp` as primary time field → Click **Create index pattern**.
4. Go to **Analytics → Discover** to view real-time logs.
5. Filter logs by:

```
level:"ERROR"
```

---

## **Step 10: Stop Services**

```powershell
docker-compose down
```

* Stops Kafka, Zookeeper, Elasticsearch, and Kibana.

---

### **Tips**

* Adjust `ALERT_COOLDOWN` in `consumer.py` to control alert frequency.
* Filter logs in Kibana for ERRORs to confirm alerts are being triggered.
* Keep `.env` private — it contains your Slack webhook.

---

---

## **Contribution: Dead Letter Queue (DLQ) Validation**

### **Contributor**

**Sanjana Jaysing Redekar**  
Feature branch: `feature/dlq-validation`  
Date: **2026-08-14**

### **Enhancement Implemented**

Added **Dead Letter Queue (DLQ) handling and input validation** to improve the reliability of the Kafka log consumer.

The consumer now validates incoming Kafka log messages before indexing them into Elasticsearch.

### **Changes Made**

- Added a dedicated Kafka **Dead Letter Queue (DLQ)** topic:
  - `app-logs-dlq`
- Added validation for required log fields:
  - `timestamp`
  - `level`
  - `service`
  - `message`
- Added `validate_log()` to identify malformed or incomplete log messages.
- Added `send_to_dlq()` to route invalid messages to the DLQ.
- DLQ messages contain:
  - Original message
  - Validation failure reason
  - Failure timestamp
- Prevented invalid log messages from being indexed into Elasticsearch.
- Added error handling so that Kafka offsets are not committed when DLQ delivery fails.
- Preserved the existing ERROR-level Slack alerting and Elasticsearch indexing functionality for valid messages.

### **Validation & Testing**

The implementation was tested locally with Kafka, Elasticsearch, and the Python consumer.

Verified that:

- `consumer.py` compiles successfully.
- Kafka connectivity is working.
- `app-logs-dlq` topic exists.
- A malformed Kafka log missing the `level` field is detected by validation.
- The malformed message is successfully sent to `app-logs-dlq`.
- Valid log messages continue to be indexed into Elasticsearch.
- Invalid messages are not indexed into Elasticsearch.

## 1. Test One
### **Send one invalid message**
python -c "from kafka import KafkaProducer; import json; p=KafkaProducer(bootstrap_servers=['127.0.0.1:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8')); msg={'timestamp':'2026-08-14T13:00:00','service':'payment-service','message':'DLQ test - missing level'}; p.send('app-logs', value=msg).get(timeout=10); p.flush(); p.close(); print('Invalid test message sent:', msg)"

<img width="940" height="154" alt="image" src="https://github.com/user-attachments/assets/4b27c31a-0b83-4266-a14c-d4550bdc34cb" />

### **Output: To see the Error visible in Consumer Console**
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic app-logs-dlq --from-beginning --max-messages 1

<img width="940" height="148" alt="image" src="https://github.com/user-attachments/assets/43f1f0c2-de1b-4e44-9538-31d37eb1afa6" />

### **Output: Invalid log**

<img width="940" height="342" alt="image" src="https://github.com/user-attachments/assets/8a007bf0-7a92-42f9-bb57-40c2630cd76a" />

## **2. Test Two**
### **Send one valid message**
python -c "from kafka import KafkaProducer; import json; p=KafkaProducer(bootstrap_servers=['127.0.0.1:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8')); msg={'timestamp':'2026-08-14T13:30:00','level':'INFO','service':'payment-service','message':'Valid DLQ test message'}; p.send('app-logs', value=msg).get(timeout=10); p.flush(); p.close(); print('Valid test message sent:', msg)"

<img width="825" height="259" alt="image" src="https://github.com/user-attachments/assets/c451a9dc-bf4b-4ca3-89ea-8e1fda050fa3" />

### **Output: Elasticsearch response proves that the valid message reached app-logs-index**

<img width="940" height="583" alt="image" src="https://github.com/user-attachments/assets/069134dc-7f1f-4af6-9cca-6a93e4b536c6" />

## **Example DLQ Flow**

```text
Kafka Producer
      |
      v
  app-logs
      |
      v
Kafka Consumer
      |
      v
  Validate Log
    /      \
 Invalid    Valid
   |          |
   v          v
DLQ Topic   Elasticsearch
   |
   v
app-logs-dlq
