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