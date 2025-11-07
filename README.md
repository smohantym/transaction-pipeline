# 🧠 Spark + Kafka Real-Time Transaction Streaming Pipeline

A complete **end-to-end data streaming pipeline** built with:

- **Apache Kafka** — message broker for high-throughput ingestion  
- **Apache Spark Structured Streaming** — scalable, micro-batch stream processing  
- **Python Producer (Kafka-Python + Faker)** — generates realistic transaction events  
- **Docker Compose** — runs everything locally with isolated services  
- **Medallion Architecture** — Bronze → Silver → Gold layers + Quarantine  
- **Compact file writer** — ensures single Parquet/CSV per micro-batch (no file explosion)

---

## 🧩 Architecture Overview

```

┌────────────┐
│  Producer  │  → generates fake transactions (Python + Faker)
└──────┬─────┘
│ JSON over Kafka
▼
┌──────────────┐
│   Kafka      │  → topic: "transactions"
│ (with ZK)    │
└──────┬───────┘
│ internal listener (kafka:9092)
▼
┌────────────────┐
│ Spark Streaming│  → reads from Kafka in micro-batches
│  (app.py)      │
│                 │
│  ┌──────────┐  │
│  │ Bronze    │  → raw Kafka payloads + metadata
│  │ Silver    │  → clean, typed data + event_time
│  │ Gold      │  → 1-min aggregates (card_id)
│  │ Quarantine│  → malformed JSON
│  │ Raw CSV   │  → readable payload-only dump
│  └──────────┘  │
└────────────────┘
│
▼
📂 `data/output/parquet/` — persisted Parquet + CSV layers

````

---

## 🚀 Quick Start

### 1️⃣ Clone the repo

```bash
git clone https://github.com/smohantym/transaction-pipeline.git
cd transaction-pipeline
````

### 2️⃣ Start everything

```bash
docker compose up -d --build
```

This starts:

* **Zookeeper**
* **Kafka**
* **Kafka-init** (creates topic)
* **Spark master & worker**
* **Spark streaming job**
* **Python producer**

---

## 🧮 Verify Everything Works

### Check producer activity:

```bash
docker logs -f producer
```

You should see:

```
[producer] Connecting to kafka:9092, topic=transactions
[producer] sent 50 messages
[producer] sent 100 messages
...
```

### Verify Kafka topic:

```bash
docker exec -it kafka /usr/bin/kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --group debug-$(date +%s) \
  --from-beginning --max-messages 5
```

You should see JSON like:

```json
{"txn_id": "...", "event_ts": "...", "amount": 124.7, "status": "APPROVED", ...}
```

### Check Spark streaming logs:

```bash
docker logs -f spark-streaming
```

Spark Master UI:
👉 [http://localhost:8080](http://localhost:8080)

---

## 📂 Outputs

All written under:

```
./data/output/parquet/
```

| Layer              | Path          | Format  | Description                                      |
| ------------------ | ------------- | ------- | ------------------------------------------------ |
| 🟫 **Bronze**      | `bronze/`     | Parquet | Raw Kafka key/value + topic, partition, offset   |
| 🟪 **Silver**      | `silver/`     | Parquet | Clean, typed data with event_time                |
| 🟨 **Gold**        | `gold/`       | Parquet | 1-minute tumbling window aggregates (by card_id) |
| 🚨 **Quarantine**  | `quarantine/` | Parquet | Malformed JSON or missing txn_id/event_ts        |
| 📄 **Raw CSV**     | `raw_csv/`    | CSV     | Readable payload-only mirror                     |
| 🧱 **Checkpoints** | `_chk/`       | —       | Streaming state & offsets per sink               |

---

## ⚙️ Configuration (.env)

You can tune everything from `.env`:

```env
# Kafka
KAFKA_TOPIC=transactions
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Producer (Python)
MSGS_PER_SEC=10

# Spark
OUTPUT_PATH=/opt/spark-output/parquet
SPARK_SQL_SHUFFLE_PARTITIONS=4
TRIGGER_INTERVAL=30 seconds
MAX_FILES_PER_SINK=48
MAX_OFFSETS_PER_TRIGGER=5000

STARTING_OFFSETS=latest
FAIL_ON_DATA_LOSS=false
SHOW_PREVIEWS=0
SPARK_CHECKPOINT_DIR=/opt/spark-output/parquet/_chk
```

---

## 🧠 How It Works (Step-by-Step)

### 1️⃣ Data Generation (`producer/producer.py`)

* Generates fake transactions using `faker`.
* Sends JSON messages to Kafka topic `"transactions"` at a configurable rate (`MSGS_PER_SEC`).
* Each message looks like:

  ```json
  {
    "txn_id": "uuid",
    "event_ts": "2025-11-07T04:16:53.653Z",
    "user_id": "user_1001",
    "card_id": "card_190",
    "merchant": "Amazon",
    "category": "fuel",
    "amount": 1029.27,
    "currency": "INR",
    "status": "APPROVED",
    "city": "Mumbai",
    "country": "IN",
    "lat": 19.07,
    "lon": 72.87
  }
  ```

### 2️⃣ Ingestion (`spark/app.py`)

* Spark reads the Kafka topic as a **streaming DataFrame** using the `spark-sql-kafka` connector.
* Each micro-batch (every 30s by default) processes newly arrived messages.

### 3️⃣ Bronze Layer

* Stores **exact Kafka payload** with metadata (`key`, `value`, `timestamp`, `partition`, `offset`).
* Used for auditing or replaying.

### 4️⃣ Validation

* Parses the JSON using a strict schema.
* Valid rows: well-formed JSON with `txn_id` and `event_ts`.
* Invalid rows → quarantined.

### 5️⃣ Silver Layer

* Parses timestamps into proper `event_time` (handles timezones, microseconds).
* Cleans and flattens the schema.
* Outputs one compact Parquet per batch.

### 6️⃣ Gold Layer

* Applies watermark (5 minutes).
* Groups by card_id and 1-minute tumbling window:

  * `txn_count`, `sum_amount`, `avg_amount`, `max_amount`.
* Writes to compact Parquet per batch.

### 7️⃣ Raw CSV Mirror

* Writes readable CSV snapshots (payload only) for quick checks:

  ```
  txn_id, event_time, user_id, card_id, merchant, category, amount, ...
  ```

---

## 🧰 Developer Workflow

**View containers:**

```bash
docker ps
```

**Follow logs:**

```bash
docker logs -f spark-streaming
docker logs -f producer
```

**Inspect outputs:**

```bash
head -n 10 data/output/parquet/raw_csv/*.csv
```

**Restart clean:**

```bash
docker compose down
rm -rf ./data/output
docker volume rm spark_kafka_pipeline_kafka-data
docker compose up -d --build
```

---

## 🔍 Internals: The Compact File Writer

By default, Spark streaming creates many small files.
This pipeline fixes that using a custom **`foreachBatch` sink**:

* `coalesce(1)` to a single partition per batch
* Write to a staging directory
* Rename to `final_dir/prefix_timestamp_batchid.parquet`
* Delete staging
* Keep only latest `N` files (`MAX_FILES_PER_SINK`)

Result → few big files, not thousands of tiny ones ✅

---

## 🧱 Folder Structure

```
.
├── docker-compose.yml
├── .env
├── README.md
├── .gitignore
├── data/
│   └── output/           # Parquet, CSV, checkpoints
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
└── spark/
    └── app.py
```

---

## 🧠 How to Stop

```bash
docker compose down
```

To wipe all data and start fresh:

```bash
docker compose down
docker volume rm spark_kafka_pipeline_kafka-data
rm -rf ./data/output
docker compose up -d --build
```

---

## 🧾 License

MIT — Feel free to modify, extend, and experiment!

---

## ✨ Author Notes

✅ Follows **Medallion Architecture** best practices (Bronze → Silver → Gold)
✅ Uses **compact sinks** for efficient storage
✅ **Quarantine** ensures no data loss during schema evolution
✅ Built for **Apple Silicon (arm64)** but runs fine on x86 (change image tags if needed)
