# What starts first, and in what order

## 1) `docker-compose.yml` orchestrates everything

When you run `docker compose up -d --build`, Compose brings services up in dependency order:

1. **`zookeeper`**

   * Starts first.
   * Healthcheck: simple `zookeeper-shell` probe on `localhost:2181`.

2. **`kafka`**

   * Depends on `zookeeper: healthy`.
   * Exposes two listeners:

     * `INTERNAL://kafka:9092` (containers use this)
     * `EXTERNAL://localhost:9094` (host CLI uses this)
   * Healthcheck: `kafka-topics --list` against `localhost:9092`.

3. **`kafka-init`** (one-shot)

   * Depends on `kafka: healthy`.
   * Creates topic `${KAFKA_TOPIC:-transactions}` if missing (3 partitions). Then exits.

4. **`spark-master`** and **`spark-worker`**

   * Start a small Spark Standalone cluster.
   * UI on `http://localhost:8080`.

5. **`spark-streaming`**

   * After `kafka-init` and `spark-master` are ready, it `spark-submit`s your streaming job:

     ```
     /opt/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
       --conf spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4} \
       --conf spark.jars.ivy=/tmp/.ivy2 \
       /opt/spark-app/app.py
     ```
   * Mounts your code at `/opt/spark-app`, outputs at `/opt/spark-output` (mapped to `./data/output` on host).

6. **`producer`**

   * After Kafka is healthy and topic exists, runs `python -u producer.py`.
   * Emits synthetic transaction records to Kafka (`kafka:9092`).

> At this point, **events flow**: `producer → Kafka → spark-streaming sinks → Parquet/CSV files`.

---

# Where execution starts in code

## A) The Producer (`producer/producer.py`)

### Entry point:

```python
if __name__ == "__main__":
    main()
```

### What `main()` does:

1. **Reads env**

   * `BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")`
   * `TOPIC = os.getenv("KAFKA_TOPIC", "transactions")`
   * `MSGS_PER_SEC = float(os.getenv("MSGS_PER_SEC", "5"))`
2. **Constructs a KafkaProducer** (from `kafka-python`) with:

   * small batching: `linger_ms=50`, `batch_size=32768`
   * buffering: `buffer_memory=33554432`
   * reliability/backoff: `retries=3`, `retry_backoff_ms=200`
   * **compression**: `compression_type="gzip"` (in your optimized version)
   * serializers: `value_serializer=json.dumps(...).encode("utf-8")`, `key_serializer=str.encode`
3. **Warms up metadata** (optional sanity): `producer.partitions_for(TOPIC)`.
4. **Generates messages** in a loop (until SIGINT/SIGTERM):

   * `tx = make_transaction()` produces a realistic record (uuid, ISO event_ts, faker fields).
   * `producer.send(TOPIC, key=tx["card_id"], value=tx)`.
   * Every 50 sends, prints a progress line.
   * Periodically `flush(timeout=10)` with a try/except so the app doesn’t crash on hiccups.
5. **Shutdown**: final flush (guarded) + `producer.close()`.

**Signals**: `SIGINT`/`SIGTERM` set a `_running` flag to stop the loop gracefully.

**What you’ll see**:
`docker logs -f producer` → `[producer] Connecting to kafka:9092, topic=transactions` and `sent 50/100/... messages`.

---

## B) The Spark app (`spark/app.py`)

### Entry point:

Spark executes the whole script when `spark-submit` runs it. The “start” is top-of-file; there’s **no `if __name__ == "__main__"`**—that’s normal for Spark jobs.

### Phases inside `app.py`

#### 1) Read configuration (ENV)

```python
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "transactions")
OUTPUT          = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")

TRIGGER_INTERVAL         = os.getenv("TRIGGER_INTERVAL", "30 seconds")
MAX_OFFSETS_PER_TRIGGER  = os.getenv("MAX_OFFSETS_PER_TRIGGER")  # optional throttle
MAX_FILES_PER_SINK       = int(os.getenv("MAX_FILES_PER_SINK", "48"))
STARTING                 = os.getenv("STARTING_OFFSETS", "latest")
FAIL_ON_LOSS             = os.getenv("FAIL_ON_DATA_LOSS", "false")
SHOW_PREVIEWS            = os.getenv("SHOW_PREVIEWS", "0") == "1"
```

* Output subfolders: `bronze/`, `silver/`, `gold/`, `quarantine/`, `raw_csv/`, and checkpoints `_chk/`.

#### 2) Define the JSON schema

```python
schema = StructType([...])
```

Typed fields for `txn_id`, `event_ts`, `amount`, etc.
This is used by `from_json` to parse the Kafka `value` string.

#### 3) Helper system: **single-file streaming writer**

To avoid hundreds of tiny files:

* **`single_file_stream(...)`** uses `foreachBatch` per sink:

  * `coalesce(1)` to a single partition in memory.
  * Write to **staging** path (Parquet or CSV).
  * Move the single `part-*` file to a **stable filename** in the final directory (e.g., `silver_YYYYmmdd_HHMMSS_42.parquet`).
  * **Prune** older files, keeping only the **last N** (`MAX_FILES_PER_SINK`).
* Uses Hadoop FS API via Spark JVM bridge for renaming / deleting.

This pattern is applied to **every sink** (bronze, silver, gold, quarantine, raw_csv).

#### 4) Build the SparkSession

```python
spark = (SparkSession.builder
         .appName("TransactionStream")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

#### 5) **Source: Kafka** (stream read)

```python
kafka_reader = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING)
    .option("failOnDataLoss", FAIL_ON_LOSS))

if MAX_OFFSETS_PER_TRIGGER:
    kafka_reader = kafka_reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)

kafka_df = kafka_reader.load()
```

* This creates a streaming DataFrame with columns `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, etc.
* Offsets + progress are checkpointed per sink directory.

#### 6) **Bronze**: raw landing with metadata

```python
bronze_df = kafka_df.selectExpr(
    "CAST(key AS STRING)   AS key",
    "CAST(value AS STRING) AS value",
    "timestamp             AS kafka_ts",
    "topic", "partition", "offset"
)

bronze_q = single_file_stream(
    bronze_df,
    chk_name="bronze",
    final_dir=BRONZE_PATH,
    fmt="parquet",
    interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK,
    filename_prefix="bronze"
)
```

* One compact Parquet file per micro-batch, includes Kafka metadata for replay/forensics.

#### 7) **Validation** + **Quarantine**

```python
raw_json = kafka_df.selectExpr("CAST(value AS STRING) AS json_str",
                               "timestamp AS kafka_ts")

parsed_flag = raw_json.select(
    from_json(col("json_str"), schema).alias("data"),
    col("json_str"), col("kafka_ts")
)

valid_rows = (parsed_flag
    .where(col("data").isNotNull())
    .select("data.*", "kafka_ts", "json_str")
    .where(col("txn_id").isNotNull() & col("event_ts").isNotNull()))

invalid_rows = (parsed_flag
    .where(col("data").isNull() |
           col("data.txn_id").isNull() |
           col("data.event_ts").isNull())
    .select(col("json_str").alias("bad_json"),
            col("kafka_ts").alias("bad_ingest_ts")))
```

* **valid_rows** → parsed JSON with required fields.
* **invalid_rows** → bad/missing JSON lands in `quarantine/`.

```python
quarantine_q = single_file_stream(
    invalid_rows, chk_name="quarantine", final_dir=QUAR_PATH,
    fmt="parquet", interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK, filename_prefix="quarantine"
)
```

#### 8) **Silver**: clean, typed rows with `event_time`

```python
silver_stream = (valid_rows
    .withColumn(
        "event_time",
        coalesce(
            to_timestamp(col("event_ts")),
            to_timestamp(
                regexp_replace(
                    regexp_replace(col("event_ts"), "T", " "),
                    r"([+-]\d{2}):(\d{2})$", r"\1\2"
                ),
                "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"
            ),
            to_timestamp(
                regexp_replace(col("event_ts"), "T", " "),
                "yyyy-MM-dd HH:mm:ss.SSSSSS"
            )
        )
    )
    .drop("event_ts"))

silver_q = single_file_stream(
    silver_stream, chk_name="silver", final_dir=SILVER_PATH,
    fmt="parquet", interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK, filename_prefix="silver"
)
```

* Robust ISO-8601 parsing (with/without timezone colon & microseconds).
* Keeps **typed columns + `event_time`**, drops raw `event_ts`.

#### 9) **Gold**: 1-minute aggregates by `card_id`

```python
gold_agg = (silver_stream
    .withWatermark("event_time", "5 minutes")
    .groupBy(window(col("event_time"), "1 minute").alias("w"), col("card_id"))
    .agg(
        _count("*").alias("txn_count"),
        _sum("amount").alias("sum_amount"),
        _avg("amount").alias("avg_amount"),
        _max("amount").alias("max_amount")
    )
    .select(
        col("card_id"),
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        "txn_count", "sum_amount", "avg_amount", "max_amount"
    ))

gold_q = single_file_stream(
    gold_agg, chk_name="gold", final_dir=GOLD_PATH,
    fmt="parquet", interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK, filename_prefix="gold"
)
```

* Watermarking lets slightly late events (≤ 5 min) still count.

#### 10) **Raw CSV**: readable payload-only mirror

```python
raw_csv_q = single_file_stream(
    valid_rows.select(
        "txn_id", "event_time", "user_id", "card_id", "merchant",
        "category", "amount", "currency", "status",
        "city", "country", "lat", "lon"
    ),
    chk_name="raw_csv", final_dir=RAW_CSV_PATH,
    fmt="csv", header=True, interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK, filename_prefix="raw"
)
```

* Note we write **`event_time`** here (already parsed), not the raw `event_ts`.
* No Kafka metadata; this is meant for quick human inspection.

#### 11) (Optional) Console previews

```python
if SHOW_PREVIEWS:
    (silver_stream.select("event_time","card_id","amount","merchant","status")
     .writeStream.queryName("silver_preview")
     .format("console").option("truncate","false")
     .option("numRows",10).outputMode("append").start())
```

#### 12) **Block the driver**

```python
spark.streams.awaitAnyTermination()
```

* Keeps the job alive, micro-batching every `TRIGGER_INTERVAL`.

---

# Micro-batch lifecycle (what happens every trigger)

1. Spark asks Kafka for up to `maxOffsetsPerTrigger` records/partition (if set) since last committed offset.
2. Spark computes each sink’s DAG:

   * Bronze → write 1 Parquet file
   * Quarantine → write 0/1 Parquet file
   * Silver → write 1 Parquet file
   * Gold → write 0/1 Parquet file (depends on window & watermark)
   * Raw CSV → write 1 CSV file
3. Each sink’s `foreachBatch` moves its single `part-*` file from staging to a timestamped name and prunes older files beyond `MAX_FILES_PER_SINK`.
4. Offsets + state are **checkpointed** for each sink.
5. Wait for next `TRIGGER_INTERVAL`.

---

# How to run (step-by-step)

## 1) One-time: build & start

```bash
# From the project root (where docker-compose.yml lives)
docker compose up -d --build
```

## 2) Confirm producer sending

```bash
docker logs -f producer
# Expect: [producer] Connecting..., then [producer] sent 50/100/...
```

## 3) Confirm Kafka has messages

```bash
docker exec -it kafka /usr/bin/kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --group debug-$(date +%s) \
  --from-beginning --max-messages 5
```

## 4) Confirm Spark is running

* Spark master UI: [http://localhost:8080](http://localhost:8080) → you should see the app registered.
* Streaming driver logs:

  ```bash
  docker logs -f spark-streaming
  ```

## 5) Inspect outputs on host

Files appear under `./data/output/parquet`:

```
bronze/       # raw + kafka metadata (parquet)
quarantine/   # malformed rows (parquet)
silver/       # clean typed + event_time (parquet)
gold/         # 1-min aggregates by card_id (parquet)
raw_csv/      # readable payload-only (csv with header)
_chk/         # checkpoints (do not edit)
```

Quick peek:

```bash
# CSV mirror (human readable)
head -n 5 data/output/parquet/raw_csv/*.csv 2>/dev/null | sed -n '1,20p'

# Silver parquet using spark-sql (optional) or python/pandas/pyarrow.
```

---

# Reset / reprocess

To “start fresh” (wipe outputs + offsets):

```bash
docker compose down
rm -rf ./data/output
mkdir -p ./data/output
docker compose up -d --build
```

If Kafka metadata got corrupted (e.g., you changed listeners and see “Invalid cluster.id”):

```bash
docker compose down
docker volume rm $(docker volume ls -q | grep kafka-data)   # or the exact volume name
rm -rf ./data/output
docker compose up -d --build
```