import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, to_timestamp,
    sum as _sum, avg as _avg, count as _count, max as _max,
    regexp_replace, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# -------- ENV --------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "transactions")
OUTPUT          = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
STARTING        = os.getenv("STARTING_OFFSETS", "latest")     # earliest|latest
FAIL_ON_LOSS    = os.getenv("FAIL_ON_DATA_LOSS", "false")     # "true"|"false"
SHOW_PREVIEWS   = os.getenv("SHOW_PREVIEWS", "0") == "1"

TRIGGER_INTERVAL        = os.getenv("TRIGGER_INTERVAL", "30 seconds")
MAX_FILES_PER_SINK      = int(os.getenv("MAX_FILES_PER_SINK", "48"))
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER")

CHK_ROOT      = os.getenv("SPARK_CHECKPOINT_DIR", os.path.join(OUTPUT, "_chk"))
BRONZE_PATH   = os.path.join(OUTPUT, "bronze")
SILVER_PATH   = os.path.join(OUTPUT, "silver")
GOLD_PATH     = os.path.join(OUTPUT, "gold")
QUAR_PATH     = os.path.join(OUTPUT, "quarantine")
RAW_CSV_PATH  = os.path.join(OUTPUT, "raw_csv")

# -------- Schema --------
schema = StructType([
    StructField("txn_id",   StringType()),
    StructField("event_ts", StringType()),
    StructField("user_id",  StringType()),
    StructField("card_id",  StringType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("amount",   DoubleType()),
    StructField("currency", StringType()),
    StructField("status",   StringType()),
    StructField("city",     StringType()),
    StructField("country",  StringType()),
    StructField("lat",      DoubleType()),
    StructField("lon",      DoubleType()),
])

# ---------- helper: Hadoop FS ops ----------
def _hadoop_fs(spark):
    jvm = spark._jvm
    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()
    return jvm.org.apache.hadoop.fs.FileSystem.get(hconf), jvm.org.apache.hadoop.fs.Path

def _write_single_file(batch_df, fmt, final_dir, tmp_dir, filename, header=False):
    writer = batch_df.coalesce(1).write.mode("overwrite")
    if fmt == "csv":
        writer.option("header", "true" if header else "false").csv(tmp_dir)
        ext = ".csv"
    else:
        writer.parquet(tmp_dir)
        ext = ".parquet"

    fs, Path = _hadoop_fs(batch_df.sparkSession)
    tmp = Path(tmp_dir)
    final = Path(final_dir)
    if not fs.exists(final):
        fs.mkdirs(final)

    for status in fs.listStatus(tmp):
        name = status.getPath().getName()
        if name.startswith("part-") and name.endswith(ext):
            src = status.getPath()
            dst = Path(final_dir.rstrip("/") + "/" + filename + ext)
            if fs.exists(dst):
                fs.delete(dst, False)
            fs.rename(src, dst)
            break

    fs.delete(tmp, True)

def _prune_old_files(spark, final_dir, keep=48):
    fs, Path = _hadoop_fs(spark)
    final = Path(final_dir)
    if not fs.exists(final):
        return
    files = [s for s in fs.listStatus(final) if s.isFile()]
    files.sort(key=lambda s: s.getModificationTime(), reverse=True)
    for s in files[keep:]:
        fs.delete(s.getPath(), False)

def single_file_stream(df, *, chk_name, final_dir, fmt, interval="30 seconds",
                       header=False, keep_files=48, filename_prefix="part"):
    staging = final_dir.rstrip("/") + "/_staging_" + chk_name
    def _foreach(batch_df, batch_id):
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"{filename_prefix}_{ts}_{batch_id}"
        _write_single_file(batch_df, fmt, final_dir, staging, fname, header=header)
        _prune_old_files(batch_df.sparkSession, final_dir, keep=keep_files)
    return (df.writeStream
              .trigger(processingTime=interval)
              .option("checkpointLocation", os.path.join(CHK_ROOT, chk_name))
              .foreachBatch(_foreach)
              .start())

# ---------- Spark ----------
spark = (SparkSession.builder
         .appName("TransactionStream")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.files.ignoreMissingFiles", "true")
         .config("spark.sql.parquet.mergeSchema", "false")
         .config("spark.sql.parquet.filterPushdown", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- SOURCE: Kafka ----------
kread = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING)
    .option("failOnDataLoss", FAIL_ON_LOSS))

if MAX_OFFSETS_PER_TRIGGER:
    kread = kread.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)

kafka_df = kread.load()

# ---------- BRONZE (keeps metadata) ----------
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

# ---------- VALIDATION + PARSING ----------
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

quarantine_q = single_file_stream(
    invalid_rows,
    chk_name="quarantine",
    final_dir=QUAR_PATH,
    fmt="parquet",
    interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK,
    filename_prefix="quarantine"
)

# ---------- SILVER ----------
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
    silver_stream,
    chk_name="silver",
    final_dir=SILVER_PATH,
    fmt="parquet",
    interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK,
    filename_prefix="silver"
)

# ---------- GOLD ----------
gold_agg = (silver_stream
    .withWatermark("event_time", "5 minutes")
    .groupBy(window(col("event_time"), "1 minute").alias("w"),
             col("card_id"))
    .agg(_count("*").alias("txn_count"),
         _sum("amount").alias("sum_amount"),
         _avg("amount").alias("avg_amount"),
         _max("amount").alias("max_amount"))
    .select(
        col("card_id"),
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        "txn_count", "sum_amount", "avg_amount", "max_amount"
    ))

gold_q = single_file_stream(
    gold_agg,
    chk_name="gold",
    final_dir=GOLD_PATH,
    fmt="parquet",
    interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK,
    filename_prefix="gold"
)

# ---------- RAW CSV TAP (only clean parsed fields) ----------
raw_csv_q = single_file_stream(
    valid_rows.select(
        "txn_id", "event_ts", "user_id", "card_id", "merchant",
        "category", "amount", "currency", "status",
        "city", "country", "lat", "lon"
    ),
    chk_name="raw_csv",
    final_dir=RAW_CSV_PATH,
    fmt="csv",
    header=True,
    interval=TRIGGER_INTERVAL,
    keep_files=MAX_FILES_PER_SINK,
    filename_prefix="raw"
)

# ---------- CONSOLE PREVIEW ----------
if SHOW_PREVIEWS:
    (silver_stream.select("event_time", "card_id", "amount", "merchant", "status")
        .writeStream.queryName("silver_preview")
        .format("console").option("truncate", "false")
        .option("numRows", 10).outputMode("append").start())

spark.streams.awaitAnyTermination()
