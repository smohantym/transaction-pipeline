import json, os, signal, sys, time, uuid
from datetime import datetime, timezone
from random import choice, randint, uniform
from faker import Faker
from kafka import KafkaProducer, errors as kerrors

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
MSGS_PER_SEC = float(os.getenv("MSGS_PER_SEC", "10"))

fake = Faker()
_running = True
def _stop(*_):
    global _running
    _running = False
for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, _stop)

def make_transaction():
    currency = choice(["INR", "USD", "EUR"])
    base = uniform(50, 4000) if currency == "INR" else uniform(1, 200)
    amount = round(base * (1 + uniform(-0.1, 0.1)), 2)
    return {
        "txn_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{randint(1000, 9999)}",
        "card_id": f"card_{randint(100, 199)}",
        "merchant": choice([
            "Amazon","Flipkart","Swiggy","Zomato","Apple",
            "Myntra","Uber","Rapido","Shell","RelianceFresh"
        ]),
        "category": choice(["grocery","fuel","food_delivery","ride_hailing","electronics","fashion"]),
        "amount": float(amount),
        "currency": currency,
        "status": choice(["APPROVED","APPROVED","APPROVED","DECLINED"]),
        "city": fake.city(),
        "country": fake.country_code(),
        "lat": round(uniform(-90, 90), 6),
        "lon": round(uniform(-180, 180), 6),
    }

def main():
    print(f"[producer] Connecting to {BOOTSTRAP}, topic={TOPIC}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks=1,
        linger_ms=75,                      # small batching
        batch_size=65536,                  # 64 KB
        buffer_memory=67108864,            # 64 MB
        max_block_ms=60000,
        request_timeout_ms=30000,
        retries=5,
        retry_backoff_ms=200,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",           # gzip safe everywhere; lz4/zstd if you want
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        api_version_auto_timeout_ms=20000,
    )

    # Warm up metadata
    try:
        producer.partitions_for(TOPIC)
    except Exception as e:
        sys.stderr.write(f"[producer] metadata error: {e}\n")

    interval = 1.0 / MSGS_PER_SEC if MSGS_PER_SEC > 0 else 0.0
    sent = 0
    last_flush = time.time()

    try:
        while _running:
            tx = make_transaction()
            fut = producer.send(TOPIC, key=tx["card_id"], value=tx)
            fut.add_errback(lambda ex: sys.stderr.write(f"[producer] send error: {repr(ex)}\n"))

            sent += 1
            if sent % 50 == 0:
                print(f"[producer] sent {sent} messages", flush=True)

            if time.time() - last_flush > 5:
                try:
                    producer.flush(timeout=10)
                except kerrors.KafkaTimeoutError:
                    sys.stderr.write("[producer] flush timeout (continuing)\n")
                last_flush = time.time()

            if interval:
                time.sleep(interval)
    except Exception as e:
        sys.stderr.write(f"[producer] loop error: {e}\n")
    finally:
        try:
            producer.flush(timeout=10)
        except kerrors.KafkaTimeoutError:
            sys.stderr.write("[producer] final flush timeout, closing anyway\n")
        producer.close()
        print("[producer] bye.")

if __name__ == "__main__":
    main()
