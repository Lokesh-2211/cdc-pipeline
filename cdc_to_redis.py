"""
CDC-to-Redis cache invalidation consumer.

Listens to the `cdc.public.orders` topic (populated by Debezium) and keeps
a Redis cache in sync with the source Postgres table:

  - insert (op=c) / update (op=u) -> write the row to Redis as a hash
  - delete (op=d)                  -> remove the corresponding key from Redis
  - snapshot read (op=r)           -> treated the same as insert (initial load)

Run with:  python cdc_to_redis.py
Stop with: Ctrl+C
"""

import json
import signal
import sys

from kafka import KafkaConsumer
import redis

# --- Configuration -----------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "cdc.public.orders"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_KEY_PREFIX = "order:"


def build_redis_key(order_id) -> str:
    return f"{REDIS_KEY_PREFIX}{order_id}"


def handle_message(payload: dict, r: redis.Redis) -> None:
    op = payload.get("op")
    after = payload.get("after")
    before = payload.get("before")

    if op in ("c", "u", "r"):
        # Insert, update, or initial snapshot read -> upsert into Redis
        if not after:
            return
        key = build_redis_key(after["id"])
        # Store as a Redis hash so individual fields are inspectable in RedisInsight
        r.hset(key, mapping={k: str(v) for k, v in after.items()})
        print(f"[{op}] Wrote {key} -> {after}")

    elif op == "d":
        # Delete -> remove from Redis
        if not before:
            return
        key = build_redis_key(before["id"])
        r.delete(key)
        print(f"[d] Deleted {key}")

    else:
        # Tombstone messages (value is None) or unknown op types -> ignore
        print(f"[skip] Ignoring message with op={op}")


def main():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} ...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("Connected to Redis.")

    print(f"Connecting to Redpanda at {KAFKA_BOOTSTRAP_SERVERS}, topic '{KAFKA_TOPIC}' ...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",   # process from the beginning on first run
        enable_auto_commit=True,
        group_id="cache-invalidation-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )
    print("Connected. Listening for changes... (Ctrl+C to stop)\n")

    def shutdown(sig, frame):
        print("\nShutting down consumer...")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    for message in consumer:
        raw_value = message.value
        if raw_value is None:
            # This is a Kafka tombstone message (paired with a delete event) - ignore it
            print("[skip] Tombstone message (no value)")
            continue

        payload = raw_value.get("payload")
        if payload is None:
            continue

        handle_message(payload, r)


if __name__ == "__main__":
    main()
