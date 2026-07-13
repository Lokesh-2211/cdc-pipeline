"""
Live dashboard server for the CDC pipeline.

Consumes events from `cdc.public.orders`, keeps an in-memory snapshot of the
current state of every order, and broadcasts that full snapshot to every
connected browser over a WebSocket whenever something changes.

Run with:  python dashboard_server.py
Then open: dashboard.html in a browser (or serve it - see README)
Stop with: Ctrl+C
"""

import asyncio
import base64
import json
import threading

from kafka import KafkaConsumer
import websockets

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "cdc.public.orders"
WEBSOCKET_HOST = "localhost"
WEBSOCKET_PORT = 8765
AMOUNT_DECIMAL_SCALE = 2  # matches the `amount NUMERIC(10,2)` column in init.sql


def decode_debezium_decimal(encoded: str, scale: int = AMOUNT_DECIMAL_SCALE) -> float:
    """Debezium encodes NUMERIC/DECIMAL columns as base64 of a big-endian,
    two's-complement unscaled integer (Kafka Connect's `org.apache.kafka.connect.data.Decimal`).
    This reverses that encoding back into a normal float, e.g. "C7c=" -> 29.99
    """
    raw_bytes = base64.b64decode(encoded)
    unscaled = int.from_bytes(raw_bytes, byteorder="big", signed=True)
    return unscaled / (10 ** scale)

# In-memory state: {order_id: {...fields...}}
orders_state: dict[int, dict] = {}
state_lock = threading.Lock()

# Set of currently connected websocket clients
connected_clients: set = set()


def build_snapshot() -> dict:
    """Build the payload sent to every connected browser."""
    with state_lock:
        orders = list(orders_state.values())

    orders.sort(key=lambda o: o["id"])
    status_counts: dict[str, int] = {}
    for o in orders:
        status = o.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "orders": orders,
        "total_orders": len(orders),
        "status_counts": status_counts,
    }


def kafka_consumer_thread(loop: asyncio.AbstractEventLoop):
    """Runs in a background thread; reads Kafka/Redpanda and updates shared state."""
    print(f"Connecting to Redpanda at {KAFKA_BOOTSTRAP_SERVERS}, topic '{KAFKA_TOPIC}' ...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="dashboard-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )
    print("Kafka consumer connected. Watching for changes...")

    for message in consumer:
        raw_value = message.value
        if raw_value is None:
            continue  # tombstone message, ignore

        payload = raw_value.get("payload")
        if payload is None:
            continue

        op = payload.get("op")
        after = payload.get("after")
        before = payload.get("before")

        changed = False
        with state_lock:
            if op in ("c", "u", "r") and after:
                order = dict(after)
                if "amount" in order and order["amount"] is not None:
                    try:
                        order["amount"] = decode_debezium_decimal(order["amount"])
                    except Exception:
                        pass  # leave as-is if it's not the expected encoded format
                orders_state[order["id"]] = order
                changed = True
            elif op == "d" and before:
                orders_state.pop(before["id"], None)
                changed = True

        if changed:
            # Schedule a broadcast on the asyncio event loop from this thread
            asyncio.run_coroutine_threadsafe(broadcast_snapshot(), loop)


async def broadcast_snapshot():
    if not connected_clients:
        return
    snapshot = json.dumps(build_snapshot())
    # Send to all connected clients; drop any that fail
    dead = set()
    for client in connected_clients:
        try:
            await client.send(snapshot)
        except websockets.exceptions.ConnectionClosed:
            dead.add(client)
    connected_clients.difference_update(dead)


async def handle_client(websocket):
    connected_clients.add(websocket)
    print(f"Client connected. Total clients: {len(connected_clients)}")
    try:
        # Send current state immediately on connect
        await websocket.send(json.dumps(build_snapshot()))
        async for _ in websocket:
            pass  # this server doesn't expect messages from the client
    finally:
        connected_clients.discard(websocket)
        print(f"Client disconnected. Total clients: {len(connected_clients)}")


async def main():
    loop = asyncio.get_running_loop()

    # Start the Kafka consumer in a background thread (kafka-python is sync/blocking)
    thread = threading.Thread(target=kafka_consumer_thread, args=(loop,), daemon=True)
    thread.start()

    print(f"Starting WebSocket server on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
    async with websockets.serve(handle_client, WEBSOCKET_HOST, WEBSOCKET_PORT):
        print("Dashboard server ready. Open dashboard.html in a browser.")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down dashboard server...")
