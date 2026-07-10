# Real-Time CDC Pipeline

A local, fully open-source Change Data Capture (CDC) pipeline: Postgres → Debezium → Redpanda.
Every insert/update/delete on the `orders` table is captured from the Postgres WAL and streamed
as an event in real time — no polling, no triggers.

## Stack
- **Postgres 16** — source database, logical replication enabled
- **Debezium 2.6** (Kafka Connect) — reads the WAL, emits change events
- **Redpanda** — Kafka-API-compatible message broker (lighter than running real Kafka + Zookeeper)
- **Redpanda Console** — web UI to watch events flow through topics

## Prerequisites
- Docker Desktop installed and running

## 1. Start the stack

```bash
docker compose up -d
```

Wait ~20-30 seconds for everything to come up healthy. Check with:

```bash
docker compose ps
```

## 2. Register the Debezium connector

Once containers are healthy, register the Postgres source connector:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @register-postgres-connector.json \
  http://localhost:8083/connectors
```

Verify it's running:

```bash
curl http://localhost:8083/connectors/orders-connector/status
```

## 3. Watch events flow

Open Redpanda Console at **http://localhost:8080**, go to Topics, and find
`cdc.public.orders`. You should see one event per row from the initial snapshot.

## 4. Trigger a live change

Connect to Postgres and make a change:

```bash
docker exec -it cdc-postgres psql -U cdcuser -d cdcdb
```

```sql
UPDATE orders SET status = 'shipped' WHERE customer_name = 'Alice Smith';
INSERT INTO orders (customer_name, product, amount, status) VALUES ('Dana Lee', 'Widget D', 39.99, 'pending');
DELETE FROM orders WHERE customer_name = 'Carla Diaz';
```

Watch the corresponding events appear in Redpanda Console within milliseconds.

## 5. Tear down

```bash
docker compose down -v
```

## Next steps
- [ ] Write a Python consumer that reads `cdc.public.orders` and applies changes to Redis (cache invalidation)
- [ ] Add a second consumer that aggregates order totals and pushes to a WebSocket dashboard
- [ ] Handle schema evolution (add a column mid-stream, confirm the pipeline doesn't break)
