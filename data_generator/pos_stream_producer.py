"""Real-time point-of-sale (POS) event producer.

Sends JSON events over the Apache Kafka protocol to a Fabric Eventstream
"Custom endpoint" source (Kafka tab). The same code works against any
Kafka broker (local Docker Kafka, Confluent Cloud, Azure Event Hubs).

Setup: copy .env.example to .env and fill in the values from
  Eventstream > Custom endpoint source > Details > Kafka > SAS Key Authentication

    python pos_stream_producer.py --dry-run                # print events, no Kafka
    python pos_stream_producer.py --rate 5                 # ~5 baskets/sec
    python pos_stream_producer.py --anomaly return_spike --store S003   # trigger alerts
    python pos_stream_producer.py --anomaly big_ticket
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import config as C

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def load_reference():
    base = os.path.join(C.OUTPUT_DIR, "sql")
    with open(os.path.join(base, "stores.csv"), encoding="utf-8") as f:
        stores = list(csv.DictReader(f))
    with open(os.path.join(base, "products.csv"), encoding="utf-8") as f:
        products = list(csv.DictReader(f))
    return stores, products


def make_basket(stores, products, anomaly: str | None, target_store: str | None):
    store = next((s for s in stores if s["store_id"] == target_store), None) if target_store else None
    store = store or random.choice(stores)
    ccy = store["currency"]
    fx = C.INR_PER_UNIT[ccy]
    basket_id = str(uuid.uuid4())
    customer_id = random.randint(1, C.N_CUSTOMERS) if random.random() < 0.45 else None
    register_id = f"{store['store_id']}-R{random.randint(1, 6)}"
    payment = random.choice(C.PAYMENT_METHODS if ccy == "INR" else C.PAYMENT_METHODS[1:])
    event_type = "sale"
    n_lines = random.randint(1, 5)
    qty_mult = 1
    if anomaly == "return_spike" and random.random() < 0.7:
        event_type = "return"
    if anomaly == "big_ticket" and random.random() < 0.3:
        qty_mult = random.randint(20, 60)
    elif random.random() < 0.04:
        event_type = "return"

    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    events = []
    for line in range(1, n_lines + 1):
        p = random.choice(products)
        qty = random.randint(1, 3) * qty_mult
        price = round(float(p["unit_price_inr"]) / fx, 2)
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_ts": now,
            "basket_id": basket_id,
            "line_no": line,
            "store_id": store["store_id"],
            "register_id": register_id,
            "customer_id": customer_id,
            "product_id": p["product_id"],
            "quantity": qty,
            "unit_price": price,
            "line_amount": round(qty * price * (-1 if event_type == "return" else 1), 2),
            "currency": ccy,
            "payment_method": payment,
        })
    # ~0.5% duplicate delivery, like a retrying POS client — Silver dedupes on event_id
    if random.random() < 0.005:
        events.append(dict(events[0]))
    return events


def build_producer():
    from confluent_kafka import Producer  # imported lazily so --dry-run needs no Kafka lib
    cfg = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": os.environ["KAFKA_CONNECTION_STRING"],
        "client.id": "retail-pos-simulator",
        "linger.ms": 50,
        "acks": "all",
        "enable.idempotence": False,   # Event Hubs-compatible endpoints don't support it
    }
    return Producer(cfg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=2.0, help="baskets per second")
    ap.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = run until Ctrl+C)")
    ap.add_argument("--anomaly", choices=["return_spike", "big_ticket"], default=None)
    ap.add_argument("--store", default=None, help="send all traffic to one store id (e.g. S003)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    stores, products = load_reference()
    topic = os.environ.get("KAFKA_TOPIC", "")
    producer = None if args.dry_run else build_producer()
    stop_at = time.time() + args.minutes * 60 if args.minutes else None
    sent = errors = 0

    def on_delivery(err, _msg):
        nonlocal errors
        if err is not None:
            errors += 1
            print(f"delivery failed: {err}", file=sys.stderr)

    print(f"Producing ~{args.rate} baskets/s  anomaly={args.anomaly}  store={args.store}  "
          f"{'(dry run)' if args.dry_run else 'topic=' + topic}")
    try:
        while not (stop_at and time.time() > stop_at):
            for ev in make_basket(stores, products, args.anomaly, args.store):
                payload = json.dumps(ev)
                if args.dry_run:
                    print(payload)
                else:
                    producer.produce(topic, key=ev["store_id"], value=payload, callback=on_delivery)
                sent += 1
            if producer:
                producer.poll(0)
            time.sleep(random.expovariate(args.rate))
            if args.dry_run and sent > 20:
                break
    except KeyboardInterrupt:
        print("\nStopping...")
    if producer:
        producer.flush(30)
    print(f"\nSent {sent} events, {errors} delivery errors.")


if __name__ == "__main__":
    main()
