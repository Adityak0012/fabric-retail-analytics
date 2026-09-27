"""Generate a consistent, realistic retail dataset for every source system.

Run:  python generate_data.py
Out:  output/
        sql/          customers, products, stores, orders, order_items  (-> SQL Server)
        blob/inventory/inventory_snapshot_YYYY-MM-DD.csv            (-> Blob Storage)
        blob/reviews/reviews_YYYY-MM.jsonl                          (-> Blob Storage)
        salesforce/Case.csv                                         (-> Salesforce / fallback blob)
        _truth/customer_churn_truth.csv   (hidden ground truth, NEVER load into Fabric;
                                           use it only to sanity-check the ML model)

Design notes
------------
* Every customer gets a hidden churn "risk". Risky customers order less often,
  open more support cases, leave worse reviews and are more likely to stop
  buying altogether. That gives the churn model a real signal to learn.
* A small amount of BAD data is injected on purpose (bad ratings, duplicate
  review ids, negative stock, null product refs) so the Great Expectations
  checks in Silver have something to catch.
"""
from __future__ import annotations

import csv
import json
import math
import os
import random
from datetime import date, datetime, timedelta

import numpy as np
from faker import Faker

import config as C

rng = np.random.default_rng(C.SEED)
random.seed(C.SEED)
fake = Faker(["en_IN", "en_GB", "en_US"])
Faker.seed(C.SEED)

OUT = C.OUTPUT_DIR
for sub in ("sql", "blob/inventory", "blob/reviews", "blob/crm", "salesforce", "_truth"):
    os.makedirs(os.path.join(OUT, sub), exist_ok=True)


def write_csv(path: str, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {len(rows):>8,} rows -> {path}")


def rand_ts(start: date, end: date) -> datetime:
    """Random timestamp between two dates, biased to shopping hours."""
    days = max((end - start).days, 0)
    d = start + timedelta(days=int(rng.integers(0, days + 1)))
    hour = int(np.clip(rng.normal(15, 4), 0, 23))
    return datetime(d.year, d.month, d.day, hour, int(rng.integers(0, 60)), int(rng.integers(0, 60)))


# --------------------------------------------------------------------------
# Stores
# --------------------------------------------------------------------------
countries = list(C.COUNTRIES)
weights = np.array([C.COUNTRIES[c]["weight"] for c in countries])
weights = weights / weights.sum()

stores = []
sid = 1
for country, meta in C.COUNTRIES.items():
    n_stores = max(1, round(20 * meta["weight"]))
    for i in range(n_stores):
        city, state = meta["cities"][i % len(meta["cities"])]
        stores.append({
            "store_id": f"S{sid:03d}",
            "store_name": f"RetailCo {city} {['Central', 'Mall', 'Airport', 'High Street'][i % 4]}",
            "city": city, "state": state, "country": country,
            "currency": meta["currency"],
            "store_format": random.choice(["Flagship", "Standard", "Express"]),
            "opened_date": str(fake.date_between(date(2015, 1, 1), date(2024, 12, 31))),
        })
        sid += 1

# --------------------------------------------------------------------------
# Products
# --------------------------------------------------------------------------
brands = [fake.unique.company().split()[0] for _ in range(60)]
products = []
for i in range(1, C.N_PRODUCTS + 1):
    cat = random.choice(list(C.CATEGORIES))
    lo, hi = C.CATEGORIES[cat]
    price = round(float(np.exp(rng.uniform(math.log(lo), math.log(hi)))), -1) or lo
    products.append({
        "product_id": f"P{i:04d}",
        "product_name": f"{fake.word().title()} {fake.word().title()} {cat.split()[0]}",
        "category": cat,
        "brand": random.choice(brands),
        "unit_price_inr": price,
        "unit_cost_inr": round(price * rng.uniform(0.45, 0.75), 2),
        "is_active": 1,
        "updated_at": "2025-01-01 00:00:00",
    })
product_ids = [p["product_id"] for p in products]
product_price = {p["product_id"]: p["unit_price_inr"] for p in products}
# popularity: a few products sell a lot (long tail)
popularity = rng.pareto(1.3, len(products)) + 0.05
popularity = popularity / popularity.sum()

# --------------------------------------------------------------------------
# Customers (+ hidden churn risk)
# --------------------------------------------------------------------------
tier_risk = {"Bronze": 0.55, "Silver": 0.40, "Gold": 0.25, "Platinum": 0.15}
customers, truth = [], []
for i in range(1, C.N_CUSTOMERS + 1):
    country = rng.choice(countries, p=weights)
    meta = C.COUNTRIES[country]
    city, state = random.choice(meta["cities"])
    tier = rng.choice(C.LOYALTY_TIERS, p=[0.45, 0.30, 0.18, 0.07])
    first, last = fake.first_name(), fake.last_name()
    signup = fake.date_between(date(2022, 1, 1), C.HISTORY_END - timedelta(days=60))
    risk = float(np.clip(tier_risk[tier] + rng.normal(0, 0.18), 0.02, 0.95))
    customers.append({
        "customer_id": i,
        "first_name": first, "last_name": last,
        "email": f"{first}.{last}.{i}@example.com".lower().replace(" ", ""),
        "phone": fake.msisdn()[:10],
        "city": city, "state": state, "country": country,
        "loyalty_tier": tier,
        "marketing_opt_in": int(rng.random() > 0.3),
        "signup_date": str(signup),
        "updated_at": "2025-01-01 00:00:00",
    })
    # does this customer churn inside the history window, and when?
    start = max(signup, C.HISTORY_START)
    churns = rng.random() < risk * 0.9
    churn_date = None
    if churns and (C.HISTORY_END - start).days > 45:
        churn_date = start + timedelta(days=int(rng.integers(30, (C.HISTORY_END - start).days)))
    truth.append({"customer_id": i, "risk": round(risk, 3),
                  "churn_date": str(churn_date) if churn_date else ""})

cust_by_id = {c["customer_id"]: c for c in customers}
truth_by_id = {t["customer_id"]: t for t in truth}

# --------------------------------------------------------------------------
# Online orders
# --------------------------------------------------------------------------
orders, items = [], []
oid = 100_000
for c in customers:
    t = truth_by_id[c["customer_id"]]
    start = max(date.fromisoformat(c["signup_date"]), C.HISTORY_START)
    end = date.fromisoformat(t["churn_date"]) if t["churn_date"] else C.HISTORY_END
    if end <= start:
        continue
    months = (end - start).days / 30.4
    # risky customers buy less often
    lam = C.AVG_ORDERS_PER_ACTIVE_MONTH * (1.4 - t["risk"]) * months
    n_orders = int(rng.poisson(max(lam, 0.2)))
    currency = C.COUNTRIES[c["country"]]["currency"]
    fx = C.INR_PER_UNIT[currency]
    for _ in range(n_orders):
        oid += 1
        ts = rand_ts(start, end)
        status = rng.choice(["completed", "cancelled", "returned"],
                            p=[0.90 - 0.1 * t["risk"], 0.05, 0.05 + 0.1 * t["risk"]])
        orders.append({
            "order_id": oid, "customer_id": c["customer_id"],
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "channel": random.choice(C.ORDER_CHANNELS),
            "currency": currency, "order_status": status,
            "payment_method": random.choice(C.PAYMENT_METHODS[1:] if currency != "INR" else C.PAYMENT_METHODS),
            "ship_city": c["city"], "ship_country": c["country"],
            "updated_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
        })
        n_lines = int(np.clip(rng.poisson(1.6) + 1, 1, 6))
        chosen = rng.choice(product_ids, size=n_lines, replace=False, p=popularity)
        for ln, pid in enumerate(chosen, start=1):
            local_price = round(product_price[pid] / fx, 2)
            items.append({
                "order_id": oid, "line_no": ln, "product_id": pid,
                "quantity": int(np.clip(rng.poisson(0.6) + 1, 1, 8)),
                "unit_price": local_price,
                "discount_pct": float(rng.choice([0, 0, 0, 5, 10, 15, 20])),
            })

# --------------------------------------------------------------------------
# Inventory snapshots (CSV, one file per day) -> Blob
# --------------------------------------------------------------------------
store_assortment = {s["store_id"]: rng.choice(product_ids, size=150, replace=False) for s in stores}
state = {(s, p): int(rng.integers(5, 120)) for s, ps in store_assortment.items() for p in ps}
for d_off in range(C.INVENTORY_DAYS, 0, -1):
    snap = C.HISTORY_END - timedelta(days=d_off - 1)
    rows = []
    for (s, p), qty in state.items():
        sold = int(rng.poisson(2))
        qty = qty - sold
        restock = qty < 15 and rng.random() < 0.6
        if restock:
            qty += int(rng.integers(40, 120))
        state[(s, p)] = max(qty, 0)
        on_hand = state[(s, p)]
        if rng.random() < 0.002:            # injected bad data: negative stock
            on_hand = -int(rng.integers(1, 20))
        rows.append({"snapshot_date": str(snap), "store_id": s, "sku": p,
                     "on_hand_qty": on_hand, "reorder_point": 15,
                     "last_restock_date": str(snap) if restock else ""})
    # write only first & last file loudly
    path = os.path.join(OUT, "blob/inventory", f"inventory_snapshot_{snap}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
print(f"  wrote {C.INVENTORY_DAYS} inventory snapshot files -> {OUT}/blob/inventory/")

# --------------------------------------------------------------------------
# Product reviews (nested JSON lines, one file per month) -> Blob
# --------------------------------------------------------------------------
reviews_by_month: dict[str, list[dict]] = {}
rid = 0
completed = [o for o in orders if o["order_status"] != "cancelled"]
order_lines: dict[int, list[str]] = {}
for it in items:
    order_lines.setdefault(it["order_id"], []).append(it["product_id"])
for o in completed:
    t = truth_by_id[o["customer_id"]]
    if rng.random() > 0.18:
        continue
    pid = random.choice(order_lines[o["order_id"]])
    rid += 1
    mean = 4.5 - 2.6 * t["risk"]
    rating = int(np.clip(round(rng.normal(mean, 0.9)), 1, 5))
    ts = datetime.fromisoformat(o["order_ts"]) + timedelta(days=int(rng.integers(2, 20)))
    if ts.date() > C.HISTORY_END:
        continue
    rec = {
        "review_id": f"R{rid:07d}",
        "product": {"sku": pid, "category_hint": None},
        "customer": {"id": o["customer_id"], "email": cust_by_id[o["customer_id"]]["email"]},
        "rating": rating,
        "title": fake.sentence(nb_words=5).rstrip("."),
        "body": fake.paragraph(nb_sentences=2),
        "submitted_at": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": random.choice(["web", "app", "email_survey"]),
        "helpful_votes": int(rng.poisson(1.5)),
    }
    # injected bad data
    r = rng.random()
    if r < 0.004:
        rec["rating"] = random.choice([0, 6, 11])
    elif r < 0.007:
        rec["product"]["sku"] = None
    reviews_by_month.setdefault(ts.strftime("%Y-%m"), []).append(rec)
    if rng.random() < 0.01:                       # duplicate delivery
        reviews_by_month[ts.strftime("%Y-%m")].append(dict(rec))
n_rev = 0
for month, recs in sorted(reviews_by_month.items()):
    with open(os.path.join(OUT, "blob/reviews", f"reviews_{month}.jsonl"), "w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    n_rev += len(recs)
print(f"  wrote {n_rev:>8,} reviews in {len(reviews_by_month)} files -> {OUT}/blob/reviews/")

# --------------------------------------------------------------------------
# Support cases (Salesforce Case object shape)
# --------------------------------------------------------------------------
reasons = ["Delivery delay", "Damaged item", "Refund request", "Wrong item",
           "Payment issue", "Product question", "Loyalty points"]
cases = []
cnum = 0
for c in customers:
    t = truth_by_id[c["customer_id"]]
    n = int(rng.poisson(0.3 + 2.2 * t["risk"] ** 2))
    start = max(date.fromisoformat(c["signup_date"]), C.HISTORY_START)
    end = date.fromisoformat(t["churn_date"]) + timedelta(days=10) if t["churn_date"] else C.HISTORY_END
    end = min(end, C.HISTORY_END)
    if end <= start:
        continue
    for _ in range(n):
        cnum += 1
        opened = rand_ts(start, end)
        resolved = rng.random() > 0.15 + 0.35 * t["risk"]
        closed = opened + timedelta(hours=float(rng.gamma(2, 20 + 60 * t["risk"]))) if resolved else None
        if closed and closed.date() > C.HISTORY_END:
            closed, resolved = None, False
        reason = random.choice(reasons)
        cases.append({
            "Legacy_Case_Id__c": f"C{cnum:07d}",
            "Legacy_Customer_Id__c": str(c["customer_id"]),
            "SuppliedEmail": c["email"],
            "Subject": f"{reason} - order issue",
            "Description": fake.sentence(nb_words=14),
            "Status": "Closed" if resolved else random.choice(["New", "Working", "Escalated"]),
            "Priority": rng.choice(["Low", "Medium", "High"], p=[0.4, 0.4, 0.2]) if t["risk"] < 0.5
                        else rng.choice(["Low", "Medium", "High"], p=[0.15, 0.45, 0.4]),
            "Origin": random.choice(["Email", "Phone", "Web"]),
            "Reason": reason,
            "Opened_At__c": opened.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Resolved_At__c": closed.strftime("%Y-%m-%dT%H:%M:%SZ") if closed else "",
            "CSAT_Score__c": (int(np.clip(round(rng.normal(4.3 - 2.5 * t["risk"], 0.8)), 1, 5))
                              if resolved else ""),
        })

# --------------------------------------------------------------------------
# Write everything
# --------------------------------------------------------------------------
print("Writing files...")
write_csv(os.path.join(OUT, "sql/stores.csv"), stores)
write_csv(os.path.join(OUT, "sql/products.csv"), products)
write_csv(os.path.join(OUT, "sql/customers.csv"), customers)
write_csv(os.path.join(OUT, "sql/orders.csv"), orders)
write_csv(os.path.join(OUT, "sql/order_items.csv"), items)
write_csv(os.path.join(OUT, "salesforce/Case.csv"), cases)
# fallback copy for people who skip Salesforce: same shape, lands in blob/crm
write_csv(os.path.join(OUT, "blob/crm/salesforce_case_export.csv"), cases)
write_csv(os.path.join(OUT, "_truth/customer_churn_truth.csv"), truth)
print("Done.")
