"""Simulate a day of OLTP activity so Mirroring (CDC) and SCD Type 2 have
something to track. Run it once before each pipeline run you want to demo.

What it does (all in one transaction):
  * moves some customers to a new city / loyalty tier / email   -> SCD2 on dim_customer
  * re-prices some products and re-categorises a few             -> SCD2 on dim_product
  * inserts new online orders timestamped "today"                -> new facts
  * flips some recent completed orders to "returned"             -> late-arriving update

    python simulate_sql_changes.py --customers 40 --products 10 --orders 300
"""
import argparse
import random
from datetime import datetime, timedelta

import pyodbc

import config as C


def connect(args):
    auth = (f"UID={args.user};PWD={args.password};" if args.user else "Trusted_Connection=yes;")
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={args.server};DATABASE=RetailOLTP;"
        f"{auth}TrustServerCertificate=yes;", autocommit=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--server", default="localhost")
    p.add_argument("--user"); p.add_argument("--password")
    p.add_argument("--customers", type=int, default=40)
    p.add_argument("--products", type=int, default=10)
    p.add_argument("--orders", type=int, default=300)
    p.add_argument("--returns", type=int, default=15)
    args = p.parse_args()

    conn = connect(args)
    cur = conn.cursor()
    now = datetime.utcnow().replace(microsecond=0)

    # --- customer changes (SCD2 drivers) --------------------------------
    cur.execute("SELECT customer_id, country, loyalty_tier, email FROM dbo.customers")
    custs = cur.fetchall()
    for cid, country, tier, email in random.sample(custs, args.customers):
        change = random.choice(["move", "tier", "email"])
        if change == "move":
            city, state = random.choice(C.COUNTRIES[country]["cities"])
            cur.execute("UPDATE dbo.customers SET city=?, state=?, updated_at=? WHERE customer_id=?",
                        city, state, now, cid)
        elif change == "tier":
            idx = C.LOYALTY_TIERS.index(tier)
            new = C.LOYALTY_TIERS[min(max(idx + random.choice([-1, 1]), 0), 3)]
            cur.execute("UPDATE dbo.customers SET loyalty_tier=?, updated_at=? WHERE customer_id=?",
                        new, now, cid)
        else:
            user, dom = email.split("@")
            cur.execute("UPDATE dbo.customers SET email=?, updated_at=? WHERE customer_id=?",
                        f"{user}.new@{dom}", now, cid)

    # --- product changes -------------------------------------------------
    cur.execute("SELECT product_id FROM dbo.products")
    pids = [r[0] for r in cur.fetchall()]
    for pid in random.sample(pids, args.products):
        factor = random.choice([0.85, 0.9, 1.05, 1.1, 1.2])
        cur.execute("UPDATE dbo.products SET unit_price_inr = ROUND(unit_price_inr*?,0), "
                    "updated_at=? WHERE product_id=?", factor, now, pid)

    # --- new orders --------------------------------------------------------
    cur.execute("SELECT ISNULL(MAX(order_id),100000) FROM dbo.orders")
    oid = cur.fetchone()[0]
    cur.execute("SELECT product_id, unit_price_inr FROM dbo.products WHERE is_active=1")
    prices = dict(cur.fetchall())
    active = random.sample(custs, min(args.orders, len(custs)))
    cur.execute("SELECT customer_id, city FROM dbo.customers")
    city_of = dict(cur.fetchall())
    for cid, country, _, _ in active:
        oid += 1
        ccy = C.COUNTRIES[country]["currency"]
        ts = now - timedelta(minutes=random.randint(0, 600))
        cur.execute("INSERT INTO dbo.orders (order_id, customer_id, order_ts, channel, currency, "
                    "order_status, payment_method, ship_city, ship_country, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    oid, cid, ts, random.choice(C.ORDER_CHANNELS), ccy, "completed",
                    random.choice(C.PAYMENT_METHODS[1:]), city_of[cid], country, now)
        for ln, pid in enumerate(random.sample(list(prices), random.randint(1, 4)), start=1):
            cur.execute("INSERT INTO dbo.order_items VALUES (?,?,?,?,?,?)",
                        oid, ln, pid, random.randint(1, 3),
                        round(float(prices[pid]) / C.INR_PER_UNIT[ccy], 2),
                        random.choice([0, 0, 5, 10]))

    # --- late returns ------------------------------------------------------
    cur.execute("SELECT TOP (?) order_id FROM dbo.orders WHERE order_status='completed' "
                "AND order_ts > DATEADD(day,-30,?) ORDER BY NEWID()", args.returns, now)
    for (rid,) in cur.fetchall():
        cur.execute("UPDATE dbo.orders SET order_status='returned', updated_at=? WHERE order_id=?",
                    now, rid)

    conn.commit()
    print(f"{now}: changed {args.customers} customers, {args.products} products, "
          f"added {len(active)} orders (up to id {oid}), {args.returns} returns.")


if __name__ == "__main__":
    main()
