"""Bulk-load the generated CSVs into the RetailOLTP SQL Server database.

Prereqs: run sql/01_create_source_db.sql first, and install the
"ODBC Driver 18 for SQL Server".

    python load_sql_server.py                       # Windows auth, localhost
    python load_sql_server.py --server "localhost\\SQLEXPRESS"
    python load_sql_server.py --user sa --password ...
"""
import argparse
import csv
import os

import pyodbc

import config as C

TABLES = ["stores", "products", "customers", "orders", "order_items"]  # FK order
BATCH = 5_000


def connect(args) -> pyodbc.Connection:
    auth = (f"UID={args.user};PWD={args.password};" if args.user
            else "Trusted_Connection=yes;")
    cs = (f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={args.server};"
          f"DATABASE={args.database};{auth}TrustServerCertificate=yes;")
    return pyodbc.connect(cs, autocommit=False)


def load_table(conn, table: str) -> None:
    path = os.path.join(C.OUTPUT_DIR, "sql", f"{table}.csv")
    with open(path, encoding="utf-8") as f:
        reader = csv.reader(f)
        cols = next(reader)
        rows = [[v if v != "" else None for v in r] for r in reader]
    cur = conn.cursor()
    cur.fast_executemany = True
    sql = f"INSERT INTO dbo.{table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
    for i in range(0, len(rows), BATCH):
        cur.executemany(sql, rows[i:i + BATCH])
    conn.commit()
    print(f"  {table:<12} {len(rows):>8,} rows")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--server", default="localhost")
    p.add_argument("--database", default="RetailOLTP")
    p.add_argument("--user")
    p.add_argument("--password")
    args = p.parse_args()

    conn = connect(args)
    cur = conn.cursor()
    for t in reversed(TABLES):                    # clear children first
        cur.execute(f"DELETE FROM dbo.{t}")
    conn.commit()
    print("Loading...")
    for t in TABLES:
        load_table(conn, t)
    print("Done. Now set up Mirroring in Fabric (see docs/BUILD_GUIDE.md, Phase 2).")


if __name__ == "__main__":
    main()
