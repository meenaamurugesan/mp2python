import os
import sys
import time
import sqlite3
import pandas as pd
import psycopg2

from utils import get_db_url   # your prof's helper


# ---------- CONFIG ----------
SQLITE_DB_FILE = "normalized.db"      # your existing SQLite DB
DATABASE_URL = get_db_url()          # Postgres URL
CSV_DIR = "/tmp"                     # temp directory for CSV during COPY

# Tables as they exist in SQLite
SQLITE_TABLES = [
    "Region",
    "Country",
    "Customer",
    "ProductCategory",
    "Product",
    "OrderDetail",
]

# Tables as they exist in Postgres (lowercase, because unquoted identifiers are folded)
PG_TABLES = [
    "region",
    "country",
    "customer",
    "productcategory",
    "product",
    "orderdetail",
]

# ---------- SCHEMA FOR YOUR 6 TABLES ----------
SCHEMA_SQL = """
-- Only touch your 6 tables; leave prof's stage_* tables alone
DROP TABLE IF EXISTS orderdetail    CASCADE;
DROP TABLE IF EXISTS customer       CASCADE;
DROP TABLE IF EXISTS product        CASCADE;
DROP TABLE IF EXISTS productcategory CASCADE;
DROP TABLE IF EXISTS country        CASCADE;
DROP TABLE IF EXISTS region         CASCADE;

CREATE TABLE region(
    regionid INTEGER PRIMARY KEY,
    region   TEXT NOT NULL
);

CREATE TABLE country(
    countryid INTEGER PRIMARY KEY,
    country   TEXT NOT NULL,
    regionid  INTEGER NOT NULL REFERENCES region(regionid)
);

CREATE TABLE customer(
    customerid INTEGER PRIMARY KEY,
    firstname  TEXT NOT NULL,
    lastname   TEXT NOT NULL,
    address    TEXT NOT NULL,
    city       TEXT NOT NULL,
    countryid  INTEGER NOT NULL REFERENCES country(countryid)
);

CREATE TABLE productcategory(
    productcategoryid          INTEGER PRIMARY KEY,
    productcategory            TEXT NOT NULL,
    productcategorydescription TEXT NOT NULL
);

CREATE TABLE product(
    productid         INTEGER PRIMARY KEY,
    productname       TEXT NOT NULL,
    productunitprice  REAL    NOT NULL,
    productcategoryid INTEGER NOT NULL REFERENCES productcategory(productcategoryid)
);

CREATE TABLE orderdetail(
    orderid         INTEGER PRIMARY KEY,
    customerid      INTEGER NOT NULL REFERENCES customer(customerid),
    productid       INTEGER NOT NULL REFERENCES product(productid),
    orderdate       TEXT    NOT NULL,
    quantityordered INTEGER NOT NULL
);
"""


# ---------- HELPERS ----------

def list_sqlite_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [r[0] for r in cur.fetchall()]
    cur.close()
    print("SQLite tables found:", tables)
    return tables


def truncate_pg_tables(pg_conn, tables):
    """TRUNCATE ONLY the tables that actually exist in Postgres (public schema)."""
    cur = pg_conn.cursor()

    # find existing tables
    cur.execute("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public';
    """)
    existing = {r[0] for r in cur.fetchall()}

    to_truncate = [t for t in tables if t in existing]
    if not to_truncate:
        print("No matching tables to truncate in Postgres.")
        cur.close()
        return

    print("Truncating Postgres tables:", to_truncate)
    truncate_sql = "TRUNCATE " + ", ".join(to_truncate) + " CASCADE;"
    cur.execute(truncate_sql)
    pg_conn.commit()
    cur.close()
    print("✅ Truncate complete\n")


def migrate_small_table(sqlite_conn, pg_conn, sqlite_table, pg_table):
    """
    For: Region, Country, Customer, ProductCategory, Product
    - Read all rows from SQLite
    - Insert into Postgres
    """
    print(f"→ Migrating {sqlite_table} (SQLite) → {pg_table} (Postgres)...")

    # SQLite is case-insensitive for table names; quote just to be safe
    df = pd.read_sql(f'SELECT * FROM "{sqlite_table}"', sqlite_conn)

    if df.empty:
        print(f"   No data in {sqlite_table}, skipping.")
        return

    # Postgres created columns as lowercase (RegionID -> regionid)
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.where(pd.notnull(df), None)  # NaN -> None for psycopg2

    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO {pg_table} ({', '.join(cols)}) VALUES ({placeholders})"

    cur = pg_conn.cursor()
    cur.executemany(insert_sql, df.values.tolist())
    pg_conn.commit()
    cur.close()

    print(f"   Inserted {len(df)} rows into {pg_table}")


def migrate_large_table(sqlite_conn, pg_conn, sqlite_table, pg_table):
    """
    For OrderDetail (~600k rows)
    - Read from SQLite into pandas
    - Export to CSV
    - Use Postgres COPY for fast bulk load
    """
    print(f"→ Migrating LARGE table {sqlite_table} (SQLite) → {pg_table} (Postgres) via COPY...")

    df = pd.read_sql(f'SELECT * FROM "{sqlite_table}"', sqlite_conn)

    if df.empty:
        print(f"   No data in {sqlite_table}, skipping.")
        return

    df.columns = [c.strip().lower() for c in df.columns]
    df = df.where(pd.notnull(df), None)

    os.makedirs(CSV_DIR, exist_ok=True)
    csv_path = os.path.join(CSV_DIR, f"{pg_table}.csv")
    df.to_csv(csv_path, index=False)
    print(f"   Exported {len(df)} rows from {sqlite_table} to {csv_path}")

    cur = pg_conn.cursor()
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(f'COPY {pg_table} FROM STDIN WITH CSV HEADER', f)
    pg_conn.commit()
    cur.close()

    print(f"   ✅ Bulk loaded {len(df)} rows into {pg_table}")


# ---------- MAIN ----------

if __name__ == "__main__":
    # 0) check SQLite file exists
    if not os.path.exists(SQLITE_DB_FILE):
        sys.exit(f"ERROR: SQLite DB file not found at {SQLITE_DB_FILE}")

    print("Connecting to SQLite...")
    sqlite_conn = sqlite3.connect(SQLITE_DB_FILE)
    tables_found = list_sqlite_tables(sqlite_conn)
    missing = [t for t in SQLITE_TABLES if t not in tables_found]
    if missing:
        print("⚠️ Warning: these expected tables are missing in SQLite:", missing)

    print("Connecting to Postgres...")
    pg_conn = psycopg2.connect(DATABASE_URL)

    # small perf tweak for bulk load
    pg_conn.autocommit = False
    with pg_conn.cursor() as c:
        c.execute("SET synchronous_commit = OFF;")

    start = time.monotonic()

    # 1) CREATE / RECREATE your 6 tables
    print("Creating / recreating region/country/customer/productcategory/product/orderdetail tables...")
    cur = pg_conn.cursor()
    cur.execute(SCHEMA_SQL)
    pg_conn.commit()
    cur.close()
    print("✅ Schema created\n")

    # 2) TRUNCATE existing data (now tables definitely exist)
    truncate_pg_tables(pg_conn, PG_TABLES)

    # 3) migrate small tables in FK order
    migrate_small_table(sqlite_conn, pg_conn, "Region", "region")
    migrate_small_table(sqlite_conn, pg_conn, "Country", "country")
    migrate_small_table(sqlite_conn, pg_conn, "ProductCategory", "productcategory")
    migrate_small_table(sqlite_conn, pg_conn, "Product", "product")
    migrate_small_table(sqlite_conn, pg_conn, "Customer", "customer")

    # 4) migrate big OrderDetail table
    migrate_large_table(sqlite_conn, pg_conn, "OrderDetail", "orderdetail")

    sqlite_conn.close()
    pg_conn.close()

    elapsed = time.monotonic() - start
    print(f"\n🎉 Migration complete! Total time: {elapsed:.2f} seconds")
