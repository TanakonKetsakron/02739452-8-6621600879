"""ETL: scrape used Honda Civic listings -> clean -> UPSERT into PostgreSQL.

Level 1: one source (One2car). Further sources plug in as extra extract tasks.
Intermediate data is exchanged through JSON files under /opt/airflow/data
(XCom only carries the small file path).
"""
import json
import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_DIR = "/opt/airflow/data"
CONN_ID = "cars_db"

DDL = """
CREATE TABLE IF NOT EXISTS car_listings (
    id                SERIAL PRIMARY KEY,
    source            TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    title             TEXT,
    brand             TEXT,
    model             TEXT,
    variant           TEXT,
    year              INT,
    mileage_km        INT,
    price_thb         INT,
    transmission      TEXT,
    fuel              TEXT,
    province          TEXT,
    url               TEXT,
    scraped_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_listing_id)
);
"""

COLS = ["source", "source_listing_id", "title", "brand", "model", "variant", "year",
        "mileage_km", "price_thb", "transmission", "fuel", "province", "url", "scraped_at"]

_updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLS if c not in ("source", "source_listing_id"))
UPSERT = (
    f"INSERT INTO car_listings ({', '.join(COLS)}) VALUES ({', '.join(['%s'] * len(COLS))}) "
    f"ON CONFLICT (source, source_listing_id) DO UPDATE SET {_updates}"
)


@dag(dag_id="car_listings_pipeline", schedule=None, start_date=datetime(2026, 1, 1),
     catchup=False, tags=["cars"])
def car_listings_pipeline():

    @task
    def extract_one2car() -> str:
        from scrapers.one2car import scrape
        os.makedirs(DATA_DIR, exist_ok=True)
        rows = scrape(max_pages=10, delay=1.5)
        path = f"{DATA_DIR}/raw_one2car.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
        return path

    @task
    def transform(raw_paths: list[str]) -> str:
        """Validate/clean: drop rows without price, implausible values, duplicates."""
        clean, seen = [], set()
        for p in raw_paths:
            with open(p, encoding="utf-8") as f:
                rows = json.load(f)
            for r in rows:
                key = (r["source"], r["source_listing_id"])
                if key in seen or not r.get("price_thb") or r["price_thb"] < 10_000:
                    continue
                if r.get("year") and not 1990 <= r["year"] <= datetime.now().year + 1:
                    r["year"] = None
                if r.get("mileage_km") is not None and r["mileage_km"] > 2_000_000:
                    r["mileage_km"] = None
                for k in ("title", "variant", "province"):
                    if r.get(k):
                        r[k] = r[k].strip()
                seen.add(key)
                clean.append(r)
        out = f"{DATA_DIR}/clean.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(clean, f, ensure_ascii=False)
        return out

    @task
    def load(clean_path: str) -> int:
        with open(clean_path, encoding="utf-8") as f:
            rows = json.load(f)
        conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(DDL)
                cur.executemany(UPSERT, [[r.get(c) for c in COLS] for r in rows])
        finally:
            conn.close()
        return len(rows)

    load(transform([extract_one2car()]))


car_listings_pipeline()
