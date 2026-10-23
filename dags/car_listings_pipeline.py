"""ETL: scrape used Honda Civic listings from 3 sites -> merge -> dedup -> QC -> PostgreSQL.

    extract (TaskGroup, parallel): one2car | carsome | taladrod
        -> merge (ALL_DONE: continues even if a source failed)
        -> transform (clean + cross-source dedup)
        -> quality_check (branch)
              |-- pass -> load (UPSERT car_listings)
              '-- fail -> quarantine (car_listings_quarantine, main table untouched)
        -> log_run (one row in pipeline_runs)

Intermediate data lives in JSON files under /opt/airflow/data (XCom only carries paths/summaries).
Demo knobs (DAG params, set via "Trigger DAG w/ config"):
    fail_source : "one2car" | "carsome" | "taladrod"  -> that extract task raises on purpose
    min_rows    : minimum rows for the quality check (default 100); a huge value forces quarantine
"""
import json
import os
import re
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

DATA_DIR = "/opt/airflow/data"
CONN_ID = "cars_db"
SOURCES = ["one2car", "carsome", "taladrod"]

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
ALTER TABLE car_listings ADD COLUMN IF NOT EXISTS also_listed_on TEXT;

CREATE TABLE IF NOT EXISTS car_listings_quarantine (
    id         SERIAL PRIMARY KEY,
    run_id     TEXT NOT NULL,
    reason     TEXT NOT NULL,
    row_data   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    sources_ok        TEXT[],
    sources_failed    TEXT[],
    rows_extracted    JSONB,
    rows_after_dedup  INT,
    dupes_removed     INT,
    quality_passed    BOOLEAN,
    quality_reason    TEXT,
    branch_taken      TEXT,
    rows_loaded       INT
);
"""

COLS = ["source", "source_listing_id", "title", "brand", "model", "variant", "year",
        "mileage_km", "price_thb", "transmission", "fuel", "province", "url", "scraped_at",
        "also_listed_on"]
_updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLS if c not in ("source", "source_listing_id"))
UPSERT = (
    f"INSERT INTO car_listings ({', '.join(COLS)}) VALUES ({', '.join(['%s'] * len(COLS))}) "
    f"ON CONFLICT (source, source_listing_id) DO UPDATE SET {_updates}"
)


def _hook():
    return PostgresHook(postgres_conn_id=CONN_ID)


def _extract(source: str, **scrape_kwargs) -> str:
    """Run one scraper, dump raw JSON, return the file path."""
    import importlib
    ctx = get_current_context()
    path = f"{DATA_DIR}/raw_{source}.json"
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(path):
        os.remove(path)  # never let a stale file from an older run look like fresh data
    if (ctx["params"].get("fail_source") or "").strip() == source:
        raise RuntimeError(f"Deliberate failure of source '{source}' (param fail_source)")
    rows = importlib.import_module(f"scrapers.{source}").scrape(**scrape_kwargs)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    return path


def _norm(s) -> str:
    return "".join(ch for ch in str(s or "").lower() if ch.isalnum())


def _trim_tokens(r) -> frozenset:
    """Trim-level words of the variant ('TURBO EL 1.5' -> {turbo, el}); engine sizes dropped
    because sites write them differently ('EL' vs 'EL 1.8')."""
    return frozenset(re.findall(r"[a-z]+", str(r.get("variant") or "").lower()))


def _same_car(a, b) -> bool:
    """Normalised match across two different sources: same model, year, price within 5,000 THB,
    mileage within 3,000 km (One2car rounds mileage) and compatible trim words; if mileage is
    missing on one side the rule is stricter (same price, same trim words)."""
    if a["source"] == b["source"] or a.get("year") is None or a["year"] != b.get("year"):
        return False
    if _norm(a.get("model")) != _norm(b.get("model")):
        return False
    ta, tb = _trim_tokens(a), _trim_tokens(b)
    if a.get("mileage_km") is not None and b.get("mileage_km") is not None:
        return (abs(a["price_thb"] - b["price_thb"]) <= 5000
                and abs(a["mileage_km"] - b["mileage_km"]) <= 3000 and (ta <= tb or tb <= ta))
    # mileage missing on one side: only an exact price + identical trim words counts
    return a["price_thb"] == b["price_thb"] and ta == tb


@dag(dag_id="car_listings_pipeline", schedule=None, start_date=datetime(2026, 1, 1),
     catchup=False, tags=["cars"], max_active_runs=1,
     params={"fail_source": "", "min_rows": 100},
     default_args={"retries": 1, "retry_delay": timedelta(seconds=20)})
def car_listings_pipeline():

    with TaskGroup("extract", tooltip="3 sources scraped in parallel"):
        @task
        def extract_one2car() -> str:
            return _extract("one2car", max_pages=10, delay=1.5)

        @task
        def extract_carsome() -> str:
            return _extract("carsome", max_pages=10, delay=1.5)

        @task
        def extract_taladrod() -> str:
            # list page already holds all listings; detail enrichment is capped to stay polite
            return _extract("taladrod", max_pages=10, delay=1.5, max_details=20, detail_delay=4.0)

        p1, p2, p3 = extract_one2car(), extract_carsome(), extract_taladrod()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def merge(one2car_path, carsome_path, taladrod_path) -> dict:
        """Union of whatever sources succeeded (failed ones return no XCom -> None)."""
        paths = dict(zip(SOURCES, [one2car_path, carsome_path, taladrod_path]))
        merged, counts, ok, failed = [], {}, [], []
        for src, p in paths.items():
            if not p or not os.path.exists(p):
                failed.append(src)
                counts[src] = 0
                continue
            with open(p, encoding="utf-8") as f:
                rows = json.load(f)
            ok.append(src)
            counts[src] = len(rows)
            merged.extend(rows)
        if not ok:
            raise RuntimeError("All sources failed - nothing to merge")
        out = f"{DATA_DIR}/merged.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False)
        return {"path": out, "sources_ok": ok, "sources_failed": failed,
                "rows_extracted": counts}

    @task
    def transform(m: dict) -> dict:
        """Clean/normalise rows, then remove cross-source duplicates."""
        with open(m["path"], encoding="utf-8") as f:
            raw = json.load(f)
        now_year = datetime.now().year
        clean, seen, no_price = [], set(), 0
        for r in raw:
            key = (r["source"], r["source_listing_id"])
            if key in seen:
                continue
            seen.add(key)
            if not r.get("price_thb") or r["price_thb"] < 10_000:
                no_price += 1
                continue
            if r.get("year") and not 1990 <= r["year"] <= now_year + 1:
                r["year"] = None
            if r.get("mileage_km") is not None and not 0 <= r["mileage_km"] <= 2_000_000:
                r["mileage_km"] = None
            for k in ("title", "variant", "province"):
                if r.get(k):
                    r[k] = r[k].strip()
            clean.append(r)

        # cross-source dedup (see _same_car): richest record wins, ties broken by source name (deterministic)
        clean.sort(key=lambda r: (-sum(v is not None for v in r.values()), r["source"],
                                  r["source_listing_id"]))
        kept, by_year, dropped = [], {}, []
        for r in clean:
            winner = next((w for w in by_year.get(r.get("year"), [])
                           if _same_car(w, r) and r["source"] not in (w.get("also_listed_on") or "")),
                          None)  # one match per other source: two listings of one site stay distinct
            if winner is not None:
                dropped.append([r["source"], r["source_listing_id"]])
                others = set(filter(None, (winner.get("also_listed_on") or "").split(",")))
                others.add(r["source"])
                winner["also_listed_on"] = ",".join(sorted(others))
                continue
            by_year.setdefault(r.get("year"), []).append(r)
            r.setdefault("also_listed_on", None)
            kept.append(r)

        out = f"{DATA_DIR}/clean.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"rows": kept, "dropped": dropped}, f, ensure_ascii=False)
        total_raw = len(raw)
        return {**m, "clean_path": out, "rows_raw": total_raw, "rows_after_dedup": len(kept),
                "dupes_removed": len(dropped),
                "price_null_rate": no_price / total_raw if total_raw else 1.0}

    @task.branch
    def quality_check(t: dict) -> str:
        """Pass -> 'load', fail -> 'quarantine'. The report is pushed for log_run."""
        ctx = get_current_context()
        min_rows = int(ctx["params"].get("min_rows", 100))
        with open(t["clean_path"], encoding="utf-8") as f:
            rows = json.load(f)["rows"]
        now_year = datetime.now().year
        problems = []
        if not t["sources_ok"]:
            problems.append("no source succeeded")
        if len(rows) < min_rows:
            problems.append(f"rows {len(rows)} < min_rows {min_rows}")
        if t["price_null_rate"] >= 0.05:
            problems.append(f"price null rate {t['price_null_rate']:.1%} >= 5%")
        bad_year = sum(1 for r in rows if r.get("year") and not 1990 <= r["year"] <= now_year + 1)
        if bad_year:
            problems.append(f"{bad_year} rows with year outside 1990..{now_year + 1}")
        neg = sum(1 for r in rows if (r.get("mileage_km") or 0) < 0)
        if neg:
            problems.append(f"{neg} rows with negative mileage")
        passed = not problems
        ctx["ti"].xcom_push(key="report", value={
            "passed": passed, "reason": "; ".join(problems) or "all checks passed"})
        return "load" if passed else "quarantine"

    @task
    def load(t: dict) -> int:
        with open(t["clean_path"], encoding="utf-8") as f:
            data = json.load(f)
        conn = _hook().get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(DDL)
                # a listing that is now a cross-source duplicate must not linger from an older run
                cur.executemany("DELETE FROM car_listings WHERE source=%s AND source_listing_id=%s",
                                data["dropped"])
                cur.executemany(UPSERT, [[r.get(c) for c in COLS] for r in data["rows"]])
        finally:
            conn.close()
        return len(data["rows"])

    @task
    def quarantine(t: dict) -> int:
        ctx = get_current_context()
        reason = ctx["ti"].xcom_pull(task_ids="quality_check", key="report")["reason"]
        with open(t["clean_path"], encoding="utf-8") as f:
            rows = json.load(f)["rows"]
        conn = _hook().get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(DDL)
                cur.executemany(
                    "INSERT INTO car_listings_quarantine (run_id, reason, row_data) VALUES (%s,%s,%s)",
                    [[ctx["run_id"], reason, json.dumps(r, ensure_ascii=False)] for r in rows])
        finally:
            conn.close()
        return len(rows)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_run(t: dict, loaded, quarantined) -> None:
        ctx = get_current_context()
        report = ctx["ti"].xcom_pull(task_ids="quality_check", key="report")
        branch = "load" if loaded is not None else "quarantine"
        conn = _hook().get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(DDL)
                cur.execute(
                    """INSERT INTO pipeline_runs (run_id, started_at, finished_at, sources_ok,
                         sources_failed, rows_extracted, rows_after_dedup, dupes_removed,
                         quality_passed, quality_reason, branch_taken, rows_loaded)
                       VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (run_id) DO UPDATE SET finished_at = now(),
                         sources_ok=EXCLUDED.sources_ok, sources_failed=EXCLUDED.sources_failed,
                         rows_extracted=EXCLUDED.rows_extracted,
                         rows_after_dedup=EXCLUDED.rows_after_dedup,
                         dupes_removed=EXCLUDED.dupes_removed, quality_passed=EXCLUDED.quality_passed,
                         quality_reason=EXCLUDED.quality_reason, branch_taken=EXCLUDED.branch_taken,
                         rows_loaded=EXCLUDED.rows_loaded""",
                    (ctx["run_id"], ctx["dag_run"].start_date, t["sources_ok"], t["sources_failed"],
                     json.dumps(t["rows_extracted"]), t["rows_after_dedup"], t["dupes_removed"],
                     report["passed"], report["reason"], branch, loaded or 0))
        finally:
            conn.close()

    m = merge(p1, p2, p3)
    t = transform(m)
    branch = quality_check(t)
    ld, qr = load(t), quarantine(t)
    branch >> [ld, qr]
    log_run(t, ld, qr)


car_listings_pipeline()
