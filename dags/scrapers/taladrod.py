"""Taladrod scraper (HTTP client + BeautifulSoup, no browser automation).

* The search page for Honda Civic (mk:29 = HONDA, md:132 = CIVIC; verified from
  the page title and the `lists` model table) embeds ALL matching listings as a
  JSON literal (`var SchDataJSON={...}`) - up to 600 per response, so one request
  returns every Civic and no pagination is needed. Parsing this JSON is far
  more robust than CSS selectors.
* The JSON lacks mileage / transmission / province, so the first `max_details`
  listings are enriched from their detail page (CarDet.aspx?cid=...). Only the
  registration PROVINCE is read (never the plate, seller, phone or LINE id: PDPA).
* AWS WAF may answer with a JS challenge (HTTP 202 / empty <title>) when we
  request too fast; we do not try to solve it - we back off, then give up
  politely and keep what we already have.
"""
import json
import logging
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from .common import make_session, polite_sleep, robots_allows

log = logging.getLogger(__name__)

BASE = "https://www.taladrod.com/w40/isch/schc.aspx?fno:all+mk:29+md:132"
DETAIL = "https://www.taladrod.com/w40/iCar/CarDet.aspx?cid={}"
SOURCE = "taladrod"

_JSON_RE = re.compile(r"var SchDataJSON=(\{.*?\});var CacheName", re.S)


def _int(v):
    try:
        return int(re.sub(r"[^\d]", "", str(v))) if v not in (None, "") else None
    except ValueError:
        return None


def _year(v):
    y = _int(v)
    if y is not None and y >= 2400:  # Buddhist era
        y -= 543
    return y


def _decode(resp) -> str:
    """Page is UTF-8 today; fall back to windows-874 (TIS-620) if not."""
    raw = resp.content
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp874", errors="replace")


def _is_challenge(resp, text: str) -> bool:
    return resp.status_code == 202 or "AwsWafIntegration" in text[:6000]


def _get(session, url, retries=2, backoff=45):
    """GET with WAF-challenge backoff. Returns text or None."""
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=60)
        except Exception as e:  # network error
            log.warning("taladrod GET failed %s: %s", url, e)
            polite_sleep(backoff)
            continue
        text = _decode(r)
        if r.status_code == 200 and not _is_challenge(r, text):
            return text
        log.warning("taladrod %s -> HTTP %s (challenge=%s), attempt %s",
                    url, r.status_code, _is_challenge(r, text), attempt + 1)
        polite_sleep(backoff)
    return None


def parse_list(html: str) -> list[dict]:
    m = _JSON_RE.search(html)
    if not m:
        return []
    data = json.loads(m.group(1))
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for c in data.get("cars", []):
        if c.get("mk") != "29" or c.get("md") != "132" or c.get("issold") == "Y":
            continue
        cid = c.get("cid")
        price = _int(c.get("prc"))
        if not cid:
            continue
        trim = c.get("atrim") or c.get("model") or None  # e.g. "2.0 e:HEV RS"
        is_hybrid = bool(re.search(r"HYBRID|HEV", (c.get("namemmt") or "").upper()))
        rows.append({
            "source": SOURCE,
            "source_listing_id": str(cid),
            "title": c.get("namemmt"),   # canonical model text, not the seller's free text
            "brand": "Honda",
            "model": "Civic",
            "variant": trim,
            "year": _year(c.get("yr4")),
            "mileage_km": None,          # filled from detail page
            "price_thb": price if price else None,
            "transmission": None,        # filled from detail page
            "fuel": "Hybrid" if is_hybrid else None,
            "province": None,            # filled from detail page
            "url": DETAIL.format(cid),
            "scraped_at": now,
        })
    return rows


def parse_detail(html: str) -> dict:
    """Extract mileage, transmission and registration province only."""
    text = BeautifulSoup(html, "lxml").get_text("|", strip=True)
    out = {}
    m = re.search(r"เลขไมล์\s*(?:ไม่เกิน)?\s*([\d,]+)\s*กม", text)
    if m:
        out["mileage_km"] = _int(m.group(1))  # for "ไม่เกิน N" this is the upper bound
    m = re.search(r"เกียร์\s*(ออโต้|ธรรมดา|[A-Za-z]+)", text)
    if m:
        out["transmission"] = {"ออโต้": "Automatic", "ธรรมดา": "Manual"}.get(m.group(1), m.group(1))
    # "ทะเบียน <plate> <province>" - plate deliberately discarded
    m = re.search(r"ทะเบียน\s+\S+\s+([ก-๙\.]+)", text)
    if m:
        out["province"] = m.group(1)
    return out


def scrape(max_pages: int = 10, delay: float = 1.5, max_details: int = 100,
           detail_delay: float = 4.0) -> list[dict]:
    """`max_pages` kept for interface parity; the list needs a single request."""
    session = make_session()
    if not robots_allows(session, BASE):
        raise RuntimeError("robots.txt disallows scraping this URL")
    html = _get(session, BASE)
    rows = parse_list(html) if html else []
    if not rows:
        raise RuntimeError("taladrod: no records (blocked or layout changed)")
    log.info("taladrod list -> %s rows", len(rows))
    if len(rows) >= 600:
        log.warning("taladrod: list hit the 600 cap, some listings may be missing")

    for i, row in enumerate(rows[:max_details]):
        polite_sleep(max(delay, detail_delay))
        page = _get(session, row["url"], retries=1, backoff=60)
        if page is None:
            log.warning("taladrod detail blocked at %s/%s; stopping enrichment", i, max_details)
            break
        row.update(parse_detail(page))
    return rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape()
    print("rows:", len(data))
    for r in data[:3]:
        print(r)
