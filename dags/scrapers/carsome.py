"""Carsome scraper (HTTP client + BeautifulSoup, no browser automation).

Listing cards are server-rendered (`article.mod-b-card`). The page also ships a
Nuxt state blob, but it is a minified function with positional arguments
(`window.__NUXT__=(function(a,b,...){...})`), far more fragile than the card
markup, so the cards are parsed. The listing id is the last URL segment
(e.g. .../2021-honda-civic-turbo-el-1.5/cqt5000 -> "cqt5000").
Pagination: `?pageNo=N`; stops at the first page without new cards.
"""
import logging
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from .common import make_session, polite_sleep, robots_allows

log = logging.getLogger(__name__)

SITE = "https://www.carsome.co.th"
BASE = SITE + "/buy-car/honda/civic"
SOURCE = "carsome"


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


def parse_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for art in soup.select("article.mod-b-card"):
        a = art.select_one("a.mod-b-card__title")
        if not a or not a.get("href"):
            continue
        href = a["href"]
        lid = href.rstrip("/").split("/")[-1]
        # title block: <p>2021 Honda</p><p>CIVIC TURBO EL 1.5</p>
        ps = [" ".join(p.get_text(" ", strip=True).split()) for p in a.find_all("p")]
        line1 = ps[0] if ps else ""
        line2 = ps[1] if len(ps) > 1 else ""
        ym = re.search(r"\b(\d{4})\b", line1)
        variant = re.sub(r"^civic\s*", "", line2, flags=re.I).strip() or None
        # "<mileage> กม." / transmission / province in the car-other row
        spans = art.select(".mod-b-card__car-other > span")
        mileage = trans = prov = None
        for sp in spans:
            txt = sp.get_text(" ", strip=True)
            if "กม" in txt:
                mileage = _int(txt)
            elif re.fullmatch(r"(Automatic|Manual)", txt, re.I):
                trans = txt.title()
            elif txt:
                prov = txt
        price_el = art.select_one(".mod-card__price__total strong")
        price = _int(price_el.get_text()) if price_el else None
        rows.append({
            "source": SOURCE,
            "source_listing_id": lid,
            "title": " ".join(x for x in (line1, line2) if x) or None,
            "brand": "Honda",
            "model": "Civic",
            "variant": variant,
            "year": _year(ym.group(1)) if ym else None,
            "mileage_km": mileage,
            "price_thb": price,
            "transmission": trans,
            "fuel": "Hybrid" if re.search(r"hybrid", line2, re.I) else None,
            "province": prov,
            "url": SITE + href,
            "scraped_at": now,
        })
    return rows


def scrape(max_pages: int = 10, delay: float = 1.5) -> list[dict]:
    session = make_session()
    if not robots_allows(session, BASE):
        raise RuntimeError("robots.txt disallows scraping this URL")
    seen, rows = set(), []
    for page in range(1, max_pages + 1):
        url = BASE if page == 1 else f"{BASE}?pageNo={page}"
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            log.warning("carsome page %s failed: %s", page, e)
            polite_sleep(delay)
            continue
        page_rows = [x for x in parse_page(r.text) if x["source_listing_id"] not in seen]
        log.info("carsome page %s -> %s new rows", page, len(page_rows))
        if not page_rows:
            break
        seen.update(x["source_listing_id"] for x in page_rows)
        rows.extend(page_rows)
        polite_sleep(delay)
    if not rows:
        raise RuntimeError("carsome: no records (blocked or layout changed)")
    return rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape()
    print("rows:", len(data))
    for r in data[:3]:
        print(r)
