"""One2car scraper (requests + BeautifulSoup, no browser automation).

Each listing is an <article data-listing-id=...> whose data-* attributes carry
car fields. Price and province are read from the page's JSON-LD (ItemList of
Product), joined by listing id. Seller name / phone / LINE id attributes exist
in the HTML but are deliberately NEVER read (PDPA).
"""
import json
import logging
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from .common import make_session, polite_sleep, robots_allows

log = logging.getLogger(__name__)

BASE = (
    "https://www.one2car.com/%E0%B8%A3%E0%B8%96-%E0%B8%AA%E0%B8%B3%E0%B8%AB"
    "%E0%B8%A3%E0%B8%B1%E0%B8%9A-%E0%B8%82%E0%B8%B2%E0%B8%A2/honda/civic"
)
SOURCE = "one2car"


def _int(v):
    try:
        return int(re.sub(r"[^\d]", "", str(v))) if v not in (None, "") else None
    except ValueError:
        return None


def _jsonld_products(soup):
    """Map listing_id -> {price, province, fuel} from JSON-LD blocks."""
    out = {}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except ValueError:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                if "offers" in node and "url" in node:
                    m = re.search(r"#listing_(\d+)", node["url"])
                    if m:
                        offers = node["offers"] or {}
                        addr = (((offers.get("seller") or {}).get("homeLocation") or {})
                                .get("address") or {})
                        out[m.group(1)] = {
                            "price": offers.get("price"),
                            "province": addr.get("addressLocality"),
                            "fuel": node.get("fuelType"),
                        }
                stack.extend(v for v in node.values() if isinstance(v, (dict, list)))
    return out


def parse_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    extra = _jsonld_products(soup)
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for art in soup.select("article[data-listing-id]"):
        lid = art.get("data-listing-id")
        if not lid or art.get("data-make", "").lower() != "honda":
            continue
        ex = extra.get(lid, {})
        price = _int(ex.get("price"))
        if price is None:
            price = _int(art.get("data-price-raw"))  # fallback (may be obfuscated)
        rows.append({
            "source": SOURCE,
            "source_listing_id": lid,
            "title": art.get("data-title"),
            "brand": art.get("data-make"),
            "model": (art.get("data-model") or "").title(),
            "variant": art.get("data-variant") or None,
            "year": _int(art.get("data-year")),
            "mileage_km": _int(art.get("data-mileage")),
            "price_thb": price,
            "transmission": art.get("data-transmission") or None,
            "fuel": ex.get("fuel"),
            "province": ex.get("province"),
            "url": art.get("data-url"),
            "scraped_at": now,
        })
    return rows


def scrape(max_pages: int = 10, delay: float = 1.5) -> list[dict]:
    session = make_session()
    if not robots_allows(session, BASE):
        raise RuntimeError("robots.txt disallows scraping this URL")
    seen, rows = set(), []
    for page in range(1, max_pages + 1):
        url = BASE if page == 1 else f"{BASE}?page_number={page}&page_size=26"
        r = session.get(url, timeout=30)
        r.raise_for_status()
        page_rows = [x for x in parse_page(r.text) if x["source_listing_id"] not in seen]
        log.info("one2car page %s -> %s new rows", page, len(page_rows))
        if not page_rows:
            break
        for x in page_rows:
            seen.add(x["source_listing_id"])
        rows.extend(page_rows)
        polite_sleep(delay)
    return rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape()
    print("rows:", len(data))
    for r in data[:3]:
        print(r)
