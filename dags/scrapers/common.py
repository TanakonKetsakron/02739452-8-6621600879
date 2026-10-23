"""Shared helpers for all scrapers (polite HTTP session + robots.txt check)."""
import time
import urllib.robotparser
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "th,en;q=0.8",
}
TIMEOUT = 30


def make_session():
    """HTTP session. One2car sits behind Cloudflare, which returns 403 to the
    TLS fingerprint of plain `requests`; curl_cffi (plain HTTP client, no
    browser automation) presents a normal Chrome TLS fingerprint."""
    s = cffi_requests.Session(impersonate="chrome")
    s.headers.update(HEADERS)
    return s


def robots_allows(session, url: str) -> bool:
    """Return True if robots.txt of the site permits fetching `url`."""
    p = urlparse(url)
    rp = urllib.robotparser.RobotFileParser()
    try:
        r = session.get(f"{p.scheme}://{p.netloc}/robots.txt", timeout=TIMEOUT)
        rp.parse(r.text.splitlines())
    except Exception:
        return True  # robots.txt unreachable -> treated as no restrictions
    return rp.can_fetch(USER_AGENT, url)


def polite_sleep(seconds: float = 1.5) -> None:
    time.sleep(seconds)
