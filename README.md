# Used Honda Civic Listings in Thailand - Data Pipeline

Topic: used Honda Civic prices in Thailand. Listings from 3 websites are
collected with Apache Airflow into PostgreSQL and shown on a Streamlit dashboard.

| # | Source | Method | Status |
|---|--------|--------|--------|
| 1 | One2car (one2car.com) | requests-style HTTP (curl_cffi) + BeautifulSoup: `data-*` attributes + JSON-LD | done (Phase 1) |
| 2 | Carsome (carsome.co.th) | static HTML | planned |
| 3 | Taladrod (taladrod.com) | static HTML | planned |

No Selenium/Playwright. robots.txt is checked before scraping, 1.5 s delay between pages,
browser-like User-Agent, timeouts. Pagination uses `?page_number=N` (max 10 pages = 260 rows).
**PDPA:** only car data is stored. Seller name, phone, LINE ID and seller URLs are never read or stored.

## Architecture
```
One2car --> Airflow DAG car_listings_pipeline (extract_one2car -> transform -> load)
        --> PostgreSQL (db `cars`, table car_listings, UPSERT on (source, source_listing_id))
        --> Streamlit dashboard (reads PostgreSQL directly)
```
Table `car_listings`: id, source, source_listing_id, title, brand, model, variant, year,
mileage_km, price_thb, transmission, fuel, province, url, scraped_at, UNIQUE(source, source_listing_id).

## Run
```bash
cp .env.example .env        # then edit passwords; generate AIRFLOW_FERNET_KEY (command in the file)
docker compose up -d --build
```
1. Airflow UI: http://localhost:8080 (user/password from `.env`), unpause and trigger `car_listings_pipeline`.
2. Dashboard: http://localhost:8501
3. Postgres from host: `localhost:5434` (db `cars`).

Re-running the DAG does not duplicate rows (UPSERT).
Standalone scraper test: `cd dags && pip install -r ../requirements.txt && python -m scrapers.one2car`

## Layout
`dags/` DAG + `scrapers/` modules, `web/` Streamlit app, `initdb/` creates the `cars` DB,
`docker-compose.yml`, `Dockerfile.airflow`, `.env.example`.

## Notes
One2car is behind Cloudflare, which returns 403 to the TLS fingerprint of Python `requests`;
`curl_cffi` (an ordinary HTTP client with a Chrome TLS fingerprint, not browser automation) is used instead.
