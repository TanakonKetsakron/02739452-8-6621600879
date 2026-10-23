# Used Honda Civic Listings in Thailand - Data Pipeline

## หัวข้อ (Topic)
วิเคราะห์ราคารถ Honda Civic มือสองในประเทศไทย โดยดึงประกาศขายจาก 3 เว็บไซต์ด้วย Apache Airflow
รวมและกำจัดประกาศซ้ำข้ามเว็บ ตรวจคุณภาพข้อมูล แล้วเก็บลง PostgreSQL และแสดงผลบน Streamlit Dashboard

## แหล่งข้อมูล (Sources)
| # | Source | URL | วิธีดึงข้อมูล |
|---|--------|-----|---------------|
| 1 | One2car | https://www.one2car.com (honda/civic) | HTTP client (`curl_cffi` ใช้ TLS fingerprint แบบ Chrome เพราะ Cloudflare ตอบ 403 ให้ `requests`) + BeautifulSoup อ่าน `data-*` และ JSON-LD, แบ่งหน้า `?page_number=N` |
| 2 | Carsome | https://www.carsome.co.th (buy-car/honda/civic) | HTTP + BeautifulSoup (HTML ฝั่งเซิร์ฟเวอร์) ประกาศทั้งเว็บมีประมาณ 21 รายการ |
| 3 | Taladrod | https://www.taladrod.com | HTTP + อ่าน JSON ที่ฝังในหน้า list (`SchDataJSON`) ได้ ~460 รายการ, ดึงหน้า detail เสริมสูงสุด 20 รายการ (หน่วง 4 วินาที) เพื่อไม่รบกวนเซิร์ฟเวอร์ |

- ไม่ใช้ Selenium / Playwright
- เช็ค `robots.txt` ก่อนดึงทุกครั้ง (ถ้าไม่อนุญาต scraper จะ raise error) หน่วงเวลาระหว่างคำขอ ตั้ง timeout
- **PDPA:** เก็บเฉพาะข้อมูลรถ ไม่อ่านและไม่เก็บชื่อผู้ขาย เบอร์โทร LINE ID หรือลิงก์โปรไฟล์ผู้ขาย
- ไม่มี secret ใน repo: รหัสผ่านทั้งหมดอยู่ใน `.env` (ถูก gitignore) มีเพียง `.env.example`

## สถาปัตยกรรม (Architecture)
```
 TaskGroup "extract"  (รันขนาน 3 task, retries=1)
 +-----------------+  +----------------+  +------------------+
 | extract_one2car |  | extract_carsome|  | extract_taladrod |   -> /opt/airflow/data/raw_<source>.json
 +--------+--------+  +--------+-------+  +---------+--------+
          \                  |                     /
           v                 v                    v
        merge  (trigger_rule=ALL_DONE: แหล่งที่ล้มเหลวถูกข้าม, ที่เหลือยังถูก merge;
                raise เฉพาะเมื่อล้มทั้ง 3 แหล่ง)
           |
        transform  (clean + schema เดียวกัน + cross-source dedup)
           |
        quality_check  (@task.branch)
          |  ผ่าน                          |  ไม่ผ่าน
          v                                v
        load (UPSERT car_listings)     quarantine (car_listings_quarantine + เหตุผล,
          |                                         ไม่แตะตารางหลัก)
          +----------------+---------------+
                           v
                       log_run  (1 แถวใน pipeline_runs)
                           |
 PostgreSQL (db `cars`) --> Streamlit :8501  (อ่านจาก PostgreSQL โดยตรง)
```

### ตาราง
- `car_listings`: id, source, source_listing_id, title, brand, model, variant, year, mileage_km,
  price_thb, transmission, fuel, province, url, scraped_at, also_listed_on,
  `UNIQUE(source, source_listing_id)` -> UPSERT (`ON CONFLICT DO UPDATE`) รันซ้ำแล้วจำนวนแถวไม่เพิ่ม
- `car_listings_quarantine`: แถวที่ไม่ผ่านการตรวจคุณภาพ พร้อม run_id และ reason
- `pipeline_runs`: run_id, started_at, finished_at, sources_ok[], sources_failed[], rows_extracted (jsonb),
  rows_after_dedup, dupes_removed, quality_passed, quality_reason, branch_taken, rows_loaded

### Cross-source dedup
รถคันเดียวกันที่ลงหลายเว็บ ถือว่าซ้ำเมื่อ model เดียวกัน, ปีเดียวกัน, ราคาต่างกัน <= 5,000 บาท,
เลขไมล์ต่างกัน <= 3,000 กม. และคำบอกรุ่นย่อย (trim) เข้ากันได้ (normalize เป็นตัวพิมพ์เล็ก/ตัดขนาดเครื่องยนต์)
ถ้าฝั่งใดไม่มีเลขไมล์ จะใช้เกณฑ์เข้มกว่า (ราคาเท่ากันเป๊ะและ trim เหมือนกัน) และเทียบเฉพาะกับเว็บอื่นเท่านั้น
เก็บ 1 แถวที่มี field ครบที่สุด และบันทึกเว็บอื่นที่ลงซ้ำไว้ในคอลัมน์ `also_listed_on`

### Quality check (branch)
ผ่านเมื่อ: มีอย่างน้อย 1 แหล่งสำเร็จ, จำนวนแถว >= `min_rows` (default 100), สัดส่วนราคาว่าง < 5%,
ปีอยู่ในช่วง 1990..ปีปัจจุบัน+1, ไม่มีเลขไมล์ติดลบ ผ่าน -> `load`, ไม่ผ่าน -> `quarantine`

## วิธีรัน (How to run)
```bash
cp .env.example .env      # แก้รหัสผ่าน และสร้าง AIRFLOW_FERNET_KEY ตามคำสั่งในไฟล์
docker compose up -d --build
```
1. เปิด Airflow http://localhost:8080 (user/password จาก `.env`: `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`)
2. DAG `car_listings_pipeline` ถูกตั้งให้ไม่ pause ตั้งแต่สร้าง กด Trigger (ใช้เวลาประมาณ 4 นาที ส่วนใหญ่คือ Taladrod)
3. เปิด Dashboard http://localhost:8501 (แท็บ **Market** และ **Pipeline status**)
4. PostgreSQL จากเครื่อง host: `localhost:5434` (db `cars`)

`docker-compose` มีขั้นตอน `airflow-init` ที่ chown โฟลเดอร์ `logs/` และ `data/` ให้ `AIRFLOW_UID` อัตโนมัติ (แก้ปัญหา permission บน Linux)

## สาธิต failure path และ quality branch
Trigger DAG w/ config (Airflow UI: Trigger DAG w/ config) หรือ CLI:
```bash
# 1) บังคับให้ source หนึ่งล้ม: task extract_carsome จะ failed แต่ merge/transform/load ยังสำเร็จด้วย 2 แหล่งที่เหลือ
docker compose exec airflow-scheduler airflow dags trigger car_listings_pipeline -c '{"fail_source":"carsome"}'
# 2) บังคับให้ quality check ไม่ผ่าน: branch ไป quarantine, ตาราง car_listings ไม่ถูกแก้
docker compose exec airflow-scheduler airflow dags trigger car_listings_pipeline -c '{"min_rows":100000}'
```
ดูผลที่ Grid/Graph view และแท็บ "Pipeline status" บน Dashboard
(DAG run จะเป็น success เพราะ task ที่ล้มไม่ใช่ leaf แต่ task `extract_*` ที่ล้มเป็นสีแดงใน Grid view)

## Checklist ตามระดับ
- [x] Level 1: 1 แหล่ง, DAG extract -> transform -> load, UPSERT, dashboard อ่าน PostgreSQL
- [x] Level 2: 3 extract task รันขนานใน `TaskGroup`, schema รวมเดียว, UPSERT รันซ้ำแถวไม่เพิ่ม, Dashboard มี KPI cards + filters
- [x] Level 3: 3 แหล่ง, แหล่งใดล้ม DAG ยัง merge/load ที่เหลือ, cross-source dedup, quality check แบบ branch (`@task.branch`)
- [x] Bonus: หน้า Pipeline status บนเว็บ (อ่านตาราง `pipeline_runs`)
- [x] ไม่ใช้ Selenium/Playwright, เช็ค robots.txt, ไม่เก็บข้อมูลส่วนบุคคล, ข้อมูล >= 100 แถว, ไม่มี secret ใน repo

## โครงสร้างโฟลเดอร์
`dags/` DAG + `scrapers/` (one2car, carsome, taladrod, common), `web/` Streamlit, `initdb/` สร้าง DB `cars`,
`docker-compose.yml`, `Dockerfile.airflow`, `.env.example`, `.gitattributes`

ทดสอบ scraper เดี่ยว: `cd dags && pip install -r ../requirements.txt && python -m scrapers.one2car`
