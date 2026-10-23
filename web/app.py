import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

st.set_page_config(page_title="Used Honda Civic - Thailand", layout="wide")


@st.cache_resource
def engine():
    return create_engine(URL.create(
        "postgresql+psycopg2",
        username=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "postgres"), port=int(os.environ.get("DB_PORT", 5432)),
        database=os.environ["DB_NAME"]))


@st.cache_data(ttl=30)
def load_listings() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT source, title, model, variant, year, mileage_km, price_thb, transmission, "
        "fuel, province, also_listed_on, url, scraped_at FROM car_listings", engine())


@st.cache_data(ttl=30)
def load_runs() -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM pipeline_runs ORDER BY started_at DESC", engine())


st.title("Used Honda Civic listings - Thailand")
tab_market, tab_status = st.tabs(["Market", "Pipeline status"])

# ------------------------------------------------------------------ Market
with tab_market:
    try:
        df = load_listings()
    except Exception as e:  # table not created yet (DAG not run)
        st.info("No data yet. Trigger the DAG `car_listings_pipeline` in Airflow (port 8080).")
        st.caption(str(e).splitlines()[0])
        df = None
    if df is not None and df.empty:
        st.info("The table is empty. Trigger the DAG `car_listings_pipeline` in Airflow.")
    elif df is not None:
        with st.sidebar:
            st.header("Filters")
            sources = st.multiselect("Source", sorted(df.source.unique()),
                                     default=sorted(df.source.unique()))
            years = df.year.dropna().astype(int)
            yr = st.slider("Year", int(years.min()), int(years.max()),
                           (int(years.min()), int(years.max()))) if len(years) else None
            pmin, pmax = int(df.price_thb.min()), int(df.price_thb.max())
            pr = st.slider("Price (THB)", pmin, pmax, (pmin, pmax), step=10_000)
            provinces = st.multiselect("Province", sorted(df.province.dropna().unique()))
            trans = st.multiselect("Transmission", sorted(df.transmission.dropna().unique()))

        f = df[df.source.isin(sources) & df.price_thb.between(*pr)]
        if yr:
            f = f[f.year.between(*yr) | f.year.isna()]
        if provinces:
            f = f[f.province.isin(provinces)]
        if trans:
            f = f[f.transmission.isin(trans)]

        if f.empty:
            st.warning("No listings match the filters.")
        else:
            k = st.columns(5)
            k[0].metric("Total listings", f"{len(f):,}")
            k[1].metric("Avg price (THB)", f"{f.price_thb.mean():,.0f}")
            k[2].metric("Median price (THB)", f"{f.price_thb.median():,.0f}")
            k[3].metric("Sources", f.source.nunique())
            k[4].metric("Newest scrape", f.scraped_at.max().strftime("%Y-%m-%d %H:%M"))

            a, b = st.columns(2)
            by_year = f.dropna(subset=["year"]).groupby("year", as_index=False).price_thb.mean()
            a.subheader("Average price by year")
            a.plotly_chart(px.bar(by_year, x="year", y="price_thb",
                                  labels={"price_thb": "Avg price (THB)", "year": "Year"}),
                           use_container_width=True)
            per_src = f.groupby("source", as_index=False).size().rename(columns={"size": "listings"})
            b.subheader("Listings per source")
            b.plotly_chart(px.bar(per_src, x="source", y="listings", color="source"),
                           use_container_width=True)

            st.subheader("Price vs mileage")
            sc = f.dropna(subset=["mileage_km"])
            st.plotly_chart(px.scatter(sc, x="mileage_km", y="price_thb", color="source",
                                       hover_data=["title", "year"],
                                       labels={"mileage_km": "Mileage (km)",
                                               "price_thb": "Price (THB)"}),
                            use_container_width=True)

            st.subheader("Listings")
            st.dataframe(f, use_container_width=True, hide_index=True,
                         column_config={"url": st.column_config.LinkColumn("url")})

# --------------------------------------------------------- Pipeline status
with tab_status:
    try:
        runs = load_runs()
    except Exception as e:
        st.info("No pipeline runs recorded yet.")
        st.caption(str(e).splitlines()[0])
        runs = None
    if runs is not None and runs.empty:
        st.info("No pipeline runs recorded yet.")
    elif runs is not None:
        last = runs.iloc[0]
        st.subheader("Latest run")
        st.caption(f"run_id: {last.run_id}")
        c = st.columns(5)
        c[0].metric("Branch taken", last.branch_taken)
        c[1].metric("Quality check", "PASSED" if last.quality_passed else "FAILED")
        c[2].metric("Rows after dedup", int(last.rows_after_dedup))
        c[3].metric("Duplicates removed", int(last.dupes_removed))
        c[4].metric("Rows loaded", int(last.rows_loaded))
        st.write(f"**Sources OK:** {', '.join(last.sources_ok) or '-'}")
        failed = ", ".join(last.sources_failed) if last.sources_failed else "none"
        (st.error if last.sources_failed else st.success)(f"Sources failed: {failed}")
        st.write(f"**Quality result:** {last.quality_reason}")
        st.write("**Rows extracted per source:**", dict(last.rows_extracted))

        st.subheader("Run history")
        hist = runs.copy()
        hist["sources_ok"] = hist.sources_ok.apply(", ".join)
        hist["sources_failed"] = hist.sources_failed.apply(", ".join)
        hist["rows_extracted"] = hist.rows_extracted.astype(str)
        st.dataframe(hist, use_container_width=True, hide_index=True)
