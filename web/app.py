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
def load() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT source, title, model, variant, year, mileage_km, price_thb, "
        "transmission, fuel, province, url, scraped_at FROM car_listings", engine())


st.title("Used Honda Civic listings - Thailand")
try:
    df = load()
except Exception as e:  # table not created yet (DAG not run)
    st.info("No data yet. Trigger the DAG `car_listings_pipeline` in Airflow (port 8080).")
    st.caption(str(e).splitlines()[0])
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Listings", len(df))
c2.metric("Avg price (THB)", f"{df.price_thb.mean():,.0f}")
c3.metric("Sources", df.source.nunique())

st.subheader("Average price by year")
by_year = df.dropna(subset=["year"]).groupby("year", as_index=False).price_thb.mean()
st.plotly_chart(px.bar(by_year, x="year", y="price_thb",
                       labels={"price_thb": "Avg price (THB)", "year": "Year"}),
                use_container_width=True)

st.subheader("Listings")
st.dataframe(df, use_container_width=True, hide_index=True,
             column_config={"url": st.column_config.LinkColumn("url")})
