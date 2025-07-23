# app.py
import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

# ——————————————————————————————————————————
# Helpers
# ——————————————————————————————————————————

# Place this somewhere in your app.py file, after the st.title line.

# st.markdown("---")
# st.subheader("Authentication Debugger")

#if st.button("Check My Identity"):
#     try:
#        # This SQL command asks Databricks "who am I?"
#        identity_df = sqlQuery("SELECT current_user()")
        
        # Extract the identity from the DataFrame
#        current_identity = identity_df.iloc[0, 0]
        
#        st.success(f"The application is authenticating as: **{current_identity}**")
#        st.info("This is the user or service principal you need to grant permissions to.")
        
#    except Exception as e:
#        st.error(f"Failed to check identity: {e}")

def sql_query(query: str) -> pd.DataFrame:
    """Run a SQL query against the configured Databricks SQL warehouse."""
    cfg = Config()  # reads DATABRICKS_HOST, DATABRICKS_TOKEN from env
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()

@st.cache_data(ttl=60)
def load_countries() -> list[str]:
    df = sql_query("""
        SELECT DISTINCT Action_Geo_Fullname AS country
        FROM gdeltcatalog.gdeltdlt.gdelt_silver
        ORDER BY country
    """)
    return df["country"].dropna().tolist()

@st.cache_data(ttl=60)
def load_gdelt_events(country: str, start: datetime, end: datetime) -> pd.DataFrame:
    q = f"""
      SELECT CAST(event_timestamp AS DATE) AS date, COUNT(*) AS event_count
      FROM gdeltcatalog.gdeltdlt.gdelt_silver
      WHERE Action_Geo_FullName = '{country}'
        AND CAST(event_timestamp AS DATE) BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'
      GROUP BY date
      ORDER BY date
    """
    return sql_query(q)

@st.cache_data(ttl=60)
def load_wikimedia_edits(country: str, start: datetime, end: datetime) -> pd.DataFrame:
    q = f"""
      SELECT CAST(event_timestamp AS DATE) AS date, COUNT(*) AS edit_count
      FROM gdeltcatalog.gdeltdlt.wikimedia_silver
      WHERE wiki = 'enwiki'
        AND title LIKE '%{country}%'
        AND CAST(event_timestamp AS DATE) BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'
      GROUP BY date
      ORDER BY date
    """
    return sql_query(q)


# ——————————————————————————————————————————
# Streamlit UI
# ——————————————————————————————————————————

st.set_page_config(layout="wide")
st.title("🌎 Global Events & Wikipedia Activity Explorer")

with st.sidebar:
    st.header("Filters")
    today = datetime.today().date()
    start_date = st.date_input("Start date", value=today.replace(day=1))
    end_date   = st.date_input("End date",   value=today)
    country_list = load_countries()
    country = st.selectbox("Country", country_list or ["— no countries —"])

# Fetch data
gdelt_df = load_gdelt_events(country, start_date, end_date) if country_list else pd.DataFrame()
wiki_df  = load_wikimedia_edits(country, start_date, end_date) if country_list else pd.DataFrame()

# Summary metrics
st.subheader("Summary Metrics")
c1, c2 = st.columns(2)
c1.metric("Total GDELT Events", f"{int(gdelt_df['event_count'].sum()) if not gdelt_df.empty else 0}")
c2.metric("Total Wikipedia Edits", f"{int(wiki_df ['edit_count'].sum()) if not wiki_df.empty  else 0}")

# Timelines
st.subheader("Timelines")
g1, g2 = st.columns(2)

with g1:
    st.markdown("**GDELT Events Over Time**")
    if not gdelt_df.empty:
        fig1 = px.line(gdelt_df, x="date", y="event_count", title="", labels={"event_count":"Events"})
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("No GDELT events in this range.")

with g2:
    st.markdown("**Wikipedia Edits Over Time**")
    if not wiki_df.empty:
        fig2 = px.line(wiki_df, x="date", y="edit_count", title="", labels={"edit_count":"Edits"})
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No Wikipedia edits in this range.")
