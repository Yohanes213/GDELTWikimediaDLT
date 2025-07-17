import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import pandas as pd
import plotly.express as px
import logging
import sys
import os
import subprocess
import glob

# ---- Java Discovery ----
# New candidate list based on your environment
java_candidates = [
    "/usr/lib/jvm/zulu11",
    "/usr/lib/jvm/zulu11-ca-amd64",
    "/usr/lib/jvm/zulu17",
    "/usr/lib/jvm/zulu17-ca-amd64",
    "/usr/lib/jvm/zulu8",
    "/usr/lib/jvm/zulu8-ca-amd64"
]

found_java = None
java_bin_path = None

for candidate in java_candidates:
    # Check for java binary in standard location
    candidate_bin = os.path.join(candidate, "bin", "java")
    if os.path.isfile(candidate_bin):
        found_java = candidate
        java_bin_path = candidate_bin
        break
    
    # Check for java binary in JRE location (zulu8 uses this)
    jre_candidate_bin = os.path.join(candidate, "jre", "bin", "java")
    if os.path.isfile(jre_candidate_bin):
        found_java = candidate
        java_bin_path = jre_candidate_bin
        break

if found_java:
    os.environ["JAVA_HOME"] = found_java
    
    # Add both possible bin locations to PATH
    bin_paths = [
        os.path.join(found_java, "bin"),
        os.path.join(found_java, "jre", "bin")
    ]
    existing_path = os.environ.get("PATH", "")
    new_path = ":".join([p for p in bin_paths if os.path.isdir(p)] + [existing_path])
    os.environ["PATH"] = new_path
    
    st.write(f"### Found JAVA_HOME at: {found_java}")
    st.write(f"### Updated PATH: {new_path}")
    
    # Verify Java using the found binary path
    try:
        java_version = subprocess.check_output(
            [java_bin_path, "-version"],
            stderr=subprocess.STDOUT,
            text=True
        )
        st.write(f"Java Version:\n{java_version}")
    except Exception as e:
        st.error(f"Java verification failed: {str(e)}")
        st.stop()
else:
    st.error("Java not found in common locations. Searched paths:")
    st.write(java_candidates)
    st.stop()

# ---- Spark Initialization ----
try:
    spark = SparkSession.builder \
        .appName("GlobalEventsApp") \
        .config("spark.driver.extraJavaOptions", f"-Djava.home={found_java}") \
        .config("spark.executor.extraJavaOptions", f"-Djava.home={found_java}") \
        .getOrCreate()
    
    st.write(f"Spark version: {spark.version}")
except Exception as e:
    st.error(f"Spark initialization failed: {str(e)}")
    st.stop()


# Debug environment
st.write("### Environment Debug")
st.write(f"JAVA_HOME: {os.environ['JAVA_HOME']}")
st.write(f"PATH: {os.environ['PATH']}")

# Verify Java installation
java_bin = f"{os.environ['JAVA_HOME']}/bin/java"
if os.path.isfile(java_bin):
    try:
        java_version = subprocess.run(
            [java_bin, "-version"],
            capture_output=True,
            text=True,
            check=True
        )
        st.write(f"Java Version:\n{java_version.stderr}")
    except subprocess.CalledProcessError as e:
        st.error(f"Java error: {str(e)}")
else:
    st.error(f"Java binary not found at {java_bin}")

# Initialize Spark session
try:
    
    st.write(f"Spark version: {spark.version}")
    # logger.info(f"Spark session initialized successfully. Spark version: {spark.version}")
except Exception as e:
    # logger.error(f"Failed to initialize Spark session: {str(e)}")
    st.error(f"Failed to initialize Spark session: {str(e)}")
    st.stop()

# Streamlit app
st.title("Global Events and Wikipedia Activity Explorer")

# UI components
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date")
end_date = st.sidebar.date_input("End Date")

# Fetch distinct countries with error handling
try:
    countries = (spark.table("gdelt_silver")
                 .select("ActionGeo_Fullname")
                 .distinct()
                 .toPandas()["ActionGeo_Fullname"]
                 .tolist())
    # logger.info(f"Successfully fetched {len(countries)} distinct countries")
except Exception as e:
    # logger.error(f"Error fetching countries: {str(e)}")
    st.error(f"Error fetching countries: {str(e)}")
    countries = []
country = st.sidebar.selectbox("Select Country", countries if countries else ["No countries available"])

# Query GDELT Silver table
try:
    gdelt_df = (spark.table("gdelt_silver")
                .filter((col("ActionGeo_Fullname") == country) & 
                        (col("timestamp").cast("date").between(start_date, end_date)))
                .groupBy(to_date(col("timestamp")).alias("date"))
                .count()
                .toPandas()
                .rename(columns={"count": "event_count"}))
    # logger.info(f"Successfully queried GDELT data: {len(gdelt_df)} rows")
except Exception as e:
    # logger.error(f"Error querying GDELT data: {str(e)}")
    st.error(f"Error querying GDELT data: {str(e)}")
    gdelt_df = pd.DataFrame()

# Query Wikimedia Silver table
try:
    wikimedia_df = (spark.table("wikimedia_silver")
                    .filter((col("wiki") == "enwiki") & 
                            (col("page_title").like(f"%{country}%")) & 
                            (col("timestamp").cast("date").between(start_date, end_date)))
                    .groupBy(to_date(col("timestamp")).alias("date"))
                    .count()
                    .toPandas()
                    .rename(columns={"count": "edit_count"}))
    # logger.info(f"Successfully queried Wikimedia data: {len(wikimedia_df)} rows")
except Exception as e:
    # logger.error(f"Error querying Wikimedia data: {str(e)}")
    st.error(f"Error querying Wikimedia data: {str(e)}")
    wikimedia_df = pd.DataFrame()

# Display metrics
st.subheader("Summary Metrics")
st.write(f"Total GDELT Events: {gdelt_df['event_count'].sum() if not gdelt_df.empty else 0}")
st.write(f"Total Wikipedia Edits: {wikimedia_df['edit_count'].sum() if not wikimedia_df.empty else 0}")

# Plot timelines
st.subheader("GDELT Events Timeline")
if not gdelt_df.empty:
    fig_gdelt = px.line(gdelt_df, x="date", y="event_count", title="GDELT Events")
    st.plotly_chart(fig_gdelt)
else:
    st.write("No GDELT events found for the selected filters.")

st.subheader("Wikipedia Edits Timeline")
if not wikimedia_df.empty:
    fig_wiki = px.line(wikimedia_df, x="date", y="edit_count", title="Wikipedia Edits")
    st.plotly_chart(fig_wiki)
else:
    st.write("No Wikipedia edits found for the selected filters.")