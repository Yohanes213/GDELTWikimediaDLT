# Databricks notebook source
# MAGIC %pip install python-dotenv

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, coalesce, lit, from_unixtime, udtf, to_date
import os
import sys


project_root = os.path.abspath(os.path.join('..'))
sys.path.append(project_root)

from src.config import GDELT_PATH, WIKIMEDIA_PATH, storage_account_name, storage_account_key
from src.wikimedia_udtf import WikimediaStreamUDTF
from src.schemas.gdelt_schema import gdelt_schema
from src.schemas.wikimedia_schema import wiki_schema
from src.dq_rules.gdelt_dq import GDLET_DQ_RULES
from src.dq_rules.wiki_dq import WIKI_DQ_RULES

# COMMAND ----------

# Configure Spark for Azure Blob Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# from wikimedia_udtf import WikimediaUDTF

# # THIS MUST RUN before any dlt.table definitions:
# spark.udtf.register("wikimedia_udtf", WikimediaUDTF)
# print("✅ UDTF registered")

# COMMAND ----------

# GDELT Bronze: Auto Loader of CSVs with full 61-column schema
@dlt.table(
    name="gdelt_bronze",
    comment="Raw GDELT events loaded via Auto Loader (full schema)"
)
def gdelt_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("delimiter", "\t")
             .option("cloudFiles.schemaLocation", "/mnt/gdelt-raw/_schema")
             .schema(gdelt_schema)
             .load(GDELT_PATH)
    )

# GDELT Silver: Cleaned records with data quality checks
@dlt.table(
    name="gdelt_silver",
    comment="Records passing all quality checks"
)
@dlt.expect_all_or_drop(expectations=GDLET_DQ_RULES)
def gdelt_silver_clean():
    df = dlt.read_stream("gdelt_bronze")

    # Drop unnecessary columns
    df = df.drop("Actor1_Type2_Code", "Actor2_Type2_Code")
    
    # Define StringType columns for null handling
    string_columns = [
        "Actor1_Code", "Actor1_Name", "Actor1_Country_Code", "Actor1_Known_Group_Code",
        "Actor1_Ethnic_Code", "Actor1_Religion1_Code", "Actor1_Religion2_Code", 
        "Actor1_Religion3_Code", "Actor1_Type1_Code", "Actor1_Type3_Code",
        "Actor2_Code", "Actor2_Name", "Actor2_Known_Group_Code", "Actor2_Ethnic_Code",
        "Actor2_Religion1_Code", "Actor2_Religion2_Code", "Actor2_Religion3_Code",
        "Actor2_Type1_Code", "Event_Code", "Event_Base_Code", "Event_Root_Code",
        "Actor1_Geo_FullName", "Actor1_Geo_Country_Code", "Actor1_Geo_ADM1_Code", 
        "Actor1_Geo_Geonames_ID", "Actor2_Geo_FullName", "Actor2_Geo_Country_Code",
        "Actor2_Geo_ADM1_Code", "Actor2_Geo_Geonames_ID", "Action_Geo_FullName",
        "Action_Geo_Country_Code", "Action_Geo_ADM1_Code", "Action_Geo_Geonames_ID",
        "Source_URL"
    ]
    
    # Apply null handling for StringType columns
    for col_name in string_columns:
        df = df.withColumn(col_name, coalesce(col(col_name), lit("Unknown")))

    df = df.withColumn(
        "event_timestamp",
        to_date(col("SQL_Date").cast("string"), "yyyyMMdd")
        )
    return df

# COMMAND ----------

# Wikimedia Bronze: Raw JSON payloads from Wikimedia recent-change stream
# @dlt.table(
#     name="wikimedia_bronze",
#     comment="Raw JSON payloads from Wikimedia recent-change stream"
# )
# def wikimedia_bronze():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.schemaLocation", "/mnt/wikimedia-raw/_schema")
#         .schema(wiki_schema)
#         .load(WIKIMEDIA_PATH)
#     )


# COMMAND ----------

@dlt.table(
    name="wikimedia_bronze",
    comment="Raw Wikimedia recent changes data streamed directly from HTTP source."
)
def wikimedia_raw_bronze():
    """
    Calls the DEBUG version of the UDTF to force an error or success.
    """
    return WikimediaStreamUDTF()



# Wikimedia Silver: Cleaned and flattened Wikimedia events
@dlt.table(
    name="wikimedia_silver",
    comment="Cleaned and flattened Wikimedia events"
)
@dlt.expect_all_or_drop(expectations=WIKI_DQ_RULES)
def wikimedia_silver():
    bronze = dlt.read_stream("wikimedia_bronze")

    # Drop rescued data
    bronze = bronze.drop("_rescued_data")

    # Filter for English Wikipedia
    bronze = bronze.filter(col("wiki") == "enwiki")

    # Extract nested fields
    df = (bronze
          .withColumn("length_new", col("length.new"))
          .withColumn("length_old", col("length.old"))
          .withColumn("revision_new", col("revision.new"))
          .withColumn("revision_old", col("revision.old"))
          .withColumn("meta_domain", col("meta.domain"))
          .withColumn("meta_dt", col("meta.dt"))
          .withColumn("meta_offset", col("meta.offset"))
          .withColumn("meta_partition", col("meta.partition"))
          .withColumn("meta_request", col("meta.request_id"))
          .withColumn("meta_stream", col("meta.stream"))
          .withColumn("meta_topic", col("meta.topic"))
          .withColumn("meta_uri", col("meta.uri"))
     )

    # Drop original structs
    df = df.drop("length", "revision", "meta")

    # List string columns for null handling
    string_columns = [
        "comment", "log_action", "log_action_comment", "log_params", "log_type",
        "notify_url", "parsedcomment", "server_name", "server_script_path",
        "server_url", "title", "title_url", "type", "user", "wiki",
        "meta_domain", "meta_dt", "meta_request", "meta_stream",
        "meta_topic", "meta_uri"
    ]

    # Coalesce NULL to "Unknown" for string columns
    for c in string_columns:
        df = df.withColumn(c, coalesce(col(c), lit("Unknown")))

    df = df.withColumn(
        "event_timestamp",
        to_date(col("SQL_Date").cast("string"), "yyyyMMdd")
        )


    return df

# COMMAND ----------