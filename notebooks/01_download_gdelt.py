# Databricks notebook source
from dotenv import load_dotenv
import os
import sys
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

project_root = os.path.abspath(os.path.join('..'))
sys.path.append(project_root)

from src.download_and_upload_gdelt import download_and_upload_gdelt_data

# COMMAND ----------

# Load environment variables from .env file
load_dotenv()

# Read and validate required environment variables
container_name = os.getenv("GDLET_CONTAINER_NAME")
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("STORAGE_KEY")

# Ensure all required environment variables are set
required_vars = {
    "CONTAINER_NAME": container_name,
    "STORAGE_ACCOUNT_NAME": storage_account_name,
    "STORAGE_KEY": storage_account_key,
}

missing_vars = [var for var, value in required_vars.items() if value is None]

if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# Initialize Blob Storage client
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

if not container_client.exists():
    raise ValueError(f"The container '{container_name}' does not exist or the connection is invalid.")

print("Connection to Blob Storage is valid.")

# COMMAND ----------

current_time = datetime.utcnow()
gdelt_time = current_time - timedelta(minutes=current_time.minute % 15, seconds=current_time.second)
gdelt_url = f"http://data.gdeltproject.org/gdeltv2/{gdelt_time.strftime('%Y%m%d%H%M%S')}.export.CSV.zip"

# COMMAND ----------

download_and_upload_gdelt_data(gdelt_url, container_client)

# COMMAND ----------

df1 = spark.read.option("inferSchema", True).option("delimiter", "\t").csv(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/*.CSV")

# COMMAND ----------

display(df.limit(15))

# COMMAND ----------

from pyspark.sql.types import StructField, LongType, StringType, IntegerType, FloatType, DoubleType, TimestampType, BinaryType, BooleanType, NullType

# COMMAND ----------

schema = StructType([
        StructField("GLOBALEVENTID", LongType()),
        StructField("SQLDATE", IntegerType()),
        StructField("MonthYear", IntegerType()),
        StructField("Year", IntegerType()),
        StructField("FractionDate", DoubleType()),
        StructField("Actor1Code", StringType()),
        StructField("Actor1Name", StringType()),
        StructField("Actor1CountryCode", StringType()),
        StructField("Actor1KnownGroupCode", StringType()),
        StructField("Actor1EthnicCode", StringType()),
        StructField("Actor1Religion1Code", StringType()),
        StructField("Actor1Religion2Code", StringType()),
        StructField("Actor1Religion3Code", StringType()),
        StructField("Actor1Type1Code", StringType()),
        StructField("Actor1Type2Code", StringType()),
        StructField("Actor1Type3Code", StringType()),
        StructField("Actor2Code", StringType()),
        StructField("Actor2Name", StringType()),
        StructField("Actor2CountryCode", StringType()),
        StructField("Actor2KnownGroupCode", StringType()),
        StructField("Actor2EthnicCode", StringType()),
        StructField("Actor2Religion1Code", StringType()),
        StructField("Actor2Religion2Code", StringType()),
        StructField("Actor2Religion3Code", StringType()),
        StructField("Actor2Type1Code", StringType()),
        StructField("Actor2Type2Code", StringType()),
        StructField("Actor2Type3Code", StringType()),
        StructField("IsRootEvent", IntegerType()),
        StructField("EventCode", IntegerType()),
        StructField("EventBaseCode", StringType()),
        StructField("EventRootCode", StringType()),
        StructField("QuadClass", IntegerType()),
        StructField("GoldsteinScale", DoubleType()),
        StructField("NumMentions", IntegerType()),
        StructField("NumSources", IntegerType()),
        StructField("NumArticles", IntegerType()),
        StructField("AvgTone", DoubleType()),
        StructField("Actor1Geo_Type", IntegerType()),
        StructField("Actor1Geo_FullName", StringType()),
        StructField("Actor1Geo_CountryCode", StringType()),
        StructField("Actor1Geo_ADM1Code", IntegerType()),
        StructField("Actor1Geo_Lat", DoubleType()),
        StructField("Actor1Geo_Long", DoubleType()),
        StructField("Actor1Geo_FeatureID", IntegerType()),
        StructField("Actor2Geo_Type", IntegerType()),
        StructField("Actor2Geo_FullName", StringType()),
        StructField("Actor2Geo_CountryCode", StringType()),
        StructField("Actor2Geo_ADM1Code", IntegerType()),
        StructField("Actor2Geo_Lat", DoubleType()),
        StructField("Actor2Geo_Long", DoubleType()),
        StructField("Actor2Geo_FeatureID", IntegerType()),
        StructField("ActionGeo_Type", IntegerType()),
        StructField("ActionGeo_FullName", StringType()),
        StructField("ActionGeo_CountryCode", StringType()),
        StructField("ActionGeo_ADM1Code", IntegerType()),
        StructField("ActionGeo_Lat", DoubleType()),
        StructField("ActionGeo_Long", DoubleType()),
        StructField("ActionGeo_FeatureID", IntegerType()),
        StructField("EventTimeDate", TimestampType()),
        StructField("DATEADDED", TimestampType()),
        StructField("SOURCEURL", StringType())])


# COMMAND ----------

df = spark.read.option("inferSchema", True).option("delimiter", "\t").schema(schema).csv(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/*.CSV")

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

display(df.printSchema())

# COMMAND ----------

display(df.describe())

# COMMAND ----------

EventBaseCode
EventRootCode

# COMMAND ----------

from pyspark.sql.functions import to_date, lit, to_timestamp, col, desc, isnan, isnull, countDistinct, sum as _sum, when, count, unix_timestamp, year, month, dayofmonth, greatest, hour, dayofweek, coalesce, round, avg, rank, concat_ws, corr


# COMMAND ----------

null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
display(null_counts)

# COMMAND ----------

display(df.orderBy(rand()).limit(25))

# COMMAND ----------

df.count()

# COMMAND ----------

df1.count()

# COMMAND ----------

null_counts = df1.select([count(when(col(c).isNull(), c)).alias(c) for c in df1.columns])
display(null_counts)

# COMMAND ----------

null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
display(null_counts)

# COMMAND ----------

IsRootEvent
Actor2Geo_Type prev: 182, now 1301



# COMMAND ----------

num_columns = len(df.columns)
display(num_columns)

# COMMAND ----------

num_columns = len(df1.columns)
display(num_columns)

# COMMAND ----------

