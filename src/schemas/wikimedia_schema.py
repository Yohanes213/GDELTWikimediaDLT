from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType

# Define nested structs for Wikimedia schema
length_type = StructType([
    StructField("new", LongType(), True),
    StructField("old", LongType(), True),
])
revision_type = StructType([
    StructField("new", LongType(), True),
    StructField("old", LongType(), True),
])
meta_type = StructType([
    StructField("domain", StringType(), True),
    StructField("dt", StringType(), True),
    StructField("id", StringType(), True),
    StructField("offset", LongType(), True),
    StructField("partition", LongType(), True),
    StructField("request_id", StringType(), True),
    StructField("stream", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("uri", StringType(), True),
])

# Wikimedia schema definition
wiki_schema = StructType([
    StructField("$schema", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("comment", StringType(), True),
    StructField("id", LongType(), True),
    StructField("length", length_type, True),
    StructField("log_action", StringType(), True),
    StructField("log_action_comment", StringType(), True),
    StructField("log_id", LongType(), True),
    StructField("log_params", StringType(), True),
    StructField("log_type", StringType(), True),
    StructField("meta", meta_type, True),
    StructField("minor", BooleanType(), True),
    StructField("namespace", LongType(), True),
    StructField("notify_url", StringType(), True),
    StructField("parsedcomment", StringType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("revision", revision_type, True),
    StructField("server_name", StringType(), True),
    StructField("server_script_path", StringType(), True),
    StructField("server_url", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("type", StringType(), True),
    StructField("user", StringType(), True),
    StructField("wiki", StringType(), True),
])