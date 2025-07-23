import requests
import json
import dlt
from pyspark.sql.functions import udtf
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType
from src.schemas.wikimedia_schema import wiki_schema

MAX_EVENTS = 10

@udtf(
    returnType=wiki_schema
)
class WikimediaStreamUDTF:
    def __init__(self):
        self.event_count = 0

    def eval(self):
        url = "https://stream.wikimedia.org/v2/stream/recentchange"
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line and line.startswith(b"data:"):
                    raw = line[6:].decode("utf-8")
                    try:
                        event = json.loads(raw)

                        # Unpack nested fields
                        length = event.get("length", {})
                        revision = event.get("revision", {})
                        meta = event.get("meta", {})

                        # Yield a tuple matching wiki_schema fields:
                        yield (
                            event.get("$schema"),                # 1
                            event.get("bot"),                    # 2
                            event.get("comment"),                # 3
                            event.get("id"),                     # 4
                            # 5 length struct (new, old)
                            # (length.get("new"), length.get("old")),
                            length,
                            event.get("log_action"),             # 6
                            event.get("log_action_comment"),     # 7
                            event.get("log_id"),                 # 8
                            event.get("log_params"),             # 9
                            event.get("log_type"),               # 10
                            # 11 meta struct (9 fields)
                            # (
                            #     meta.get("domain"),
                            #     meta.get("dt"),
                            #     meta.get("id"),
                            #     meta.get("offset"),
                            #     meta.get("partition"),
                            #     meta.get("request_id"),
                            #     meta.get("stream"),
                            #     meta.get("topic"),
                            #     meta.get("uri")
                            # )
                            meta,
                            event.get("minor"),                  # 12
                            event.get("namespace"),              # 13
                            event.get("notify_url"),             # 14
                            event.get("parsedcomment"),          # 15
                            event.get("patrolled"),              # 16
                            # 17 revision struct (new, old)
                            # (revision.get("new"), revision.get("old")),
                            revision,
                            event.get("server_name"),            # 18
                            event.get("server_script_path"),     # 19
                            event.get("server_url"),             # 20
                            event.get("timestamp"),              # 21
                            event.get("title"),                  # 22
                            event.get("title_url"),              # 23
                            event.get("type"),                   # 24
                            event.get("user"),                   # 25
                            event.get("wiki")                    # 26
                        )
                    except Exception as e:
                        print(f"Error parsing event: {e}")
                    self.event_count += 1
                    if self.event_count >= MAX_EVENTS:
                        print(f"Reached {MAX_EVENTS} events; closing UDTF.")
                        break


# import requests
# import json
# import dlt
# from pyspark.sql.functions import udtf
# from pyspark.sql.types import StructType, StructField, StringType

# # 1. Define the UDTF that reads from the Wikimedia SSE endpoint
# @udtf(
#     returnType=StructType([
#         StructField("data", StringType(), True)
#     ])
# )
# class WikimediaStreamUDTF:
#     def __init__(self):
#         # No max count—runs indefinitely
#         self.url = "https://stream.wikimedia.org/v2/stream/recentchange"

#     def eval(self):
#         # Establish a streaming HTTP connection
#         with requests.get(self.url, stream=True, timeout=30) as response:
#             response.raise_for_status()
#             for line in response.iter_lines():
#                 # Skip heartbeat blank lines
#                 if not line or not line.startswith(b"data:"):
#                     continue
#                 # Extract JSON payload
#                 raw_json = line[len(b"data:"):].decode("utf-8")
#                 # Yield the entire JSON as a single string column
#                 yield (raw_json,)