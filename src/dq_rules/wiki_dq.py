# Wikimedia data quality rules
WIKI_DQ_RULES = {
    "valid_id": "id IS NOT NULL",
    "valid_timestamp": "timestamp > 0",
    "non_empty_title": "length(title) > 0",
}