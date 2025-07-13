# GDELT data quality rules
GDLET_DQ_RULES = {
    "valid_global_event_id": "Global_Event_ID IS NOT NULL",
    "valid_event_code": "Event_Code IS NOT NULL",
    "valid_sql_date": "SQL_Date >= 19000101 AND SQL_Date <= 99991231",
    "valid_quad_class": "Quad_Class IS NULL OR Quad_Class BETWEEN 1 AND 4",
    "valid_goldstein_scale": "Goldstein_Scale IS NULL OR Goldstein_Scale BETWEEN -10.0 AND 10.0",
    "valid_is_root_event": "Is_Root_Event IN (0, 1)",
    "valid_actor1_geo_lat": "Actor1_Geo_Lat IS NULL OR Actor1_Geo_Lat BETWEEN -90.0 AND 90.0",
    "valid_actor1_geo_long": "Actor1_Geo_Long IS NULL OR Actor1_Geo_Long BETWEEN -180.0 AND 180.0",
    "valid_actor2_geo_lat": "Actor2_Geo_Lat IS NULL OR Actor2_Geo_Lat BETWEEN -90.0 AND 90.0",
    "valid_actor2_geo_long": "Actor2_Geo_Long IS NULL OR Actor2_Geo_Long BETWEEN -180.0 AND 180.0",
    "valid_action_geo_lat": "Action_Geo_Lat IS NULL OR Action_Geo_Lat BETWEEN -90.0 AND 90.0",
    "valid_action_geo_long": "Action_Geo_Long IS NULL OR Action_Geo_Long BETWEEN -180.0 AND 180.0",
    "valid_num_mentions": "Num_Mentions IS NULL OR Num_Mentions >= 0",
    "valid_num_sources": "Num_Sources IS NULL OR Num_Sources >= 0",
    "valid_num_articles": "Num_Articles IS NULL OR Num_Articles >= 0",
    "valid_actor1_geo_type": "Actor1_Geo_Type IS NULL OR Actor1_Geo_Type BETWEEN 0 AND 5",
    "valid_actor2_geo_type": "Actor2_Geo_Type IS NULL OR Actor2_Geo_Type BETWEEN 0 AND 5",
    "valid_action_geo_type": "Action_Geo_Type IS NULL OR Action_Geo_Type BETWEEN 0 AND 5"
}