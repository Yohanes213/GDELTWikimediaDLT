from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, StringType

# GDELT schema definition
gdelt_schema = StructType([
    StructField("Global_Event_ID", LongType()),          # _c0 (e.g., 1250384900)
    StructField("SQL_Date", IntegerType()),             # _c1 (e.g., 20240705)
    StructField("Month_Year", IntegerType()),           # _c2 (e.g., 202407)
    StructField("Year", IntegerType()),                # _c3 (e.g., 2024)
    StructField("Fraction_Date", DoubleType()),         # _c4 (e.g., 2024.5068)
    # Actor1 Metadata (columns 5-15)
    StructField("Actor1_Code", StringType()),           # _c5 (e.g., CHN, LEG)
    StructField("Actor1_Name", StringType()),           # _c6 (e.g., CHINA, LAWMAKER)
    StructField("Actor1_Country_Code", StringType()),    # _c7 (e.g., CHN)
    StructField("Actor1_Known_Group_Code", StringType()), # _c8
    StructField("Actor1_Ethnic_Code", StringType()),     # _c9
    StructField("Actor1_Religion1_Code", StringType()),  # _c10
    StructField("Actor1_Religion2_Code", StringType()),  # _c11
    StructField("Actor1_Religion3_Code", StringType()),  # _c12
    StructField("Actor1_Type1_Code", StringType()),      # _c13
    StructField("Actor1_Type2_Code", StringType()),      # _c14
    StructField("Actor1_Type3_Code", StringType()),      # _c15
    # Actor2 Metadata (columns 16-24)
    StructField("Actor2_Code", StringType()),           # _c16 (e.g., CHN, LEG)
    StructField("Actor2_Name", StringType()),           # _c17 (e.g., CHINA, LAWMAKER)
    StructField("Actor2_Known_Group_Code", StringType()), # _c18
    StructField("Actor2_Ethnic_Code", StringType()),     # _c19
    StructField("Actor2_Religion1_Code", StringType()),  # _c20
    StructField("Actor2_Religion2_Code", StringType()),  # _c21
    StructField("Actor2_Religion3_Code", StringType()),  # _c22
    StructField("Actor2_Type1_Code", StringType()),      # _c23
    StructField("Actor2_Type2_Code", StringType()),      # _c24
    # Event Attributes (columns 25-34)
    StructField("Is_Root_Event", IntegerType()),         # _c25 (1 or 0)
    StructField("Event_Code", StringType()),            # _c26 (e.g., 057, 014, 120)
    StructField("Event_Base_Code", StringType()),        # _c27 (e.g., 057, 014, 120)
    StructField("Event_Root_Code", StringType()),        # _c28 (e.g., 05, 01, 012)
    StructField("Quad_Class", IntegerType()),           # _c29 (1-4)
    StructField("Goldstein_Scale", DoubleType()),       # _c30 (e.g., 8.0, -4.0)
    StructField("Num_Mentions", IntegerType()),         # _c31 (e.g., 8, 3)
    StructField("Num_Sources", IntegerType()),          # _c32 (e.g., 1, 2)
    StructField("Num_Articles", IntegerType()),         # _c33 (e.g., 8, 2)
    StructField("Avg_Tone", DoubleType()),              # _c34 (e.g., 4.189, -1.698)
    # Actor1 Geography (columns 35-42)
    StructField("Actor1_Geo_Type", IntegerType()),      # _c35 (0-5)
    StructField("Actor1_Geo_FullName", StringType()),   # _c36
    StructField("Actor1_Geo_Country_Code", StringType()),# _c37
    StructField("Actor1_Geo_ADM1_Code", StringType()),   # _c38 (e.g., CH30, USMD)
    StructField("Actor1_Geo_Geonames_ID", StringType()),# _c39
    StructField("Actor1_Geo_Lat", DoubleType()),        # _c40
    StructField("Actor1_Geo_Long", DoubleType()),       # _c41
    StructField("Actor1_Geo_Feature_ID", LongType()),    # _c42 (e.g., -1907161)
    # Actor2 Geography (columns 43-50)
    StructField("Actor2_Geo_Type", IntegerType()),      # _c43 (0-5)
    StructField("Actor2_Geo_FullName", StringType()),   # _c44
    StructField("Actor2_Geo_Country_Code", StringType()),# _c45
    StructField("Actor2_Geo_ADM1_Code", StringType()),   # _c46
    StructField("Actor2_Geo_Geonames_ID", StringType()),# _c47
    StructField("Actor2_Geo_Lat", DoubleType()),        # _c48
    StructField("Actor2_Geo_Long", DoubleType()),       # _c49
    StructField("Actor2_Geo_Feature_ID", LongType()),    # _c50
    # Action Geography (columns 51-58)
    StructField("Action_Geo_Type", IntegerType()),      # _c51 (0-5)
    StructField("Action_Geo_FullName", StringType()),   # _c52
    StructField("Action_Geo_Country_Code", StringType()),# _c53
    StructField("Action_Geo_ADM1_Code", StringType()),   # _c54
    StructField("Action_Geo_Geonames_ID", StringType()),# _c55
    StructField("Action_Geo_Lat", DoubleType()),        # _c56
    StructField("Action_Geo_Long", DoubleType()),       # _c57
    StructField("Action_Geo_Feature_ID", LongType()),    # _c58 (e.g., -1907161)
    # Metadata (columns 59-60)
    StructField("Date_Added", LongType()),              # _c59 (e.g., 20250705111500)
    StructField("Source_URL", StringType())             # _c60
])