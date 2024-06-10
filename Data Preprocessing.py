# Databricks notebook source
import json

config_file_path = "config.json"

# COMMAND ----------

with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)
    shared_catalog_name = config['shared-catalog']
    shared_schema_name = config['shared-schema']
    custom_catalog_name = config['custom-catalog']
    custom_schema_name = config['custom-schema']

# COMMAND ----------

custom_schema_name

# COMMAND ----------

property_df = spark.read.table(f"{shared_catalog_name}.{shared_schema_name}.us_listings_daily")

property_df.write.mode("overwrite").saveAsTable(f"{custom_catalog_name}.{custom_schema_name}.us_listings_daily_pre_processed")

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

# COMMAND ----------


school_rating_schema = ArrayType(StructType([
    StructField("distance", StringType()),
    StructField("name", StringType()),
    StructField("rating", IntegerType()),
    StructField("level", StringType()),
    StructField("studentsPerTeacher", StringType()),
    StructField("assigned", StringType()),
    StructField("grades", StringType()),
    StructField("link", StringType()),
    StructField("type", StringType()),
    StructField("size", StringType()),
    StructField("totalCount", StringType()),
    StructField("isAssigned", StringType()),
]))


window_spec = Window.partitionBy('pid').orderBy(f.desc('date'))

# COMMAND ----------

property_pre_processed_df = spark.read.table(f"{custom_catalog_name}.{custom_schema_name}.us_listings_daily_pre_processed")

selected_property_df = property_pre_processed_df.select('pid', 'address', 'baths', 'beds', 
                                          'home_type', 'sqft', 'price', 'rent', 
                                          'city', 'state', 'zip', 
                                          'postingIsRental', 'description', 'great_schools_rating', 'date') 

filtered_property_df = selected_property_df.filter(f.col('price').isNotNull())\
                        .withColumn('date', f.col('date').cast('date'))\
                        .withColumn('row_number', f.row_number().over(window_spec))\
                        .filter(f.col('row_number') == 1)\
                            .drop('row_number')


ratings_df = filtered_property_df.withColumn("parsed_rating", f.from_json("great_schools_rating", school_rating_schema))

# Explode the array to create a row for each element and include the 'pid' for joining
df_exploded = ratings_df.selectExpr("pid", "explode(parsed_rating) as schools")

# Find the maximum rating for each 'pid'
max_rating_df = df_exploded.groupBy("pid").agg(f.max("schools.rating").alias("max_rating"))


# Find the minimum distance for each 'pid'
min_distance_df = df_exploded.groupBy("pid").agg(f.min("schools.distance").alias("min_distance"))

# Join back to the original ratings_df to include the minimum distance
# Join back to the original ratings_df to include the maximum rating
df_joined = ratings_df.join(max_rating_df, "pid")\
    .join(min_distance_df, "pid")\
        .drop('parsed_rating')

df_joined.write.mode("overwrite").saveAsTable(f"{custom_catalog_name}.{custom_schema_name}.us_listings_daily_processed")
