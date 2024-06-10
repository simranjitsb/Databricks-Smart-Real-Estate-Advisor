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

property_pre_processed_df = spark.read.table(f"{custom_catalog_name}.{custom_schema_name}.us_listings_daily")

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

# COMMAND ----------

df_joined.display()

# COMMAND ----------

selected_property_df = property_df.select('pid', 'address', 'baths', 'beds', 
                                          'home_type', 'sqft', 'price', 'rent', 
                                          'city', 'state', 'zip', 
                                          'postingIsRental', 'description', 'great_schools_rating', 'date')

filtered_property_df = selected_property_df.filter(f.col('price').isNotNull())\
                        .withColumn('date', f.col('date').cast('date'))\
                        .withColumn('row_number', f.row_number().over(window_spec))\
                        .filter(f.col('row_number') == 1)\
                            .drop('row_number') 

filtered_property_df.write.mode("overwrite").saveAsTable(f"{custom_catalog_name}.{custom_schema_name}.us_listings_daily_test")

# COMMAND ----------

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as f

schema = ArrayType(StructType([
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
df_test = spark.read.table(f"{custom_catalog_name}.{custom_schema_name}.us_listings_daily_test")

# Assuming 'schema' and 'df_test' are already defined as per your previous code
df_test = df_test.withColumn("parsed_rating", f.from_json("great_schools_rating", schema))

# Explode the array to create a row for each element and include the 'pid' for joining
df_exploded = df_test.selectExpr("pid", "explode(parsed_rating) as schools")

# Find the maximum rating for each 'pid'
max_rating_df = df_exploded.groupBy("pid").agg(f.max("schools.rating").alias("max_rating"))



# Find the minimum distance for each 'pid'
min_distance_df = df_exploded.groupBy("pid").agg(f.min("schools.distance").alias("min_distance"))

# Join back to the original df_joined to include the minimum distance
# Join back to the original df_test to include the maximum rating
df_joined = df_test.join(max_rating_df, "pid")\
    .join(min_distance_df, "pid")

df_joined.display()

# COMMAND ----------

df_test = spark.read.table(f"{custom_catalog_name}.{custom_schema_name}.us_listings_daily_test")

df1 = df_test.withColumn("rating", f.get_json_object(df_test["great_schools_rating"], "$[0].rating")).withColumn("level", f.get_json_object(df_test["great_schools_rating"], "$[0].level"))

df1.select('state').distinct().display()

# COMMAND ----------

df.display()

# COMMAND ----------

filtered_property_df.count()

# COMMAND ----------

filtered_property_df.display()

# COMMAND ----------

filtered_property_df.count()

# COMMAND ----------

filtered_property_df.show()

# COMMAND ----------

cleaned_property_df = property_df.fillna({
    'baths': 0,
    'beds': 0,
    'applications': 0,
    'contacts': 0,
    'days_listed': 0,
    'great_schools_rating': 0
})
cleaned_property_df = cleaned_property_df.withColumn("great_schools_rating", col("great_schools_rating").cast("double"))

# COMMAND ----------

baths 
beds
city
state
price

# COMMAND ----------



# COMMAND ----------

user_df.show()
user_df.printSchema()

# COMMAND ----------

cleaned_user_df = user_df.fillna({
    'budget': 0,
    'preferred_beds': 0,
    'preferred_baths': 0,
    'preferred_city': '',
    'preferred_home_type': ''
})

# COMMAND ----------

cleaned_property_df.write.mode("overwrite").saveAsTable("catalog_name.schema_name.cleaned_real_estate_data")
cleaned_user_df.write.mode("overwrite").saveAsTable("catalog_name.schema_name.cleaned_user_preferences")
