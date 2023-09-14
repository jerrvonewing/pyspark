from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func 

# SparkSession Instance
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

# Data File Location
file_path = "file:////Users/jerrvon/Documents/pyspark-projects/data-files/fakefriends-header.csv" 

# Infer the data schema
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(file_path)

# Select the age and friends data, then group by age. Get
# average friends for each age group.
friendsByAge = people.select("age","friends")
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.close()