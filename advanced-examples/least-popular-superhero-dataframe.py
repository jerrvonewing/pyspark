"""
Created on Wed Oct 4 2023

@author: Jerrvon Ewing

Description:    This PySpark script loads superhero data and their co-appearances from text files, 
                processes the data using DataFrame operations and functions from the pyspark.sql 
                library, and finally identifies and prints the least popular superheroes along with 
                their count of their co-appearances.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

"""
The function below defines the schema and returns it
"""
def define_schema():
    schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

    return schema

# Create Spark Configuration
spark = SparkSession.builder.appName("LeastPopularSuperhero").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the schema
schema = define_schema()

# File location for names and graph
names_file =  "file:///Users/jerrvon/Documents/pyspark-projects/data-files/marvel/Marvel+Names.txt"
graph_file = "file:///Users/jerrvon/Documents/pyspark-projects/data-files/marvel/Marvel+Graph.txt"

# Read in names and graph files
names = spark.read.schema(schema).option("sep", " ").csv(names_file)
lines = spark.read.text(graph_file)

# Trim each line of whitespace as that could throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Sort our data by least popular and retrieve the first value  
minConnectCount = connections.agg(func.min("connections")).first()[0]

# Match hero ID to hero name
minConnections = connections.filter(func.col("connections") == minConnectCount)

minConnecWithNames = minConnections.join(names,"id")

# Print our results
print(f"The following characters have only {minConnectCount} connection(s):")
minConnecWithNames.select("name").show()

