from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# Set up Spark session and log level 
spark = SparkSession.builder.appName("CustomerPurchases").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# File location
file_path = "file:////Users/jerrvon/Documents/pyspark-projects/data-files/customer-orders.csv"

# Define the schema
schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("itemCost", FloatType(), True)])

# Read the file as dataframe
customer_df = spark.read.schema(schema).csv(file_path)

# Select only customerID and itemCost
customerPurchases = customer_df.select("customerID", "itemCost")

# Get the total spent per customer and then sorted
customerPurchases.groupBy("customerID").agg(func.round(func.sum("itemCost"),2).alias("totalsByCustomer")).sort("totalsByCustomer").show()


