from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

# Set SparkSession configuration
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Datafile location
file_path = "file:////Users/jerrvon/Documents/pyspark-projects/data-files/ml-100k/u.data"

# Define the schema
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as a dataframe
moviesDF = spark.read.option("sep","\t").schema(schema).csv(file_path)

# Sort all movies by popularity
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Retrieve top 10 movies
topMovieIDs.show(10)

# End session
spark.stop()