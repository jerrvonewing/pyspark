# -*- coding: utf-8 -*-
"""
Created on Wed Oct 4 2023

@author: Jerrvon Ewing

Description: This PySpark script loads movie data, counts the occurrences of each movie, 
             and displays the top 10 most popular movies along with their names using a 
             broadcasted dictionary of movie names.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

"""
The function below reads in the data file containing the list of movie details.
Then splits the data set using the | delimiter, matching movie IDs to movies names
"""
def loadMovieNames():
    movieNames = {}
    filename = "/Users/jerrvon/Documents/pyspark-projects/data-files/ml-100k/u.item"

    with codecs.open(filename, "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

"""
The function below defines the schema and returns it
"""
def defineSchema():
    schema = StructType([ \
                        StructField("userID", IntegerType(), True), \
                        StructField("movieID", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestamp", LongType(), True)])
    return schema

"""
The function below takes a given movie ID and returns the name
"""
def lookupName(movieID):
    return nameDict.value[movieID]

# Create Spark Configuration
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read in your movies from the data file and create a broadcast object
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = defineSchema()

# Load up movie data as dataframe
data_file = "/Users/jerrvon/Documents/pyspark-projects/data-files/ml-100k/u.data"
moviesDF = spark.read.option("sep", "\t").schema(schema).csv(data_file)

# Get a count of how many times each movie appears
movieCounts = moviesDF.groupBy("movieID").count()

# Call the user-defined function to look up movie names from our broadcasted dictionary
lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
