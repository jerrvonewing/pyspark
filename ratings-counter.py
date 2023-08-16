# Overview:
# 
# This script reads in a big data file containing 100 thousand movie reviews. 
# The script parses the data and calculates how many reviews are of 1,2,3,4 and
# 5 stars respectively.The dataset is then visualized into a histogram 

from pyspark import SparkConf, SparkContext
import collections

# Set up the SparkContext configuration and object
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# Define the file location and the mapping split for the dataset
lines = sc.textFile("file:////Users/jerrvon/Documents/pyspark-projects/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

# Sort the reults and print out each rating group, and the number of times that review was given 
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
