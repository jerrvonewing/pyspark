from pyspark import SparkConf, SparkContext

# Set up configuration for Spark
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# Function to parse the SparkContext and 
# returns the age and number of friends
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

file_path = "file:////Users/jerrvon/Documents/pyspark-projects/data-files/fakefriends.csv"

# Read in the texfile and pass the data along 
# to be parsed by the parseLine function
lines = sc.textFile(file_path)
rdd = lines.map(parseLine)

# Convert the rdd into a tuple, then reduce by 
# the key to get key value pair including the age as
# the key, and the number of total friends for the age group
# and the total number of people at that age group as the values
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Get the average for each age group and print each results
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]).sortBy(lambda x: x[0])
results = averagesByAge.collect()
for result in results:
    print(result)
