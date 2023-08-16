from pyspark import SparkConf, SparkContext

# Setup Spark configuration
conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

# Function to parse a data value returning
# a the three corresponding split values
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# Read in the data file and parse each entry 
# into three values, stationID, entryType and temperature
lines = sc.textFile("file:////Users/jerrvon/Documents/pyspark-projects/1800.csv")
parsedLines = lines.map(parseLine)

# Filter the entries based x[1] value equating 
# to "TMAX". Then we retrieve the max temp for each station
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# Retrieve the max temp for each station  
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect()

# Print formatted results
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))