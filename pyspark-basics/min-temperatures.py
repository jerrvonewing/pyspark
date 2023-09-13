from pyspark import SparkConf, SparkContext

# Setup Spark configuration
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# Function to parse a data value returning
# a the three corresponding split values
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

file_path = "file:////Users/jerrvon/Documents/pyspark-projects/data-files/1800.csv"

# Read in the data file and parse each entry 
# into three values, stationID, entryType and temperature
lines = sc.textFile(file_path)
parsedLines = lines.map(parseLine)

# Filter the entries based x[1] value equating 
# to "TMIN". Then we retrieve the min temp for each station
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# Retrieve the min temp for each station  
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()

# Print formatted results
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
