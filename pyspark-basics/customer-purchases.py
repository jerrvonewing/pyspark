# This script reads in a CSV file containing customer
# purchase history in the format of customer id, item id,
# and the amount spent on that item. The script will return 
# how much each customer spent in total.

from pyspark import SparkConf, SparkContext

# Sets the SparkConf and SparkContext
# then returns the SparkContext instance
def set_spark_config(master: str, appName: str):
    # Set up configuration for Spark 
    conf = SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext(conf = conf)

    return sc

# Function to parse a data value returning
# the three corresponding split values
def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    item_id = int(fields[1])
    item_total = float(fields[2])

    return (customer_id, item_id, item_total)

# Function to collect and print results of a given RDD
def collectResults(rdd):
    results = rdd.collect()

    for result in results:
        print(result)


# Set Spark configuration
master = "local"
appName = "customerPurchaseTotals"
sc = set_spark_config(master,appName)

file_path = "file:////Users/jerrvon/Documents/pyspark-projects/data-files/customer-orders.csv"

# Read in the file and parse each line and splitting them into 
# the three corresponding split values
lines = sc.textFile(file_path)
customerPurchaseLine = lines.map(parseLine)

# Add customer/cost key value pair so that we can get the total 
# amount spent for that customer, then reverse it to sort by total 
# amount spent per customer
totalsByCustomer = customerPurchaseLine.map(lambda x: (x[0], x[2])).reduceByKey(lambda x,y: round(x + y,2))
reversePairs = totalsByCustomer.map(lambda kv: (kv[1], kv[0]))
reverseTotals = reversePairs.sortByKey()

# Collect and print our results
collectResults(reverseTotals)

