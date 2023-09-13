# This script reads in a book as a text file and prints 
# out the most commonly used used words and their respective
# counts.

import re
from pyspark import SparkConf, SparkContext

# Takes in a string, and seperates each word in it,
# removes the punctuation, and converts it to lower case
# then returns the words as separate values
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# Set up configuration for Spark 
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Read in the data file then FlatMap through the data file (a book)
# to split each expected word into a list of words
input = sc.textFile("file:////Users/jerrvon/Documents/pyspark-projects/Book.txt")
words = input.flatMap(normalizeWords)

# Get a count of each word in the list
# When the word is in the list, the count will increase
# Sort the words by the frequency it is used where most used is
# at the bottom of the result.
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

# Encoding/Decoding to ensure words are correctly encoded
# Print each word and the number of times it occurs
for result in results:
    count = str(result[0])
    cleanWord = result[1].encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + ":\t" + count)
