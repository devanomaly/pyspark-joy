# We'll count the words in a large dataset (book) now with some regex to improve it
from pyspark import SparkContext
import re

sc = SparkContext("local", "Counting words of a book -- improved")

book = sc.textFile("data/book.txt")

words = book.flatMap(lambda x: re.split("\\W+", x)).map(lambda x: x.lower())

wordsOccurences_sorted = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending=False) #x[0] is word, x[1] is count


# Lets sort the big-data way!
for result in wordsOccurences_sorted.collect():
    print(result[1]," -- ", result[0])
