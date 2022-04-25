# We'll find the minimum temperature by weather station in a large dataset
from pyspark import SparkContext

sc = SparkContext("local", "Counting words of a book")

book = sc.textFile("data/book.txt")

words = book.flatMap(lambda x: x.split(" "))

wordsOccurences = words.countByValue()

for word, count in sorted(wordsOccurences.items(), key=lambda item: item[1]):
  print(word, count)