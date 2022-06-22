# We'll count the words in a large dataset (book)
from pyspark import SparkContext

print(currentDirectory)
sc = SparkContext("local", "Counting words of a book")

book = sc.textFile("data/book.txt")

words = book.flatMap(lambda x: x.split(" "))

wordsOccurences = words.countByValue()

for word, count in sorted(
    wordsOccurences.items(), key=lambda item: item[1], reverse=True
):
    print(word, count)
