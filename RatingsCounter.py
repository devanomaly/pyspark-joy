# Count the classifications in dataset ml-100k (how many 1 star, 2 stars etc...)
from pyspark import SparkContext
import matplotlib.pyplot as plt
import re

sc = SparkContext("local", "Ratings Counter")

lines = sc.textFile("data/ml-100k/u.data")

ratings = lines.map(lambda x: re.split("\t", x)[2]) #regex to split by tab

results = ratings.countByValue()

sortedResults = [(k, v) for k, v in sorted(results.items(), key=lambda item: item[1])]

# Now for some visualization
for result in sortedResults:
    print(result)
    plt.scatter(result[0], result[1])
plt.xlabel("Estrelas", fontsize=17)
plt.ylabel("# avaliações", fontsize=17)
plt.show()    
