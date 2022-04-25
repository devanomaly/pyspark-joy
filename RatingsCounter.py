# Conta as classificações do dataset ml-100k (quantas sao 1 estrela, 2 estrelas etc...)
from pyspark import SparkContext
import matplotlib.pyplot as plt
import re

sc = SparkContext("local", "Ratings Counter")

lines = sc.textFile("data/ml-100k/u.data")

ratings = lines.map(lambda x: re.split("\t", x)[2])

results = ratings.countByValue()

# ordenando o resultado com base no número de avaliações
sortedResults = [(k, v) for k, v in sorted(results.items(), key=lambda item: item[1])]

histogram = []
for result in sortedResults:
    print(result)
    plt.scatter(result[0], result[1])
plt.xlabel("Estrelas", fontsize=17)
plt.ylabel("# avaliações", fontsize=17)
plt.show()    
# vemos que a maioria dos filmes foi avaliado em 4 estrelas
