# We'll compute the mean number of friends per age in a large dataset
from pyspark import SparkContext
import matplotlib.pyplot as plt

sc = SparkContext("local", "Mean friends by Age")

lines = sc.textFile("data/fakefriends-noheader.csv")


def splitCSV_getLastTwo(x):
    # last two are age then number of friends, that's why we want'em
    splitted = x.split(",")
    # print("splitted", splitted)
    return (int(splitted[-2]), int(splitted[-1]))


ageFriendsPairs = lines.map(splitCSV_getLastTwo)

totalsByAge = ageFriendsPairs.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1])
)

print(totalsByAge.collect()[0])

means = totalsByAge.mapValues(lambda x: x[0] / x[1]).collect()
for mean in means:
    print(mean[0], mean[1])
    plt.scatter(mean[0], mean[1], color="blue", s=100)

# let us visualize the data
plt.title("Mean number of friends per age", fontsize=18)
plt.xlabel("Age", fontsize=17)
plt.ylabel("Friend Number", fontsize=17)
plt.show()
