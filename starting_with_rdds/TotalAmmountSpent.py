# We'll compute the total ammount spent per costumer in a large dataset
from pyspark import SparkContext

sc = SparkContext("local", "Mean friends by Age")

lines = sc.textFile("data/customer-orders.csv")


def getIdAndAmmount(x):
    splitted = x.split(",")
    # first one is user_id, last is ammount in that particular order that's why we want'em
    return splitted[0], float(splitted[-1])


userAmmountPairs = lines.map(getIdAndAmmount)

print("Number of orders =>", userAmmountPairs.count())
totalsByUser = userAmmountPairs.reduceByKey(
    lambda x, y: x + y
).map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

print("Number of users =>", totalsByUser.count())
print("UserID -- Total ammount spent")
for result in totalsByUser.collect():
    print(result[1]," -- ", round(result[0],2))