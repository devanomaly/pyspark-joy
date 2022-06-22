# We'll find the minimum temperature by weather station in a large dataset
from pyspark import SparkContext
import matplotlib.pyplot as plt


def getStationAndMinTemp(x):
    splittedFields = x.split(",")
    station = splittedFields[0]
    entryType = splittedFields[2]
    tempOrPrecipitation = float(splittedFields[3])
    # print("splitted", splitted)
    return (station, entryType, tempOrPrecipitation)


sc = SparkContext("local", "Minimum Temperature by Weather Station")

weatherData = sc.textFile("data/1800.csv")

relevantData = weatherData.map(getStationAndMinTemp)

howManyStations = len(relevantData.map(lambda x: (x[0], x[2])).countByKey().keys())

print(howManyStations)

allMinTemps = relevantData.filter(lambda x: x[1] == "TMIN").map(lambda x: (x[0], x[2]))

minTempByStation = allMinTemps.reduceByKey(lambda x, y: min(x, y))

results = minTempByStation.collect()

for result in results:
    print(result[0], result[1]+str("ºC"))
    plt.scatter(result[0], result[1], color="blue", s=100)

# let us visualize the data
plt.title("Minimum temperature per station", fontsize=18)
plt.xlabel("Station", fontsize=17)
plt.ylabel("Temp (ºC)", fontsize=17)
plt.show()
# Two out of three stations were monitoring the minimum temperature.