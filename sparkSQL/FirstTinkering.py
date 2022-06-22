# We'll compute the total ammount spent per costumer in a large dataset
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local")
    .appName("Tinkering Around with SparkSQL")
    .getOrCreate()
)

schemaPeople = spark.read.option("header", "true").option("inferSchema", "true").csv(path="data/fakefriends.csv")

schemaPeople.printSchema()

schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql(sqlQuery="SELECT * FROM people WHERE age >= 13 AND age <= 19")
results = teenagers.collect()
for result in results:
  print(result)
spark.stop()