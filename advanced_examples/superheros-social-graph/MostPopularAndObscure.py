# Finds the most popular and most obscure superheroes in the marvel-graph.txt dataset; at the end, it prints the most popular and most obscure 10 respectively.

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, split, size, sum as sqlSUM, broadcast
from pyspark.sql.types import *
from pyspark import SparkContext

spark = (
    SparkSession.builder.master("local")
    .appName("MostPopularMarvelHeroes")
    .getOrCreate()
)

# defining the schema to read marvel-names.txt
heroesNamesSchema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

# building a ID <-> name mapping
heroesNames = (
    spark.read.option(
        "header", "false"
    )  # by inspecting u.data, we see that there is no header
    .option("sep", " ")  # separators are spaces
    .schema(heroesNamesSchema)
    .csv(path="data/marvel-names.txt")
)

# reading the graph
lines = spark.read.text("data/marvel-graph.txt")

# we calculate the connections for each hero id
heroesConnections = (
    lines.withColumn(
        "id", split(col("value"), pattern=" ")[0]
    )  # getting the current hero id in present line
    .withColumn(
        "connections", size(split(col("value"), " ")) - 1
    )  # number of connections equals the splitting minus the hero's own ID occurrence
    .groupBy("id")  # lines that begin with the same first hero id are grouped
    .agg(
        sqlSUM("connections").alias("connections")
    )  # we sum all the connections for a given hero id
)

# size(heroesNames) >> size(heroesConnections), so we broadcast connections...
namesAndConnections = heroesNames.join(broadcast(heroesConnections), "id").select(
    ["name", "connections"]
)

# ... and finally print the most popular 10
print("Top 10 most popular Marvel heroes")
print(namesAndConnections.sort("connections", ascending=False).show(10))
print("Top 10 most obscure Marvel heroes")
print(namesAndConnections.sort("connections", ascending=True).show(10))

spark.stop()