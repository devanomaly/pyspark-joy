# Finds the most popular movies in the ml-100k dataset

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from pyspark import SparkContext

spark = (
    SparkSession.builder.master("local").appName("Most Popular Movies").getOrCreate()
)

# defining the schema
moviesSchema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)
# reading u.data (userIDs, movieIDs, ratings and timestamps)
moviesDS = (
    spark.read.option(
        "header", "false"
    )  # by inspecting u.data, we see that there is no header
    .option("sep", "\t")  # separators are tabs
    .schema(moviesSchema)
    .csv(path="data/ml-100k/u.data")
)
# peeking at the data
print(moviesDS.show(5))

# the movie names are in the u.item file... let us broadcast the key-value pairs of movieID-movieName
def loadMovieNames():
    # here we load u.item, split each line by the '|' separator and then get movieID (index 0) and movieName (index 1)
    idNameDict = (
        {}
    )  # this will be our 'mapping' that connects ids to names; it is this variable that we'll broadcast

    # since there are not so many movies, we can do it in a "python-only" way
    with open(
        "data/ml-100k/u.item", encoding="ISO-8859-1"
    ) as f:  # remember to always care for the proper encoding!
        lines = f.readlines()
        for line in lines:
            fields = line.split("|")
            idNameDict[int(fields[0])] = fields[
                1
            ]  # here we remember to use an INTEGER as our key!
        f.close()

    return idNameDict


movieCounts = moviesDS.groupBy("movieID").count()

# we now do the broadcasting in the present context
broadcastIdNameDict = spark.sparkContext.broadcast(loadMovieNames())

# we define a function to get the movie name, given its id
lookupName = lambda movie_ID: broadcastIdNameDict.value[movie_ID]
lookupNameUDF = udf(lookupName)

# We now add a movie title column using our udf
moviesWithNames = movieCounts.withColumn(
    "movieTitle", lookupNameUDF(col("movieID"))
)  # "col" with look up for "this.givenColumn", where this is the DF that called withColumn

# We sort the results (descending order)...
sortedMoviesWithNames = moviesWithNames.orderBy("count")

# ... and finally print them
print(sortedMoviesWithNames.show(sortedMoviesWithNames.count(), truncate=False))

spark.stop()
