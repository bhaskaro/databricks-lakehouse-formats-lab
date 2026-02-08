from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WordCount-Demo") \
    .getOrCreate()

data = [
    "spark makes big data simple",
    "spark runs everywhere",
    "docker makes spark easy",
    "learning spark is fun"
]

df = spark.createDataFrame(data, "string").toDF("line")

words = df.selectExpr("explode(split(line, ' ')) as word")
counts = words.groupBy("word").count()

counts.show(truncate=False)

spark.stop()

