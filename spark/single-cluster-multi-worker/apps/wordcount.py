from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SingleCluster-10Workers-WordCount") \
    .getOrCreate()

data = [
    "spark scales horizontally",
    "spark uses multiple workers",
    "this job runs on one cluster",
    "parallelism comes from workers"
]

df = spark.createDataFrame(data, "string").toDF("line")

words = df.selectExpr("explode(split(line, ' ')) as word")
counts = words.groupBy("word").count()

counts.show(truncate=False)

spark.stop()

