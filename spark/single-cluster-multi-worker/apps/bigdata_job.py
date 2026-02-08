from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr
import time, uuid

spark = SparkSession.builder \
    .appName("BigData-5Min-Stress-Test") \
    .getOrCreate()

sc = spark.sparkContext

# ----------------------------
# PARAMETERS (tune runtime)
# ----------------------------
NUM_ROWS = 300_000_000
NUM_PARTITIONS = 400

spark.conf.set("spark.sql.shuffle.partitions", NUM_PARTITIONS)

# UNIQUE output paths (IMPORTANT)
BASE_PATH = "/opt/spark-data"
RUN_ID = uuid.uuid4().hex

RAW_PATH = f"{BASE_PATH}/bigdata_raw_{RUN_ID}"
RESULT_PATH = f"{BASE_PATH}/bigdata_result_{RUN_ID}"

start = time.time()

# ----------------------------
# STEP 1: Generate data
# ----------------------------
df = (
    spark.range(0, NUM_ROWS, numPartitions=NUM_PARTITIONS)
    .withColumn("user_id", (col("id") % 5_000_000).cast("long"))
    .withColumn("value", (rand() * 1000).cast("int"))
    .withColumn(
        "category",
        expr("""
          CASE
            WHEN value % 5 = 0 THEN 'A'
            WHEN value % 5 = 1 THEN 'B'
            WHEN value % 5 = 2 THEN 'C'
            WHEN value % 5 = 3 THEN 'D'
            ELSE 'E'
          END
        """)
    )
)

# ----------------------------
# STEP 2: Write (IO heavy)
# ----------------------------
df.repartition(50) \
  .write \
  .mode("overwrite") \
  .parquet(RAW_PATH)

# ----------------------------
# STEP 3: Read back
# ----------------------------
df2 = spark.read.parquet(RAW_PATH)

# ----------------------------
# STEP 4: Shuffle-heavy aggregation
# ----------------------------
agg = (
    df2.groupBy("user_id", "category")
       .agg(
           expr("count(*) as cnt"),
           expr("sum(value) as total_value"),
           expr("avg(value) as avg_value")
       )
)

# ----------------------------
# STEP 5: Expensive global sort
# ----------------------------
final_df = agg.orderBy(col("total_value").desc())

# ----------------------------
# STEP 6: Final write
# ----------------------------
final_df.repartition(50) \
        .write \
        .mode("overwrite") \
        .parquet(RESULT_PATH)

end = time.time()

print(f"Total runtime: {(end - start) / 60:.2f} minutes")
print("RAW_PATH:", RAW_PATH)
print("RESULT_PATH:", RESULT_PATH)

spark.stop()
