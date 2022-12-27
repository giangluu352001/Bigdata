from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("analysis-uber-trip-data")
         .getOrCreate())

schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("datetime", StringType(), nullable=False),
    StructField("distance", DoubleType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("destination", DoubleType(), nullable=False)]
  )

dfStream = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "uber")
  .option("startingOffsets", "earliest")
  .option("failOnDataLoss", "false")
  .load())
dfStream.printSchema()

dfStream = dfStream.selectExpr("CAST(value AS STRING)")
dataStream = dfStream.select(from_json(col("value"), schema).alias("data")).select("data.*")
real = dataStream.withColumn("prediction", lit(100))
(real.writeStream
            .format("console")
            .outputMode("append")
            .start()
            .awaitTermination())