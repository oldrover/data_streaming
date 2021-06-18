from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)


# {"accountNumber":"814840107","location":"France"}
customerLocationSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

spark = SparkSession.builder.appName("customer-location").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


redisServerRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "redis-server")\
    .option("startingOffsets", "earliest")\
    .load()

redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

redisServerStreamingDF.withColumn("value", from_json("value", redisMessageSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("RedisData")

zSetEntriesEncodedStreamingDF = spark.sql("SELECT key, zSetEntries[0].element AS customerLocation FROM RedisData")

zSetEntriesDecodedStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("customerLocation", unbase64(zSetEntriesEncodedStreamingDF.customerLocation).cast("string"))

zSetEntriesDecodedStreamingDF\
    .withColumn("customerLocation", from_json("customerLocation", customerLocationSchema))\
    .select(col("customerLocation.*"))\
    .createOrReplaceTempView("CustomerLocation")

customerLocationStreamingDF = spark.sql("SELECT * FROM CustomerLocation WHERE location IS NOT NULL")

# output will look something like this:
# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+

customerLocationStreamingDF\
    .writeStream\
    .outputMode("append")\
    .format("console")\
    .start()\
    .awaitTermination()


