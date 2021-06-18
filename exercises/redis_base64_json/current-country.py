from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

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

# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}
customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

# {"accountNumber":"814840107","location":"France"}
customerLocationSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

spark = SparkSession.builder.appName("current-country").getOrCreate()

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

zSetEntriesEncodedStreamingDF = spark.sql("SELECT key, zSetEntries[0].element as redisEvent from RedisData")

zSetEntriesDecodedStramingDF1 = zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))

zSetEntriesDecodedStramingDF2 = zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))

zSetEntriesDecodedStramingDF1\
    .withColumn("customer", from_json("redisEvent", customerSchema))\
    .select("customer.*")\
    .createOrReplaceTempView("Customer")

zSetEntriesDecodedStramingDF2\
    .withColumn("customerLocation", from_json("redisEvent", customerLocationSchema))\
    .select("customerLocation.*")\
    .createOrReplaceTempView("CustomerLocation")

customerStreamingDF = spark.sql("SELECT accountNumber AS customerAccountNumber, location AS homeLocation, birthDay FROM Customer WHERE birthDay IS NOT NULL")

relevantCustomerStreamingDF = customerStreamingDF\
    .select("customerAccountNumber", "homeLocation", split(customerStreamingDF.birthDay, "-").getItem(0).alias("birthYear"))

customerLocationStreamingDF = spark.sql("SELECT accountNumber as locationAccountNumber, location FROM CustomerLocation")

currentAndHomeLocationStreamingDF = relevantCustomerStreamingDF.join(customerLocationStreamingDF, expr("customerAccountNumber = locationAccountNumber"))

customerLocationStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# When calling the customer, customer service will use their birth year to help
# establish their identity, to reduce the risk of fraudulent transactions.
# +---------------------+-----------+---------------------+------------+---------+
# |locationAccountNumber|   location|customerAccountNumber|homeLocation|birthYear|
# +---------------------+-----------+---------------------+------------+---------+
# |            982019843|  Australia|            982019843|   Australia|     1943|
# |            581813546|Phillipines|            581813546| Phillipines|     1939|
# |            202338628|Phillipines|            202338628|       China|     1944|
# |             33621529|     Mexico|             33621529|      Mexico|     1941|
# |            266358287|     Canada|            266358287|      Uganda|     1946|
# |            738844826|      Egypt|            738844826|       Egypt|     1947|
# |            128705687|    Ukraine|            128705687|      France|     1964|
# |            527665995|   DR Congo|            527665995|    DR Congo|     1942|
# |            277678857|  Indonesia|            277678857|   Indonesia|     1937|
# |            402412203|   DR Congo|            402412203|    DR Congo|     1945|
# +---------------------+-----------+---------------------+------------+---------+
