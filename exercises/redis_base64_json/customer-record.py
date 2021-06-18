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

# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}
customerSchema = StructType (
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

spark = SparkSession.builder.appName("customer-records").getOrCreate()

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

zSetEntriesEncodedStreamingDF = spark.sql("SELECT key, zSetEntries[0].element as customer FROM RedisData")

zSetEntriesDecodedStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("customer", unbase64(zSetEntriesEncodedStreamingDF.customer).cast("string"))

zSetEntriesDecodedStreamingDF\
    .withColumn("customer",from_json("customer", customerSchema))\
    .select(col("customer.*"))\
    .createOrReplaceTempView("Customer")

customerStreamingDF = spark.sql("SELECT accountNumber, location, birthDay from Customer where birthDay IS NOT NULL")

relevantCustomerStreamingDF = customerStreamingDF.select("accountNumber", "location", split(customerStreamingDF.birthDay, "-").getItem(0).alias("birthYear"))
    
relevantCustomerStreamingDF.selectExpr("cast(accountNumber as string) as key", "to_json(struct(*)) as value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "customer-attributes")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()


#You will need to type /data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic customer-attributes --from-beginning to see the JSON data
#
# The data will look like this: {"accountNumber":"288485115","location":"Brazil","birthYear":"1938"}


