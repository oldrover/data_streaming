from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType, LongType


#  {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
bankWithdrawalsSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dateAndTime", StringType()),
        StructField("transactionId", LongType())
    ]
)

# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}
atmWithdrawalsSchema = StructType(
    [
        StructField("transactionDate", StringType()),
        StructField("transactionId", LongType()),
        StructField("atmLocation", StringType())
    ]
)

spark = SparkSession.builder.appName("bank-withdrawals").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


bankWithdrawalsRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "bank-withdrawals")\
    .option("startingOffset", "earliest")\
    .load()

bankWithdrawalsStreamingDF = bankWithdrawalsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

bankWithdrawalsStreamingDF.withColumn("value", from_json("value", bankWithdrawalsSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("BankWithdrawals")

bankWithdrawalsSelectDF = spark.sql("SELECT * FROM BankWithdrawals")

atmWithdrawalsRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "atm-withdrawals")\
    .option("startingOffsets", "earliest")\
    .load()

atmWithdrawalsStreamingDF = atmWithdrawalsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

atmWithdrawalsStreamingDF.withColumn("value", from_json("value", atmWithdrawalsSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("AtmWithdrawals")

atmWithdrawalsSelectDF = spark.sql("SELECT transactionId AS atmTransactionId, atmLocation FROM AtmWithdrawals")

withdrawalsDF = bankWithdrawalsSelectDF.join(atmWithdrawalsSelectDF, expr("transactionId = atmTransactionId"))

withdrawalsDF.selectExpr("cast(transactionId as string) as key", "to_json(struct(*)) as value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "withdrawals-location")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()

# {"accountNumber":"862939503","amount":"844.8","dateAndTime":"Oct 7, 2020 12:33:34 AM","transactionId":"1602030814320","transactionDate":"Oct 7, 2020 12:33:34 AM","atmTransactionId":"1602030814320","atmLocation":"Ukraine"}
