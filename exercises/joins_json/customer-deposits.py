from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, ArrayType, DateType

# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
# Cast the amount as a FloatType
atmVisitsMessageSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dateAndTime", StringType())
    ]
)

# {"customerName":"Trevor Anandh","email":"Trevor.Anandh@test.com","phone":"1015551212","birthDay":"1962-01-01","accountNumber":"45204068","location":"Togo"}
customersMessageSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

spark = SparkSession.builder.appName("customer-deposits").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
 
atmVisitsRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "bank-deposits")\
    .option("startingOffsets", "earliest")\
    .load()

atmVisitsStreamingDF = atmVisitsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

atmVisitsStreamingDF.withColumn("value", from_json("value", atmVisitsMessageSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("BankDeposits")

atmVisitsSelectStarDF = spark.sql("SELECT * FROM BankDeposits WHERE amount > 200.00")

customersRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "bank-customers")\
    .option("startingOffsets", "earliest")\
    .load()

customersStreamingDF = customersRawStreamingDF.selectExpr("cast(key as string) string", "cast(value as string) value")

customersStreamingDF.withColumn("value", from_json("value", customersMessageSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("BankCustomers")

customersSelectDF = spark.sql("SELECT customerName, accountNumber as customerNumber FROM BankCustomers")

depositsDF = atmVisitsSelectStarDF.join(customersSelectDF, expr("accountNumber = customerNumber"))

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
#. +-------------+------+--------------------+------------+--------------+
#. |accountNumber|amount|         dateAndTime|customerName|customerNumber|
#. +-------------+------+--------------------+------------+--------------+
#. |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
#. |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
#. |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
#. +-------------+------+--------------------+------------+--------------+
depositsDF.writeStream.outputMode("append").format("console").start().awaitTermination()


