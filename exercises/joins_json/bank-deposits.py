from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType
# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date or the amount in sql calculations, we are going
# to cast them as strings
kafkaMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", StringType()),
        StructField("dateAndTime", StringType())
    ]
    
)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("bank-deposits").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

bankDepositsRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-deposits")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
bankDepositsStreamingDF = bankDepositsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
bankDepositsStreamingDF.withColumn("value",from_json("value",kafkaMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("BankDeposits")

# Using spark.sql we can select any valid select statement from the spark view
bankDepositsSelectStarDF=spark.sql("select * from BankDeposits")

# this takes the stream and "sinks" it to the console as it is updated one message at a time:
# +-------------+------+--------------------+
# |accountNumber|amount|         dateAndTime|
# +-------------+------+--------------------+
# |    103397629| 800.8|Oct 6, 2020 1:27:...|
# +-------------+------+--------------------+

bankDepositsSelectStarDF.writeStream.outputMode("append").format("console").start().awaitTermination()

