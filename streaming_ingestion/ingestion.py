from pyspark.sql import functions as F, SparkSession, types as T

spark = (
    SparkSession.builder
    .appName("streaming-ingestion")
        .master("local[*]")
            .getOrCreate()

)

spark.sparkContext.setLogLevel("ERROR")

schema = T.StructType([
    T.StructField("user_id", T.LongType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("game_id", T.LongType()),
    T.StructField("payload", T.MapType(T.StringType(), T.StringType())),
])

raw_data = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-server:9092")
    .option("subscribe", "app-events")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(F.from_json(F.col("json"), schema).alias("data"))
    .select("data.*")
)

raw_data \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# def write_to_mysql(df, epoch_id):
#     df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:mysql://mysql-server:3306/events_storage") \
#         .option("dbtable", "events") \
#         .option("user", "root") \
#         .option("password", "root") \
#         .mode("append") \
#         .save()

# writing in mysql db
# raw_data \
#     .writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_to_mysql) \
#     .start() \
#     .awaitTermination()

