from pyspark.sql import functions as F, SparkSession, types as T, window as W

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

# aggreate to event_counts table

event_counts = (
    raw_data
    .withWatermark("timestamp", "1 minute")  # Define um watermark de 10 minutos
    .groupBy("event_type")
    .agg(F.count("event_type").alias("occurrences"))
    .writeStream
    .outputMode("update")  # "append" não é permitido para agregações em streaming
    .format("console")
    # .format("jdbc")
    # .option("url", "jdbc:mysql://your-db:3306/events_storage")
    # .option("dbtable", "event_counts")
    # .option("user", "your_user")
    # .option("password", "your_password")
    .trigger(processingTime="1 minute")  # Garante execução a cada 1 minuto
    .start()
    # .awaitTermination()
)

# aggreate to user_event_counts table

user_event_counts = (
    raw_data
    .withWatermark("timestamp", "1 minute")  # Define um watermark de 10 minutos
    .groupBy("user_id", "event_type")
    .agg(F.count("event_type").alias("occurrences"))
    .orderBy("user_id")
    .writeStream
    .outputMode("update")  # "append" não é permitido para agregações em streaming
    .format("console")
    # .format("jdbc")
    # .option("url", "jdbc:mysql://your-db:3306/events_storage")
    # .option("dbtable", "event_counts")
    # .option("user", "user_event_counts")
    # .option("password", "your_password")
    .trigger(processingTime="1 minute")  # Garante execução a cada 1 minuto
    .start()
    # .awaitTermination()
)

# aggreate to user_avg_waiting_time table

# (
#     # raw_data
#     # .withColumn("prev_timestamp", F.lag("timestamp").over(W.Window.partitionBy("user_id")))


# )


event_counts.awaitTermination()
user_event_counts.awaitTermination()


# raw_data \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()

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

