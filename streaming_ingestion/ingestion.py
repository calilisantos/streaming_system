from pyspark.sql import SparkSession, functions as F, types as T


TABLE_COLUMNS = {
    "raw_events": ["value", "topic", "partition", "offset", "timestamp"],
    "parsed_events": ["user_id", "event_type", "timestamp", "game_id", "payload"],
    "event_counts": ["event_type", "occurrences"],
    "user_event_counts": ["event_type", "user_id", "occurrences"],
    "user_avg_waiting_time": ["user_id", "avg_waiting_time"],
}

spark = (
    SparkSession.builder
    .appName("streaming-ingestion")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

schema = T.StructType([
    T.StructField("user_id", T.LongType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("game_id", T.LongType()),
    T.StructField("payload", T.MapType(T.StringType(), T.StringType())),
])

# foreachBatch function
def write_to_postgres(df, epoch_id, table_name):
    selected_columns = TABLE_COLUMNS[table_name]

    if "payload" in selected_columns:
        df = df.withColumn("payload", F.to_json("payload"))

    df = df.select(
        F.lit(epoch_id).alias("batch_id"),
        *selected_columns
    )

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://events_storage:5432/events_storage") \
        .option("dbtable", table_name) \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# reading topic
raw_data = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-server:9092")
    .option("subscribe", "app-events")
    .option("startingOffsets", "earliest")
    .load()
)

# writing in bronze layer
(
    raw_data
    .writeStream
    .outputMode("append")
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "raw_events"))
    .start()
)

parsed_data = (
    raw_data.selectExpr("CAST(value AS STRING) as json")
    .select(F.from_json(F.col("json"), schema).alias("data"))
    .select("data.*")
)

# writing in silver layer
(
    parsed_data
    .writeStream
    .outputMode("append")
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "parsed_events"))
    .start()
)

# writing in gold - event_counts
(
    parsed_data
    .withWatermark("timestamp", "5 minutes")
    .groupBy("event_type")
    .agg(F.count("event_type").alias("occurrences"))
    .writeStream
    .outputMode("update")
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "event_counts"))
    .trigger(processingTime="1 minute")
    .start()
)

# writing in gold - user_event_counts
(
    parsed_data
    .withWatermark("timestamp", "5 minutes")
    .groupBy("user_id", "event_type")
    .agg(F.count("event_type").alias("occurrences"))
    .writeStream
    .outputMode("update")
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "user_event_counts"))
    .trigger(processingTime="1 minute")
    .start()
)

# writing in gold - user_avg_waiting_time
(
    parsed_data
    .withWatermark("timestamp", "5 minutes")
    .groupBy("user_id", F.window("timestamp", "2 minutes"))
    .agg((F.max("timestamp").cast("long") - F.min("timestamp").cast("long")).alias("time_diff"))
    .groupBy("user_id")
    .agg(F.round(F.avg("time_diff"), 2).alias("avg_waiting_time"))
    .writeStream
    .outputMode("update")
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "user_avg_waiting_time"))
    .trigger(processingTime="1 minute")
    .start()
)

# show data
console_data = parsed_data

def debug_console(df, epoch_id):
    print(f"\n[DEBUG] EPOCH: {epoch_id}")
    df.show(10, truncate=False)

(
    console_data
    .writeStream
    .outputMode("append")
    .foreachBatch(debug_console)
    .start()
)

spark.streams.awaitAnyTermination()
