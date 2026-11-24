from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("MusicLogs_KafkaToHDFS_Ingestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("platform", StringType(), False),
    StructField("user_region", StringType(), True),
    StructField("track_id", StringType(), False),
    StructField("track_name", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("played_ms", IntegerType(), True),
    StructField("audio_features", StructType([
        StructField("energy", DoubleType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("tempo", DoubleType(), True)
    ]), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "music-play-logs") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_kafka \
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("data")) \
    .select("data.*")

def process_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.persist()

    batch_df.write \
        .mode("append") \
        .json("hdfs://namenode:9000/data/raw/music_logs/")

    df_clean = batch_df \
        .dropDuplicates(["event_id"]) \
        .dropna(subset=["event_id", "track_id", "platform", "timestamp"]) \
        .filter((F.col("duration_ms") > 0) & (F.col("played_ms") >= 0) & (F.col("played_ms") <= F.col("duration_ms"))) \
        .withColumn("platform", F.lower(F.trim(F.col("platform")))) \
        .withColumn("event_ts", F.to_timestamp("timestamp")) \
        .withColumn("date", F.to_date("event_ts")) \
        .withColumn("completion_rate", F.round(F.col("played_ms") / F.col("duration_ms"), 2)) \
        .withColumn("is_skipped", F.when(F.col("played_ms") < 30000, 1).otherwise(0)) \
        .select(
            "event_id", "event_ts", "date", "platform", "user_region",
            "track_id", "track_name", "artist", "genre",
            "duration_ms", "played_ms", "completion_rate", "is_skipped",
            "audio_features.energy", "audio_features.danceability", "audio_features.tempo"
        )

    df_clean.write \
        .mode("append") \
        .partitionBy("platform", "date") \
        .parquet("hdfs://namenode:9000/data/clean/music_stats/")

    batch_df.unpersist()
    print(f"Batch {batch_id} selesai disimpan ke HDFS Raw (JSON) & Clean (Parquet).")

query = df_parsed.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/music_ingestion/") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()