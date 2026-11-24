import json
import math
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("MusicLogs_Layer3_Analytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

CLEAN_DATA_PATH = "hdfs://namenode:9000/data/clean/music_stats/"
SERVING_HDFS_PATH = "hdfs://namenode:9000/data/serving"
SERVING_LOCAL_DIR = "/opt/spark-scripts/serving_data"
os.makedirs(SERVING_LOCAL_DIR, exist_ok=True)

def save_serving_table(df, table_name):
    """Menyimpan hasil analitik ke HDFS Serving Zone (Parquet) & JSON lokal untuk Serving API."""
    df.write.mode("overwrite").parquet(f"{SERVING_HDFS_PATH}/{table_name}/")
    records = df.toPandas().to_dict(orient="records")
    with open(os.path.join(SERVING_LOCAL_DIR, f"{table_name}.json"), "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    print(f"[SERVING] Tabel '{table_name}' tersimpan ({len(records)} baris).")

print("1. Membaca data bersih (Parquet) dari HDFS Layer 2...")
df = spark.read.parquet(CLEAN_DATA_PATH).cache()
total_events = df.count()
print(f"Total event bersih yang dianalisis: {total_events}")

# ANALITIK 1: DESKRIPTIF (Apa yang terjadi?)
print("\n2. Menjalankan Analitik Deskriptif...")

df_top_songs = df.groupBy("platform", "track_id", "track_name", "artist", "genre") \
    .agg(
        F.count("event_id").alias("total_plays"),
        F.round(F.sum("played_ms") / 60000.0, 2).alias("total_minutes"),
        F.round(F.avg("completion_rate") * 100, 1).alias("avg_completion_pct")
    ) \
    .orderBy(F.desc("total_plays"))

save_serving_table(df_top_songs, "serving_top_songs")

df_genre_stats = df.groupBy("platform", "genre") \
    .agg(
        F.count("event_id").alias("total_plays"),
        F.countDistinct("track_id").alias("unique_tracks")
    ) \
    .orderBy("platform", F.desc("total_plays"))

save_serving_table(df_genre_stats, "serving_genre_stats")

# ANALITIK 2: DIAGNOSTIK (Mengapa terjadi? Uji hubungan fitur audio vs retensi)
print("\n3. Menjalankan Analitik Diagnostik...")

df_diagnostic = df.groupBy("platform", "track_id", "track_name", "artist", "genre") \
    .agg(
        F.count("event_id").alias("play_count"),
        F.round(F.avg("is_skipped"), 3).alias("skip_rate"),
        F.round(F.avg("completion_rate"), 3).alias("avg_completion_rate"),
        F.round(F.avg("energy"), 2).alias("energy"),
        F.round(F.avg("danceability"), 2).alias("danceability"),
        F.round(F.avg("tempo"), 1).alias("tempo"),
        F.round(F.avg("duration_ms") / 1000.0, 1).alias("duration_sec")
    )

save_serving_table(df_diagnostic, "serving_diagnostic_audio")

def safe_corr(dataframe, col1, col2):
    val = dataframe.stat.corr(col1, col2)
    return 0.0 if (val is None or math.isnan(val)) else round(float(val), 4)

corr_records = [
    {"feature": "Energy", "corr_with_completion": safe_corr(df, "energy", "completion_rate")},
    {"feature": "Danceability", "corr_with_completion": safe_corr(df, "danceability", "completion_rate")},
    {"feature": "Tempo", "corr_with_completion": safe_corr(df, "tempo", "completion_rate")},
    {"feature": "Durasi Lagu", "corr_with_completion": safe_corr(df, "duration_ms", "completion_rate")}
]
df_corr = spark.createDataFrame(corr_records)
save_serving_table(df_corr, "serving_correlations")

# ANALITIK 3: PREDIKTIF (Apa yang akan terjadi? Spark MLlib Random Forest)
print("\n4. Menjalankan Analitik Prediktif (Random Forest vs Baseline)...")

df_track_agg = df.groupBy("platform", "track_id", "track_name", "artist", "genre") \
    .agg(
        F.count("event_id").alias("play_count"),
        F.round(F.avg("is_skipped"), 3).alias("skip_rate"),
        F.round(F.avg("completion_rate"), 3).alias("avg_completion_rate"),
        F.round(F.avg("energy"), 3).alias("energy"),
        F.round(F.avg("danceability"), 3).alias("danceability"),
        F.round(F.avg("tempo"), 1).alias("tempo"),
        F.round(F.avg("duration_ms") / 1000.0, 1).alias("duration_sec")
    )

window_rank = Window.orderBy(F.desc("play_count"), F.desc("avg_completion_rate"))
total_tracks = df_track_agg.count()
top_cutoff = max(1, int(total_tracks * 0.30))

df_labeled = df_track_agg \
    .withColumn("rank_num", F.row_number().over(window_rank)) \
    .withColumn("label", F.when(F.col("rank_num") <= top_cutoff, 1.0).otherwise(0.0)) \
    .drop("rank_num")

feature_cols = ["energy", "danceability", "tempo", "duration_sec", "avg_completion_rate", "skip_rate"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_ml = assembler.transform(df_labeled)

train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=30, maxDepth=5, seed=42)
rf_model = rf.fit(train_df)

test_preds = rf_model.transform(test_df)
acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
auc_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

rf_acc = round(acc_eval.evaluate(test_preds), 4)
rf_f1 = round(f1_eval.evaluate(test_preds), 4)
rf_auc = round(auc_eval.evaluate(test_preds), 4) if test_preds.select("label").distinct().count() > 1 else 0.5

test_baseline = test_df.withColumn("baseline_pred", F.lit(0.0))
base_acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="baseline_pred", metricName="accuracy")
baseline_acc = round(base_acc_eval.evaluate(test_baseline), 4)

metrics_records = [{
    "total_events_analyzed": total_events,
    "total_unique_track_platform": total_tracks,
    "model_name": "Random Forest Classifier",
    "rf_accuracy": rf_acc,
    "rf_f1_score": rf_f1,
    "rf_auc": rf_auc,
    "naive_baseline_accuracy": baseline_acc
}]
df_metrics = spark.createDataFrame(metrics_records)
save_serving_table(df_metrics, "serving_model_metrics")

full_preds = rf_model.transform(df_ml)
extract_prob = F.udf(lambda v: round(float(v[1]), 4), "double")

df_scored = full_preds \
    .withColumn("chart_probability", extract_prob(F.col("probability"))) \
    .drop("features", "rawPrediction", "probability")

# ANALITIK 4: PRESKRIPTIF (Apa yang harus dilakukan? Rule-Based Decision Engine)
print("\n5. Menjalankan Analitik Preskriptif (Aturan Keputusan Otomatis)...")

df_plat_pivot = df_scored.groupBy("track_id").pivot("platform", ["spotify", "apple_music"]).agg(F.first("play_count")).na.fill(0)

df_prescriptive = df_scored.join(df_plat_pivot, on="track_id", how="left") \
    .withColumn(
        "prioritas",
        F.when((F.col("chart_probability") >= 0.65) & (F.col("skip_rate") < 0.25), "TINGGI")
         .when((F.col("apple_music") > F.col("spotify") * 1.3) & (F.col("platform") == "apple_music"), "EKSPANSI")
         .when((F.col("chart_probability") >= 0.50) & (F.col("skip_rate") >= 0.30), "EVALUASI")
         .otherwise("RUTIN")
    ) \
    .withColumn(
        "rekomendasi_aksi",
        F.when(
            F.col("prioritas") == "TINGGI",
            "Masukkan ke Playlist Utama & Tingkatkan Anggaran Promosi"
        ).when(
            F.col("prioritas") == "EKSPANSI",
            "Kuat di Apple Music — Dorong Promosi Lintas Platform ke Audiens Spotify"
        ).when(
            F.col("prioritas") == "EVALUASI",
            "Potensi Tinggi namun Skip Tinggi, Uji Potongan Audio (Reff) untuk Teaser"
        ).otherwise(
            "Pertahankan Rotasi Organik & Pantau Tren Harian"
        )
    ) \
    .select(
        "track_id", "track_name", "artist", "genre", "platform",
        "play_count", "skip_rate", "avg_completion_rate",
        "chart_probability", "prioritas", "rekomendasi_aksi"
    ) \
    .orderBy(F.desc("chart_probability"), F.desc("play_count"))

save_serving_table(df_prescriptive, "serving_prescriptive_actions")

df.unpersist()
print("\nSeluruh proses Layer 3 (4 Jenis Analitik) selesai dan siap disajikan di Layer 4!")