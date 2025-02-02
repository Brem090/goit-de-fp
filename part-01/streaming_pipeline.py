from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, avg, current_timestamp, lit, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Конфігурація
mysql_config = {
    "host": "217.61.57.46",
    "port": "3306",
    "db": "olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp"
}

kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "input_topic": "athlete_event_results",
    "output_topic": "athlete_enriched_agg_eugene"
}

jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['db']}?useSSL=false"

# Spark Session
print("[INFO] Creating SparkSession")
spark = (
    SparkSession.builder
    .appName("GoIT_EndToEnd_Streaming_Pipeline")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.jars", "/mnt/f/Educations/goit-de-fp/part-01/mysql-connector-j-8.0.32.jar")
    .config("spark.driver.extraClassPath", "/mnt/f/Educations/goit-de-fp/part-01/mysql-connector-j-8.0.32.jar")
    .getOrCreate()
)

# Функція для налаштування Kafka
def kafka_options(topic):
    return {
        "kafka.bootstrap.servers": kafka_config["bootstrap_servers"],
        "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
        "kafka.security.protocol": kafka_config["security_protocol"],
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
        "subscribe": topic
    }

# Етап 1: Зчитування даних із MySQL таблиці athlete_bio
print("[INFO] Stage 1: Reading athlete_bio table")
athlete_bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url, driver="com.mysql.cj.jdbc.Driver", dbtable="athlete_bio",
        user=mysql_config["user"], password=mysql_config["password"]
    ).load()
)

# Етап 2: Фільтрація даних
print("[INFO] Stage 2: Data filtering (height and weight)")
filtered_athlete_bio_df = (
    athlete_bio_df
    .filter(
        (col("height").isNotNull() & (col("height") != "") & (col("height") > 0)) &
        (col("weight").isNotNull() & (col("weight") != "") & (col("weight") > 0))
    )
    .withColumn("height", col("height").cast(DoubleType()))
    .withColumn("weight", col("weight").cast(DoubleType()))
)

# Етап 3: Зчитування athlete_event_results і запис у Kafka
print("[INFO] Stage 3: Reading athlete_event_results and writing to Kafka")
athlete_event_results_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url, driver="com.mysql.cj.jdbc.Driver", dbtable="athlete_event_results",
        user=mysql_config["user"], password=mysql_config["password"]
    ).load()
)

athlete_event_results_df.select(to_json(struct("*")).alias("value")).write \
    .format("kafka") \
    .options(**kafka_options(kafka_config["input_topic"])) \
    .option("topic", kafka_config["input_topic"]) \
    .save()

# Етап 4: Читання Kafka Stream і парсинг JSON
print("[INFO] Stage 4: Reading Kafka Stream and parsing JSON")
kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options(kafka_config["input_topic"]))
    .option("startingOffsets", "earliest")
    .load()
)

schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("event", StringType(), True)
])

parsed_stream_df = kafka_stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Етап 5: Збагачення та агрегування
def log_stage(stage_num, message):
    print(f"[INFO] Stage {stage_num}: {message}")

log_stage(5, "Enrichment and average calculation")

enriched_stream_df = parsed_stream_df.join(filtered_athlete_bio_df, on="athlete_id", how="inner")

agg_stream_df = (
    enriched_stream_df
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        round(avg("height"), 2).alias("avg_height"),
        round(avg("weight"), 2).alias("avg_weight")
    )
    .withColumn("timestamp", current_timestamp())
    .withColumn("author", lit("eugene"))
)

# Етап 6: Запис результатів у Kafka та MySQL
def foreach_batch_function(batch_df, batch_id):
    log_stage(6, f"Processing micro-batch {batch_id}")
    if batch_df.isEmpty():
        print(f"[BATCH {batch_id}] -> No new data, skipping.")
        return

    print(f"[BATCH {batch_id}] -> First 10 records after aggregation:")
    batch_df.show(10, truncate=False)

    log_stage(6, "Writing to Kafka")
    batch_as_json = batch_df.select(to_json(struct("*")).alias("value"))
    batch_as_json.write \
        .format("kafka") \
        .options(**kafka_options(kafka_config["output_topic"])) \
        .option("topic", kafka_config["output_topic"]) \
        .save()

    log_stage(6, "Writing to MySQL")
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "athlete_enriched_agg_eugene") \
        .option("user", mysql_config["user"]) \
        .option("password", mysql_config["password"]) \
        .mode("append") \
        .save()

# Запуск стримінгу
log_stage(6, "Starting streaming")
query = (
    agg_stream_df.writeStream
    .foreachBatch(foreach_batch_function)
    .outputMode("complete")  
    .option("checkpointLocation", "/mnt/data/checkpoints/streaming_query")
    .start()
)

query.awaitTermination()