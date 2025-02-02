from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, stddev, lit, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # SparkSession з ім'ям додатку "SilverToGold"
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

    # Схема для athlete_bio
    bio_schema = StructType([
        StructField("athlete_id", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("weight", DoubleType(), True),
        StructField("height", DoubleType(), True)
    ])

    # Зчитування даних з папок silver/athlete_bio та silver/athlete_event_results
    bio_df = spark.read.parquet("silver/athlete_bio", schema=bio_schema)
    results_df = spark.read.parquet("silver/athlete_event_results")

    # Вибір колонок з athlete_event_results та обробка відсутніх значень у колонці medal
    results_selected = results_df.select("athlete_id", "sport", "medal") \
        .fillna({"medal": "No Medal"})

    # Вибір колонок з athlete_bio
    bio_selected = bio_df.select("athlete_id", "sex", "country_noc", "weight", "height")

    # Об'єднання датафреймів і фільтрація індивідуальних аномалій
    joined_df = bio_selected.join(results_selected, on="athlete_id", how="inner") \
        .filter((col("weight").isNotNull()) & (col("height").isNotNull())) \
        .filter((col("weight") > 30) & (col("weight") < 500) &  # Логічний діапазон для ваги
                (col("height") > 100) & (col("height") < 300))  # Логічний діапазон для зросту

    # Групування та обчислення середніх значень
    avg_stats = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        round(avg("weight"), 2).alias("avg_weight"),  # Округлення середньої ваги
        round(avg("height"), 2).alias("avg_height"),  # Округлення середнього зросту
        round(stddev("weight"), 2).alias("stddev_weight"),  # Округлення стандартного відхилення ваги
        round(stddev("height"), 2).alias("stddev_height")   # Округлення стандартного відхилення зросту
    ) \
    .withColumn("timestamp", current_timestamp())

    # Фільтрація потенційних аномалій за стандартним відхиленням
    filtered_stats = avg_stats.filter(
        (col("stddev_weight") < lit(50)) &  # Відсіювання великих відхилень у вазі
        (col("stddev_height") < lit(30))    # Те ж саме для зросту
    )

    # Логування результатів
    logger.info("Final aggregated data preview:")
    filtered_stats.show()

    # Запис результатів з оптимізацією партицій
    filtered_stats.repartition("sport", "medal").write.parquet("gold/avg_stats", mode="overwrite")

    # Завершення сесії
    spark.stop()