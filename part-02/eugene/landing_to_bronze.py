import requests
from pyspark.sql import SparkSession
import os
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Функція для завантаження даних з URL
def download_data(table_name):
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    local_file = f"{table_name}.csv"
    try:
         # Перевірка, чи файл вже завантажено
        if os.path.exists(local_file):
            logger.info(f"File {local_file} already downloaded.")
            return

        logger.info(f"Downloading data from {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Перевірка на помилки HTTP
        
        # Збереження вмісту у локальний файл
        with open(local_file, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Successfully downloaded {local_file}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {table_name}: {e}")
        raise

# Функція для обробки таблиці
def process_table(spark, table_name):
    download_data(table_name)

    # Читання CSV-файлу у DataFrame
    df = spark.read.csv(f"{table_name}.csv", header=True, inferSchema=True, mode="DROPMALFORMED")
    
    # Перевірка, чи DataFrame не порожній
    if df.count() == 0:
        logger.warning(f"Table {table_name} is empty or has malformed data.")
        return
    
    # Логування попереднього перегляду даних
    logger.info(f"Preview of table {table_name}:")
    df.show(20)
    
    # Шлях для збереження даних у форматі Parquet
    output_path = f"bronze/{table_name}"
    df.write.parquet(output_path, mode="overwrite")
    logger.info(f"Saved table {table_name} to {output_path}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    # Список таблиць для обробки
    tables = ["athlete_bio", "athlete_event_results"]

    # Обробка кожної таблиці
    for table in tables:
        process_table(spark, table)
    
    spark.stop()