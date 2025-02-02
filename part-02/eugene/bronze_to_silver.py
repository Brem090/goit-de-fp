from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
import re, os, logging

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    if text is None:
        return None
    # Видалення небажаних символів
    return re.sub(r"[^a-zA-Z0-9,.'\" ]", '', str(text))

# Створення UDF для очистки тексту
clean_text_udf = udf(clean_text, StringType())

def process_table(spark, table_name):
    # Шлях до вхідних даних
    bronze_path = f"bronze/{table_name}"
    if not os.path.exists(bronze_path):
        logger.error(f"Bronze table {bronze_path} not found!")
        return
    
    # Зчитування даних з Parquet-файлу
    df = spark.read.parquet(bronze_path)
    
    # Перевірка та заповнення відсутніх значень у стовпці medal значенням "No Medal"
    if "medal" in df.columns:
        df = df.fillna({"medal": "No Medal"})
    
    # Пошук усіх текстових колонок
    text_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    
    # Очищення текстових колонок за допомогою select
    df = df.select(
        *[clean_text_udf(col(c)).alias(c) if c in text_cols else col(c) for c in df.columns]
    )

    # Видалення дублікатів рядків
    df = df.dropDuplicates()

    # Логування попереднього перегляду
    logger.info(f"Data preview after cleaning for {table_name}:")
    df.show(20)  
    
    # Запис очищених даних у Parquet у папку silver
    silver_path = f"silver/{table_name}"
    df.write.parquet(silver_path, mode="overwrite")
    logger.info(f"Processed {table_name} to {silver_path}")

if __name__ == "__main__":
    # Ініціалізація SparkSession
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()

    # Список таблиць
    tables = ["athlete_bio", "athlete_event_results"]
    
    # Обробка кожної таблиці
    for table in tables:
        process_table(spark, table)
    
    # Завершення сесії 
    spark.stop()