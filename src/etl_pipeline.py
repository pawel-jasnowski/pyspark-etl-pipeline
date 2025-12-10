# src/etl_pipeline.py
import logging
import os
import zipfile
import sys
import psycopg2

from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, current_timestamp, struct, udf
from pyspark.sql.types import DecimalType, TimestampType, StructType, StructField, BooleanType, StringType
from models import RawTransaction, ValidationError
from pathlib import Path

from config import HIGH_RISK_COUNTRIES, HIGH_AMOUNT_THRESHOLD

#TODO implementacja połączenia do bazy przed TRANSOFORM - sprawdzenie SCHEMA na bazie i dodanie pole LAST_TIME_UPDATE

load_dotenv()

python_executable = sys.executable
# env for this process
os.environ['PYSPARK_PYTHON'] = python_executable
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable

logging.info(f"PYSPARK_PYTHON set to: {python_executable}")

LOG_DIR = "logs"
LOG_FILE = "pipeline.log"
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, LOG_FILE)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='a'), # write to pipeline.log // append to file
        logging.StreamHandler()             # print to console
    ]
)

def zip_source_for_spark(source_dir: str = "src", zip_name: str = "src.zip"):
    """
    Packs the source directory into a zip file for Spark to distribute to workers.
    This ensures that UDFs have access to all necessary modules.
    """
    logging.info(f"source directory for ZIP '{source_dir}' into '{zip_name}' for Spark...")

    # pathlib to work with paths
    source_path = Path(source_dir)
    zip_path = Path(zip_name)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in source_path.rglob('*.py'):
            # Dodajemy plik do zip, zachowując jego względną ścieżkę wewnątrz 'src'
            # np. 'src/models.py' jest zapisywane jako 'models.py' w archiwum
            zipf.write(file_path, file_path.relative_to(source_path))

    logging.info("source code packaged successfully")
    return str(zip_path)

# def create_spark_session() -> SparkSession:   # BACKUP
#     """spark session + PostgreSQL driver"""
#
#     spark = SparkSession.builder \
#         .appName("AMLPipeline") \
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
#         .master("local[*]") \
#         .getOrCreate()
#     return spark

def create_spark_session() -> SparkSession:
    """
    Packs the source code and creates a Spark session configured
    to distribute the code to workers.
    """
    logging.info("creating Spark session")

    # zip source code
    py_files_zip = zip_source_for_spark()
    # --------------------------------

    spark = SparkSession.builder \
        .appName("AMLPipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .master("local[*]") \
        .config("spark.submit.pyFiles", py_files_zip) \
        .getOrCreate()

    logging.info("spark session created successfully")
    return spark

#validation schema for @udf
validation_schema = StructType([
    StructField("is_valid", BooleanType(), nullable=False),
    StructField("validation_error", StringType(), nullable=True)
])

@udf(returnType=validation_schema)
def validate_transaction(struct_col) -> dict:
    """
    UDF, który przyjmuje wiersz jako strukturę i waliduje go
    za pomocą modelu Pydantic.
    """
    try:
        # Konwertujemy obiekt Row (strukturę) Sparka na słownik Pythona
        row_dict = struct_col.asDict(recursive=True)
        # Walidacja za pomocą Pydantic
        RawTransaction.model_validate(row_dict)
        return {"is_valid": True, "validation_error": None}
    except ValidationError as e:
        # Zwracamy informację o błędzie w czytelnym formacie JSON
        return {"is_valid": False, "validation_error": e.json()}
    except Exception as e:
        # Łapiemy inne, nieoczekiwane błędy
        return {"is_valid": False, "validation_error": f"Unexpected error: {str(e)}"}


def find_new_files(directory: str) -> list[str]| None:
    """finding TRANSACTION files to be processed"""
    logging.info(f"searching for files to be processed in {directory}")
    try:
        files = [os.path.join(directory, filename)
                 for filename in os.listdir(directory)
                 if filename.endswith('.csv') and os.path.isfile(os.path.join(directory, filename))]
    except FileNotFoundError as e:
        logging.error(f"{e} - `{directory}` does not exist")
        return None
    if not files:
        logging.info("no transaction files found")
        return None

    return files

def move_file(source_path: str, success: bool):
    """move file after processing"""

    if not source_path:
        return

    os.makedirs("data/done", exist_ok=True)
    os.makedirs("data/error", exist_ok=True)

    file_name = os.path.basename(source_path)       #get the file name out of the source_path
    if success:
        destination_path = "data/done"
    else:
        destination_path = "data/error"
        
    destination_path = os.path.join(destination_path, file_name)
    os.rename(source_path, destination_path)


def extract_data(spark: SparkSession, file_path: str) -> DataFrame:
    """read data to spark session"""

    df = spark.read.csv(
        path=file_path,
        sep="|",  # Delimiter (default: comma)
        header=True,  # First row as column names
        inferSchema=True,  # Auto-detect data types
        encoding="UTF-8"  # Character encoding
    )
    return df


# def transform_data(df: DataFrame) -> (DataFrame, DataFrame):         ## BCKUP
#     """transform data and create dataframe with alerts"""
#
#     # check data type: 'amount' to Decimal / 'timestamp' to Timestamp
#     transformed_df = df.withColumn("amount", col("amount").cast(DecimalType(18, 2))) \
#         .withColumn("timestamp", col("timestamp").cast(TimestampType()))
#
#     # print("transformed data:")
#     # transformed_df.printSchema()
#     # return transformed_df
#
#     # ALERTS LOGIC:
#     alerts_logic = when(col("amount") > HIGH_AMOUNT_THRESHOLD, "High Amount Transaction").when(
#         col("country_code").isin(HIGH_RISK_COUNTRIES), "High Risk Country Transaction").otherwise(lit(None))
#
#     df_with_alerts = transformed_df.withColumn("alert_reason", alerts_logic)
#     df_alerts = df_with_alerts.filter(col("alert_reason").isNotNull())  # df with alerts only
#
#     # print("alert data:")
#     # df_alerts.printSchema()
#     # df_alerts.show(5)
#
#     return df_with_alerts, df_alerts

def transform_data(df: DataFrame) -> (DataFrame, DataFrame, DataFrame):
    logging.info("Starting Pydantic validation...")
    df_with_validation = df.withColumn("validation_result", validate_transaction(struct(*df.columns)))

    valid_df = df_with_validation.filter(col("validation_result.is_valid") == True).drop("validation_result")
    invalid_df = df_with_validation.filter(col("validation_result.is_valid") == False).select(
        *df.columns, col("validation_result.validation_error").alias("error_details")
    )

    valid_df.cache()
    invalid_df.cache()
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logging.info(f"Validation finished. Valid rows: {valid_count}, Invalid rows: {invalid_count}")

    if invalid_count > 0:
        logging.warning("Found invalid records. Sample of errors (JSON format):")
        invalid_df.select("error_details").show(truncate=False)

    logging.info("Transforming valid data...")
    transformed_df = valid_df.withColumn("amount", col("amount").cast(DecimalType(18, 2))) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    alerts_logic = when(col("amount") > HIGH_AMOUNT_THRESHOLD, "High Amount Transaction").when(
        col("country_code").isin(HIGH_RISK_COUNTRIES), "High Risk Country Transaction").otherwise(lit(None))

    df_with_alerts = transformed_df.withColumn("alert_reason", alerts_logic)
    alerts_df = df_with_alerts.filter(col("alert_reason").isNotNull())
    logging.info("Transformation finished.")
    return df_with_alerts, alerts_df, invalid_df


def load_data(df: DataFrame, table_name: str, mode: str = "append"):
    """saving from dataframe to DB"""

    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

    logging.info(f"saving data to db table: {table_name}")
    try:
    # data preparation
        df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("user", os.getenv("DB_USER")) \
            .option("password", os.getenv("DB_PASSWORD")) \
            .option("driver", "org.postgresql.Driver") \
            .option("isolationLevel", "NONE")\
        .mode(mode).save()
        logging.info(f"saving to DB: {table_name} successed")

    except Exception as e:
        logging.critical(f"exception: {e} - saving to db failed for table: '{table_name}' ", exc_info=True)
        raise e

# def db_schema(table_name:str, df_schema: dict):
#     """ensure to update db_Schema if needed"""
#
#     conn = None
#     cur = None
#     try:
#         conn = psycopg2.connect(
#             dbname=os.getenv("DB_NAME"),
#             user=os.getenv("DB_USER"),
#             password=os.getenv("DB_PASSWORD"),
#             host=os.getenv("DB_HOST"),
#             port=os.getenv("DB_PORT")
#         )
#         cur = conn.cursor()
#         #existing columns in db:
#         cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema ='public' and table_name = '{table_name}';")
#         existing_columns = {row[0] for row in cur.fetchall()}
#
#         spark_to_sql_map = {
#             'StringType()': 'VARCHAR(255)',
#             'DecimalType(18,2)': 'DECIMAL(18, 2)',
#             'TimestampType()': 'TIMESTAMP',
#             'LongType()': 'BIGINT',
#             'IntegerType()': 'INTEGER',
#             'DoubleType()': 'DOUBLE PRECISION'
#         }
#
#         df_columns = set(df_schema.keys())
#         missing_columns = df_columns - existing_columns
#
#         if not missing_columns:
#             logging.info(f"Schema for table '{table_name}' is up to date. No changes needed.")
#             return
#
#         logging.warning(f"Found missing columns in table '{table_name}': {missing_columns}. Starting migration.")
#
#         # Dodaj każdą brakującą kolumnę
#         for column_name in missing_columns:
#             # Przekształcamy obiekt typu Spark na string, aby go znaleźć w mapie
#             spark_type_str = str(df_schema[column_name])
#             sql_type = spark_to_sql_map.get(spark_type_str, 'TEXT')  # Domyślnie TEXT
#
#             alter_sql = f'ALTER TABLE public."{table_name}" ADD COLUMN "{column_name}" {sql_type};'
#             logging.info(f"Executing: {alter_sql}")
#             cur.execute(alter_sql)
#
#         # Zatwierdź zmiany w bazie danych
#         conn.commit()
#         logging.info(f"Schema migration for '{table_name}' completed successfully.")
#
#     except psycopg2.errors.UndefinedTable:
#     # Błąd, gdy tabela nie istnieje - to jest OK. Spark ją stworzy.
#     logging.warning(f"Table '{table_name}' does not exist. Spark will create it automatically.")
#     if conn:
#         conn.rollback()  # Wycofaj transakcję
#     except Exception as e:
#         logging.error(f"Error during schema migration for table '{table_name}'.", exc_info=True)
#     if conn:
#         conn.rollback()
#         raise e
#     finally:
#     # ZAWSZE zamykaj kursor i połączenie
#     if cur:
#         cur.close()
#     if conn:
#         conn.close()


# def main():           ## BCKUP
#     """main"""
#     logging.info("=========ETL PROCESSING START=========")
#     spark = None
#     try:
#         spark = create_spark_session()
#         raw_data_dir = "data/raw"
#         files = find_new_files(raw_data_dir)        # list of files to be processed
#
#         if not files:
#             logging.info("no files for processing")
#             spark.stop()
#             return
#
#         logging.info(f"number of files to processed: {len(files)}")
#
#         # ETL
#         # PROCESSING EVERY FILE FOUNd IN DATa/RAW folder
#         for file_path in files:
#             logging.info(f"processing of: {file_path}")
#             try:
#                 #EXTRACT
#                 raw_df = extract_data(spark, file_path)
#                 #TRANSFORM
#                 all_transactions, only_alerts_transactions = transform_data(raw_df)  # --> df_with_alerts, df_alerts
#                 #LOAD
#                 load_data(all_transactions, "transactions", mode="append")
#                 only_alerts_transactions.cache()
#                 if only_alerts_transactions.count() > 0:
#                     load_data(only_alerts_transactions, "alerts", mode="append")
#                 else: logging.info("no alerts found to save in db")
#
#                 move_file(file_path, success=True)
#
#             except Exception as e:
#                 logging.critical(f"critical error on file processing :{file_path}", exc_info=True)
#                 move_file(file_path, success=False)
#                 continue        #move to next file
#
#     except Exception as e:
#         logging.critical(f"critical error during SPARK session starting", exc_info=True)
#
#     finally:
#         if spark:
#             logging.info("job is finished ... stoping SPARK")
#             spark.stop()
#             logging.info("=========ETL PROCESSING END=========")

def main():
    logging.info("\n=============================================\n"
                 "=========    ETL PROCESSING START     =========\n"
                 "=============================================")
    spark = None
    try:
        spark = create_spark_session()
        raw_data_dir = "data/raw"
        files_to_process = find_new_files(raw_data_dir)

        if not files_to_process:
            logging.info("No new files for processing.")
            return

        logging.info(f"Number of files to process: {len(files_to_process)}")

        for file_path in files_to_process:
            logging.info(f"----- Processing file: {os.path.basename(file_path)} -----")
            try:
                raw_df = extract_data(spark, file_path)

                # POPRAWKA: Odbieramy 3 DataFrame'y
                all_transactions_df, alerts_only_df, invalid_records_df = transform_data(raw_df)

                # Zapisujemy tylko poprawne transakcje
                load_data(all_transactions_df, "transactions", mode="append")

                # Zapisujemy alerty (jeśli istnieją)
                alerts_only_df.cache()
                alerts_count = alerts_only_df.count()
                if alerts_count > 0:
                    logging.info(f"Found {alerts_count} alerts. Loading to 'alerts' table.")
                    load_data(alerts_only_df, "alerts", mode="append")
                else:
                    logging.info("No alerts found in this file.")

                # Zapisujemy błędne rekordy do kwarantanny (jeśli istnieją)
                invalid_records_df.cache()
                invalid_count = invalid_records_df.count()
                if invalid_count > 0:
                    logging.warning(f"Found {invalid_count} invalid records. Loading to 'quarantine' table.")
                    load_data(invalid_records_df, "quarantine", mode="append")

                move_file(file_path, success=True)

            except Exception as e:
                logging.critical(f"Critical error during processing file: {file_path}", exc_info=True)
                move_file(file_path, success=False)
                continue

    finally:
        if spark:
            logging.info("Job finished... stopping Spark session.")
            spark.stop()
        logging.info("\n=============================================\n"
                     "=========     ETL PROCESSING END      =========\n"
                     "=============================================")


if __name__ == "__main__":
    main()
