import os
import logging
import datetime from datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import DecimalType, TimestampType
from dotenv import load_dotenv
from config import HIGH_RISK_COUNTRIES, HIGH_AMOUNT_THRESHOLD

load_dotenv()

LOG_DIR = "logs"
LOG_FILE = "pipeline.log"
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, LOG_FILE)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='a'), # write to pipeline.log
        logging.StreamHandler()             # print to console
    ]
)

def create_spark_session() -> SparkSession:
    """spark session + PostgreSQL driver"""

    spark = SparkSession.builder \
        .appName("AMLPipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .master("local[*]") \
        .getOrCreate()
    return spark


def find_new_files(directory: str) -> list[str]| None:
    """finding TRANSACTION files to be processed"""
    logging.info(f"searching for files to be processed in {directory}")
    try:
        files = [os.path.join(directory, filename)
                 for filename in os.listdir(directory)
                 if filename.endswith('.csv') and os.path.isfile(os.path.join(directory, filename))]
    except FileNotFoundError as e:
        logging.error(f'{e} - `{directory}` does not exist')
        return None
    if not files:
        logging.info('no transaction files found')
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


def transform_data(df: DataFrame) -> (DataFrame, DataFrame):
    """transform data and create dataframe with alerts"""

    # check data type: 'amount' to Decimal / 'timestamp' to Timestamp   
    transformed_df = df.withColumn("amount", col("amount").cast(DecimalType(18, 2))) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # print("transformed data:")
    # transformed_df.printSchema()
    # return transformed_df

    # ALERTS LOGIC:
    alerts_logic = when(col("amount") > HIGH_AMOUNT_THRESHOLD, "High Amount Transaction").when(
        col("country_code").isin(HIGH_RISK_COUNTRIES), "High Risk Country Transaction").otherwise(lit(None))

    df_with_alerts = transformed_df.withColumn("alert_reason", alerts_logic)
    df_alerts = df_with_alerts.filter(col("alert_reason").isNotNull())  # df with alerts only 

    # print("alert data:")
    # df_alerts.printSchema()
    # df_alerts.show(5)

    return df_with_alerts, df_alerts


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
        logging.info(f'saving to DB: {table_name} successed')

    except Exception as e:
        logging.critical(f"exception: {e} - saving to db failed for table: '{table_name}' ", exc_info=True)
        raise e

def main():
    """main"""
    logging.info("=========ETL PROCESSING START=========")
    spark = None
    try:
        spark = create_spark_session()
        raw_data_dir = "data/raw"
        files = find_new_files(raw_data_dir)        # list of files to be processed

        if not files:
            logging.info("no files for processing")
            spark.stop()
            return

        logging.info(f"number of files to processed: {len(files)}")

        # ETL
        # PROCESSING EVERY FILE FOUNd IN DATa/RAW folder
        for file_path in files:
            logging.info(f"processing of: {file_path}")
            try:
                #EXTRACT
                raw_df = extract_data(spark, file_path)
                #TRANSFORM
                all_transactions, only_alerts_transactions = transform_data(raw_df)  # --> df_with_alerts, df_alerts
                #LOAD
                load_data(all_transactions, "transactions", mode="append")
                only_alerts_transactions.cache()
                if only_alerts_transactions.count() > 0:
                    load_data(only_alerts_transactions, "alerts", mode="append")
                else: logging.info("no alerts found to save in db")

                move_file(file_path, success=True)

            except Exception as e:
                logging.critical(f"critical error on file processing :{file_path}", exc_info=True)
                move_file(file_path, success=False)
                continue        #move to next file

    except Exception as e:
        logging.critical(f"critical error during SPARK session starting", exc_info=True)

    finally:
        if spark:
            logging.info("job is finished ... stoping SPARK")
            spark.stop()
            logging.info("=========ETL PROCESSING END=========")

if __name__ == "__main__":
    main()
