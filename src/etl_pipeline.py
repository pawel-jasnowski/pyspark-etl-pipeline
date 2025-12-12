# src/etl_pipeline.py
import os
import sys
import traceback
import zipfile
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, struct, udf, when
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import HIGH_AMOUNT_THRESHOLD, HIGH_RISK_COUNTRIES
from models import RawTransaction, ValidationError

load_dotenv()

python_executable = sys.executable
# env for this process
os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable


def zip_source_for_spark(source_dir: str = "src", zip_name: str = "src.zip"):
    """
    Packs the source directory into a zip file for Spark to distribute to workers.
    This ensures that UDFs have access to all necessary modules.
    """
    print(f"preparing zip file for spark workers")

    # pathlib to work with paths
    source_path = Path(source_dir)
    zip_path = Path(zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in source_path.rglob("*.py"):
            zipf.write(file_path, file_path.relative_to(source_path))

    print("source code packed successfully")
    return str(zip_path)


def create_spark_session() -> SparkSession:
    """
    Packs the source code and creates a Spark session configured
    to distribute the code to workers.
    """
    print("creating spark session")

    # zip source code
    py_files_zip = zip_source_for_spark()
    # --------------------------------

    spark = (
        SparkSession.builder.appName("AMLPipeline")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        .master("local[*]")
        .config("spark.submit.pyFiles", py_files_zip)
        .getOrCreate()
    )

    print("spark session created successfully")
    return spark


# validation schema for @udf
validation_schema = StructType(
    [
        StructField("is_valid", BooleanType(), nullable=False),
        StructField("validation_error", StringType(), nullable=True),
    ]
)


@udf(returnType=validation_schema)
def validate_transaction(struct_col) -> dict:
    """
    UDF user-defined function
    """
    try:
        # SPARK Row to Python dict
        row_dict = struct_col.asDict(recursive=True)
        # pydantic validation
        RawTransaction.model_validate(row_dict)
        return {"is_valid": True, "validation_error": None}
    except ValidationError as e:
        # return error in json format
        return {"is_valid": False, "validation_error": e.json()}
    except Exception as e:
        # unexpected errors
        return {"is_valid": False, "validation_error": f"Unexpected error: {str(e)}"}


def find_new_files(directory: str) -> list[str] | None:
    """
    Finding TRANSACTION files to be processed
    """
    print(f"searching for files to be processed in {directory}")
    try:
        files = [
            os.path.join(directory, filename)
            for filename in os.listdir(directory)
            if filename.endswith(".csv")
            and os.path.isfile(os.path.join(directory, filename))
        ]
    except FileNotFoundError as e:
        print(f"{e} - `{directory}` does not exist")
        return None
    if not files:
        print("no transaction files found")
        return None

    return files


def move_file(source_path: str, success: bool):
    """
    Move file after processing
    """

    if not source_path:
        return

    os.makedirs("data/done", exist_ok=True)
    os.makedirs("data/error", exist_ok=True)

    file_name = os.path.basename(
        source_path
    )  # get the file name out of the source_path
    if success:
        destination_path = "data/done"
    else:
        destination_path = "data/error"

    destination_path = os.path.join(destination_path, file_name)
    os.rename(source_path, destination_path)


def extract_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read data to spark session
    """

    df = spark.read.csv(
        path=file_path,
        sep="|",  # Delimiter (default: comma)
        header=True,  # First row as column names
        inferSchema=True,  # Auto-detect data types
        encoding="UTF-8",  # Character encoding
    )
    df = df.withColumn("source_file", lit(os.path.basename(file_path)))
    return df


def transform_data(df: DataFrame) -> (DataFrame, DataFrame, DataFrame):
    print("Starting Pydantic validation...")
    df_with_validation = df.withColumn(
        "validation_result", validate_transaction(struct(*df.columns))
    )

    valid_df = df_with_validation.filter(
        col("validation_result.is_valid") == True
    ).drop("validation_result")
    invalid_df = df_with_validation.filter(
        col("validation_result.is_valid") == False
    ).select(
        *df.columns, col("validation_result.validation_error").alias("error_details")
    )

    valid_df.cache()
    invalid_df.cache()
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    print(
        f"Validation finished. Valid rows: {valid_count}, Invalid rows: {invalid_count}"
    )

    if invalid_count > 0:
        print("Found invalid records. Sample of errors (JSON format):")
        invalid_df.select("error_details").show(truncate=False)

    print("Transforming valid data...")
    transformed_df = valid_df.withColumn(
        "amount", col("amount").cast(DecimalType(18, 2))
    ).withColumn("timestamp", col("timestamp").cast(TimestampType()))

    alerts_logic = (
        when(col("amount") > HIGH_AMOUNT_THRESHOLD, "High Amount Transaction")
        .when(
            col("country_code").isin(HIGH_RISK_COUNTRIES),
            "High Risk Country Transaction",
        )
        .otherwise(lit(None))
    )

    df_with_alerts = transformed_df.withColumn("alert_reason", alerts_logic)
    alerts_df = df_with_alerts.filter(col("alert_reason").isNotNull())
    print("Transformation finished.")
    return df_with_alerts, alerts_df, invalid_df


def load_data(df: DataFrame, table_name: str, mode: str = "append"):
    """
    Saving dataframe to DB
    """

    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

    print(f"saving data to db table: {table_name}")
    try:
        # data preparation
        df.write.format("jdbc").option("url", db_url).option(
            "dbtable", table_name
        ).option("user", os.getenv("DB_USER")).option(
            "password", os.getenv("DB_PASSWORD")
        ).option(
            "driver", "org.postgresql.Driver"
        ).option(
            "isolationLevel", "NONE"
        ).mode(
            mode
        ).save()
        print(f"saving to DB: {table_name} successed")

    except Exception as e:
        print(f"exception: {e} - saving to db failed for table: '{table_name}' ")
        traceback.print_exc()
        raise e


##################################### MAIN ##########################################


def main():
    print(
        "\n=============================================\n"
        "=========    ETL PROCESSING START     =========\n"
        "============================================="
    )
    spark = None
    try:
        spark = create_spark_session()
        raw_data_dir = "data/raw"
        files_to_process = find_new_files(raw_data_dir)

        if not files_to_process:
            print("no new files for processing")
            return

        print(f"Number of files to process: {len(files_to_process)}")

        for file_path in files_to_process:
            print(f"----- Processing file: {os.path.basename(file_path)} -----")
            try:
                raw_df = extract_data(spark, file_path)

                # 3xdataframe
                all_transactions_df, alerts_only_df, invalid_records_df = (
                    transform_data(raw_df)
                )

                # bad records if exists
                invalid_records_df.cache()
                invalid_count = invalid_records_df.count()
                if invalid_count > 0:

                    print(
                        f"Found {invalid_count} invalid records. Loading to 'quarantine' table."
                    )
                    quarantine_df = invalid_records_df.select(
                        [col(c).cast(StringType()) for c in invalid_records_df.columns]
                    )
                    print(f"schema for quarantine table is:")
                    quarantine_df.printSchema()
                    load_data(quarantine_df, "quarantine", mode="append")
                    raise ValueError(f"invalid records inside the file")  # go to EXCEPT

                else:
                    # only correct transactions
                    load_data(all_transactions_df, "transactions", mode="append")

                    # only alerts if exists
                    alerts_only_df.cache()
                    alerts_count = alerts_only_df.count()
                    if alerts_count > 0:
                        print(
                            f"Found {alerts_count} alerts. Loading to 'alerts' table."
                        )
                        load_data(alerts_only_df, "alerts", mode="append")
                    else:
                        print("No alerts found in this file.")

                    move_file(file_path, success=True)

            except Exception as e:
                print(
                    f"file: {os.path.basename(file_path)} is invalid. check quarantine table"
                )
                print(f"reason: {e}")
                move_file(file_path, success=False)
                continue

    finally:
        if spark:
            print("job finished... stopping spark session.")
            spark.stop()
        print(
            "\n=============================================\n"
            "=========     ETL PROCESSING END      =========\n"
            "============================================="
        )


if __name__ == "__main__":
    main()
