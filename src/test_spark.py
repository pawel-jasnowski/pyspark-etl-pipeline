# # src/test_spark.py
import os
import sys

python_executable_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_executable_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable_path

print("SPARK test")
print(f"!!! Forcing PYSPARK_PYTHON to: {python_executable_path}")

# env
print(f"JAVA_HOME: {os.getenv('JAVA_HOME')}")
print(f"SPARK_HOME: {os.getenv('SPARK_HOME')}")
print(f"PYTHONPATH: {os.getenv('PYTHONPATH')}")

try:
    from pyspark.sql import SparkSession

    print("\nstep1: SPARK import")

    spark = (
        SparkSession.builder.appName("SparkConnectionTest")
        .master("local[*]")
        .getOrCreate()
    )

    print("\nstep2: SPARK session OK")

    # simple SPARK operation
    data = [("Test", 1)]
    df = spark.createDataFrame(data, ["name", "value"])
    df.show()

    print("\nstep3: df created and shown")

    spark.stop()
    print("\ntest is over - success")

except Exception as e:
    print(f"\nCRITICAL error")
    print(f"error: {type(e).__name__}")
    print(f"error: {e}")

    import traceback  # error trace

    traceback.print_exc()
