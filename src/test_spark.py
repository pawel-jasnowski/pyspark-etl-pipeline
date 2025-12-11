# # src/test_spark.py
import os

print("SPARK test")

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
