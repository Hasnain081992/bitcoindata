import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Initialize Spark session
    spark = SparkSession.builder.master("local").appName("Incrementalload").enableHiveSupport().getOrCreate()
    yield spark
    spark.stop()

def test_incremental_load(spark):
    last_Cumulative_Volume = 36805900.826118246

    # Build the query to get data from PostgreSQL where Cumulative_Volume > last Cumulative_Volume
    query = "SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > {}".format(last_Cumulative_Volume)

    # Read data from PostgreSQL using the query
    new_data = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .option("query", query) \
        .load()

    # Check if data was loaded
    assert new_data.count() > 0, "No new data loaded!"

    # Optionally: Check for expected columns in the dataframe
    expected_columns = ["column1", "column2", "Cumulative_Volume"]  # Update with actual column names
    assert all(col in new_data.columns for col in expected_columns), "Missing expected columns"

    # Optionally: Save data to Hive and check if it's saved
    new_data.write.mode("append").saveAsTable("project2024.bitcoin_inc_team")

    # Verify data is written into Hive (you can check the row count or sample data)
    hive_data = spark.sql("SELECT * FROM project2024.bitcoin_inc_team LIMIT 10")
    assert hive_data.count() > 0, "No data found in Hive table"
