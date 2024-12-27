from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("IncrementalCumulativeVolumeLoad").enableHiveSupport().getOrCreate()

try:
    # Step 1: Set the last Cumulative_Volume value
    last_cumulative_volume = 36805902.179584436

    # Step 2: Load new data from PostgreSQL where Cumulative_Volume > last_cumulative_volume
    query = f'SELECT * FROM bitcoin_2025 WHERE "Cumulative_Volume" > {last_cumulative_volume}'

    more_data = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
        .option("driver", "org.postgresql.Driver") \
        .option("query", query) \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .load()

    # Step 3: Display the newly loaded data
    print("Newly Loaded Data:")
    more_data.show()

    # Step 4: Write the incremental data to Hive (append mode)
    more_data.write.mode("append").saveAsTable("bigdata_nov_2024.hasan_person")
    print("Incremental load based on Cumulative_Volume completed successfully.")

except Exception as e:
    print(f"Error during incremental load: {e}")

finally:
    # Stop the SparkSession
    spark.stop()
