from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Incrementalload").enableHiveSupport().getOrCreate()

# Step 1: Set the last Cumulative_Volume value manually (this can be dynamic in your production environment)
last_Cumulative_Volume = 36805902.179584436
print("Max Cumulative_Volume: {}".format(last_Cumulative_Volume))  # Print the last Cumulative_Volume value

# Step 2: Build the query to get data from PostgreSQL where Cumulative_Volume > last Cumulative_Volume
query = "SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > {}".format(last_Cumulative_Volume)

# Step 3: Read data from PostgreSQL using the query
new_data = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("query", query) \
    .load()

# Show the new data that was loaded
new_data.show()

# Step 4: Write the new data to Hive
new_data.write.mode("append").saveAsTable("project2024.bitcoin1_inc_team")

print("Successfully loaded data into Hive")

# Optionally: Check for further transformations or actions
# For example, if you want to join with other data, you can do so as follows:
# df2 = spark.read.csv("path/to/other_file.csv", header=True, inferSchema=True)
# joined_df = new_data.join(df2, on=["ID"], how="inner")
# joined_df.show()