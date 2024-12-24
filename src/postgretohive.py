from pyspark.sql import *
from pyspark.sql.functions import *

# Create SparkSession with Hive support
spark = SparkSession.builder .master("local") .appName("projectbit") .enableHiveSupport() .getOrCreate()

# Read data from PostgreSQL
df = spark.read.format("jdbc") .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver") .option("dbtable", "bitcoin_2025").option("user", "consultants").option("password", "WelcomeItc@2022").load()

# Print schema to verify structure
df.printSchema()

# Partition the data based on a column (e.g., "year")
partitioned_column = "year"  # Replace this with a relevant column from your dataset
df_with_partition = df.withColumn(partitioned_column, year(col("timestamp")))  # Example partition logic

# Write data to Hive table with partitioning
df_with_partition.write.mode("overwrite").partitionBy(partitioned_column).saveAsTable("project2024.hasan_bitcoin")

print("Successfully loaded to Hive with partitioning")
