from pyspark.sql import *
from pyspark.sql.functions import *

# Create SparkSession with Hive support
spark = SparkSession.builder .master("local") .appName("projectbit") .enableHiveSupport() .getOrCreate()

# Read data from PostgreSQL
df = spark.read.format("jdbc") .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver") .option("dbtable", "bitcoin_2025").option("user", "consultants").option("password", "WelcomeItc@2022").load()

# Print schema to verify structure
df.printSchema()

df.write.mode("overwrite").saveAsTable("project2024.hasancoin")
print("Successfully Load to Hive")


