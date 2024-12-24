from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("projectbit").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").config("spark.executor.cores", "2").config("spark.sql.shuffle.partitions", "200") .enableHiveSupport()enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "bitcoin_2025").option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()
#done
# Add year and month columns for partitioning
df_with_partition_cols = df.withColumn("year", year(col("Datetime"))) .withColumn("month", month(col("Datetime")))
# Write data to Hive table with partitioning by year and month
df_with_partition_cols.repartition("year","month").write .mode("overwrite") .partitionBy("year", "month") .saveAsTable("project2024.hasan_bitcoin")

#df.write.mode("overwrite").saveAsTable("project2024.hasan_bitcoin")
print("Successfully Load to Hive")