from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg
from pyspark.sql.functions import col, lit, when, sum, avg
from pyspark.sql.functions import sum, format_number

spark = SparkSession.builder \
    .appName("Transfrom") \
    .master("local[*]") \
    .getOrCreate()
path= "/tmp/bigdata_nov_2024/Hasnain//btcusd.csv"
data1 = spark.read.csv(path,header = True, inferSchema = True)
data1.show()