from pyspark.sql import SparkSession

x = SparkSession.getActiveSession()
print(x)
