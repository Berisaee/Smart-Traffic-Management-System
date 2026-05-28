from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("Test").getOrCreate() 
print("Spark session created successfully") 
spark.stop() 
