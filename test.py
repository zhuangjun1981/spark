import os
import sys

# Set explicit paths, important
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Set explicit paths
os.environ['SPARK_HOME'] = r"C:\Users\wood_\anaconda3\envs\spark\Lib\site-packages\pyspark"
os.environ['HADOOP_HOME'] = r"C:\Users\wood_\anaconda3\envs\spark\Lib\site-packages\pyspark"
sys.path.append(r"C:\Users\wood_\anaconda3\envs\spark\Lib\site-packages\pyspark\bin")

from pyspark.sql import SparkSession

# Create SparkSession with explicit configuration
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# # More robust configuration
# spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName("MySparkApp") \
#     .config("spark.driver.host", "localhost") \
#     .config("spark.pyspark.python", sys.executable) \
#     .getOrCreate()

# Your data creation and processing
data = [("java", "20000"), ("python", "100000"), ("scala", "3000")]
columns = ["language", "users_count"]
df = spark.createDataFrame(data).toDF(*columns)
df.show()


