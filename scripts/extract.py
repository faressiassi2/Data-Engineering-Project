from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, MinMaxScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("exam project").getOrCreate()
	
df1 = spark.read.csv("./flights.csv", header=True, inferSchema=True)
df2 = spark.read.csv("./airports.csv", header=True, inferSchema=True)


