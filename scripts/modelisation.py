from extract import *
from analyse_traitement import *

#on creer une pipeline
strIdx = StringIndexer(inputCol = "Carrier", outputCol = "CarrierIdx")
catVect = VectorAssembler(inputCols = ["CarrierIdx", "DayofMonth", "DayOfWeek", "OriginAirportID", "DestAirportID"], outputCol="catFeatures")
catIdx = VectorIndexer(inputCol = catVect.getOutputCol(), outputCol = "idxCatFeatures")
numVect = VectorAssembler(inputCols = ["DepDelay"], outputCol="numFeatures")
minMax = MinMaxScaler(inputCol = numVect.getOutputCol(), outputCol="normFeatures")
featVect = VectorAssembler(inputCols=["idxCatFeatures", "normFeatures"], outputCol="features")
lr = LogisticRegression(labelCol="Label",featuresCol="features",maxIter=10,regParam=0.3)
pipeline = Pipeline(stages=[strIdx, catVect, catIdx, numVect, minMax, featVect, lr])


piplineModel = pipeline.fit(train_set)
