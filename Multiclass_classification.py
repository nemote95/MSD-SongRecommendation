
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.ml.feature import PCA as PCAml
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.classification import LogisticRegression,RandomForestClassifier, NaiveBayes,OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import functions as F

import numpy as np


labeled_features= (
	spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("msd/labeled_spectral_features.csv")
    )

correlated_cols=['Spectral_Rolloff_Point_Overall_Average_1',
				 'Zero_Crossings_Overall_Average_1',
				 'Spectral_Flux_Overall_Average_1',
				 'Root_Mean_Square_Overall_Average_1'] 

assembler = VectorAssembler(inputCols=labeled_features.columns[2:], outputCol="features")
indexer = StringIndexer(inputCol="genre", outputCol="label", handleInvalid='skip')
pipeline = Pipeline(stages=[assembler,indexer])
pipelineFit = pipeline.fit(labeled_features)
data = pipelineFit.transform(labeled_features)

training, test = data.randomSplit([0.8,0.2],seed=100)

lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
# Create ParamGrid for Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.1, 0.3, 0.5]) # regularization parameter
             .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) # Elastic Net Parameter (Ridge = 0)
             .build())
# Create 5-fold CrossValidator
cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training)

predictions = cvModel.transform(test)
# Evaluate best model
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions)
#to know the best regParm
cvModel.bestModel._java_obj.getRegParam()

# Metrics


def displayMetrics(pred):
	ev = MulticlassMetrics(pred.select(["label","prediction"]).rdd)

	# Overall statistics
	print("Accuracy = %s" % ev.accuracy)
	print("Precision = %s" % ev.precision())
	print("Recall = %s" % ev.recall())
	print("F1 Score = %s" % ev.fMeasure())


"""
Accuracy = 0.5706561605262536
Precision = 0.5706561605262536
Recall = 0.5706561605262536
F1 Score = 0.5706561605262536

"""

