
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.ml.feature import PCA as PCAml
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.classification import LogisticRegression,RandomForestClassifier, NaiveBayes

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F

import numpy as np

labeled_features= (
	spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("msd/labeled_spectral_features.csv")
    )

#removing highly correlated columns and keep only three of them 
correlated_cols=['Spectral_Rolloff_Point_Overall_Average_1',
				 'Zero_Crossings_Overall_Average_1',
				 'Spectral_Flux_Overall_Average_1',
				 'Root_Mean_Square_Overall_Average_1'] 

new_columns=[i for i in labeled_features.columns if (i not in correlated_cols )]
#convert ffeatures to a vector
assembler = VectorAssembler(inputCols=labeled_features.columns[2:], outputCol="features")

pipeline = Pipeline(stages=[assembler])
pipelineFit = pipeline.fit(labeled_features)
data = pipelineFit.transform(labeled_features)

#create binary labels
def is_electric(genre):
    if genre is None or len(genre) == 0:
        return 0
    if genre == 'Electronic':
        return 1
    return 0

label_udf = F.udf(is_electric, IntegerType())

data = data.select(
    F.col('track_id'),
    F.col("features"),
    label_udf(F.col('genre')).alias('label')
)

#random under-sampling to balance dataset (training set)
training, test = data.randomSplit([0.8,0.2],seed=100)
training_major_0, training_minor_0 =training.filter("label==0").randomSplit([0.9, 0.1],seed=100)
training=training_minor_0.union(training.filter("label==1"))

training.cache()
test.cache()

#metrics and evaluation

def displayMetrics(temp,predictionCol="customPrediction"):
	total = temp.count()
	nP = temp.filter((F.col(predictionCol) == 1)).count()
	nN = temp.filter((F.col(predictionCol) == 0)).count()
	TP = temp.filter((F.col(predictionCol) == 1) & (F.col('label') == 1)).count()
	FP = temp.filter((F.col(predictionCol) == 1) & (F.col('label') == 0)).count()
	FN = temp.filter((F.col(predictionCol) == 0) & (F.col('label') == 1)).count()
	TN = temp.filter((F.col(predictionCol) == 0) & (F.col('label') == 0)).count()

	print('num positive: {}'.format(nP))
	print('num negative: {}'.format(nN))
	print('true positive: {}'.format(TP))
	print('true negative: {}'.format(TN))
	print('precision: {}'.format(TP / (TP + FP)))
	print('recall: {}'.format(TP / (TP + FN)))
	print('accuracy: {}'.format((TP + TN) / total))

evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction",  metricName="areaUnderROC")


#linear regression
lr = LogisticRegression( featuresCol='features', labelCol='label')
paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.1, 0.3, 0.5]) # regularization parameter
             .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) # Elastic Net Parameter (Ridge = 0)
            .addGrid(lr.maxIter, [10, 20, 50]) #Number of iterations
             .build())

cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training)
pred = cvModel.transform(test)

def apply_custom_threshold(probability, threshold=0.45):
    return int(probability[1] > threshold)

apply_custom_threshold_udf = F.udf(apply_custom_threshold, IntegerType())

temp = pred.withColumn("customPrediction", apply_custom_threshold_udf(F.col("probability")))
displayMetrics(temp)


"""
without cross validation:
num positive: 28556
num negative: 55368
true positive: 5350
true negative: 52644
precision: 0.18735116963160106
recall: 0.6626207579886054
accuracy: 0.6910299795052667

In [87]: def apply_custom_threshold(probability, threshold=0.1):

num positive: 84443
num negative: 79
true positive: 8096
true negative: 70
precision: 0.09587532418317682
recall: 0.9988895743368291
accuracy: 0.09661389933981686


In [85]: def apply_custom_threshold(probability, threshold=0.4):
num positive: 50609
num negative: 33913
true positive: 6926
true negative: 32734
precision: 0.13685312889011836
recall: 0.8545342381246145
accuracy: 0.469226946830411

* def apply_custom_threshold(probability, threshold=0.5):

num positive: 27223
num negative: 57299
true positive: 5598
true negative: 54792
precision: 0.20563494104250082
recall: 0.6906847624922887
accuracy: 0.7144885355292113

th=0.45
num positive: 38015
num negative: 46507
true positive: 6331
true negative: 44733
precision: 0.16653952387215573
recall: 0.781122763726095
accuracy: 0.6041503987127612



"""


#random forest
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)

paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [10,15,20]) # maximum number of trees
             .addGrid(rf.maxDepth, [5, 10, 15]) # max depth
             .build())

cv = CrossValidator(estimator=rf, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training)
pred = cvModel.transform(test)


def apply_custom_threshold(probability, threshold=0.5):
    return int(probability[1] > threshold)

apply_custom_threshold_udf = F.udf(apply_custom_threshold, IntegerType())

temp = pred.withColumn("customPrediction", apply_custom_threshold_udf(F.col("probability")))

displayMetrics(temp)



"""
without cv:
th=0.1
num positive: 21200
num negative: 62724
true positive: 5314
true negative: 59964
precision: 0.25066037735849056
recall: 0.6581620014862521
accuracy: 0.77782279204995

In [107]: def apply_custom_threshold(probability, threshold=0.15):
num positive: 15183
num negative: 68741
true positive: 4494
true negative: 65161
precision: 0.2959889349930844
recall: 0.5566014367104285
accuracy: 0.8299771221581431


th=0.05
num positive: 51360
num negative: 32564
true positive: 7208
true negative: 31698
precision: 0.14034267912772586
recall: 0.8927421352489472
accuracy: 0.4635861017110719

In [109]: def apply_custom_threshold(probability, threshold=0.07):

num positive: 61449
num negative: 23073
true positive: 7311
true negative: 22279
precision: 0.11897671239564517
recall: 0.90203578038248
accuracy: 0.35008636804618914

cross val 
th=0.1
num positive: 77558
num negative: 6964
true positive: 8067
true negative: 6926
precision: 0.10401248098197478
recall: 0.995311536088834
accuracy: 0.17738576938548545

*th=0.5
num positive: 23803
num negative: 60719
true positive: 6105
true negative: 58719
precision: 0.2564802755955132
recall: 0.7532387415175817
accuracy: 0.7669482501597217

th=0.6
num positive: 17441
num negative: 66483
true positive: 5360
true negative: 63769
precision: 0.3073218278768419
recall: 0.6638593014614813
accuracy: 0.8237095467327582

th=0.4
num positive: 32026
num negative: 52496
true positive: 6769
true negative: 51160
precision: 0.2113595203896834
recall: 0.8351634793337446
accuracy: 0.6853718558481815

"""

#Naive Bayes

#random forest
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

paramGrid = (ParamGridBuilder()
             .addGrid(nb.smoothing, [0.0, 0.5, 1.0]) # regularization parameter
             .build())


cv = CrossValidator(estimator=nb, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training)
pred = cvModel.transform(test)

def apply_custom_threshold(probability, threshold=0.07):
    return int(probability[1] > threshold)

apply_custom_threshold_udf = F.udf(apply_custom_threshold, IntegerType())

temp = pred.withColumn("customPrediction", apply_custom_threshold_udf(F.col("probability")))

displayMetrics(temp)

"""
th=0.4
num positive: 33426
num negative: 50498
true positive: 5187
true negative: 47611
precision: 0.15517860348231916
recall: 0.6424324993807282
accuracy: 0.6291168199799819

th=0.5
num positive: 28641
num negative: 55283
true positive: 4617
true negative: 51826
precision: 0.16120247198072693
recall: 0.571835521426802
accuracy: 0.672548972880225


th=0.4
num positive: 38990
num negative: 44934
true positive: 5766
true negative: 42626
precision: 0.14788407283918953
recall: 0.7141441664602427
accuracy: 0.5766169391354082


after cross validation
th=0.3
num positive: 38643
num negative: 45879
true positive: 5598
true negative: 43372
precision: 0.1448645291514634
recall: 0.6906847624922887
accuracy: 0.5793757838195973


*th=0.4
num positive: 33135
num negative: 51387
true positive: 4975
true negative: 48257
precision: 0.15014335295005282
recall: 0.6138186304750154
accuracy: 0.6298005253070207

th=0.5
num positive: 28532
num negative: 55990
true positive: 4454
true negative: 52339
precision: 0.1561054254871723
recall: 0.5495373226403455
accuracy: 0.6719315681124441

"""