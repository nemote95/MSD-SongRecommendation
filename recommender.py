from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

#load filterd tastes
tastes= (
	spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("msd/filtered_taste.csv")
    )

print(tastes.select("song_id").distinct().count())
print(tastes.select("uid").distinct().count())
"""
384546
1019318

"""
#find the active user with the most number of play and get the number of playes songs
active_user=tastes.groupBy("uid").agg({"count":"sum"}).orderBy("sum(count)",ascending=False).collect()[0].uid
print(tastes.filter(F.col("uid")==active_user).select("song_id").distinct().count())
"""
202
202/384546=0.0005252947631752768 #portion of the whole songs


#find active user 
active_user=tastes.groupBy("uid").agg({"song_id":"count"}).orderBy("count(song_id)",ascending=False).collect()[0].uid
print(tastes.filter(F.col("uid")==active_user).select("song_id").distinct().count())
4400
0.011442064148372367

"""
#get the total number of plays for each song
#filtering those with less than 100 plays for better visualization
song_popularity=tastes.groupBy("song_id").agg({"count":"sum"}).filter("sum(count)>100") 
song_popularity.write.csv("msd/song_popularity.csv")

#get user activities based on number of plays
users_activity=tastes.groupBy("uid").agg({"count":"sum"})
users_activity.write.csv("msd/users_activity.csv",header=True)

#filtere unuseful information and clean data
clean_tastes=tastes.join(song_popularity,"song_id")
important_users=clean_tastes.groupBy("uid").agg({"song_id":"count"}).filter("count(song_id)>20")
clean_tastes=clean_tastes.join(important_users,"uid")

#calculate ratings 
clean_tastes=clean_tastes.select(["uid","song_id","count"])
clean_tastes=clean_tastes.join(clean_tastes.groupBy("uid").agg({"count":"sum"}),"uid")
clean_tastes=clean_tastes.withColumn("rating_out_of_5", clean_tastes["count"]/clean_tastes["sum(count)"]*5)

#indexing and converting ids to integers
user_indexer = StringIndexer(inputCol="uid", outputCol="uid-int", handleInvalid='skip')
song_indexer = StringIndexer(inputCol="song_id", outputCol="song_int", handleInvalid='skip')

pipeline = Pipeline(stages=[user_indexer,song_indexer])
pipelineFit = pipeline.fit(clean_taste)
data = pipelineFit.transform(clean_taste)

data.write.csv("msd/clean_indx_tastes.csv",header=True)



#making sure that theres no unseen user
(training, test) = data.select(["uid","uid-int","song_int","rating_out_of_5"]).randomSplit([0.8, 0.2],seed=100)

unseen_uids=test.select("uid").subtract(training.select("uid")).collect()

random_replacement_tastes=training.groupBy("uid").agg({"uid":"count"}).filter("count(uid)>24").sample(False, 0.1, seed=0).limit(len(unseen_uids))

for r in unseen_uids:
	test=test.filter(F.col("uid")!=r.uid)
	training.union(test.filter(F.col("uid")==r.uid))

test=test.union(random_replacement_tastes.join(training,"uid").limit(len(unseen_uids)).select(["uid","uid-int","song_int","rating_out_of_5"]))

print(test.select("uid").subtract(training.select("uid")).count())

# Build the recommendation model using ALS on the training data
# set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="uid-int", itemCol="song_int", ratingCol="rating_out_of_5",
          coldStartStrategy="drop")
model = als.fit(training)
predictions = model.transform(test)


#have a look at predictions
predictions.show(3,0)
"""
+----------------------------------------+-------+--------+-------------------+----------+
|uid                                     |uid-int|song_int|rating             |prediction|
+----------------------------------------+-------+--------+-------------------+----------+
|526344eb1d14188a8ec97fdff3bef494fbf3fcfb|225.0  |0.0     |0.08496176720475786|0.11148132|
|680523f7eb7e35253991b5effc816947d5682602|458.0  |0.0     |0.09250693802035154|0.14967582|
|e037f60e0656dfbff77768f8f68276bdad868121|779.0  |0.0     |0.07598784194528875|0.1756605 |
+----------------------------------------+-------+--------+-------------------+----------+
"""

#evaluation

from pyspark.mllib.evaluation import RankingMetrics

#creating list of recommendations for users
play_list=predictions.orderBy(F.col("rating").desc()).groupby("uid-int").agg(F.collect_list("song_int"))
recommendations=predictions.orderBy(F.col("prediction").desc()).groupby("uid-int").agg(F.collect_list("song_int"))

"""
+-------+----------------------+
|uid_int|collect_list(song_int)|
+-------+----------------------+
|   13.0|  [31523.0, 28068.0...|
|   58.0|  [6262.0, 5098.0, ...|
|   60.0|  [22740.0, 43558.0...|
|   63.0|  [356.0, 17779.0, ...|
|   87.0|  [47439.0, 85636.0...|
|  100.0|  [69406.0, 32391.0...|
|  113.0|  [92657.0, 51942.0...|
|  132.0|  [98176.0, 59662.0...|
|  136.0|  [13596.0, 1227.0,...|
|  141.0|  [17.0, 1622.0, 14...|
|  179.0|  [35598.0, 34509.0...|
|  259.0|  [37069.0, 13225.0...|
|  272.0|  [12702.0, 5575.0,...|
|  282.0|  [2204.0, 26489.0,...|
|  286.0|  [4028.0, 38361.0,...|
|  305.0|  [98809.0, 80023.0...|
|  306.0|  [22641.0, 91985.0...|
|  309.0|  [106756.0, 95795....|
|  319.0|  [60186.0, 38915.0...|
|  320.0|  [4289.0, 1.0, 175...|
+-------+----------------------+

"""

#creating predictionAndLabel dataframe

recom=recommendations.withColumnRenamed("collect_list(song_int)","prediction").select(["uid-int","prediction"])
pl=play_list.withColumnRenamed("collect_list(song_int)","label").select(["uid-int","label"])
predictionAndLabel=recom.join(pl,"ui-int")

#evaluating
metric=RankingMetrics(predictionAndLabel.select(["label","prediction"]).rdd)

metric.precisionAt(5)
#0.9460611747290315

metric.meanAveragePrecision
# 1.0

metric.ndcgAt(10)
#1.0
