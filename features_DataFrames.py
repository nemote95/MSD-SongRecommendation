from pyspark.sql.types import *
from pyspark.sql.functions import substring

#load taste set
taste_schema= StructType([
    StructField('uid', StringType()),
    StructField('song_id', StringType()),
    StructField('count', IntegerType())])

taste_df = (
spark.read.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "false")
			.option("sep","\t")
			.schema(taste_schema)
.load("hdfs:///data/msd/tasteprofile/triplets.tsv"))


#load mismatches
#they are fixed_length text files and need to be manually define columns 
#using substing function

mis_df = (
spark.read.format("com.databricks.spark.csv")
			.option("header", "false")
			.option("inferSchema", "false")
			.option("sep","\t")
.load("hdfs:///data/msd/tasteprofile/mismatches/sid_mismatches.txt"))

mis_df=mis_df.withColumn('song_id', mis_df['_c0'].substr(9,19)) \
.withColumn('track_id', mis_df['_c0'].substr(27,27).substr(1,19))\
.withColumn('name', substring(mis_df["_c0"],48,250))\
.drop("_c0")

mis_manual_df = (
spark.read.format("com.databricks.spark.csv")
			.option("header", "false")
			.option("inferSchema", "false")
			.option("sep","\t")
.load("hdfs:///data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt"))

mis_manual_df=mis_manual_df.withColumn('song_id', mis_manual_df['_c0'].substr(11,18)) \
.withColumn('track_id', mis_manual_df['_c0'].substr(30,30).substr(1,18))\
.withColumn('name', substring(mis_manual_df["_c0"],50,250))\
.drop("_c0")

# join and then subtract to remove mismatches
manual_mismatched_taste=taste_df.join(mis_manual_df, "song_id").select(["song_id","uid","count"])
mismatched_tast=taste_df.join(mis_df, "song_id").select(["song_id","uid","count"])
filtered_taste=taste_df.subtract(mismatched_tast).subtract(manual_mismatched_taste)

#save the result
filtered_taste.write.csv("msd/filtered_taste.csv",header=True)