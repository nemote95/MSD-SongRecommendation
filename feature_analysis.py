 audio_feature=data_dict["/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv"]
 for i in range (len(audio_feature.schema.names)-1):
         for j in range(len(audio_feature.schema.names)-1):
         	if (i!=j and abs(audio_feature.stat.corr(audio_feature.schema.names[i],audio_feature.schema.names[j]))>0.89 ):
             	print((audio_feature.schema.names[i],audio_feature.schema.names[j]),audio_feature.stat.corr(audio_feature.schema.names[i],audio_feature.schema.names[j]))

print(audio_feature.describe().show())
"""
correlations:
('Spectral_Centroid_Overall_Standard_Deviation_1', 'Spectral_Rolloff_Point_Overall_Standard_Deviation_1') 0.9546097947367713
('Spectral_Centroid_Overall_Standard_Deviation_1', 'Zero_Crossings_Overall_Standard_Deviation_1') 0.9715596937065418
('Spectral_Rolloff_Point_Overall_Standard_Deviation_1', 'Spectral_Centroid_Overall_Standard_Deviation_1') 0.9546097947367711
('Spectral_Rolloff_Point_Overall_Standard_Deviation_1', 'Zero_Crossings_Overall_Standard_Deviation_1') 0.9713487769380657
('Spectral_Flux_Overall_Standard_Deviation_1', 'Spectral_Variability_Overall_Standard_Deviation_1') 0.9000568018005148
('Spectral_Flux_Overall_Standard_Deviation_1', 'Spectral_Flux_Overall_Average_1') 0.8918057299534197
('Spectral_Variability_Overall_Standard_Deviation_1', 'Spectral_Flux_Overall_Standard_Deviation_1') 0.900056801800515
('Spectral_Variability_Overall_Standard_Deviation_1', 'Root_Mean_Square_Overall_Standard_Deviation_1') 0.9846613685039245
('Root_Mean_Square_Overall_Standard_Deviation_1', 'Spectral_Variability_Overall_Standard_Deviation_1') 0.9846613685039245
('Zero_Crossings_Overall_Standard_Deviation_1', 'Spectral_Centroid_Overall_Standard_Deviation_1') 0.9715596937065417
('Zero_Crossings_Overall_Standard_Deviation_1', 'Spectral_Rolloff_Point_Overall_Standard_Deviation_1') 0.9713487769380657
('Spectral_Centroid_Overall_Average_1', 'Spectral_Rolloff_Point_Overall_Average_1') 0.9782393317938247
('Spectral_Centroid_Overall_Average_1', 'Zero_Crossings_Overall_Average_1') 0.9501200266030028
('Spectral_Rolloff_Point_Overall_Average_1', 'Spectral_Centroid_Overall_Average_1') 0.9782393317938247
('Spectral_Rolloff_Point_Overall_Average_1', 'Zero_Crossings_Overall_Average_1') 0.9651844658025998
('Spectral_Flux_Overall_Average_1', 'Spectral_Flux_Overall_Standard_Deviation_1') 0.8918057299534197
('Spectral_Flux_Overall_Average_1', 'Spectral_Variability_Overall_Average_1') 0.9179600847427083
('Spectral_Flux_Overall_Average_1', 'Root_Mean_Square_Overall_Average_1') 0.9111885931835688
('Spectral_Variability_Overall_Average_1', 'Spectral_Flux_Overall_Average_1') 0.9179600847427084
('Spectral_Variability_Overall_Average_1', 'Root_Mean_Square_Overall_Average_1') 0.99567025999665
('Root_Mean_Square_Overall_Average_1', 'Spectral_Flux_Overall_Average_1') 0.9111885931835689
('Root_Mean_Square_Overall_Average_1', 'Spectral_Variability_Overall_Average_1') 0.9956702599966503
('Zero_Crossings_Overall_Average_1', 'Spectral_Centroid_Overall_Average_1') 0.9501200266030028
('Zero_Crossings_Overall_Average_1', 'Spectral_Rolloff_Point_Overall_Average_1') 0.9651844658025995

descriprive statistics
+-------+----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+
|suaudio_featureary|Spectral_Centroid_Overall_Standard_Deviation_1|Spectral_Rolloff_Point_Overall_Standard_Deviation_1|Spectral_Flux_Overall_Standard_Deviation_1|Compactness_Overall_Standard_Deviation_1|Spectral_Variability_Overall_Standard_Deviation_1|Root_Mean_Square_Overall_Standard_Deviation_1|Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1|Zero_Crossings_Overall_Standard_Deviation_1|Spectral_Centroid_Overall_Average_1|Spectral_Rolloff_Point_Overall_Average_1|Spectral_Flux_Overall_Average_1|Compactness_Overall_Average_1|Spectral_Variability_Overall_Average_1|Root_Mean_Square_Overall_Average_1|Fraction_Of_Low_Energy_Windows_Overall_Average_1|Zero_Crossings_Overall_Average_1|         MSD_TRACKID|
+-------+----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+
|  count|                                        994623|                                             994623|                                    994623|                                  994623|                                           994623|                                       994623|                                                     994623|                                     994623|                             994623|                                  994623|                         994623|                       994623|                                994623|                            994623|                                          994623|                          994623|              994623|
|   mean|                            6.9450807384808515|                               0.055706619550725325|                      0.003945426548426958|                       222.5177754102614|                             0.002227142323568505|                           0.0742012564681895|                                        0.06020736022995658|                         16.802865692749744|                    9.1102540598077|                     0.06194319533235306|           0.002932152349630...|            1638.732052945103|                  0.004395477754344148|                0.1659278730850465|                              0.5562832020867303|              26.680411300033022|                null|
| stddev|                            3.6317955553970958|                                0.02650015516431772|                      0.003265325935538...|                       59.72621371192935|                             0.001039740286439...|                          0.03176618685339571|                                        0.01851647440361713|                          7.530118067018138|                  3.843630960366653|                    0.029016687503605202|           0.002491147126895...|           106.10606646605164|                  0.001995888300231307|               0.07429851374658547|                            0.047553705701118026|              10.394715710580604|                null|
|    min|                                           0.0|                                                0.0|                                       0.0|                                     0.0|                                              0.0|                                          0.0|                                                        0.0|                                        0.0|                                0.0|                                     0.0|                            0.0|                          0.0|                                   0.0|                               0.0|                                             0.0|                             0.0|'TRAAAAK128F9318786'|
|    max|                                         73.31|                                             0.3739|                                   0.07164|                                 10290.0|                                          0.01256|                                       0.3676|                                                     0.4938|                                      141.6|                              133.0|                                  0.7367|                        0.07549|                      24760.0|                               0.02366|                            0.8564|                                          0.9538|                           280.5|'TRZZZZO128F428E2D4'|
+-------+----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+

"""
#loading genre dataset
genre_schema= StructType([
    StructField('track_id', StringType()),
    StructField('genre', StringType())])

genre_df = (
spark.read.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "true")
			.option("sep","\t")
			.schema(genre_schema)
.load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv"))

#removing mismatches from genre dataset 
manual_mismatched_genre=genre_df.join(mis_manual_df, "track_id").select(["track_id","genre"])
mismatched_genre=genre_df.join(mis_df, "track_id").select(["track_id","genre"])
filtered_genre=genre_df.subtract(mismatched_genre).subtract(manual_mismatched_genre)

filtered_genre.groupBy("genre").agg({"track_id":"count"}).write.csv("msd/genre_count.csv")

#merging genre dataset woth audio features
audio_feature=audio_feature.withColumn("track_id",substring(audio_feature.MSD_TRACKID,2,18)).drop(audio_feature.MSD_TRACKID)
labled_features=filtered_genre.join(audio_feature,"track_id")
labled_features.write.csv("msd/labeled_spectral_features.csv",header=True)