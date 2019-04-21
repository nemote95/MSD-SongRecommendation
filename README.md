# MSD-SongRecommendation
A song recommandation system using sound features
Million Song Dataset (MSD) is a project initiated by Echo Nest which studies on audio and textual data used in Spotify. 
This dataset contains information about audio features of songs, genres and users taste profiles (the songs that each user has listened to and the number of times that they were played) and a great range of other information.  
In this project, we explore through data for different goals. Firstly, I discuss how to preprocess this data then some ways of tracks classifications based on audio features will
be examined and finally, I provide a solution for a song recommender.


## Dataset 
I needed to read the dataset documentation to understand the structure of dataset, and how to read the required data. 
Then, I used a script code to know the structure and data types and size of files.
As a result, the audio features dataset has the largest size (approximately 12 GB). 
The files in audio dataset are in “csv” format whereas files in genre and taste profile triplets are in “tsv” format and mismatches songs datasets are in “txt” format.
Besides, some files such as statistics dataset are compressed as “gzip” files. 
According to the structure of the dataset structure, each dataset is partitioned into 8 replications. By running hdfs fsck /msd/tasteprofile/triplets.tsv -files –blocks we can see the size of each partition is 134217728 bytes and the default level of parallelism is 8 ( using rdd_name.getNumPartitions() ). For this context, this level is sufficient but to change it, df.repartition(number_of_partiitions) should be called. 
In order to find the number of lines in each file, the following script was run: for i in `hdfs dfs -ls -R /data/msd | awk '{print $8}'`; do echo $i ; hdfs dfs -cat $i | wc -l; done However, to ensure about the number of lines in the compressed files such as metadata which includes the information about songs, dataframes was loaded and analysis_df.distinct().count() was run and received 999959 as a result which is less than the number of rows in triplets.
