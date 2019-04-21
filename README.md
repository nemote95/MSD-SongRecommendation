# MSD-SongRecommendation
A song recommandation system using sound features
Million Song Dataset (MSD) is a project initiated by Echo Nest which studies on audio and textual data used in Spotify. 
This dataset contains information about audio features of songs, genres and users taste profiles (the songs that each user has listened to and the number of times that they were played) and a great range of other information.  
In this project, we explore through data for different goals. Firstly, I discuss how to preprocess this data then some ways of tracks classifications based on audio features will
be examined and finally, I provide a solution for a song recommender.

## Dataset
This Million Song Dataset (MSD) is included on the [million song dataset benchmarks downloads page](http://www.ifs.tuwien.ac.at/mir/msd/download.html) . for getting the data you can follow [this guide](https://labrosa.ee.columbia.edu/millionsong/pages/getting-dataset).
assuming that the data is under hdfs:///data/msd. The main/summary directory contains all the metadata for the main million song dataset but none of the audio analysis, similar artists, or tags (see getting the dataset).
The tasteprofile directory contains the user-song play counts from the Taste Proﬁle dataset as well as logs identifying mismatches that were identiﬁed and matches that were manually accepted.
The audio directory contains audio features sets from the Music Information Retrieval research group at the Vienna University of Technology. The audio/attributes directory contains attributes names from the header of the ARFF, the audio/features directory contains the audio features themselves, and the audio/statistics directory contains additional track statistics.

Firstly, I used the following bash script by Christina Wong to know the structure and data types and size of files.

```hdfs dfs -ls -R -h /data/msd | awk '{ if($2 == "-") print $8; else if($6 != "K" && $6 != "M") print $8, $5, "B"; else print $9,$5,$6;}' | sed -e "s/[^-][^\/]*\// |/g" -e "s/|\([^ ]\)/|-\1/" ```

As a result, the audio features dataset has the largest size (approximately 12 GB). 
The files in audio dataset are in “csv” format whereas files in genre and taste profile triplets are in “tsv” format and mismatches songs datasets are in “txt” format.
Besides, some files such as statistics dataset are compressed as “gzip” files. 
According to the structure of the dataset structure, each dataset is partitioned into 8 replications. By running the following script we can see the size of each partition is 134217728 bytes and the default level of parallelism is 8 ( using rdd_name.getNumPartitions() ).
For this context, this level is sufficient but to change it, df.repartition(number_of_partiitions) should be called. 

```hdfs fsck /msd/tasteprofile/triplets.tsv -files –blocks ```

In order to find the number of lines in each file, the following script was run: 

```for i in `hdfs dfs -ls -R /data/msd | awk '{print $8}'`; do echo $i ; hdfs dfs -cat $i | wc -l; ```

However, to ensure the number of lines in the compressed files such as metadata which includes the information about songs, dataframes was loaded and analysis_df.distinct().count() was run and received 999959 as a result which is less than the number of rows in triplets.


## Data preprocessing

First, it is required to filter the Taste Profile dataset to remove the mismatched songs. There are two types of mismatches: manually reported mismatches and mismatches detected by the system. To remove them, mismatches were loaded, joined to taste triplets and subtracted from the triplets. 
Audio dataset includes attributes which define schemas for the audio features. To obtain the audio features dataframes, the attributes of each audio feature dataset were loaded and used to construct a stuctTypes and Schemas for them in [clean_tastes.py] (https://github.com/nemote95/MSD-SongRecommendation/blob/master/clean_tastes.py). Then, each feature dataset was loaded using the corresponding schema and appended in a dictionary which holds audio features dataframes. 
