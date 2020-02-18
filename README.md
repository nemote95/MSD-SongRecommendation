# MSD-SongRecommendation
A song recommandation system using sound features
Million Song Dataset (MSD) from studies on audio and textual data used in Spotify.   
this project includes tracks classifications based on audio features and a song recommender using Spark.

## Data preprocessing

First, it is required to filter the Taste Profile dataset to remove the mismatched songs. There are two types of mismatches: manually reported mismatches and mismatches detected by the system. To remove them, mismatches were loaded, joined to taste triplets and subtracted from the triplets. 
Audio dataset includes attributes which define schemas for the audio features. To obtain the audio features dataframes, the attributes of each audio feature dataset were loaded and used to construct a stuctTypes and Schemas for them in [clean_tastes.py] (https://github.com/nemote95/MSD-SongRecommendation/blob/master/clean_tastes.py). Then, each feature dataset was loaded using the corresponding schema and appended in a dictionary which holds audio features dataframes. 
