# MSD-SongRecommendation
A song recommandation engine using sound features
Million Song Dataset (MSD) from studies on audio and textual data used in Spotify.This recommendation engine has two main stages: tracks classifications based on audio features and a song recommender using Spark machine learning libraries.

## Data preprocessing
1. dataframes for each datasets is defined in [features_DataFrames.py](/feature_DataFrames.py) .
2. mismatches in Taste Profile dataset are removed in [clean_tastes.py](/clean_tastes.py) . 
3. Audio dataset includes attributes which define schemas for the audio features. this schemas are defined in [clean_tastes.py](/clean_tastes.py). 

## Feature analysis 
1. after looking at the statistic descriotion of the audio features, we removed highly correlated features top reduce the size of our feature set in [feature_analysis.py](/feature_analysis.py) .
2. we then merged these features with the genere dataset. (note: the mismatches were also removed in this dataset)
