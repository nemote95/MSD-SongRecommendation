# Audio attributes to schema example

import json
from pyspark.sql.types import *

DTYPES_MAPPING = {
    "real": DoubleType(),
    "string": StringType(),
    "NUMERIC": DoubleType(),
    "STRING": StringType(),
}

#function to convet attributes to schema
def attributes_to_schema(hdfs_path, dtypes_mapping=DTYPES_MAPPING):

    rows = spark.read.csv(hdfs_path).collect() # collect will take a dataframe to a python [Row(...), ...]
    structfields = []
    for row in rows:
        colname = row[0]
        dtypestring = row[1]
        structfields.append(StructField(colname, dtypes_mapping[dtypestring], True))

    return StructType(structfields)


if __name__ == "__main__":

    # For loop to iterate through all audio datasets

    files = [
        "/data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-rh-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-rp-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-trh-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv"
    ]

    import re
    data_dict={}
    for attributes_file in files:
        features_file = re.sub("\.attributes", "", attributes_file)
        features_file = re.sub("attributes", "features", features_file)
       
        schema = attributes_to_schema(attributes_file) #get schema 
        data = spark.read.schema(schema).csv(features_file) #load data 
        data_dict[features_file]=data #put dataframe in a dictionary of audio dataframes

