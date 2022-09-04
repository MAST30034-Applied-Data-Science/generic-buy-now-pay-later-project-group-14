#TODO: ETL documentation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

import geopandas as gpd
from shapely.geometry import Point

spark = (
    SparkSession.builder.appName("MAST30034 Project 2 etl")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "2g")
    .config("spark.executer.memory", "4g")
    .getOrCreate()
)

merchant_path = "data/tables/tbl_merchants.parquet"
consumer_path = "data/tables/tbl_consumer.csv"
id_lookup_path = "data/tables/consumer_user_details.parquet"
output_path = "data/curated/"
external_output_path = 'data/external/'


def preprocess_merchant():
    merchant_df = pd.read_parquet(merchant_path)

    # extract tags, revenue level and take rate from the "tags" column
    merchant_df["tags"] = merchant_df["tags"].str.findall(r'[\(\[]+([^\)\]]*)[\)\]]')
    merchant_df["revenue_level"] = merchant_df["tags"].str[1]
    merchant_df["take_rate"] = merchant_df["tags"].str[2].str.extract(r'[^\d]*([\d.]*)').astype(float)

    # convert all letters in tags to lowercase
    merchant_df["tags"] = merchant_df["tags"].str[0].str.lower() 
    
    # save the processed data in a csv file
    merchant_df.to_csv(output_path + "merchant.csv")


def preprocess_consumer():
    consumer_sdf = spark.read.option("delimiter", "|").csv(consumer_path, inferSchema =True, header=True)
    id_sdf = spark.read.parquet(id_lookup_path)
    for field in ("consumer", "user"):
        field = f"{field}_id"
        id_sdf = id_sdf.withColumn(field, F.col(field).cast("INT"))

    # map user id to consumer id
    output = id_sdf.join(consumer_sdf,["consumer_id"],how="outer")

    # save the processed data
    output.write.mode("overwrite").parquet(output_path + "consumer")


def postcode_SA2_lookup():
    sf = gpd.read_file(external_output_path + "SA2_2021/SA2_2021_AUST_GDA2020.shp")
    sf = sf.set_index('SA2_CODE21')
    sf = sf.loc[sf.geometry != None]

    all_postcodes = pd.read_csv(external_output_path + "australian_postcodes.csv")

    # extract latitude and longitude of each postal area and remove duplicate postcodes
    all_postcodes = all_postcodes[['postcode', 'locality', 'long', 'lat']]
    all_postcodes = all_postcodes.drop_duplicates(subset='postcode', keep="first")
    all_postcodes["coordinate"] = all_postcodes.apply(lambda x: Point(x.long, x.lat), axis=1)

    # check if the coordinate of a particular postal area is in a SA2 district
    all_postcodes["SA2_code"] = all_postcodes.apply(lambda x: postcode_to_SA2(x.coordinate, sf), axis=1)
    all_postcodes.to_csv(output_path + "processed_postcode.csv", index=False)


def postcode_to_SA2(coordinate, sf):
    for SA2_code, row in sf.iterrows():
        if coordinate.within(row['geometry']):
            return SA2_code


preprocess_consumer()
preprocess_merchant()
postcode_SA2_lookup()
