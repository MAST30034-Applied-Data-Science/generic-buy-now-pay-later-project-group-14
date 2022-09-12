#TODO: ETL documentation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import argparse

spark = (
    SparkSession.builder.appName("MAST30034 Project 2 etl")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "2g")
    .config("spark.executer.memory", "4g")
    .getOrCreate()
)

# create the parser
parser = argparse.ArgumentParser()
parser.add_argument('tables', type=str, help='enter the location of data files (e.g. "../data/tables/")')
args = parser.parse_args()

merchant_path = args.tables + "tbl_merchants.parquet"
consumer_path = args.tables + "tbl_consumer.csv"
id_lookup_path = args.tables + "consumer_user_details.parquet"
transaction_path = args.tables + "transactions_*/*"

output_path = "../data/curated/"
external_output_path = '../data/external/'

# merchant_path = "data/tables/tbl_merchants.parquet"
# consumer_path = "data/tables/tbl_consumer.csv"
# id_lookup_path = "data/tables/consumer_user_details.parquet"
# transaction_path = "data/tables/transactions_*/*"

def merge():
    # read curated datasets
    consumer_sdf = spark.read.parquet(output_path + "consumer")
    transaction_sdf = spark.read.parquet(transaction_path)\
        .withColumn(
            "order_datetime", 
            F.to_date(F.regexp_extract(F.split(F.input_file_name(), "=")[1], "(.*)/",1), "yyyy-MM-dd")
        )
    postcode_SA2_sdf = spark.read.csv(output_path + "processed_postcode.csv", inferSchema =True, header=True)
    income_sdf = spark.read.csv(output_path + "processed_income.csv", inferSchema =True, header=True)
    merchant_sdf = spark.read.csv(output_path + "merchant.csv", inferSchema =True, header=True)
    state_income = pd.read_csv(output_path + "state_mean_income.csv").set_index("state").to_dict()["mean_total_income"]

    # combine consumer with mean total income based on SA2 code
    sdf = consumer_sdf.join(postcode_SA2_sdf,["postcode"],how="left")
    sdf = sdf.join(income_sdf, ["SA2_code"], how="left")

    # fill missing total income value with state mean
    abbrv = ['NSW', 'VIC','QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
    for state in abbrv:
        sdf = sdf.withColumn("mean_total_income", 
        F.when(((sdf.mean_total_income == 0) | (sdf.mean_total_income.isNull())) & (sdf.state == state), state_income[state]) \
         .otherwise(sdf.mean_total_income))
        sdf.cache()

    # combine transaction with merchant
    sdf2 = transaction_sdf.join(merchant_sdf, ["merchant_abn"], how="right")
    
    # merge consumer with transaction data
    data = sdf.join(sdf2, ["user_id"], how="right")

    # save the final combined data
    data.write.mode("overwrite").parquet(output_path + "full_data")


def preprocess_merchant():
    df = pd.read_parquet(merchant_path)
    # extract tags, revenue level and take rate from the "tags" column
    df["tags"] = df["tags"].str.findall(r'[\(\[]+([^\)\]]*)[\)\]]')
    df["revenue_level"] = df["tags"].str[1]
    df["take_rate"] = df["tags"].str[2].str.extract(r'[^\d]*([\d.]*)').astype(float)

    # convert all letters in tags to lowercase
    df["tags"] = df["tags"].str[0].str.lower() 
    
    # classify merchants using tags
    df['tags'] = df['tags'].str.split(',')
    df['tags'] = df['tags'].str[0]
    df['tags'] = df['tags'].str.split(' ')
    df['tags'] = df['tags'].str[0]

    df.loc[df.tags == 'computers', 'tags'] = 'computer'
    df.loc[df.tags == 'artist', 'tags'] = 'artist supply'
    df.loc[df.tags == 'art', 'tags'] = 'art dealer'
    df.loc[df.tags == 'digital', 'tags'] = 'digital goods'
    df.loc[df.tags == 'lawn', 'tags'] = 'garden supply'

    # save the processed data 
    df.to_csv(output_path + "merchant.csv")

def preprocess_consumer():
    consumer_sdf = spark.read.option("delimiter", "|").csv(consumer_path, inferSchema =True, header=True)
    id_sdf = spark.read.parquet(id_lookup_path)
    for field in ("consumer", "user"):
        field = f"{field}_id"
        id_sdf = id_sdf.withColumn(field, F.col(field).cast("INT"))

    # map user id to consumer id
    sdf = id_sdf.join(consumer_sdf,["consumer_id"],how="outer")

    # delete irrelevant features, such as name and address
    sdf = sdf.drop("name", "address")

    # save the processed data 
    sdf.write.mode("overwrite").parquet(output_path + "consumer")


def main():
    preprocess_consumer()
    preprocess_merchant()
    merge()
    
main()
