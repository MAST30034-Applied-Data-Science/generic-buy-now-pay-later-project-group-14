#TODO: ETL documentation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import argparse

# Create the parser
# parser = argparse.ArgumentParser()
# parser.add_argument('merchant', type=str, help='enter the merchant file name')
# parser.add_argument('consumer', type=str, help='enter the consumer file name')
# parser.add_argument('id_lookup', type=str, help='enter the consumer-user-details file name')

# args = parser.parse_args()

# tables_path = "data/tables/"
# merchant_path = tables_path + args.merchant
# consumer_path = tables_path + args.consumer
# id_lookup_path = tables_path + args.id_lookup


spark = (
    SparkSession.builder.appName("MAST30034 Project 2 etl")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "2g")
    .config("spark.executer.memory", "4g")
    .getOrCreate()
)

output_path = "data/curated/"
external_output_path = 'data/external/'

merchant_path = "data/tables/tbl_merchants.parquet"
consumer_path = "data/tables/tbl_consumer.csv"
id_lookup_path = "data/tables/consumer_user_details.parquet"
transaction_path = "data/tables/transactions_*/*"


def merge():
    # read curated datasets
    consumer_sdf = spark.read.parquet(output_path + "consumer")
    transaction_sdf = spark.read.parquet(transaction_path)
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

def preprocess_income():
    df = pd.read_excel(external_output_path + "total_income.xlsx", sheet_name='Table 1.4')

    # find and store mean total income of each state
    state_income = df.drop(df.index[0:6], inplace=False).reset_index(drop=True)
    states = ['New South Wales', 'Victoria', 'Queensland', 'South Australia', 'Western Australia', 'Tasmania', 'Northern Territory', 'Australian Capital Territory']
    state_income = state_income[state_income.iloc[:, 0].isin(states)].iloc[:, [0, 26]]
    abbrv = ['NSW', 'VIC','QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
    state_income.iloc[:,0] = state_income.iloc[:,0].replace(states,abbrv)
    state_income.set_axis(['state', 'mean_total_income'], axis=1, inplace=True)
    state_income.to_csv(output_path + "state_mean_income.csv", index=False)

    # TODO: need more comments HERE
    df.columns = df.iloc[5].values.flatten().tolist()
    df = df.drop(df.index[0:6], inplace=False).reset_index(drop=True)
    df = (df.iloc[:, [0, 26]])
    df.drop(df.index[2297:2300], inplace=True)
    df.set_axis(['SA2_code', 'mean_total_income'], axis=1, inplace=True)

    # temporarily replace missing values with 0
    df.replace({'mean_total_income': {'np': 0}}, inplace=True)
    # ignore state mean income 
    df = df.dropna().reset_index(drop = True)

    # save the processed data
    df.to_csv(output_path + "processed_income.csv", index=False)


def main():
    preprocess_consumer()
    preprocess_merchant()
    preprocess_income()
    merge()
    
main()
