from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "2g")
    .config("spark.executer.memory", "4g")
    .getOrCreate()
)

def create_features(sdf):
    sdf = sdf.groupBy('merchant_abn')\
      .agg(
         F.countDistinct('consumer_id').alias('total_num_consumer'),
         F.mean('dollar_value').alias('avg_dollar_value'),
         F.countDistinct('order_id').alias('total_num_transaction'),
         F.mean('mean_total_income').alias('mean_income'),
         F.first('revenue_level').alias('revenue_level'),
         F.sum(F.col('dollar_value') * F.col('take_rate')).alias('total_revenue'),
         F.countDistinct('postcode').alias('total_num_postcode'),
         F.first('tags').alias('tag'),
      )
    return sdf

def create_label(sdf):
    label = sdf.groupBy('merchant_abn')\
      .agg(
         F.countDistinct('consumer_id').alias('y_total_num_consumer'),
         F.sum(F.col('dollar_value') * F.col('take_rate')).alias('y_total_revenue'),
         F.countDistinct('order_id').alias('y_total_num_transaction')
      )
    return label


def main():
    sdf = spark.read.parquet("../data/curated/full_data_without_fraud/")

    # discard fraud transactions
    sdf = sdf.filter(F.col('is_fraud')==0)

    # split the dataset 
    train_sdf = sdf.filter((F.col('order_datetime') >= '2021-02-28') & (F.col('order_datetime') < '2021-08-28'))
    label_sdf = sdf.filter((F.col('order_datetime') >= '2022-02-28') & (F.col('order_datetime') < '2022-08-28'))
    test_sdf = sdf.filter((F.col('order_datetime') >= '2021-02-28') & (F.col('order_datetime') < '2022-02-28'))

    # create features and labels for train/test set
    train_data = create_features(train_sdf)
    train_label = create_label(label_sdf)
    test_data = create_features(test_sdf)

    # features left join label since if no historical data is provided,
    # we cannot predict the future value of a merchant
    train_data = train_data.join(train_label, ["merchant_abn"], how="left") 

    train_data.write.format('parquet').mode('overwrite').save("../data/curated/train_data")
    test_data.write.format('parquet').mode('overwrite').save("../data/curated/test_data")
    

main()
