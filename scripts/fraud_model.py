from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline 
from pyspark.ml.feature import VectorAssembler 
from pyspark.ml.feature import OneHotEncoder
from pyspark.sql.functions import rand
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

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

def preprocess():
    # read in merged dataset and delta files
    full = spark.read.parquet("../../data/curated/full_data/")
    probs_merchant = spark.read.option('header', True).csv('../../data/tables/merchant_fraud_probability.csv')
    probs_consumer= spark.read.option('header', True).csv('../../data/tables/consumer_fraud_probability.csv')   

    # convert features to appropriate data types
    probs_consumer =  probs_consumer.withColumn('user_id', F.col('user_id').cast('long'))\
        .withColumn('fraud_probability', F.col('fraud_probability').cast('float'))
    probs_merchant =  probs_merchant.withColumn('merchant_abn', F.col('merchant_abn').cast('long'))\
        .withColumn('fraud_probability', F.col('fraud_probability').cast('float'))

    # merge transaction file with merchants'/consumers' fraud probability 
    # based on merchant abn or user id respectively by left join
    full = full.join(probs_merchant, on=['merchant_abn', 'order_datetime'], how = 'left')\
        .withColumnRenamed('fraud_probability', 'merchant_prob')
    full = full.join(probs_consumer, on=['user_id', 'order_datetime'], how = 'left')\
        .withColumnRenamed('fraud_probability', 'consumer_prob')
    
    # replace all the missing value with 0.01 as default fraud prob
    full = full.na.fill(value=0.01, subset=['merchant_prob', 'consumer_prob'])

    # set benchmark as 5% to focus on False Positive instead of False Negative
    full = full.withColumn('is_fraud', F.when((F.col('merchant_prob') > 5) | (F.col('consumer_prob') > 5), 1).otherwise(0))

    # TODO: what does this line do?
    full = full.filter(F.col('dollar_value') >= 1).na.drop(subset = 'name')
    full =  full.withColumn('month', F.month('order_datetime'))

    return full


def feature_engineering(full):
    # give all values in non-numeric features an index in order to make it ordinal or one-hot encoded
    indexed_features = ['revenue_level', 'tags', 'gender']
    indexers =[]
    for col in indexed_features:
    indexers.append(StringIndexer(inputCol=col, outputCol = col+"_index"))

    # one-hot encode the numeric indices
    categorical_features =  ["tags_index", "gender_index","month"]
    ohe = []
    for f in categorical_features:
    ohe.append(OneHotEncoder(inputCol=f, outputCol=f+"OHE"))
    pipeline = Pipeline(stages=[indexers, ohe])
    indexed_result = pipeline.fit(full).transform(full)

    # feature selection and vectorisation
    feature_selected = ['dollar_value','take_rate','mean_total_income','monthOHE','revenue_level_index','tags_indexOHE','gender_indexOHE']
    assembler = VectorAssembler(inputCols=feature_selected ,outputCol='features')
    output = assembler.transform(indexed_result)

    return output


def model(output):
    train_data = output.filter(F.col('order_datetime') < '2022-02-28')
    predict_data = output.filter(F.col('order_datetime') >= '2022-02-28')   

    # balance distribution of two classes
    fraud_data = train_data.filter(F.col('is_fraud') == 1)
    normal_data = train_data.filter(F.col('is_fraud') == 0).randomSplit([0.01,0.99])[0]
    train_data = fraud_data.union(normal_data).orderBy(rand())

    # use logistic regression to predict fraud transactions after 2020-02
    lr = LogisticRegression(labelCol='is_fraud')
    fitted_model = lr.fit(train_data)
    fitted_model.setFeaturesCol("features")
    fitted_model.setPredictionCol("prediction")

    predict_data = predict_data.drop("is_fraud")
    predicted = fitted_model.transform(predict_data)

    cols = ['user_id','order_datetime','merchant_abn','SA2_code','postcode','consumer_id','state','gender','mean_total_income','dollar_value','order_id','name','tags','revenue_level','take_rate','is_fraud'	]
    predicted =  predicted.select(cols)
    ???


