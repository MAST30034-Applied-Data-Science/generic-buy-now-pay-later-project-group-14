import pandas as pd
import numpy as np
from sklearn.metrics import *
from sklearn.model_selection import train_test_split, GridSearchCV, cross_validate
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.neural_network import MLPRegressor

def preprocess():
    # read data
    train_df = pd.read_parquet('../data/curated/train_data/')
    test_df = pd.read_parquet('../data/curated/test_data/')

    # drop columns not needed
    train_df = train_df[['merchant_abn', 'total_num_consumer', 'tag', 'avg_dollar_value', 'total_revenue', 'revenue_level', 
        'total_num_transaction', 'y_total_num_consumer', 'y_total_revenue', 'y_total_num_transaction']].dropna()
    train_df = train_df.set_index('merchant_abn')

    test_df = test_df[['merchant_abn', 'total_num_consumer', 'tag', 'avg_dollar_value', 'total_revenue',
                    'revenue_level', 'total_num_transaction']].dropna()
    test_df = test_df.set_index('merchant_abn')

    # change tags and revenue level into numeric features using one hot encoding
    cat_features = ["tag", "revenue_level"]
    train_df = pd.get_dummies(train_df, columns = cat_features)
    test_df = pd.get_dummies(test_df, columns = cat_features)

    return train_df, test_df


def consumer_model(train_df, test_df):
    labels = ['y_total_num_consumer', 'y_total_revenue', 'y_total_num_transaction']
    features = [i for i in train_df.columns if i not in labels]
    X_train = consumer_df[features]
    y_train = consumer_df['y_total_num_consumer']

    # use linear regression to predict total number of conumers next year
    lr = LinearRegression()
    fitted_model = lr.fit(X_train, y_train)
    y_pred = fitted_model.predict(test_df)
    test_df['y_pred_total_num_consumer'] = y_pred

    # save prediction 
    result_df = test_df['y_pred_total_num_consumer'].reset_index()
    result_df.to_csv('../data/curated/pred_total_num_consumer.csv', index=False)

