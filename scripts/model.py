import pandas as pd
import numpy as np
from sklearn.metrics import *
from sklearn.model_selection import train_test_split, GridSearchCV, cross_validate
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.neural_network import MLPRegressor

labels = ['y_total_num_consumer', 'y_total_revenue', 'y_total_num_transaction']


def preprocess():
    """
    This function reads in training and test data and leaves only the selected features for modelling later.
    Any instances with missing values are dropped. 

    :returns: preprocessed train & test dataset
    """
    # read data
    train_df = pd.read_parquet('../data/curated/train_data/')
    test_df = pd.read_parquet('../data/curated/test_data/')

    # drop columns not needed
    train_df = train_df[['merchant_abn', 'total_num_consumer', 'tag', 'total_revenue', 'total_num_postcode', 'revenue_level', 
        'total_num_transaction', 'y_total_num_consumer', 'y_total_revenue', 'y_total_num_transaction']].dropna()
    train_df = train_df.set_index('merchant_abn')

    test_df = test_df[['merchant_abn', 'total_num_consumer', 'tag', 'total_revenue', 'total_num_postcode', 
        'revenue_level', 'total_num_transaction']].dropna()
    test_df = test_df.set_index('merchant_abn')

    # change tags and revenue level into numeric features using one hot encoding
    cat_features = ["tag", "revenue_level"]
    train_df = pd.get_dummies(train_df, columns = cat_features)
    test_df = pd.get_dummies(test_df, columns = cat_features)

    return train_df, test_df


def consumer_model(train_df, test_df):
    '''
    This function constructs a Linear Regression model for the prediction of number of consumers 
    in the next period of time. Predicted value will be saved in '../data/curated/pred_total_num_consumer.csv'.

    :param train_df: preprocessed training data
    :param test_df: preprocessed test data
    '''
    # select useful features (exclude revenue_level)
    features = [i for i in train_df.columns if i not in labels and not i.startswith('revenue')]

    X_train = train_df[features]
    y_train = train_df['y_total_num_consumer']
    test_df = test_df[features]

    # use linear regression to predict total number of consumers next year
    lr = LinearRegression()
    fitted_model = lr.fit(X_train, y_train)
    y_pred = fitted_model.predict(test_df)
    test_df['pred_total_num_consumer'] = y_pred

    # save prediction 
    result_df = test_df['pred_total_num_consumer'].reset_index()
    result_df.to_csv('../data/curated/pred_total_num_consumer.csv', index=False)


def transaction_model(train_df, test_df):
    '''
    This function constructs a Linear Regression model for the prediction of number of transactions 
    in the next period of time. Predicted value will be saved in '../data/curated/pred_total_num_transaction.csv'.

    :param train_df: preprocessed training data
    :param test_df: preprocessed test data
    '''
    # select useful features (exclude revenue_level)
    features = [i for i in train_df.columns if i not in labels and not i.startswith('revenue')]

    X_train = train_df[features]
    y_train = train_df['y_total_num_transaction']
    test_df = test_df[features]

    # use linear regression to predict total number of transactions next year
    lr = LinearRegression()
    fitted_model = lr.fit(X_train, y_train)
    y_pred = fitted_model.predict(test_df)
    test_df['pred_total_num_transaction'] = y_pred

    # save prediction 
    result_df = test_df['pred_total_num_transaction'].reset_index()
    result_df.to_csv('../data/curated/pred_total_num_transaction.csv', index=False)


def revenue_model(train_df, test_df):
    '''
    This function constructs a Multi-layer perceptron for the prediction of tota revenue for each merchant 
    in the next period of time. Predicted value will be saved in '../data/curated/pred_total_revenue.csv'.

    :param train_df: preprocessed training data
    :param test_df: preprocessed test data
    '''
    # select useful features 
    features = [i for i in train_df.columns if i not in labels]
    X_train = train_df[features]
    y_train = train_df['y_total_revenue']
    test_df = test_df[features]

    # scale train and test dataset in order to be standard normally distributed with zero mean
    sc_X = StandardScaler()
    X_trainscaled = sc_X.fit_transform(X_train)
    X_testscaled = sc_X.transform(test_df)

    # use multi-layer perceptron to predict total revenue next year
    mlp_reg = MLPRegressor(hidden_layer_sizes=(128,128,128,128),activation="relu" ,solver = 'adam', 
              random_state=0, max_iter=20000).fit(X_trainscaled, y_train)
    y_pred = mlp_reg.predict(X_testscaled)
    test_df['pred_total_revenue'] = y_pred

    # save prediction 
    result_df = test_df['pred_total_revenue'].reset_index()
    result_df.to_csv('../data/curated/pred_total_revenue.csv', index=False)


train_df, test_df = preprocess()
consumer_model(train_df, test_df)
transaction_model(train_df, test_df)
revenue_model(train_df, test_df)
