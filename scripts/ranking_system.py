import pandas as pd
from functools import reduce

segment = {
    'furniture': 'personal & household good retail',
    'cable': 'technical & machinery service',
    'watch': 'personal & household good retail',
    'music': 'personal & household good retail',
    'gift': 'personal & household good retail',
    'computer': 'technical & machinery service',
    'equipment': 'technical & machinery service',
    'artist supply': 'personal & household good retail',
    'florists': 'personal & household good retail',
    'motor': 'technical & machinery service',
    'books': 'recreational good retailing',
    'jewelry': 'personal & household good retail',
    'stationery': 'recreational good retailing',
    'tent': 'recreational good retailing',
    'art dealer': 'personal & household good retail',
    'bicycle': 'recreational good retailing',
    'digital goods': 'recreational good retailing',
    'shoe': 'personal & household good retail',
    'opticians': 'health service',
    'antique': 'personal & household good retail',
    'health': 'health service',
    'hobby': 'recreational good retailing',
    'garden supply': 'personal & household good retail',
    'telecom': 'technical & machinery service'
}

def merge_data():
    """
    This function merges the predictions for the three labels with the merchant information
    for the final scoring and ranking. Missing values are filled with 0 in case we do not lose
    any merchants.
    Output: The merged dataset
    """
    # read data
    num_consumer = pd.read_csv('../data/curated/pred_total_num_consumer.csv')
    num_transaction = pd.read_csv('../data/curated/pred_total_num_transaction.csv')
    revenue = pd.read_csv('../data/curated/pred_total_revenue.csv')
    merchant_info = pd.read_csv('../data/curated/merchant.csv')

    # merge three data frames based on merchant abn
    data_frames = [merchant_info, num_consumer, num_transaction, revenue]
    df_merged = reduce(lambda left,right: pd.merge(left,right,on=['merchant_abn'], how='outer'), data_frames)

    # impute missing values and negative values with zero
    df_merged = df_merged.fillna(0)
    num = df_merged._get_numeric_data()
    num[num < 0] = 0

    return df_merged

def cal_score_rank(df_merged):
    """
    For each label, we normalise the predictions and sum them up with decided weights. Then we choose
    the top 100 merchants and scale their score into the range of (0, 100).
    Output: Top 100 merchants with their information, label values, scores and ranking
            stored in the directory "../data/curated/top100.csv"
    """
    # min-max normalization
    features = ['pred_total_num_consumer', 'pred_total_num_transaction', 'pred_total_revenue']
    for col in features:
        df_merged[f'scaled_{col}'] = 100 * (df_merged[col] - df_merged[col].min()) / (df_merged[col].max() - df_merged[col].min())

    # calculate ranking score for each merchant
    df_merged['score'] = 0.3*df_merged['scaled_pred_total_num_consumer'] + 0.3*df_merged['scaled_pred_total_num_transaction'] + 0.4*df_merged['scaled_pred_total_revenue']

    df_merged['rank'] = df_merged['score'].rank(ascending=False)
    df_merged = df_merged.set_index('rank').sort_index()

    # split merchants into 4 segments
    df_merged['segment'] = df_merged['tags'].map(segment)

    # save full ranking
    df_merged.to_csv('../data/curated/ranking.csv')

    # find top 100 merchants overall
    top100 = df_merged.loc[df_merged.index <= 100]
    top100.to_csv('../data/curated/top100.csv')


df = merge_data()
cal_score_rank(df)

