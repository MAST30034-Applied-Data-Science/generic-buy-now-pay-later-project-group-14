import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import math

output_path = "../data/curated/"
external_output_path = '../data/external/'

def preprocess_postcode():
    """
    This function preprocesses the postcode dataset by extracting the latitude and longitude of 
    each postcode region and matching it with the corresponding SA2 region.
    The preprocessed dataset will be stored in "../data/curated/processed_postcode.csv"
    """
    # read external dataset
    sf = gpd.read_file(external_output_path + "SA2_2021/SA2_2021_AUST_GDA2020.shp")
    sf = sf.set_index('SA2_CODE21')
    sf = sf.loc[sf.geometry != None]
    all_postcodes = pd.read_csv(external_output_path + "australian_postcodes.csv")

    # extract latitude and longitude of each postal area and remove duplicate postcodes
    all_postcodes = all_postcodes[['postcode', 'locality', 'long', 'lat', 'SA2_MAINCODE_2016']]
    all_postcodes = all_postcodes.drop_duplicates(subset='postcode', keep="first")
    all_postcodes.rename(columns={'SA2_MAINCODE_2016':'SA2_code'}, inplace=True)
    all_postcodes["coordinate"] = all_postcodes.apply(lambda x: Point(x.long, x.lat), axis=1)

    # when no SA2 code available, check if the coordinate of a particular postal area is in a SA2 district
    all_postcodes["SA2_code"] = all_postcodes.apply(lambda x: postcode_to_SA2(x, sf), axis=1)
    all_postcodes = all_postcodes[['postcode', 'SA2_code']]

    # save the processed data
    all_postcodes.to_csv(output_path + "processed_postcode.csv", index=False)


def postcode_to_SA2(df, sf):
    """
    This function looks for the SA2 region that contains the given (latitude, longitude) pair.

    :param df: instance of one postcode region
    :param sf: SA2 shapefile
    :returns: the corresponding SA2 code
    """
    if math.isnan(df['SA2_code']):
        for SA2_code, row in sf.iterrows():
            if df['coordinate'].within(row['geometry']):
                return SA2_code
    else:
        return int(df['SA2_code'])


def preprocess_income():
    """
    This function preprocesses the external income dataset can convert it into a csv file.
    """
    df = pd.read_excel(external_output_path + "total_income.xlsx", sheet_name='Table 1.4')

    # find and store mean total income of each state
    state_income = df.drop(df.index[0:6], inplace=False).reset_index(drop=True)
    states = ['New South Wales', 'Victoria', 'Queensland', 'South Australia', 'Western Australia', 'Tasmania', 'Northern Territory', 'Australian Capital Territory']
    state_income = state_income[state_income.iloc[:, 0].isin(states)].iloc[:, [0, 26]]
    abbrv = ['NSW', 'VIC','QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
    state_income.iloc[:,0] = state_income.iloc[:,0].replace(states,abbrv)
    state_income.set_axis(['state', 'mean_total_income'], axis=1, inplace=True)
    state_income.to_csv(output_path + "state_mean_income.csv", index=False)

    # extract mean income of each SA2 region
    df.columns = df.iloc[5].values.flatten().tolist()
    df = df.drop(df.index[0:6], inplace=False).reset_index(drop=True)
    df = (df.iloc[:, [0, 26]])
    df.drop(df.index[2297:2300], inplace=True)
    df.set_axis(['SA2_code', 'mean_total_income'], axis=1, inplace=True)

    # temporarily replace missing values with 0
    df.replace({'mean_total_income': {'np': 0}}, inplace=True)
    df['mean_total_income'] = df['mean_total_income'].clip(lower=0)
    df = df.dropna().reset_index(drop = True)

    # save the processed data
    df.to_csv(output_path + "processed_income.csv", index=False)


preprocess_postcode()
preprocess_income()
