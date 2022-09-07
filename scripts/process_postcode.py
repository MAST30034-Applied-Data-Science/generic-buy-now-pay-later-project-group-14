import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import math

output_path = "data/curated/"
external_output_path = 'data/external/'

def preprocess_postcode():
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
    if math.isnan(df['SA2_code']):
        for SA2_code, row in sf.iterrows():
            if df['coordinate'].within(row['geometry']):
                return SA2_code
    else:
        return int(df['SA2_code'])

preprocess_postcode()
