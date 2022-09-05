from urllib.request import urlretrieve, Request, urlopen
import zipfile
import os


external_output_path = 'data/external/'

url_all_postcodes = "https://www.matthewproctor.com/Content/postcodes/australian_postcodes.csv"
url_SA2_shapefile = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography" + \
    "-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"
url_total_income = "https://www.abs.gov.au/statistics/labour/earnings-and-working-conditions/personal-income-australia/2014-15-2018-19/6524055002_DO001.xlsx"


def download_external_data():
    # check if the output directory exists
    if not os.path.exists(external_output_path):
        os.makedirs(external_output_path)

    # download files to "external" directory
    print("Downloading postcode file......")
    req = Request(url=url_all_postcodes, headers={'User-Agent': 'Mozilla/5.0'})
    with open(external_output_path + "australian_postcodes.csv", "wb") as f:
        f.write(urlopen(req).read())
    print("Finished!")

    print("Downloading SA2 shapefile......")
    zip_path, _ = urlretrieve(url_SA2_shapefile)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(external_output_path + "SA2_2021")
    print("Finished!")

    print("Downloading total income......") 
    urlretrieve(url_total_income, external_output_path+"total_income.xlsx")

download_external_data()