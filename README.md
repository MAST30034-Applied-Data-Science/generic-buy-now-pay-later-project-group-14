# Generic Buy Now, Pay Later Project
## Group Members
- Qingyang Wu, 1069332
- Jinxuan Chen, 1075516
- Xiaotong Wang, 1132828
- Luyao Chen, 1266572
- Yuchen Luo, 1153247


## README
**Research Goal:** Analysis to find the 100 best merchants for the Buy Now, Pay Later (BNPL) Firm to work with.

**Timeline:** The timeline for the research area is 02/2021 - 10/2022 Inclusive.


**Instructions:** To run the pipeline, please visit the `scripts` directory and run the files in order:

1. `download.py`: This downloads external postcodes and mean total income datasets into the `data/external` directory.  
2. `process_external_data.py`: This script processes the "income" excel table and creates a postcode and SA2 lookup table. Processed data are stored in the `data/curated` directory.
3. `join_table.py`: This script conducts preliminary preprocessing and merges all datasets into a single data file `data/curated/full_data/`.  
4. `fraud_model.py`: This script uses delta files to build a logistic regression model and predict whether other transactions are fraud. All fraud transactions are removed from full dataset and the final clean dataset is stored in `data/curated/full_data_without_fraud/`.
5. `feature_engineering.py`: This file summarises each merchant's data into several features and split the dataset into training and test sets. 
6. `model.py`: This script preprocesses, trains and fits models on each of the 3 selected features that will be used in ranking merchants. The models utilise merchants' historical data (2021) to predict their "revenue", "number of consumers" and "number of transactions" in 2022. 
7. `ranking_system.py`: This file calculates a ranking score for each merchant and finds top 100 merhants overall and top 10 merchants in each segment. 

**Notebook Info:** Information regarding notebooks written for this project can be found in the README in the `notebooks` directory.

