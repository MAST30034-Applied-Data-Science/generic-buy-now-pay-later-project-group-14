# Generic Buy Now, Pay Later Project
## Group Members

## Instructions
To run the pipeline, please visit the `scripts` directory and run the files in order:

1. `download.py`: This downloads external datasets into the `data/external` directory.  
2. `process_external_data.py`: This script processes the "income" excel table and creates a postcode and SA2 lookup table. Processed data are stored in the `data/curated` directory.
3. `join_table.py`: This script reads raw data, conducts preliminary preprocessing and merges all data into a single data file.  
4. `fraud_model.py`: This script uses delta files to build a logistic regression model and predict whether a transaction is fraud.
5. `feature_engineering.py`: This file creates several features by aggregation which will be used in predicting features used in the ranking system.
6. `model.py`: This script preprocess, train and fit models on each of the 3 selected features that will be used in ranking merchants.
7. `ranking_system.py`: This file calculates a ranking score for each merchant and finds top 100 merhants overall and top 10 merchants in each segment.

