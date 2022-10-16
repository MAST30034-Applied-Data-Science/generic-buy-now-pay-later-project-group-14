# Notebook Info

1. `preliminary_analysis.ipynb`: This notebook conducts preliminary analysis on the provided merchant, consumer and transaction datasets.  
2. `check_missing_value.ipynb`: This notebook checks for any missing values in the merged dataset.  
3. `fraud_model.ipynb`: This notebook uses the given delta files and build a logistic regression model to predict whether other transactions are fraud. 
4. `geospatial.ipynb`: This notebook plots geospatial visualisations based on SA2 Area.
5. `feature_engineering.ipynb`: This notebook summarises each merchant's data into several features and create a training dataset
6. `model_analysis.ipynb`: This notebook preprocesses, trains and fits models on each of the 3 selected features that will be used in ranking merchants.
7. `ranking_system.ipynb`: This notebook calculates a ranking score for each merchant and finds top 100 merhants overall and top 10 merchants in each segment. 
8. `ranking_analysis.ipynb`: This notebook analyses the ranking results produced by the ranking system.
9. `summary.ipynb`: This notebook summarises the overall approach taken, limitations, assumptions, difficulties and analysis of the ranking results.
