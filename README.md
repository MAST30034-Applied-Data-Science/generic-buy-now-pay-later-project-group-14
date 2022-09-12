# Generic Buy Now, Pay Later Project
## Group Members

## Instructions
To run the pipeline, please visit the `scripts` directory and run the files in order:

1. `download.py`: This downloads external datasets into the `data/external` directory.  
2. `process_external_data.py`: This script processes the "income" excel table and creates a postcode and SA2 lookup table. Processed data are stored in the `data/curated` directory.
3. `ETL.py`: This script reads raw data, conducts preliminary preprocessing and merges all data into a single data file.  

