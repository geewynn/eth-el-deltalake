# ELT to Extract Block Data to BQ

## Description
Extract full raw ethereum block data into a data lake and load that data into bigquery. This project made use of python, and GCP.

## Environment

```
GCP_PROJECT_ID=<gcp project id>
BUCKET_NAME=<gcp bucket name>
INFURA_URL=<ethereum node rpc endpoint>
DATASET_NAME=<bq dataset name>
TABLE_NAME=<bq table name>
```

**blockBatch.py** - this python file extracts blockchain block data and store in google cloud storage.
**bqJsonLoad.py** - This file loads the extracted data into a bq dataset and table.

## To Run
Create a virtual env


`python3 -m venv env`



install requirements


`pip install -r requirements.txt`

## GCS Bucket
![image](https://user-images.githubusercontent.com/29405050/227135622-2290ab0f-79c0-409f-bcb1-f9daf12b76f3.png)


## BQ Warehouse
![image](https://user-images.githubusercontent.com/29405050/227135069-580445c5-f71a-4e91-b60f-77dd563ce873.png)


