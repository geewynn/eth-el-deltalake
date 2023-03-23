# ELT to Extract Block Data to BQ

## Description
Extract full raw ethereum block data into a data lake and load that data into bigquery. This project made use of python, and GCP.

## Environment

```
GCP_PROJECT_ID=<gcp project id>
BUCKET_NAME=<gcp bucket name>
INFURA_URL=<ethereum node rpc endpoint>
```

**blockBatch.py** - this python file extracts blockchain block data and store in google cloud storage.
**bqJsonLoad.py** - This file loads the extracted data into a bq dataset and table.

## To Run
Create a virtual env


`python3 -m venv env`



install requirements


`pip install -r requirements.txt`

