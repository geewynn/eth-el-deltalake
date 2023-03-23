import os
import time
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()


BUCKET_NAME = os.getenv('BUCKET_NAME')
FILE_NAME = 'block-full.json'

# use this if you are running locally
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<service-acct-json>"
data_file_path = f'gs://{BUCKET_NAME}/{FILE_NAME}'
print(data_file_path)


client = bigquery.Client()
table_id = 'godwin-dbt.stripe.block-test'

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("jsonrpc", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "result",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(
                    "baseFeePerGas", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("difficulty", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("extraData", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("gasLimit", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("gasUsed", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("hash", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("logsBloom", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("miner", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("mixHash", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("nonce", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("number", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("parentHash", "STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    "receiptsRoot", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("sha3Uncles", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("size", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("stateRoot", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("timestamp", "STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    "totalDifficulty", "STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    "transactions",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField(
                            "blockHash", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "blockNumber", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "from", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("gas", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "gasPrice", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "maxFeePerGas", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "maxPriorityFeePerGas", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "hash", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "input", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "nonce", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("to", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "transactionIndex", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "value", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "type", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "accessList", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "chainId", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("v", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("r", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("s", "STRING", mode="NULLABLE"),
                    ]
                ),
                bigquery.SchemaField("transactionsRoot",
                                     "STRING", mode="NULLABLE"),
                bigquery.SchemaField("uncles", "STRING", mode="NULLABLE")
            ],
        ),
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition='WRITE_APPEND',
    max_bad_records=100

)
job = client.load_table_from_uri(
    data_file_path,
    table_id,
    job_config=job_config
)

while job.state != 'DONE':
    job.reload()
    time.sleep(1)
print(job.result())
