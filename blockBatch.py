import json
import os
import time
import gcsfs
import requests
import google.auth
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv('INFURA_URL')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')
FILE_NAME = 'block-full.json'


CREDENTIALS = google.auth.default()

FS = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID, token=None)


def query_blocks(start_block: int, end_block: int):
    """Function to get eth data from a range of blocks
        start_block: the starting block
        end_block: the last block to check
        num_of_blocks: the number of blocks to loop through
    """
    with FS.open(f"{FILE_NAME}", "wb") as f:
        for i in range(start_block, end_block):
            data = {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(i), True],
                "id": 1
            }
            start_time = time.time()
            response = requests.post(URL, json=data, timeout=100)
            block_data = response.json()
            data = json.dumps(block_data).encode('utf-8')
            f.write(data)
            end_time = time.time()
            print('time taken: ', end_time -
                  start_time, ' seconds', "block: ", i)
    f.close()


if __name__ == "__main__":
    START_BLOCK = 15649594
    END_BLOCK = 15649596
    start_time = time.time()
    query_blocks(START_BLOCK, END_BLOCK)
    end_time = time.time()
    print('total time taken: ', end_time - start_time, ' seconds')
