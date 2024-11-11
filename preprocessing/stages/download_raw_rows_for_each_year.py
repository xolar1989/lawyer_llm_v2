import io
import logging

import boto3
import pandas as pd
from dask import delayed
from tqdm import tqdm

from preprocessing.utils.defaults import SEJM_API_URL, DATALAKE_BUCKET, DAG_TABLE_ID
from preprocessing.utils.dynamodb_helper import meta_DULegalDocumentsMetaData_without_filename
from preprocessing.utils.s3_helper import upload_json_to_s3
from preprocessing.utils.sejm_api_utils import make_api_call
from preprocessing.utils.stage_def import FlowStep
from dask.distributed import Client, as_completed
import pyarrow as pa
import pyarrow.parquet as pq


class DownloadRawRowsForEachYear(FlowStep):

    @staticmethod
    def get_general_info(type_document):
        logging.info(f"Fetching general info for document type: {type_document}")
        general_info = make_api_call(f"{SEJM_API_URL}/{type_document}")
        if 'years' not in general_info:
            raise Exception("There is no 'years' key in the response from the Sejm API")

        logging.info(f"Found {len(general_info['years'])} years for document type {type_document}")
        return general_info

    @staticmethod
    def upload_parquet_to_s3(df, bucket_name, s3_key):
        s3_client = boto3.client('s3', region_name='eu-west-1')
        out_buffer = io.BytesIO()

        # Convert DataFrame to Apache Arrow Table
        table = pa.Table.from_pandas(df)

        # Write the Parquet file to a buffer
        pq.write_table(table, out_buffer)
        out_buffer.seek(0)

        # Upload to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=out_buffer.getvalue())
        logging.info(f"Uploaded {s3_key} to S3")

    @classmethod
    def get_row_metadata_of_documents_each_year(cls, year, type_document, invoke_id):
        w = f"{SEJM_API_URL}/{type_document}/{year}"
        response = make_api_call(f"{SEJM_API_URL}/{type_document}/{year}")
        if 'items' not in response:
            logging.error(
                f"There is no 'documents' key in the response from the Sejm API, for year: {year} and type: {type_document}")
            raise Exception(
                f"There is no 'documents' key in the response from the Sejm API, for year: {year} and type: {type_document}")
        logging.info(f"Found {len(response['items'])} documents for year {year} and type {type_document}")

        json_data_list = response['items']

        df = pd.DataFrame(json_data_list)

        df = df.reindex(columns=meta_DULegalDocumentsMetaData_without_filename.columns)

        cls.upload_parquet_to_s3(df, DATALAKE_BUCKET, f"api-sejm/{str(invoke_id)}/{type_document}/{year}.parquet")

        logging.info(f"Uploaded data at api-sejm/{str(invoke_id)}/{type_document}/{year} to S3")
        logs_client = boto3.client('logs', region_name='eu-west-1')

        logging.info(f"Uploaded data at api-sejm/{str(invoke_id)}/{type_document}/{year} to S3")

    @classmethod
    @FlowStep.step(task_run_name='download_raw_rows_for_each_year', retries=3, retry_delay_seconds=20)
    def run(cls, flow_information: dict, dask_client: Client, type_document: str):
        general_info = cls.get_general_info("DU")
        invoke_id = flow_information[DAG_TABLE_ID]

        # Use tqdm to monitor the progress
        delayed_tasks = [
            delayed(cls.get_row_metadata_of_documents_each_year)(year, type_document, invoke_id)
            for year in general_info['years']
        ]

        futures = dask_client.compute(delayed_tasks, sync=False)

        results = []
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {type_document}", unit="year",
                           ncols=100):
            result = future.result()  # Get the result of the completed task
            results.append(result)

        # Log worker information
        for future in futures:
            who_has = dask_client.who_has(future)
            print(f"Task {future.key} executed on workers: {who_has}")
        return f"s3://{DATALAKE_BUCKET}/api-sejm/{str(invoke_id)}/{type_document}/*.parquet"
