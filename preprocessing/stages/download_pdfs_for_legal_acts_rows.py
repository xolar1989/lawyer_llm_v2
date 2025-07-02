import io
import logging

import boto3
import pandas as pd
import requests
from dask import delayed
from prefect import get_run_logger
from requests.adapters import HTTPAdapter
from tqdm import tqdm

from preprocessing.utils.dask_cluster import retry_dask_task
from preprocessing.utils.defaults import SEJM_API_URL, DATALAKE_BUCKET, DAG_TABLE_ID, AWS_REGION
from preprocessing.utils.dynamodb_helper import meta_DULegalDocumentsMetaData
from preprocessing.utils.stage_def import FlowStep, get_storage_options_for_ddf_dask
from dask.distributed import Client, as_completed
import dask.dataframe as dd


class DownloadPdfsForLegalActsRows(FlowStep):

    @classmethod
    @retry_dask_task(retries=3, delay=5)
    def download_and_upload_pdf_to_s3(cls, row, s3_bucket, s3_key, storage_options):
        pdf_url = f'{SEJM_API_URL}/{row["ELI"]}/text/U/{row["filename_of_pdf"]}'
        s3_client = cls.get_aws_client_for_dask_worker("s3", storage_options)
        try:
            session_requests = requests.Session()
            session_requests.mount(pdf_url, HTTPAdapter(max_retries=10))
            response = session_requests.get(pdf_url, timeout=10, stream=True)
            response.raise_for_status()

            pdf_buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    pdf_buffer.write(chunk)
            pdf_buffer.seek(0)
            s3_uri = f'{s3_key}/{row["filename_of_pdf"]}'
            s3_client.upload_fileobj(pdf_buffer, s3_bucket, s3_uri)
            return {'ELI': row['ELI'], 's3_pdf_path': f's3://{s3_bucket}/{s3_uri}'}
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Timeout error occurred: {timeout_err}")
            raise Exception(f"Request timed out while downloading the PDF from: {pdf_url}")
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            raise Exception(f"HTTP error occurred while downloading the PDF from: {pdf_url}")
        except Exception as err:
            logging.error(f"Other error occurred: {err}")
            raise Exception(f"An error occurred while downloading the PDF and uploading to S3: {err}")

    @classmethod
    @FlowStep.step(task_run_name='download_pdfs_for_legal_acts_rows')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int, s3_path_parquet: str):
        ddf_from_datalake = cls.read_from_datalake(s3_path_parquet, meta_DULegalDocumentsMetaData)
        rr = ddf_from_datalake.compute()

        logger = get_run_logger()

        selected_ddf = ddf_from_datalake[['ELI', 'filename_of_pdf']]

        ## here load row, but not execute download_and_upload_pdf_to_s3
        delayed_tasks = selected_ddf.map_partitions(
            lambda df: [
                delayed(cls.download_and_upload_pdf_to_s3)(
                    row=row,
                    s3_bucket=DATALAKE_BUCKET,
                    s3_key=f'stages/documents-to-download-pdfs/{flow_information[DAG_TABLE_ID]}',
                    storage_options=get_storage_options_for_ddf_dask(AWS_REGION)
                )
                for row in df.to_dict(orient='records')
            ]
        ).compute()


        # each partition is a list of delayed tasks
        flat_tasks = [task for sublist in delayed_tasks for task in sublist]

        futures = dask_client.compute(flat_tasks, sync=False)

        # TODO to wonder, to change it to lazy loading instead of put results into memory
        # TODO Depends on debugging or verbose mode, run the progress bar(so load it into memory) or not
        results = []
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Downloading pdfs", unit="document",
                           ncols=100):
            result = future.result()  # Get the result of the completed task
            results.append(result)

        # Log worker information
        for future in futures:
            who_has = dask_client.who_has(future)
            # TODO
            logger.info(f"Task {future.key} executed on workers: {who_has}")

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)

        merged_ddf = ddf_from_datalake.merge(results_ddf, on='ELI', how='left')

        kkkk = merged_ddf.compute()

        return cls.save_result_to_datalake(merged_ddf, flow_information, cls)
