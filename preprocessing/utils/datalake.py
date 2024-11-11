import io
from typing import Dict, Type

import boto3
import pandas as pd
import dask.dataframe as dd
from dask.dataframe import DataFrame

from preprocessing.utils.defaults import DAG_TABLE_ID
from preprocessing.utils.stage_def import get_storage_options_for_ddf_dask

from PyPDF2 import PdfFileWriter

from requests.models import Response


class Datalake:
    def __init__(self, datalake_bucket: str, aws_region: str = 'eu-west-1'):
        self.datalake_bucket = datalake_bucket
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)

    def save_to_datalake_parquet(self, result_ddf: DataFrame, flow_information: Dict[str, str], stage_type: Type,
                                 result_name: str = 'results') -> str:

        s3_path = f's3://{self.datalake_bucket}/stages/${flow_information[DAG_TABLE_ID]}/{stage_type.__name__}/{result_name}.parquet.gzip'
        result_ddf.to_parquet(
            s3_path,
            engine='auto',
            compression='snappy',
            write_index=False,
            storage_options=get_storage_options_for_ddf_dask(self.aws_region)
        )
        return s3_path

    def read_from_datalake_parquet(self, s3_path_parquet: str, meta: pd.DataFrame):
        ddf_from_parquet = dd.read_parquet(s3_path_parquet,
                                           engine='auto',
                                           storage_options=get_storage_options_for_ddf_dask(self.aws_region)
                                           )
        ddf_from_parquet = ddf_from_parquet.astype(
            {col: dtype.name for col, dtype in meta.dtypes.to_dict().items()})
        return ddf_from_parquet

    def read_from_datalake_pdf(self, key: str):
        return io.BytesIO(self.s3_client.get_object(Bucket=self.datalake_bucket, Key=key)['Body'].read())

    def save_to_datalake_pdf(self, **kwargs):
        if 'response' in kwargs:
            return self._save_to_datalake_pdf_http_response(kwargs['response'], kwargs['flow_information'],
                                                            kwargs['filename'], kwargs['stage_type'])
        elif 'pdf_writer' in kwargs:
            return self._save_to_datalake_pdf_bytes(kwargs['pdf_writer'], kwargs['flow_information'],
                                                    kwargs['filename'], kwargs['stage_type'])
        else:
            raise ValueError('Either response or pdf_writer must be provided')

    def _save_to_datalake_pdf_http_response(self, response: Response, flow_information: Dict, filename: str,
                                            stage_type: Type):
        s3_uri = f'stages/${flow_information[DAG_TABLE_ID]}/{stage_type.__name__}/'
        pdf_buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                pdf_buffer.write(chunk)
        pdf_buffer.seek(0)
        self.s3_client.upload_fileobj(pdf_buffer, self.datalake_bucket, f'{s3_uri}/{filename}')
        return f's3://{self.datalake_bucket}/{s3_uri}/{filename}'

    def _save_to_datalake_pdf_bytes(self, pdf_writer: PdfFileWriter, flow_information: Dict, filename: str,
                                    stage_type: Type):
        s3_uri = f'stages/${flow_information[DAG_TABLE_ID]}/{stage_type.__name__}/'
        pdf_buffer = io.BytesIO()
        pdf_writer.write(pdf_buffer)
        pdf_buffer.seek(0)
        self.s3_client.upload_fileobj(pdf_buffer, self.datalake_bucket, f'{s3_uri}/{filename}')
        return f's3://{self.datalake_bucket}/{s3_uri}/{filename}'
