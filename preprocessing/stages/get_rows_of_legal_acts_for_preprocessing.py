import logging
from typing import Union

import pandas as pd

from preprocessing.utils.defaults import SEJM_API_URL, AWS_REGION, DATALAKE_BUCKET, DAG_TABLE_ID
from preprocessing.utils.dynamodb_helper import meta_DULegalDocumentsMetaData, \
    meta_DULegalDocumentsMetaData_without_filename
from preprocessing.utils.sejm_api_utils import make_api_call
from preprocessing.utils.stage_def import FlowStep, FlowStepError, get_storage_options_for_ddf_dask
from dask.distributed import Client, as_completed
import dask.dataframe as dd
logger = logging.getLogger()

logger.setLevel(logging.INFO)

class GetRowsOfLegalActsForPreprocessing(FlowStep):

    @staticmethod
    def get_filename_for_legal_act_row(row) -> Union[str, None]:
        data = make_api_call(f"{SEJM_API_URL}/{row['ELI']}")

        # Check if there's a file with type 'U' in the 'texts' field

        ## TODO ADD checking text of html
        if 'texts' in data:
            for text in data['texts']:
                if text.get('type') == 'U':
                    return text.get('fileName')
        if row['status'] == 'akt posiada tekst jednolity':
            logger.error(f"Missing file with type 'U' for ELI: {row['ELI']}, but for status 'akt posiada tekst jednolity' we always expect a 'U' type file.")
            # raise FlowStepError(
            #     f"Missing file with type 'U' for ELI: {row['ELI']}, but for status 'akt posiada tekst jednolity' we always expect a 'U' type file.")
        return None

    @classmethod
    @FlowStep.step(task_run_name='get_rows_of_legal_acts_for_preprocessing')
    def run(cls, flow_information: dict, dask_client: Client, s3_path: str):
        ddf_legal_act_for_all_years = cls.read_from_datalake(s3_path, meta_DULegalDocumentsMetaData_without_filename)


        filtered_ddf = ddf_legal_act_for_all_years[
            (ddf_legal_act_for_all_years['type'] == 'Ustawa') & (
                    (
                            ((ddf_legal_act_for_all_years['status'] == 'obowiązujący') |
                             (ddf_legal_act_for_all_years['status'] == 'akt posiada tekst jednolity')) &
                            (~ddf_legal_act_for_all_years['title'].str.contains('o zmianie', na=False)) &
                            (~ddf_legal_act_for_all_years['title'].str.contains('budżetow', na=False))
                    ) |
                    (
                            (ddf_legal_act_for_all_years['status'] == 'akt posiada tekst jednolity') &
                            (ddf_legal_act_for_all_years['volume'] > 0)
                    )
            )
            ]

        filtered_ddf = filtered_ddf.set_index('ELI', sorted=True, drop=False, meta=pd.DataFrame({
            'ELI': pd.Series(dtype='str'),
            'document_year': pd.Series(dtype='int'),
            'status': pd.Series(dtype='str'),
            'announcementDate': pd.Series(dtype='str'),
            'volume': pd.Series(dtype='int'),
            'address': pd.Series(dtype='str'),
            'displayAddress': pd.Series(dtype='str'),
            'promulgation': pd.Series(dtype='str'),
            'pos': pd.Series(dtype='int'),
            'publisher': pd.Series(dtype='str'),
            'changeDate': pd.Series(dtype='str'),
            'textHTML': pd.Series(dtype='bool'),
            'textPDF': pd.Series(dtype='bool'),
            'title': pd.Series(dtype='str'),
            'type': pd.Series(dtype='str')
        }))
        filtered_ddf = filtered_ddf.rename(columns={'year': 'document_year'})
        filtered_ddf['filename_of_pdf'] = filtered_ddf.map_partitions(
            lambda df: df.apply(cls.get_filename_for_legal_act_row, axis=1),
            meta=('filename_of_pdf', 'str')  # Define the meta information for the new column
        )


        filtered_ddf = filtered_ddf[filtered_ddf['filename_of_pdf'].notnull()]

        result_ddf = filtered_ddf[meta_DULegalDocumentsMetaData.columns]

        return cls.save_result_to_datalake(
            result_ddf=result_ddf,
            flow_information=flow_information,
            stage_type=cls,
            result_name='rows_filtered'
        )
