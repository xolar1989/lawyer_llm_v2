import pandas as pd

from preprocessing.utils.defaults import DATALAKE_BUCKET, DAG_TABLE_ID, AWS_REGION
from preprocessing.utils.dynamodb_helper import fetch_segment_data, meta_DULegalDocumentsMetaData
from preprocessing.utils.stage_def import FlowStep
from dask.distributed import Client, as_completed
import dask.dataframe as dd


class FilterRowsFromApiAndUploadToParquetForFurtherProcessing(FlowStep):

    @staticmethod
    def compare_rows(row, columns_order, columns_to_compare):

        row_result = pd.Series({col.replace('_api', ''): row[col] for col in row.index if col.endswith('_api')})
        row_result['ELI'] = row['ELI']

        # Check if the row exists in DynamoDB (if any key field from the DynamoDB side is NaN, it's missing)
        if pd.isna(row['document_year_db']):  # or you can check another field from DynamoDB
            row_result['missing_in_dynamodb'] = True
            for col in columns_to_compare:
                row_result[f'{col}_changed'] = None
        else:
            # Compare the values from API and DynamoDB
            row_result['missing_in_dynamodb'] = False
            for col in columns_to_compare:
                if row[f'{col}_db'] != row[f'{col}_api']:
                    row_result[f'{col}_changed'] = True
                else:
                    row_result[f'{col}_changed'] = False

        return row_result.reindex(columns_order)

    meta_for_updates_filtering = pd.DataFrame({
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
        'type': pd.Series(dtype='str'),
        'filename_of_pdf': pd.Series(dtype='str'),
        'missing_in_dynamodb': pd.Series(dtype='bool'),
        'changeDate_changed': pd.Series(dtype='bool'),
        'status_changed': pd.Series(dtype='bool'),
        'title_changed': pd.Series(dtype='bool'),
        'promulgation_changed': pd.Series(dtype='bool'),
        'filename_of_pdf_changed': pd.Series(dtype='bool')
    })

    @classmethod
    @FlowStep.step(task_run_name='filter_rows_from_api_and_upload_to_parquet_for_further_processing')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet: str):
        segments = list(range(workers_count))
        table_name = "DULegalDocumentsMetaData"
        ddf_segments = dd.from_pandas(pd.DataFrame({'segment': segments}), npartitions=workers_count)
        dynamodb_ddf = ddf_segments.map_partitions(
            lambda df: fetch_segment_data(
                df_partition=df,
                table_name=table_name,
                aws_region=AWS_REGION,
                total_segments=workers_count,
                columns_order=meta_DULegalDocumentsMetaData.columns.tolist(),
                meta=meta_DULegalDocumentsMetaData
            ),
            meta=meta_DULegalDocumentsMetaData)

        ddf_from_datalake = cls.read_from_datalake(s3_path_parquet, meta_DULegalDocumentsMetaData)

        s = ddf_from_datalake.compute()

        r = dynamodb_ddf.compute()

        merged_ddf = ddf_from_datalake.merge(dynamodb_ddf, on='ELI', how='left', suffixes=('_api', '_db'))

        columns_which_could_change = [col.replace('_changed', '') for col in cls.meta_for_updates_filtering.columns if
                                      col.endswith('_changed')]
        changes_ddf = merged_ddf.map_partitions(lambda df: df.apply(
            cls.compare_rows, axis=1, args=(
                list(cls.meta_for_updates_filtering.columns), columns_which_could_change,)
        ),
                                                meta=cls.meta_for_updates_filtering)

        rara = changes_ddf.compute()

        ## First i get all rows which property have changed or are missing in dynamodb
        ddf_to_changed = changes_ddf[
            (changes_ddf['missing_in_dynamodb'] == True) |  # Rows missing in DynamoDB
            (changes_ddf['changeDate_changed'] == True) |  # Changed properties
            (changes_ddf['status_changed'] == True) |
            (changes_ddf['title_changed'] == True) |
            (changes_ddf['promulgation_changed'] == True) |
            (changes_ddf['filename_of_pdf_changed'] == True)
            ]

        ## then i filtered out rows which weren't in db (beacuse they had been e.t.c 'uchylony' before step was triggered), we wanna update rows which has been changed status but before had been 'akt posiada tekst jednolity'
        ## so when it status changed e.t.c from 'akt posiada tekst jednolity' to 'uchylony' we wanna changed it in dynamodb
        # status 'obowiązujący' as well for instance DU/2023/326 'Ustawa z dnia 26 stycznia 2023 r. o fundacji rodzinnej'
        ## where ustawa has been created recently and hasn't been updated yet so it hasn't have 'tekst jednolity'
        ## Example scenarios:
        ## Row 1: Missing in DynamoDB, Status in API: 'uchylony' -> This row will be filtered out because its status is not 'akt posiada tekst jednolity'.
        ## Row 2: Missing in DynamoDB, Status in API: 'akt posiada tekst jednolity' -> This row will be kept because its status is 'akt posiada tekst jednolity'.
        ## Row 3: Present in DynamoDB, Status changed from 'akt posiada tekst jednolity' to 'uchylony' -> This row will be kept because its status has changed and it was previously stored with 'akt posiada tekst jednolity'.
        ## Row 4: Present in DynamoDB, No change in properties -> This row will be excluded because nothing has changed.
        ddf_cleaned = ddf_to_changed[~(
                (ddf_to_changed['missing_in_dynamodb'] == True) &
                (~ddf_to_changed['status'].isin(['akt posiada tekst jednolity', 'obowiązujący']))
        )
        ]

        ddf_cleaned = ddf_cleaned.drop(columns=[col for col in ddf_cleaned.columns if col.endswith('_changed')],
                                       errors='ignore')

        ## rows which have changed rows columns or missing in dynamodb
        return cls.save_result_to_datalake(ddf_cleaned, flow_information, cls)
