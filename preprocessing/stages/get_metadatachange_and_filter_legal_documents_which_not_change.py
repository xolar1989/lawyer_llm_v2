from datetime import datetime

import numpy as np
import pandas as pd

from preprocessing.utils.defaults import DATALAKE_BUCKET, DAG_TABLE_ID, SEJM_API_URL, AWS_REGION
from preprocessing.utils.dynamodb_helper import meta_DULegalDocumentsMetaData, \
    meta_DULegalDocumentsMetaData_intermediate_result, meta_DULegalDocumentsMetaDataChanges_v2, fetch_segment_data
from preprocessing.utils.sejm_api_utils import make_api_call
from dask.distributed import Client, as_completed
import dask.dataframe as dd

from preprocessing.utils.stage_def import FlowStep


class GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange(FlowStep):

    @staticmethod
    def process_acts(row, key):
        list_of_changes = []
        if key in row['call_result']:
            for act in row['call_result'][key]:
                act_of_change = {}
                if 'act' not in act or 'ELI' not in act['act'] or 'type' not in act['act'] or 'title' not in act['act']:
                    raise Exception(f"Missing 'act' key in the act of change: {act}, for row: {row}, api error")
                if (key in ['Akty zmieniające', 'Akty uchylające', 'Uchylenia wynikające z']) and \
                        'date' not in act and 'promulgation' not in act['act']:
                    raise Exception(f"Missing 'date' key in the act of change: {act}, with {key} change, for row: {row}, api error")
                if (key in ['Akty zmieniające', 'Akty uchylające', 'Uchylenia wynikające z']) and 'date' in act:
                    act_of_change['date_of_change_annotation'] = act['date']
                elif 'promulgation' in act['act']:
                    act_of_change['date_of_change_annotation'] = act['act']['promulgation']
                elif act['act']['type'] == 'Obwieszczenie':
                    act_of_change['date_of_change_annotation'] = act['act']['announcementDate']
                else:
                    raise Exception(
                        f"There is no date for change in the act of change: {act}, for key: {key}, for row: {row}, api error")
                act_of_change['ELI_annotation'] = act['act']['ELI']
                act_of_change['type_annotation'] = act['act']['type']
                act_of_change['title_annotation'] = act['act']['title']
                act_of_change['TYPE_OF_CHANGE'] = key
                list_of_changes.append(act_of_change)
        return list_of_changes

    @classmethod
    def extract_act_info(cls, row):
        acts = cls.process_acts(row, 'Akty zmieniające') + cls.process_acts(row, 'Inf. o tekście jednolitym') \
               + cls.process_acts(row, 'Akty uchylające') + cls.process_acts(row,
                                                                             'Uchylenia wynikające z')  ## to test this

        return acts if acts else np.nan

    @staticmethod
    def explode_and_normalize_partition(df, columns_order):
        df_filtered_from_empty_references = df[df['call_result_list'].notna()]
        df_exploded = df_filtered_from_empty_references.explode('call_result_list', ignore_index=True)
        df_normalized = pd.json_normalize(df_exploded['call_result_list'])
        df_final = pd.concat([df_exploded.drop(columns=['call_result_list', 'call_result']), df_normalized], axis=1)
        return df_final.reindex(columns=columns_order, fill_value=pd.NA)

    @classmethod
    @FlowStep.step(task_run_name='from_parquet_to_process_it_and_store_in_dynamodb')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int, s3_path_parquet: str):
        ddf_from_datalake = cls.read_from_datalake(s3_path_parquet, meta_DULegalDocumentsMetaData_intermediate_result)

        rr = ddf_from_datalake.compute()

        ddf_from_datalake['call_result'] = ddf_from_datalake.apply(
            lambda row: make_api_call(f"{SEJM_API_URL}/{row['ELI']}/references"),
            axis=1,
            meta=('call_result', 'object'))

        ddf_from_datalake['call_result_list'] = ddf_from_datalake.apply(cls.extract_act_info, axis=1)

        ddf_final_api_ref = ddf_from_datalake.map_partitions(cls.explode_and_normalize_partition,
                                                             columns_order=meta_DULegalDocumentsMetaDataChanges_v2.columns.tolist(),
                                                             meta=meta_DULegalDocumentsMetaDataChanges_v2)

        segments = list(range(workers_count))
        table_name = "DULegalDocumentsMetaDataChanges_v2"
        ddf_segments = dd.from_pandas(pd.DataFrame({'segment': segments}), npartitions=workers_count)

        ## getting already stored references
        dynamodb_ddf_for_references = ddf_segments.map_partitions(lambda df: fetch_segment_data(
            df_partition=df,
            table_name=table_name,
            aws_region=AWS_REGION,
            total_segments=workers_count,
            columns_order=meta_DULegalDocumentsMetaDataChanges_v2.columns.tolist(),
            meta=meta_DULegalDocumentsMetaDataChanges_v2
        ), meta=meta_DULegalDocumentsMetaDataChanges_v2)

        ## merging already stored references with from api to check if there are any new ones not stored
        merged_ddf_for_ref_outer = ddf_final_api_ref.merge(
            dynamodb_ddf_for_references,
            on=['ELI', 'ELI_annotation'],
            how='outer',
            suffixes=('_api', '_db'),
            indicator=True
        )

        ## comparing and getting only new ones
        ddf_new_rows_from_api = merged_ddf_for_ref_outer[
            merged_ddf_for_ref_outer['_merge'] == 'left_only'
            ]

        ## getting ride of columns from db (which are null)
        ddf_new_rows_from_api = ddf_new_rows_from_api.rename(
            columns={col: col.replace('_api', '') for col in ddf_new_rows_from_api.columns if col.endswith('_api')}
        )
        ddf_new_rows_from_api = ddf_new_rows_from_api[meta_DULegalDocumentsMetaDataChanges_v2.columns.tolist()]
        ddf_new_rows_from_api['date_of_change_annotation'] = ddf_new_rows_from_api.map_partitions(
            lambda df: pd.to_datetime(df['date_of_change_annotation'], format='%Y-%m-%d', errors='coerce'),
            meta=('date_of_change_annotation', 'datetime64[ns]')
        )

        ddf_DULegalDocumentsMetaDataChanges_v2_new_rows = ddf_new_rows_from_api[
            ddf_new_rows_from_api['date_of_change_annotation'] < pd.Timestamp(datetime.now())
            ]

        ddf_DULegalDocumentsMetaDataChanges_v2_new_rows[
            'date_of_change_annotation'] = ddf_DULegalDocumentsMetaDataChanges_v2_new_rows.map_partitions(
            lambda df: df['date_of_change_annotation'].dt.strftime('%Y-%m-%d'),
            meta=('date_of_change_annotation', 'str')
        )

        ddf_checking_that_legal_act_have_new_references = ddf_from_datalake.merge(
            ddf_DULegalDocumentsMetaDataChanges_v2_new_rows[['ELI']].drop_duplicates()[['ELI']],
            on='ELI',
            how='left',
            indicator=True
        )

        ddf_checking_that_legal_act_have_new_references = ddf_checking_that_legal_act_have_new_references \
            .rename(columns={'_merge': 'have_new_changes_references'})

        ddf_checking_that_legal_act_have_new_references['have_new_changes_references'] = \
            ddf_checking_that_legal_act_have_new_references['have_new_changes_references'].replace(
                {
                    'both': True,
                    'left_only': False
                }
            )

        ddf_DULegalDocumentsMetaData_new_rows = ddf_checking_that_legal_act_have_new_references[
            (ddf_checking_that_legal_act_have_new_references[
                 'missing_in_dynamodb'] == True) |
            (ddf_checking_that_legal_act_have_new_references[
                 'have_new_changes_references'] == True)
            ]

        ddf_DULegalDocumentsMetaData_new_rows = ddf_DULegalDocumentsMetaData_new_rows[
            meta_DULegalDocumentsMetaData.columns.tolist()]

        ddf_DULegalDocumentsMetaData_result = ddf_DULegalDocumentsMetaData_new_rows.compute()

        ddf_DULegalDocumentsMetaDataChanges_v2_new_rows_result = ddf_DULegalDocumentsMetaDataChanges_v2_new_rows.compute()

        path_to_parquet_for_legal_documents = cls.save_result_to_datalake(
            result_ddf=ddf_DULegalDocumentsMetaData_new_rows,
            flow_information=flow_information,
            stage_type=cls,
            result_name="legal_documents"
        )
        path_to_parquet_for_legal_documents_changes_lists = cls.save_result_to_datalake(
            result_ddf=ddf_DULegalDocumentsMetaDataChanges_v2_new_rows,
            flow_information=flow_information,
            stage_type=cls,
            result_name="legal_documents_changes_lists"
        )
        return path_to_parquet_for_legal_documents, path_to_parquet_for_legal_documents_changes_lists
