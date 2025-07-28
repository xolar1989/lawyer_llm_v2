from dotenv import load_dotenv

import os


import re
import traceback

import boto3
import cv2
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from pdfplumber.table import Table
from dask.distributed import Client, as_completed

from prefect import flow

from prefect.context import get_run_context
from tqdm import tqdm

from preprocessing.dask.local_dask_cluster import LocalDaskCluster
from preprocessing.logging.aws_logger import aws_logger
from preprocessing.stages.create_local_dask_cluster import CreateLocalDaskCluster
from preprocessing.stages.download_pdfs_for_legal_acts_rows import DownloadPdfsForLegalActsRows
from preprocessing.stages.download_raw_rows_for_each_year import DownloadRawRowsForEachYear
from preprocessing.stages.establish_legal_act_segments_boundaries import EstablishLegalActSegmentsBoundaries
from preprocessing.stages.filter_rows_from_api_and_upload_to_parquet_for_further_processing import \
    FilterRowsFromApiAndUploadToParquetForFurtherProcessing
from preprocessing.stages.get_existing_dask_cluster import GetExistingDaskCluster
from preprocessing.stages.get_metadatachange_and_filter_legal_documents_which_not_change import \
    GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange
from preprocessing.stages.get_rows_of_legal_acts_for_preprocessing import GetRowsOfLegalActsForPreprocessing
from preprocessing.stages.line_division import LegalActLineDivision
from preprocessing.stages.structure_legal_acts import StructureLegalActs
from preprocessing.stages.update_dask_cluster_workers import UpdateDaskClusterWorkers
from preprocessing.stages.create_dask_cluster import CreateRemoteDaskCluster
from preprocessing.stages.start_dag import StartDag

from preprocessing.dask.fargate_dask_cluster import retry_dask_task
from preprocessing.utils.datalake import Datalake
from preprocessing.utils.defaults import ERROR_TOPIC_NAME, AWS_REGION, DAG_TABLE_ID, \
    DATALAKE_BUCKET
from distributed.versions import get_versions

from pdf2image import convert_from_bytes

import logging
from cloudwatch import cloudwatch

load_dotenv()

logger = aws_logger

os.environ['TZ'] = 'UTC'

r = os.environ

get_versions()

## (C:\Users\karol\Desktop\python-projects\pytorch-start\env) this env start

equations_signs = ["=", ">", "<", "≥", "≤", "≠", "≡", "≈", "≅"]


@flow
def preprocessing_api():
    flow_run_context = get_run_context()
    flow_run = flow_run_context.flow_run

    flow_information = StartDag.run(flow_run.id, flow_run.name)
    StartDag.dag_information = flow_information

    STACK_NAME = f'dask-stack-{flow_run.id}'
    CLUSTER_NAME = f'Fargate-Dask-Cluster-{flow_run.name}'
    WORKERS_SERVICE = "Dask-Workers"
    #
    #
    dask_cluster = CreateLocalDaskCluster.run(
        num_workers=5
    )
    R = 4
    ## TODO when we start flow define if stack name already exist, omit creation of stack
    # dask_cluster = CreateRemoteDaskCluster.run(
    #     stack_name=STACK_NAME,
    #     cluster_name=CLUSTER_NAME,
    #     workers_service_name=WORKERS_SERVICE,
    #     flow_run_id=flow_run.id,
    #     flow_run_name=flow_run.name,
    #     cluster_props={
    #         "EnableScaling": "false",
    #         "MemoryCapacity": "8192",
    #         "CpuCapacity": '4096'
    #     }
    # )
    #
    # dask_cluster = UpdateDaskClusterWorkers.run(
    #     dask_cluster=dask_cluster,
    #     desired_count=20
    # )

    datalake = Datalake(datalake_bucket=DATALAKE_BUCKET, aws_region=AWS_REGION)

    # dask_cluster = GetExistingDaskCluster.run(stack_name='dask-stack-e6ac5d3f-87ad-46d0-90df-45ede88d5dc3')
    # #

    # dask_cluster = RestartDaskClusterWorkers.run(
    #     dask_cluster=dask_cluster
    # )

    r = 4

    # dask_cluster = None

    client = Client(dask_cluster.get_cluster_url())
    # client = Client('idealistic-manul-TCP-0cd4f6af06afc9d5.elb.eu-west-1.amazonaws.com:8786')
    # client = Client("tcp://localhost:8786")

    r = 4
    # client = Client("agile-beagle-TCP-671138ce4c7af57a.elb.eu-west-1.amazonaws.com:8786")
    # flow222(flow_information=flow_information, dask_client=client, type_document="DU")

    # ss = download_raw_rows_for_each_year(flow_information=flow_information, dask_client=client, type_document="DU")
    # TODO UNCOMMENT THIS STAGE LATER
    # path_to_raw_rows = DownloadRawRowsForEachYear.run(flow_information=flow_information, dask_client=client,
    #                                                   type_document="DU")

    r = 4
    #

    path_to_raw_rows = 's3://datalake-bucket-123/api-sejm/a04e9d3d-1890-42cc-8cf5-3d190eb617c3/DU/*.parquet'
    # ## here we start new flow for test
    # TODO UNCOMMENT THIS STAGE LATER
    # path_to_rows = GetRowsOfLegalActsForPreprocessing.run(flow_information=flow_information,
    #                                                       dask_client=client,
    #                                                       s3_path=path_to_raw_rows)

    # path_to_rows = 's3://datalake-bucket-123/stages/$049eec07-0322-4530-a135-f66719696ede/GetRowsOfLegalActsForPreprocessing/rows_filtered.parquet.gzip'

    r = 4
    #
    # TODO UNCOMMENT THIS STAGE LATER
    # path_DULegalDocumentsMetaData_rows = FilterRowsFromApiAndUploadToParquetForFurtherProcessing.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=3,
    #     s3_path_parquet=path_to_rows)

    R = 4

    # path_DULegalDocumentsMetaData_rows = 's3://datalake-bucket-123/stages/$869382c1-1b5b-4294-80f3-d94e090c1edf/FilterRowsFromApiAndUploadToParquetForFurtherProcessing/results.parquet.gzip'
    #
    # new path to rows
    path_DULegalDocumentsMetaData_rows = 's3://datalake-bucket-123/stages/$47146fc8-fb81-443f-b9c4-05ba5486e045/FilterRowsFromApiAndUploadToParquetForFurtherProcessing/results.parquet.gzip'

    # TODO UNCOMMENT THIS STAGE LATER
    # path_to_parquet_for_legal_documents, path_to_parquet_for_legal_documents_changes_lists = GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=2,
    #     s3_path_parquet=path_DULegalDocumentsMetaData_rows
    # )

    r = 4
    #
    # path_to_parquet_for_legal_documents = 's3://datalake-bucket-123/stages/$a9239d0c-4b41-4939-a2d5-aae583a82363/GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange/legal_documents.parquet.gzip'
    # path_to_parquet_for_legal_documents_changes_lists = 's3://datalake-bucket-123/stages/$a9239d0c-4b41-4939-a2d5-aae583a82363/GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange/legal_documents_changes_lists.parquet.gzip'

    # TODO UNCOMMENT THIS STAGE LATER
    # path_to_parquet_for_legal_documents_with_s3_pdf = DownloadPdfsForLegalActsRows.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=2,
    #     s3_path_parquet=path_to_parquet_for_legal_documents
    # )

    # path_to_parquet_for_legal_documents_with_s3_pdf = 's3://datalake-bucket-123/stages/$693770a1-3055-424b-913d-4964e1e013ff/DownloadPdfsForLegalActsRows/results.parquet.gzip'

    # TODO UNCOMMENT THIS STAGE LATER
    # path_to_parquet_pages_boundaries = EstablishLegalActSegmentsBoundaries.run(flow_information=flow_information,
    #                                                                            dask_client=client,
    #                                                                            # workers_count=dask_cluster.get_workers_count(),
    #                                                                            workers_count=8,
    #                                                                            datalake=datalake,
    #                                                                            s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    path_to_parquet_pages_boundaries = 's3://datalake-bucket-123/stages/$72f213ee-7227-4e99-96f0-63a5766ed1d8/EstablishLegalActSegmentsBoundaries/results.parquet.gzip'

    # TODO UNCOMMENT THIS STAGE LATER takes 12 min
    # path_to_parquet_line_divisions_success, path_to_parquet_line_divisions_failed = LegalActLineDivision.run(flow_information=flow_information, dask_client=client, workers_count=8,
    #                                s3_path_parquet_with_eli_documents=path_to_parquet_pages_boundaries)



    path_to_parquet_line_divisions_success = 's3://datalake-bucket-123/stages/$e6ac5d3f-87ad-46d0-90df-45ede88d5dc3/LegalActLineDivision/successful_results.parquet.gzip'

    path_to_parquet_line_divisions_failed = 's3://datalake-bucket-123/stages/$e6ac5d3f-87ad-46d0-90df-45ede88d5dc3/LegalActLineDivision/failed_results.parquet.gzip'

    ## Todo rerun
    # path_to_parquet_line_divisions_success_rerun, path_to_parquet_line_divisions_failed_rerun = LegalActLineDivision.run(flow_information=flow_information, dask_client=client, workers_count=8,
    #                                                                                                          s3_path_parquet_with_eli_documents=path_to_parquet_line_divisions_failed)

    path_to_parquet_line_divisions_success_rerun = 's3://datalake-bucket-123/stages/$f8df0cb0-5f33-450a-9836-61973cdcb7e7/LegalActLineDivision/successful_results.parquet.gzip'
    path_to_parquet_line_divisions_failed_rerun = 's3://datalake-bucket-123/stages/$f8df0cb0-5f33-450a-9836-61973cdcb7e7/LegalActLineDivision/failed_results.parquet.gzip'


    # path_to_parquet_legal_parts_success_first_run, path_to_parquet_legal_parts_failed_first_run = StructureLegalActs.run(
    #     flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
    #     s3_path_parquet_with_eli_documents=path_to_parquet_line_divisions_success
    #     # s3_path_parquet_with_eli_documents=path_to_parquet_line_divisions_success_rerun
    # )

    path_to_parquet_legal_parts_success_first_run = 's3://datalake-bucket-123/stages/$b5da4ef7-d69f-45bb-a003-34a42264eb37/StructureLegalActs/successful_results.parquet.gzip'

    path_to_parquet_legal_parts_success_first_run_v2, path_to_parquet_legal_parts_failed_first_run_v2 = StructureLegalActs.run(
        flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
        s3_path_parquet_with_eli_documents=path_to_parquet_line_divisions_success
        # s3_path_parquet_with_eli_documents=path_to_parquet_line_divisions_failed_rerun`
    )

    s = 4

    r = 4

    # path_with_rows_with_established_tables_and_equations = ExtractTableAndEquationsFromLegalActs.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=dask_cluster.get_workers_count(),
    #     # workers_count=3,
    #     datalake=datalake,
    #     s3_path_parquet_with_eli_documents=path_to_parquet_pages_boundaries
    #
    # )

    # path_to_eli_documents_with_extracted_text = ExtractTextAndGetPagesWithTables.run(flow_information=flow_information,
    #                                                                                  dask_client=client,
    #                                                                                  workers_count=dask_cluster.get_workers_count(),
    #                                                                                  s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    # path_to_eli_documents_with_extracted_text = 's3://datalake-bucket-123/stages/$6129d63e-e933-4dcd-9f0a-a809c1cd7464/ExtractTextAndGetPagesWithTables/results.parquet.gzip'

    path_to_annotated_text_with_creating_functions = AnnotateTextWithCreatingFunctions.run(
        flow_information=flow_information, dask_client=client,
        workers_count=dask_cluster.get_workers_count(),
        s3_path_parquet_with_eli_documents=path_to_eli_documents_with_extracted_text)

    invoke_id = flow_information[DAG_TABLE_ID]

    # get_rows_for_preprocessing(flow_information=flow_information, dask_client=client, workers_count=2, s3_path=f"s3://{DATALAKE_BUCKET}/api-sejm/{str(invoke_id)}/DU/2023")
    # path_to_rows = GetRowsOfLegalActsForPreprocessing.run(flow_information=flow_information, dask_client=client,
    #                                                       s3_path=f"s3://datalake-bucket-123/api-sejm/ec86fc99-8aef-495d-9fb9-0c0a19beb26f/DU/2023")
    # get_rows_for_preprocessing(flow_information=flow_information, dask_client=client, workers_count=2,
    #                            s3_path=f"s3://datalake-bucket-123/api-sejm/ec86fc99-8aef-495d-9fb9-0c0a19beb26f/DU/2023")

    l = 4
    # path_DULegalDocumentsMetaData_rows = FilterRowsFromApiAndUploadToParquetForFurtherProcessing.run(
    #     flow_information=flow_information,
    #     dask_client=client, type_document="DU",
    #     workers_count=2,
    #     s3_path_parquet=path_to_rows)
    #
    # kk = dd.read_parquet(rr, engine='auto', storage_options=get_storage_options_for_ddf_dask(AWS_REGION))
    #
    # llll = kk.compute()

    w = 4
    # path_to_parquet_for_legal_documents, path_to_parquet_for_legal_documents_changes_lists = GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=2,
    #     s3_path_parquet=path_DULegalDocumentsMetaData_rows
    # )
    #

    miau = 's3://datalake-bucket-123/stages/$7a56873c-553a-4101-9b40-8a23d3e142a1/documents-to-download-pdfs/rows-legal-acts.parquet.gzip'
    # path_to_parquet_for_legal_documents_with_s3_pdf = DownloadPdfsForLegalActsRows.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=2,
    #     s3_path_parquet=miau
    # )

    # SaveMetaDataRowsIntoDynamodb.run(flow_information=flow_information,
    #                                  dask_client=client,
    #                                  type_document="DU",
    #                                  workers_count=2,
    #                                  s3_path_parquet_meta_data=path_to_parquet_for_legal_documents_with_s3_pdf,
    #                                  s3_path_parquet_meta_data_changes=path_to_parquet_for_legal_documents_changes_lists
    #                                  )

    # path_to_parquet_for_legal_documents_with_s3_pdf_test = 's3://datalake-bucket-123/stages/extract-text-from-documents-with-pdfs/$6c088812-1554-4bc8-b01b-7f076f8a68b3/documents_with_pdfs.parquet.gzip'
    #
    # path_to_parquet_for_legal_documents_changes_lists_test = 's3://datalake-bucket-123/stages/$6c088812-1554-4bc8-b01b-7f076f8a68b3/documents-to-download-pdfs/rows-legal-acts-changes-lists.parquet.gzip'
    #
    # ExtractTextAndGetPagesWithTables.run(flow_information=flow_information, dask_client=client, workers_count=2,
    #                                      s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf_test)
    ll = 4
    # general_info = get_general_info("DU")
    #
    # type_document = "DU"
    # invoke_id = flow_information[DAG_TABLE_ID]
    #
    # client = Client("tcp://localhost:8786")
    #
    # futures_for_each_year = client.map(get_row_metadata_of_documents_each_year, general_info['years'],
    #                                    [type_document] * len(general_info['years']),
    #                                    [invoke_id] * len(general_info['years']))
    #
    # results = []
    # for future in tqdm(as_completed(futures_for_each_year), total=len(futures_for_each_year),
    #                    desc=f"Processing {type_document}", unit="year", ncols=100):
    #     result = future.result()
    #     results.append(result)
    #
    # # w = client.map(square, range(10))
    # # results = []
    # s = 4
    # # l = client.gather(w)
    # for future in futures_for_each_year:
    #     who_has = client.who_has(future)
    #     print(f"Task {future.key} executed on workers: {who_has}")

    client = Client(dask_cluster.cluster_url)

    futures = client.map(square, range(10))
    results = client.gather(futures)
    print(results)

    s = 4


log = logging.getLogger(__name__)


@flow
def for_the_running_without_debugging(local_cluster: bool = True, cluster_stack_name: str | None = None):
    flow_run_context = get_run_context()
    flow_run = flow_run_context.flow_run

    flow_information = StartDag.run(flow_run.id, flow_run.name)
    StartDag.dag_information = flow_information

    STACK_NAME = f'dask-stack-{flow_run.id}'
    CLUSTER_NAME = f'Fargate-Dask-Cluster-{flow_run.name}'
    WORKERS_SERVICE = "Dask-Workers"

    if local_cluster:

        dask_cluster = CreateLocalDaskCluster.run(
            num_workers=3
        )

    else:
        if cluster_stack_name:
            dask_cluster = GetExistingDaskCluster.run(stack_name=cluster_stack_name)
        else:
            dask_cluster = CreateRemoteDaskCluster.run(
                stack_name=STACK_NAME,
                cluster_name=CLUSTER_NAME,
                workers_service_name=WORKERS_SERVICE,
                flow_run_id=flow_run.id,
                flow_run_name=flow_run.name,
                cluster_props={
                    "EnableScaling": "false",
                    "MemoryCapacity": "8192",
                    "CpuCapacity": '4096'
                }
            )

            dask_cluster = UpdateDaskClusterWorkers.run(
                dask_cluster=dask_cluster,
                desired_count=20
            )

    dask_workers_count = dask_cluster.get_workers_count()

    client = Client(dask_cluster.get_cluster_url())

    ## STEP 1
    path_to_raw_rows = DownloadRawRowsForEachYear.run(flow_information=flow_information, dask_client=client,
                                                      type_document="DU")

    ## STEP 2
    path_to_rows = GetRowsOfLegalActsForPreprocessing.run(flow_information=flow_information,
                                                          dask_client=client,
                                                          s3_path=path_to_raw_rows)
    ## STEP 3
    path_DULegalDocumentsMetaData_rows = FilterRowsFromApiAndUploadToParquetForFurtherProcessing.run(
        flow_information=flow_information,
        dask_client=client,
        workers_count=dask_workers_count,
        s3_path_parquet=path_to_rows)

    ## STEP 4
    path_to_parquet_for_legal_documents, path_to_parquet_for_legal_documents_changes_lists = GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange.run(
        flow_information=flow_information,
        dask_client=client,
        workers_count=dask_workers_count,
        s3_path_parquet=path_DULegalDocumentsMetaData_rows
    )

    ## STEP 5
    path_to_parquet_for_legal_documents_with_s3_pdf = DownloadPdfsForLegalActsRows.run(
        flow_information=flow_information,
        dask_client=client,
        workers_count=dask_workers_count,
        s3_path_parquet=path_to_parquet_for_legal_documents
    )

    ## STEP 6
    path_to_parquet_pages_boundaries = EstablishLegalActSegmentsBoundaries.run(flow_information=flow_information,
                                                                               dask_client=client,
                                                                               workers_count=dask_workers_count,
                                                                               s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    ## STEP 7  heavy cost, there are some rows which fails, however after rerun all success, there is a memory issue with cropping image, dask worker doesn't release memory
    path_to_parquet_line_divisions_success, path_to_parquet_line_divisions_failed = LegalActLineDivision.run(flow_information=flow_information, dask_client=client,
                                                              workers_count=dask_workers_count,
                                                              s3_path_parquet_with_eli_documents=path_to_parquet_pages_boundaries)


    ## STEP 8
    # TODO 43 out of 866 fails due to minor issues which such as „...“ which we don't handle it require extra logic if i had had more time for it I would have done
    path_to_parquet_legal_parts_success_first_run, path_to_parquet_legal_parts_failed_first_run = StructureLegalActs.run(
        flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
        s3_path_parquet_with_eli_documents=path_to_parquet_line_divisions_success
    )



    print(path_to_parquet_legal_parts_success_first_run)

    dask_cluster.delete_dask_cluster()


if __name__ == "__main__":
    # here we define flow
    # StartDag.run() >> CreateDaskCluster
    # try:
    #     ## TODO JUST FOCUS ON THIS
    #     for_the_running_without_debugging()
    # except Exception:
    #     log.error("Remote error:\n%s", traceback.format_exc())

    try:
        preprocessing_api()
    except Exception:
        log.error("Remote error:\n%s", traceback.format_exc())
        raise  # ważne! żeby Prefect widział błąd

# %%
