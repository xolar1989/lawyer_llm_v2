import os
import time
import traceback

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from prefect import flow
from prefect.context import get_run_context
from redis.client import Redis

from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.qustions_and_answers_objects.legal_act_dataset_with_title_embedding import \
    LegalActDatasetWithTitleEmbedding
from preprocessing.qustions_and_answers_objects.legal_question_with_annotations import LegalQuestionWithAnnotations
from preprocessing.qustions_and_answers_objects.legal_unit_annotation_factory import LegalUnitAnnotationFactory
from preprocessing.qustions_and_answers_objects.legal_unit_annotations_llm_retriever import \
    LegalUnitAnnotationsLLMRetriever
from preprocessing.qustions_and_answers_objects.legal_units_retriever import LegalUnitsRetriever
from preprocessing.qustions_and_answers_objects.llm_legal_annotations import LegalReferenceList
from preprocessing.qustions_and_answers_objects.question_with_html import QuestionWithHtml
from preprocessing.stages.attach_chunks_for_annotation_to_qa import AttachChunksForAnnotationToQA
from preprocessing.stages.build_chunks_from_legal_acts import BuildChunksFromLegalActs
from preprocessing.stages.create_dask_cluster import CreateRemoteDaskCluster
from preprocessing.stages.create_local_dask_cluster import CreateLocalDaskCluster
from preprocessing.stages.explode_question_to_question_chunk_pair import ExplodeQuestionToQuestionChunkPair
from preprocessing.stages.get_existing_dask_cluster import GetExistingDaskCluster
from preprocessing.stages.rephrase_to_legal_query_question import RephraseToLegalQueryQuestion
from preprocessing.stages.retrieve_annotations_of_legal_units_from_qa import RetrieveAnnotationsOfLegalUnitsFromQA
from preprocessing.stages.start_dag import StartDag
from dask.distributed import Client

from preprocessing.stages.update_dask_cluster_workers import UpdateDaskClusterWorkers
# from preprocessing.stages.create_local_dask_cluster import CreateLocalDaskCluster
from preprocessing.utils.defaults import AWS_REGION
from preprocessing.utils.general import get_secret

# @flow
# def preprocessing_legal_questions(local_cluster: bool = True, cluster_stack_name: str | None = None):


load_dotenv()

rate_limiter = InMemoryRateLimiter(
    requests_per_second=0.1,  # <-- Can only make a request once every 10 seconds!!
    check_every_n_seconds=0.1,  # Wake up every 100 ms to check whether allowed to make a request,
    max_bucket_size=10,  # Controls the maximum burst size.
)

# For sync context:
import asyncio


@flow
def preparing_dataset():
    flow_run_context = get_run_context()
    flow_run = flow_run_context.flow_run
    flow_information = StartDag.run(flow_run.id, flow_run.name)
    StartDag.dag_information = flow_information

    STACK_NAME = f'dask-stack-{flow_run.id}'
    CLUSTER_NAME = f'Fargate-Dask-Cluster-{flow_run.name}'
    WORKERS_SERVICE = "Dask-Workers"

    # dask_cluster = GetExistingDaskCluster.run(stack_name="dask-stack-2e234e06-5e39-449c-8d3f-8f0fe88178ee")
    #
    # dask_cluster = CreateLocalDaskCluster.run(
    #     num_workers=5
    # )
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






    # path_to_parquet_chunks_success_first_run, path_to_parquet_chunks_failed_first_run = BuildChunksFromLegalActs.run(
    #     flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
    #     model_id="sdadas/mmlw-retrieval-roberta-large-v2"
    # )


    w = 4


    # path_to_parquet_legal_annotation_success_first_run, path_to_parquet_legal_annotation_failed_first_run = \
    #     RetrieveAnnotationsOfLegalUnitsFromQA.run(
    #         flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count()
    #     )

    t = 4
    # path_to_parquet_rephrase_success_first_run, path_to_parquet_rephrase_failed_first_run = RephraseToLegalQueryQuestion.run(
    #     flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
    #     model_id="sdadas/mmlw-retrieval-roberta-large-v2"
    # )

    t = 4
    # # # TODO it should be link parquet from RetrieveAnnotationsOfLegalUnitsFromQA with nro and invoke_id
    path_to_parquet_link_chunks_success_first_run, path_to_parquet_link_chunks_failed_first_run = AttachChunksForAnnotationToQA.run(
        flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
        model_id="sdadas/mmlw-retrieval-roberta-large-v2"
    )


    invoke_id = "72f213ee-7227-4e99-96f0-63a5766ed1d8"
    exploaded_df_path = ExplodeQuestionToQuestionChunkPair.run(flow_information=flow_information, invoke_id=invoke_id, dask_client=client, workers_count=dask_cluster.get_workers_count())

    ttttt = 's3://datalake-bucket-123/stages/$172aa988-a90d-4990-a986-bf175190c578/ExplodeQuestionToQuestionChunkPair/results.parquet.gzip'

    final_result = 's3://datalake-bucket-123/stages/$4b7047db-ab3c-4fca-811c-78069268dcae/ExplodeQuestionToQuestionChunkPair/results.parquet.gzip'
    w= 4

def preprocessing_legal_questions_dag(prev_invoke_id: str, local_cluster: bool = True, cluster_stack_name: str | None = None):
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
    path_to_parquet_chunks_success_first_run, path_to_parquet_chunks_failed_first_run = BuildChunksFromLegalActs.run(
        flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
        model_id="sdadas/mmlw-retrieval-roberta-large-v2"
    )

    ## STEP 2
    path_to_parquet_legal_annotation_success_first_run, path_to_parquet_legal_annotation_failed_first_run = \
        RetrieveAnnotationsOfLegalUnitsFromQA.run(
            flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count()
        )

    ## STEP 3
    path_to_parquet_rephrase_success_first_run, path_to_parquet_rephrase_failed_first_run = RephraseToLegalQueryQuestion.run(
        flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
        model_id="sdadas/mmlw-retrieval-roberta-large-v2"
    )

    ## STEP 4
    path_to_parquet_link_chunks_success_first_run, path_to_parquet_link_chunks_failed_first_run = AttachChunksForAnnotationToQA.run(
        flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count(),
        model_id="sdadas/mmlw-retrieval-roberta-large-v2"
    )

    ## STEP 5, final result ready dataframe for training embeeding model in article-span strategy and legal-unit strategy
    exploaded_df_path = ExplodeQuestionToQuestionChunkPair.run(flow_information=flow_information, invoke_id=prev_invoke_id, dask_client=client, workers_count=dask_cluster.get_workers_count())



if __name__ == "__main__":
    ## Mean average precision
    ## Mean reciprocal rank
    ## Recall @k



    try:
        ## TODO JUST FOCUS ON THIS
        preparing_dataset()
    except Exception:
        aws_logger.error("Remote error:\n%s", traceback.format_exc())

