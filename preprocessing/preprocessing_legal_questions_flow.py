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
from preprocessing.stages.create_dask_cluster import CreateRemoteDaskCluster
from preprocessing.stages.create_local_dask_cluster import CreateLocalDaskCluster
from preprocessing.stages.get_existing_dask_cluster import GetExistingDaskCluster
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

    dask_cluster = GetExistingDaskCluster.run(stack_name="dask-stack-216d65cb-5048-4c57-ab25-b51a4f49d17d")

    # dask_cluster = CreateLocalDaskCluster.run(
    #     num_workers=4
    # )
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

    dask_workers_count = dask_cluster.get_workers_count()

    client = Client(dask_cluster.get_cluster_url())

    path_to_parquet_legal_annotation_success_first_run, path_to_parquet_legal_annotation_failed_first_run = \
        RetrieveAnnotationsOfLegalUnitsFromQA.run(
            flow_information=flow_information, dask_client=client, workers_count=dask_cluster.get_workers_count()
        )

    w= 4


if __name__ == "__main__":
    ## Mean average precision
    ## Mean reciprocal rank
    ## Recall @k

    try:
        ## TODO JUST FOCUS ON THIS
        preparing_dataset()
    except Exception:
        aws_logger.error("Remote error:\n%s", traceback.format_exc())


    dask_cluster = CreateLocalDaskCluster.run(
        num_workers=4
    )




    nro = 623753229


    invoke_id = "72f213ee-7227-4e99-96f0-63a5766ed1d8"
    rows = list(get_mongodb_collection(
        db_name="scraping_lex",
        collection_name="questions_with_html"
    ).find_many({},
        {"_id": 0}))

    df = pd.DataFrame(rows)

    w = df.to_dict(orient='records')

    question_with_html = QuestionWithHtml.from_dict(get_mongodb_collection(
        db_name="scraping_lex",
        collection_name="questions_with_html"
    ).find_one(
        {
            'nro': nro
        },
        {"_id": 0}
    ))

    tt = """
    <div class=\"qa_a\">
    <div class=\"qa_a-cont-answer-label\"><h2>ODPOWIEDŹ</h2></div>
    <div class=\"qa_a-cont\"><p>Przychody z praw autorskich wypłacane osobie niewykonującej działalności gospodarczej
        opodatkowane są według skali podatkowej. Płatnik pobiera w takim przypadku podatek (12%) po odjęciu kosztów
        uzyskania (50%). W PIT-11 i PIT-4R przychody należy wykazać w pozycjach przewidzianych dla praw majątkowych.</p>
    </div>
    <div class=\"qa_a-just-judgment-reason\"><h2>UZASADNIENIE</h2></div>
    <div class=\"qa_a-just\"><p>Na mocy <a href=\"#/document/16794311?unitId=art(22)ust(9)\" class=\"act\">art. 22 ust.
        9-9b</a> ustawy z dnia 26 lipca 1991 r. o podatku dochodowym od osób fizycznych – dalej u.p.d.o.f. – koszty
        uzyskania niektórych przychodów określa się w wysokości 50% przychodów pomniejszonych od składki na
        ubezpieczenia społeczne w części finansowanej przez ubezpieczonego. Dotyczy to między innymi przychodów z tytułu
        korzystania przez twórców z praw autorskich i artystów wykonawców z praw pokrewnych, w rozumieniu odrębnych
        przepisów, lub rozporządzania przez nich tymi prawami</p>
        <p>W roku podatkowym łączne koszty uzyskania przychodów, o których mowa w <a
                href=\"#/document/16794311?unitId=art(22)ust(9)pkt(1)\" class=\"act\">art. 22 ust. 9 pkt 1-3</a>
            u.p.d.o.f., nie mogą przekroczyć kwoty stanowiącej górną granicę pierwszego przedziału skali podatkowej
            (120.000 zł w roku 2022).</p>
        <p>Aby koszty autorskie mogły być zastosowane, działalność podatnika musi mieścić się w katalogu zawartym w <a
                href=\"#/document/16794311?unitId=art(22)ust(9(b))\" class=\"act\">art. 22 ust. 9b</a> u.p.d.o.f. – w
            katalogu tym wymieniono między innymi przychody z tytułu działalności twórczej w zakresie architektury,
            architektury wnętrz, architektury krajobrazu, inżynierii budowlanej, urbanistyki, literatury, sztuk
            plastycznych, wzornictwa przemysłowego, muzyki, fotografiki, twórczości audialnej i audiowizualnej,
            programów komputerowych, gier komputerowych, teatru, kostiumografii, scenografii, reżyserii, choreografii,
            lutnictwa artystycznego, sztuki ludowej oraz dziennikarstwa. Z pytania wynika, że wypłacane są tantiemy, ale
            nie wiadomo za co. Zwrot \"tantiemy\" wskazuje jednak na autorski charakter wynagrodzenia.</p>
        <p>Zaliczki na podatek nalicza się w tym przypadku według stawki 12%.</p>
        <p>Przychody z praw autorskich zaliczane są w praw majątkowych. Zgodnie bowiem z <a
                href=\"#/document/16794311?unitId=art(18)\" class=\"act\">art. 18</a> u.p.d.o.f. za przychód z praw
            majątkowych uważa się w szczególności przychody z praw autorskich i praw pokrewnych w rozumieniu odrębnych
            przepisów, praw do projektów wynalazczych, praw do topografii układów scalonych, znaków towarowych i wzorów
            zdobniczych, w tym również z odpłatnego zbycia tych praw.</p></div>
</div>
    """

    # legal_act_dataset = LegalActDatasetWithTitleEmbedding.build(OpenAIEmbeddings(model="text-embedding-3-large",
    #                                                                              api_key=
    #                                                                              get_secret("OpenAiApiKey", AWS_REGION)[
    #                                                                                  "OPEN_API_KEY"]), invoke_id=invoke_id)
    # legal_act_dataset.insert_into_db()




    question_inserted = LegalQuestionWithAnnotations.from_dict(get_mongodb_collection(
        db_name="preparing_dataset_for_embedding",
        collection_name="question_with_annotations"
    ).find_one(
        {
            'nro': nro
        },
        {"_id": 0}
    ))

    t = 4
    redis = Redis(
        host="localhost",
        port=6379,
        password="redis",
        decode_responses=True
    )

    redis.ping()

    print(f"✅ Redis OK from worker {str(redis.connection_pool.connection_kwargs)}")
    now = time.time()

    pipe = redis.pipeline()

    # Step 1: Clean old request timestamps
    pipe.zremrangebyscore("openai_global", 0, now - 180)
    pipe.zadd("openai_global", {str(now): now})
    pipe.zcard("openai_global")
    pipe.expire("openai_global", 180)

    # Execute all operations atomically
    _, _, req_count, _ = pipe.execute()

    # total_tokens = sum(int(float(val)) for val in token_entries)

    d = 4

    model = ChatOpenAI(model="gpt-4o-mini", temperature=0,
                       api_key=get_secret("OpenAiApiKey", AWS_REGION)["OPEN_API_KEY"], rate_limiter=rate_limiter)

    for _ in range(5):
        print("started")
        tic = time.time()
        model.invoke("hello")
        toc = time.time()
        print(toc - tic)

    s = 4

    # dask_workers_count = dask_cluster.get_workers_count()
    #
    # client = Client(dask_cluster.get_cluster_url())
    #
    #
    # futures = [client.submit(check_redis_connection) for _ in range(5)]
    # results = client.gather(futures)
    #
    # for r in results:
    #     print(r)

    # try:
    #     ## TODO JUST FOCUS ON THIS
    #     for_the_running_without_debugging()
    # except Exception:
    #     log.error("Remote error:\n%s", traceback.format_exc())
