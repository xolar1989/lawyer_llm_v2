import copy
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain
from typing import Dict, Any, List, Tuple, Set, Optional

import pandas as pd
from bs4 import BeautifulSoup
from dask.distributed import Client, as_completed
import dask.dataframe as dd
from dask import delayed
from langchain_openai import ChatOpenAI
from tqdm import tqdm

from preprocessing.chunk_matching.article_span_match_strategy import ArticleSpanMatchStrategy
from preprocessing.chunk_matching.chunk_finder import ChunkFinder
from preprocessing.chunk_matching.exact_article_match_strategy import ExactArticleMatchStrategy
from preprocessing.chunk_matching.exact_point_match_strategy import ExactPointMatchStrategy
from preprocessing.chunk_matching.exact_section_match_strategy import ExactSectionMatchStrategy
from preprocessing.chunks.article_chunks import ArticleChunks
from preprocessing.chunks.chunk import AbstractChunks, Chunk
from preprocessing.chunks.section_chunks import SectionChunks
from preprocessing.chunks.subpoint_chunks import SubpointChunks
from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds
from preprocessing.qustions_and_answers_objects.legal_question_with_annotations import LegalQuestionWithAnnotations
from preprocessing.qustions_and_answers_objects.legal_rephrased_question import LegalRephrasedQuestion
from preprocessing.qustions_and_answers_objects.question_llm_rephraser import QuestionLLMRephraser
from preprocessing.utils.defaults import DAG_TABLE_ID, AWS_REGION
from preprocessing.utils.general import get_secret
from preprocessing.utils.stage_def import FlowStep


class RephraseToLegalQueryQuestion(FlowStep):

    @classmethod
    def worker_task(cls, nro: int, invoke_id: str, model_id: str):
        try:
            legal_question = LegalQuestionWithAnnotations.from_dict(
                get_mongodb_collection(
                    db_name="preparing_dataset_for_embedding",
                    collection_name="question_with_annotations"
                ).find_one(
                    {
                        "nro": nro,
                        "invoke_id": invoke_id
                    },
                    {
                        "_id": 0,
                    }
                )
            )

            soup = BeautifulSoup(legal_question.question_content, "html.parser")
            for tag in soup.find_all("a", class_="act"):
                tag.replace_with(f"[{tag.get_text(strip=True)}]")
            clean_text = soup.get_text(strip=True, separator=" ")
            question_rephraser = QuestionLLMRephraser(ChatOpenAI(model="gpt-5-mini", temperature=1, api_key=get_secret("OpenAiApiKey", AWS_REGION)["OPEN_API_KEY"]), invoke_id)

            rephrased_question = question_rephraser.rephrase(clean_text)

            legal_rephrased_question = LegalRephrasedQuestion.from_question(legal_question, rephrased_question, model_id)

            get_mongodb_collection(
                db_name="preparing_dataset_for_embedding",
                collection_name="rephrased_question"
            ).insert_one(legal_rephrased_question.to_dict())
        except Exception as e:
            format_error = traceback.format_exc()
            r = 4

            try:
                get_mongodb_collection(
                    db_name="preparing_dataset_for_embedding",
                    collection_name="rephrasing_error"
                ).update_one(
                    {
                        "nro": nro,
                        "invoke_id": invoke_id
                    },
                    {
                        "$set": {
                            "nro": nro,
                            "invoke_id": invoke_id,
                            "error_msg": str(e),
                            "traceback": format_error,
                            "type": e.__class__.__name__
                        }
                    },
                    upsert=True
                )
            except Exception as ew:
                aws_logger.error("Failed to log error to MongoDB", exc_info=True)
                pass
            return {
                'nro': nro,
                'invoke_id': invoke_id,
                'status': 'failed'
            }

        return {
            'nro': nro,
            'invoke_id': invoke_id,
            'status': 'success'
        }

    @classmethod
    @FlowStep.step(task_run_name='chunk_legal_acts')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int, model_id: str):
        invoke_id = "72f213ee-7227-4e99-96f0-63a5766ed1d8"
        # nro = 622878879
        nro = 623752167
        www = cls.worker_task(
            nro=622884010,
            invoke_id=invoke_id,
            model_id=model_id
        )
        df = pd.DataFrame(
            list(get_mongodb_collection(
                db_name="preparing_dataset_for_embedding",
                collection_name="question_with_annotations"
            ).find_many(
                {},
                {
                    "_id": 0,
                    "nro": 1,
                    "invoke_id": 1
                }

            ))
        )

        existing_nro_set = set(
            doc["nro"] for doc in get_mongodb_collection(
                db_name="preparing_dataset_for_embedding",
                collection_name="rephrased_question"
            ).find_many({}, {"nro": 1, "_id": 0})
        )

        ddf_questions = dd.from_pandas(df, npartitions=workers_count)

        ddf_filtered = ddf_questions[~ddf_questions["nro"].isin(existing_nro_set.union(existing_nro_set))]




        t =4


        r = 4
        w = ddf_filtered.compute()

        r = 4
        delayed_tasks = ddf_filtered.map_partitions(
            lambda df: [
                delayed(cls.worker_task)(
                    nro=row["nro"],
                    invoke_id=row["invoke_id"],
                    model_id=model_id
                )
                for row in df.to_dict(orient='records')
            ]
        ).compute()

        flat_tasks = [task for sublist in delayed_tasks for task in sublist]

        futures = dask_client.compute(flat_tasks, sync=False)

        results = []
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Downloading pdfs", unit="document",
                           ncols=100):
            result = future.result()  # Get the result of the completed task
            results.append(result)

        # Log worker information
        for future in futures:
            who_has = dask_client.who_has(future)
            aws_logger.info(f"Task {future.key} executed on workers: {who_has}")

        successful_rows = [r for r in results if r['status'] == 'success']
        failed_rows = [r for r in results if r['status'] == 'failed']

        failed_df = pd.DataFrame(failed_rows).drop(columns=['status'], errors='ignore')
        successful_df = pd.DataFrame(successful_rows).drop(columns=['status'], errors='ignore')

        successful_result_ddf = dd.from_pandas(successful_df, npartitions=workers_count)
        failed_result_ddf = dd.from_pandas(failed_df, npartitions=workers_count)

        return cls.save_result_to_datalake(successful_result_ddf, flow_information, cls,
                                           result_name="successful_results"), \
            cls.save_result_to_datalake(failed_result_ddf, flow_information, cls, result_name="failed_results")
