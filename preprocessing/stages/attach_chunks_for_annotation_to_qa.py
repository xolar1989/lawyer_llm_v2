import copy
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain
from typing import Dict, Any, List, Tuple, Set, Optional

import pandas as pd
from dask.distributed import Client, as_completed
import dask.dataframe as dd
from dask import delayed
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
from preprocessing.qustions_and_answers_objects.question_with_annotation_chunks import QuestionWithAnnotationChunks
from preprocessing.utils.defaults import DAG_TABLE_ID
from preprocessing.utils.stage_def import FlowStep


class AttachChunksForAnnotationToQA(FlowStep):

    @staticmethod
    def have_legal_units_range(ids: List[str]):
        return len(ids) == 2

    @classmethod
    def get_query_for_relevant_documents(cls, legal_question: LegalRephrasedQuestion, is_article: bool):
        dict_eli_with_art_nums = defaultdict(set)

        for answer_or_question_annotation in chain(
                legal_question.question_annotations,
                legal_question.answer_annotations):
            ELI = answer_or_question_annotation.legal_act_document.ELI
            if cls.have_legal_units_range(answer_or_question_annotation.units_ids.article_ids):
                for legal_unit in answer_or_question_annotation.legal_units:
                    dict_eli_with_art_nums[ELI].add(legal_unit.unit_id)
            elif len(answer_or_question_annotation.units_ids.article_ids) == 1:
                dict_eli_with_art_nums[ELI].add(answer_or_question_annotation.units_ids.article_ids[0])
            else:
                raise ValueError("Invalid state during retrieving article ids for query db")

        id = "unit_id" if is_article else "article_id"
        return {
            "$or": [
                {
                    "ELI": eli,
                    id: {"$in": list(art_ids)}
                }
                for eli, art_ids in dict_eli_with_art_nums.items()
            ]
        }, len([art_id for art_ids in dict_eli_with_art_nums.values() for art_id in art_ids])

    @classmethod
    def get_relevant_chunks(cls, legal_question: LegalRephrasedQuestion, collection_name, from_dict_func):
        is_article = collection_name == "article_span_chunks" or collection_name == "article_chunks"

        query, required_elements = cls.get_query_for_relevant_documents(legal_question, is_article=is_article)
        results = [from_dict_func(doc) for doc in get_mongodb_collection(
            db_name="chunks",
            collection_name=collection_name
        ).find_many(
            query,
            {
                "_id": 0,
            }
        )]
        if len(results) != required_elements and (collection_name == "article_span_chunks" or collection_name == "article_chunks"):
            raise ValueError(f"Invalid state in retriving relevant {collection_name} chunks")
        return results



    @classmethod
    def worker_task(cls, nro: int, invoke_id: str, model_id: str, max_token_length: int = 512, overlap: int = 100):
        try:
            legal_question = LegalRephrasedQuestion.from_dict(
                get_mongodb_collection(
                    db_name="preparing_dataset_for_embedding",
                    collection_name="rephrased_question"
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

            article_span_finder = ChunkFinder(strategy=ArticleSpanMatchStrategy())
            relevant_article_span_chunks = cls.get_relevant_chunks(legal_question, "article_span_chunks", ArticleChunks.from_dict)
            final_article_span_chunks = article_span_finder.get_chunks(legal_question, relevant_article_span_chunks, is_span=True)

            exact_article_finder = ChunkFinder(strategy=ExactArticleMatchStrategy())
            relevant_article_chunks = cls.get_relevant_chunks(legal_question, "article_chunks", ArticleChunks.from_dict)
            final_article_chunks = exact_article_finder.get_chunks(legal_question, relevant_article_chunks)

            exact_section_finder = ChunkFinder(strategy=ExactSectionMatchStrategy())
            relevant_section_chunks = cls.get_relevant_chunks(legal_question, "section_chunks", SectionChunks.from_dict)
            final_section_chunks = exact_section_finder.get_chunks(legal_question, relevant_section_chunks)

            exact_subpoint_finder = ChunkFinder(strategy=ExactPointMatchStrategy())
            relevant_point_chunks = cls.get_relevant_chunks(legal_question, "subpoint_chunks", SubpointChunks.from_dict)
            final_subpoint_chunks = exact_subpoint_finder.get_chunks(legal_question, relevant_point_chunks)

            question_with_chunks = QuestionWithAnnotationChunks(
                nro=legal_question.nro,
                invoke_id=legal_question.invoke_id,
                question_text=legal_question.rephrased_question,
                article_span_chunks=list(final_article_span_chunks),
                article_chunks=list(final_article_chunks),
                section_chunks=list(final_section_chunks),
                point_chunks=list(final_subpoint_chunks)
            )

            get_mongodb_collection(
                db_name="datasets",
                collection_name="question_with_annotation_chunks"
            ).insert_one(question_with_chunks.to_dict())
        except Exception as e:
            format_error = traceback.format_exc()
            r = 4

            try:
                get_mongodb_collection(
                    db_name="preparing_dataset_for_embedding",
                    collection_name="attaching_chunks_error"
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
        # nro = 622936012
        nro = 622884010
        # www = cls.worker_task(
        #     nro=nro,
        #     invoke_id=invoke_id,
        #     model_id=model_id
        # )

        df = pd.DataFrame(
            list(get_mongodb_collection(
                db_name="preparing_dataset_for_embedding",
                collection_name="rephrased_question"
            ).find_many(
                {
                    "answer_annotations": {"$exists": True, "$ne": []}
                },
                {
                    "_id": 0,
                    "nro": 1,
                    "invoke_id": 1
                }

            ))
        )

        existing_nro_set = set(
            doc["nro"] for doc in get_mongodb_collection(
                db_name="datasets",
                collection_name="question_with_annotation_chunks"
            ).find_many({}, {"nro": 1, "_id": 0})
        )

        ddf_questions = dd.from_pandas(df, npartitions=workers_count)

        ddf_filtered = ddf_questions[~ddf_questions["nro"].isin(existing_nro_set.union(existing_nro_set))]



        # ddf = dd.from_pandas(df, npartitions=workers_count)

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

        successful_df = pd.DataFrame(successful_rows).drop(columns=['status'])
        failed_df = pd.DataFrame(failed_rows).drop(columns=['status'])

        successful_result_ddf = dd.from_pandas(successful_df, npartitions=workers_count)
        failed_result_ddf = dd.from_pandas(failed_df, npartitions=workers_count)

        return cls.save_result_to_datalake(successful_result_ddf, flow_information, cls,
                                           result_name="successful_results"), \
            cls.save_result_to_datalake(failed_result_ddf, flow_information, cls, result_name="failed_results")
