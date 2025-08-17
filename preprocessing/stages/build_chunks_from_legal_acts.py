import re
import traceback
from typing import Dict, List, Any, Tuple, Iterable

import pandas as pd
import unicodedata
from tqdm import tqdm
from transformers import AutoTokenizer, PreTrainedTokenizerBase

from preprocessing.chunks.article_chunker import ArticleChunker
from preprocessing.chunks.article_chunks import ArticleChunks
from preprocessing.chunks.chunk import AbstractChunks
from preprocessing.chunks.section_chunker import SectionChunker
from model.special_tokens import SPECIAL_TOKENS
from preprocessing.chunks.section_chunks import SectionChunks
from preprocessing.chunks.subpoint_chunker import SubpointChunker
from preprocessing.chunks.subpoint_chunks import SubpointChunks
from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.legal_unit import LegalUnit
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.utils.defaults import DAG_TABLE_ID
from preprocessing.utils.stage_def import FlowStep

from dask.distributed import Client, as_completed
import dask.dataframe as dd
from dask import delayed


class BuildChunksFromLegalActs(FlowStep):

    @classmethod
    def find_overlong_parts(
            cls,
            legal_unit_chunks: Iterable[AbstractChunks],
            tokenizer: PreTrainedTokenizerBase,
            max_length: int = 512
    ) -> List[Tuple[str, int, int]]:

        offending: List[Tuple[str, int, int]] = []
        for legal_unit_chunk in legal_unit_chunks:
            for idx, part in enumerate(legal_unit_chunk.parts):
                token_len = len(
                    tokenizer.encode(part.text, add_special_tokens=True)
                )
                if token_len > max_length:
                    if isinstance(legal_unit_chunk, ArticleChunks):
                        offending.append((f"Art {legal_unit_chunk.unit_id}", idx, token_len))
                    elif isinstance(legal_unit_chunk, SectionChunks):
                        offending.append((f"Art {legal_unit_chunk.article_id} Section {legal_unit_chunk.section_id}",
                                          idx, token_len))
                    elif isinstance(legal_unit_chunk, SubpointChunks):
                        offending.append(
                            (
                            f"Art {legal_unit_chunk.article_id} {f'Section {legal_unit_chunk.section_id}' if legal_unit_chunk.section_id else ''} Point {legal_unit_chunk.subpoint_id}",
                            idx, token_len
                            )
                        )
        return offending

    @classmethod
    def worker_task(cls, row: Dict[str, str], model_id: str, max_token_length: int = 512, overlap: int = 100):
        try:
            articles_rows = get_mongodb_collection(
                db_name="datasets",
                collection_name="legal_acts_articles"
            ).find_many({
                "metadata.ELI": row['ELI'],
                "metadata.invoke_id": row[DAG_TABLE_ID]
            }, {"_id": 0}
            )

            articles = list(map(lambda doc: Article.from_dict(doc), articles_rows))

            articles.sort(
                key=lambda a: (LegalUnit.extract_main_number(a.unit_id), a.unit_id)
            )
            tokenizer = AutoTokenizer.from_pretrained(model_id)
            tokenizer.add_special_tokens({
                "additional_special_tokens": list(SPECIAL_TOKENS.values())
            })
            #
            # r = list(SPECIAL_TOKENS.values())
            #
            # sample = f"{list(SPECIAL_TOKENS.values())[0]} Art. 5 § 2. Hello. {list(SPECIAL_TOKENS.values())[2]} Fun."
            # ids = tokenizer.encode(sample, add_special_tokens=True)
            # tokens = tokenizer.convert_ids_to_tokens(ids)

            point_chunker = SubpointChunker(
                tokenizer=tokenizer,
                max_token_length=max_token_length,
                overlap=overlap,
                special_token=SPECIAL_TOKENS["Point"],
                ELI=row['ELI'],
                invoke_id=row[DAG_TABLE_ID],
                model_id=model_id
            )

            section_chunker = SectionChunker(
                tokenizer=tokenizer,
                max_token_length=max_token_length,
                overlap=overlap,
                point_chunker=point_chunker,
                special_token=SPECIAL_TOKENS["Section"],
                ELI=row['ELI'],
                invoke_id=row[DAG_TABLE_ID],
                model_id=model_id
            )

            article_chunker = ArticleChunker(
                tokenizer=tokenizer,
                max_token_length=max_token_length,
                overlap=overlap,
                section_chunker=section_chunker,
                point_chunker=point_chunker,
                special_token=SPECIAL_TOKENS["Article"],
                ELI=row['ELI'],
                invoke_id=row[DAG_TABLE_ID],
                model_id=model_id
            )
            articles_spans_units_with_chunks = []
            exact_articles_with_chunks = []
            sections_with_chunks = []
            points_with_chunks = []

            for article in articles:
                articles_spans_units_with_chunks.append(
                    article_chunker.chunk_unit(article, exact_unit=False)
                )
                exact_articles_with_chunks.append(
                    article_chunker.chunk_unit(article, exact_unit=True)
                )
                for legal_unit in article.legal_units_indeed:
                    if isinstance(legal_unit, Section):
                        sections_with_chunks.append(
                            section_chunker.chunk_unit(legal_unit, article)
                        )
                        for point in legal_unit.subpoints:
                            points_with_chunks.append(
                                point_chunker.chunk_unit(
                                    point=point,
                                    section=legal_unit,
                                    article=article
                                )
                            )
                    elif isinstance(legal_unit, Subpoint):
                        points_with_chunks.append(
                            point_chunker.chunk_unit(
                                point=legal_unit,
                                section=None,
                                article=article
                            )
                        )

            over_for_span_chunks = cls.find_overlong_parts(articles_spans_units_with_chunks, tokenizer,
                                                           max_length=max_token_length)
            over_for_exact_article_units = cls.find_overlong_parts(exact_articles_with_chunks, tokenizer,
                                                                   max_length=max_token_length)
            over_for_section_chunks = cls.find_overlong_parts(sections_with_chunks, tokenizer,
                                                              max_length=max_token_length)
            over_for_point_chunks = cls.find_overlong_parts(points_with_chunks, tokenizer, max_length=max_token_length)

            all_overlong_chunks = (
                    over_for_span_chunks +
                    over_for_exact_article_units +
                    over_for_section_chunks +
                    over_for_point_chunks
            )

            if all_overlong_chunks:
                raise ValueError(
                    f"Some chunks exceed the maximum token length ({max_token_length}).\n"
                    f"Overlong chunks:\n" +
                    "\n".join([f" - {unit_name} [part {idx}] = {token_len} tokens"
                               for unit_name, idx, token_len in all_overlong_chunks])
                )
            if len(articles_spans_units_with_chunks):
                get_mongodb_collection(
                    db_name="chunks",
                    collection_name="article_span_chunks"
                ).insert_many([article.to_dict() for article in articles_spans_units_with_chunks])
            if len(exact_articles_with_chunks):
                get_mongodb_collection(
                    db_name="chunks",
                    collection_name="article_chunks"
                ).insert_many([article.to_dict() for article in exact_articles_with_chunks])
            if len(sections_with_chunks):
                get_mongodb_collection(
                    db_name="chunks",
                    collection_name="section_chunks"
                ).insert_many([section.to_dict() for section in sections_with_chunks])
            if len(points_with_chunks):
                get_mongodb_collection(
                    db_name="chunks",
                    collection_name="subpoint_chunks"
                ).insert_many([point.to_dict() for point in points_with_chunks])

        except Exception as e:
            format_error = traceback.format_exc()
            r = 4

            try:
                get_mongodb_collection(
                    db_name="chunks",
                    collection_name="errors_2"
                ).update_one(
                    {
                        "ELI": row['ELI'],
                        "invoke_id": row[DAG_TABLE_ID]
                    },
                    {
                        "$set":{
                            "ELI": row['ELI'],
                            "invoke_id": row[DAG_TABLE_ID],
                            "error": str(e),
                            "message": format_error,
                            "type": e.__class__.__name__
                        }
                    },
                    upsert=True
                )
            except Exception as ew:
                aws_logger.error("Failed to log error to MongoDB", exc_info=True)
                pass
            return {
                'ELI': row['ELI'],
                'invoke_id': row["invoke_id"],
                'status': 'failed'
            }

        return {
            'ELI': row['ELI'],
            'invoke_id': row["invoke_id"],
            'status': 'success'
        }

    @classmethod
    @FlowStep.step(task_run_name='chunk_legal_acts')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int, model_id: str):
        invoke_id = "72f213ee-7227-4e99-96f0-63a5766ed1d8"
        # www = cls.worker_task(row={
        #     "ELI": "DU/2013/455",
        #     "invoke_id": invoke_id
        # }, model_id=model_id)

        documents_from_etl = list(get_mongodb_collection(
            db_name="datasets",
            collection_name="legal_acts_articles"
        ).aggregate(
            [
                {
                    "$group": {
                        "_id": {
                            "ELI":       "$metadata.ELI",
                            "invoke_id": "$metadata.invoke_id"
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "ELI":       "$_id.ELI",
                        "invoke_id": "$_id.invoke_id"
                    }
                }
            ]
        )
        )
        pdf = pd.DataFrame(documents_from_etl)


        ddf = dd.from_pandas(pdf, npartitions=workers_count)


        r = 4
        w = ddf.compute()

        r = 4
        delayed_tasks = ddf.map_partitions(
            lambda df: [
                delayed(cls.worker_task)(
                    row=row,
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
