import gc
import traceback
from typing import List, Dict

import unicodedata
from tqdm import tqdm

from preprocessing.dask.fargate_dask_cluster import retry_dask_task
from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.mongodb_collections.legal_act_page_metadata import LegalActPageMetadata
from preprocessing.pdf_elements.chars import CharLegalAct
from preprocessing.pdf_elements.legal_act_page import LegalActPage
from preprocessing.pdf_elements.lines import TextLinePageLegalAct, TableRegionPageLegalAct, OrderedObjectLegalAct
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.splits.part_legal_unit_split import PartLegalUnitSplit
from preprocessing.pdf_structure.splits.text_split import TextSplit, CharTextSplit, StatusOfText
from preprocessing.pdf_structure.splits.title_unit_split import TitleUnitSplit
from preprocessing.pdf_structure.splitters.article_splitter import ArticleSplitter
from preprocessing.pdf_structure.splitters.chapter_splitter import ChapterSplitter
from preprocessing.pdf_structure.splitters.inside_split_text_splitter import InsideSplitTextSplitter
from preprocessing.pdf_structure.splitters.part_legal_unit_splitter import PartLegalUnitSplitter
from preprocessing.pdf_structure.splitters.point_splitter import PointSplitter
from preprocessing.pdf_structure.splitters.section_splitter import SectionSplitter
from preprocessing.pdf_structure.splitters.statute_body_splitter import StatuteBodySplitter
from preprocessing.pdf_structure.splitters.title_unit_splitter import TitleUnitSplitter
from preprocessing.utils.defaults import DAG_TABLE_ID
from preprocessing.utils.general import timeit
from preprocessing.utils.stage_def import FlowStep
from dask import delayed
from dask.distributed import Client, as_completed
import dask.dataframe as dd

import pandas as pd

from preprocessing.utils.stages_objects import GeneralInfo


class StructureLegalActs(FlowStep):
    statute_body_splitter = StatuteBodySplitter()
    title_unit_splitter = TitleUnitSplitter()
    part_legal_unit_splitter = PartLegalUnitSplitter()
    chapter_splitter = ChapterSplitter()
    article_splitter = ArticleSplitter()
    section_splitter = SectionSplitter()
    subpoint_splitter = PointSplitter()
    inside_split_text_splitter = InsideSplitTextSplitter()

    @staticmethod
    def get_paragraph_text(pages: List[LegalActPage]):
        return "".join(page.paragraph_text for page in pages)

    @staticmethod
    def get_chars_of_paragraph(pages: List[LegalActPage]):
        chars = []
        for page in pages:
            for line_paragraph in page.paragraph_lines:
                if isinstance(line_paragraph, TextLinePageLegalAct):
                    for line_char in line_paragraph.chars:
                        chars.append(line_char)
                elif isinstance(line_paragraph, TableRegionPageLegalAct):
                    width_char_table = round(
                        round(line_paragraph.end_x, 2) - round(line_paragraph.start_x, 2) / len(line_paragraph.text), 2)
                    for char_index in range(len(line_paragraph.text)):
                        char_obj = CharLegalAct(
                            x0=line_paragraph.start_x + char_index * width_char_table,
                            x1=max(line_paragraph.start_x + (char_index + 1) * width_char_table, line_paragraph.end_x),
                            bottom=line_paragraph.end_y,
                            top=line_paragraph.start_y,
                            text=line_paragraph.text[char_index],
                            index_in_legal_act=line_paragraph.index_in_act + char_index,
                            page_number=page.page_number
                        )
                        chars.append(char_obj)
                for char_index, char_delimiter in enumerate(OrderedObjectLegalAct.LINE_DELIMITER):
                    char_obj = CharLegalAct(
                        x0=line_paragraph.end_x,
                        x1=line_paragraph.end_x,
                        bottom=line_paragraph.end_y,
                        top=line_paragraph.start_y,
                        text=char_delimiter,
                        index_in_legal_act=line_paragraph.end_index + char_index,
                        page_number=page.page_number
                    )
                    chars.append(char_obj)

        return chars

    @staticmethod
    def text_from_chars(legal_chars: List[CharLegalAct]):
        return ''.join(char.text for char in legal_chars)

    @classmethod
    @timeit
    def split_legal_act(cls, pages: List[LegalActPage]) -> List[TitleUnitSplit]:
        document_chars = cls.get_chars_of_paragraph(pages)

        document_text_split = TextSplit(
            list(map(lambda char: CharTextSplit(char, StatusOfText.NOT_MATCH_CHANGE), document_chars)),
            StatusOfText.NOT_MATCH_CHANGE
        )
        statute_body_split = cls.statute_body_splitter.split(document_text_split)
        title_units_splits = cls.title_unit_splitter.split(statute_body_split)

        for title_unit_split in title_units_splits:
            part_legal_unit_divisions = cls.part_legal_unit_splitter.split(title_unit_split)
            for part_division in part_legal_unit_divisions:
                chapters = cls.chapter_splitter.split(part_division)
                for chapter in chapters:
                    chapter_articles = cls.article_splitter.split(chapter)
                    for article in chapter_articles:
                        article = cls.section_splitter.split(article)
                        article = cls.subpoint_splitter.split(article)
                        article = cls.inside_split_text_splitter.split(article)

        return title_units_splits

    @classmethod
    @timeit
    def build_articles_of_document(cls, title_units_splits: List[TitleUnitSplit], general_info: GeneralInfo, invoke_id:str):
        articles = []
        for title_unit_split in title_units_splits:
            for part_legal_split in title_unit_split.part_unit_splits:
                for chapter_split in part_legal_split.chapters:
                    for article_split in chapter_split.articles:
                        if article_split.is_current_unit:
                            article = Article.build(
                                article_split=article_split,
                                title_unit_split=title_unit_split,
                                part_unit_split=part_legal_split,
                                chapter_split=chapter_split,
                                general_info=general_info,
                                invoke_id=invoke_id
                            )
                            if article.is_up_to_date():
                                articles.append(article)
        if not Article.is_ascending(articles):
            raise RuntimeError(f"The ids of articles are not in ascending order {[item.unit_id for item in articles]}")
        return articles

    ## TODO it should be done in filtering steps, in future move it
    @classmethod
    def is_row_of_transitional_provisions(cls,general_info: GeneralInfo):
        def normalize(text: str) -> str:
            # Usuwa znaki diakrytyczne, np. ą → a, ć → c
            text = unicodedata.normalize('NFKD', text)
            return ''.join(c for c in text if not unicodedata.combining(c)).lower()
        normalized_text = normalize(general_info.title)
        return 'przepisy wprowadzajace' in normalized_text

    @classmethod
    @retry_dask_task(retries=3, delay=10)
    def worker_task(cls, row: Dict[str, str]):
        try:
            rows = get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="legal_act_page_metadata"
            ).find_many(
                {
                    "general_info.ELI": row['ELI'],
                    # "invoke_id": row[DAG_TABLE_ID]
                    "invoke_id": row[DAG_TABLE_ID]
                },
                {"_id": 0}
            )
            general_info_document = next(map(lambda doc: LegalActPageMetadata.from_dict(doc), rows)).general_info
            if cls.is_row_of_transitional_provisions(general_info_document):
                ## TODO it shouldn't be done here it should be done in filtering method
                return {
                    'ELI': row['ELI'],
                    'invoke_id': row[DAG_TABLE_ID],
                    'status': 'success'
                }
            pages_of_document = list(map(lambda doc: LegalActPageMetadata.from_dict(doc).page, rows))
            invoke_id = next(map(lambda doc: LegalActPageMetadata.from_dict(doc), rows)).invoke_id
            s3_pdf_path = general_info_document.s3_pdf_path

            ## TODO this is for check how aricles are saved
            # articles_rows = get_mongodb_collection(
            #     db_name="datasets",
            #     collection_name="legal_acts_articles"
            # ).find_many({
            #     "metadata.ELI": row['ELI'],
            #     "metadata.invoke_id": row[DAG_TABLE_ID]
            # })
            # articles_from_db = list(map(lambda doc: Article.from_dict(doc), articles_rows))

            # bucket, key = extract_bucket_and_key(s3_pdf_path)
            # pdf_content = io.BytesIO(
            #     boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())

            ## TODO it should be done, „  text" should be grouped as <> or [], however now i don't have time for it
            try:
                part_legal_unit_divisions = cls.split_legal_act(pages_of_document)
                articles: List[Article] = cls.build_articles_of_document(part_legal_unit_divisions, general_info_document, invoke_id)
            except Exception as e:
                format_error = traceback.format_exc()
                traceback.print_exc()
                get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    collection_name="document_splitting_error"
                ).update_one(
                    {
                        "ELI": row['ELI'],
                        "invoke_id": row[DAG_TABLE_ID]
                    },
                    {
                        "$set":{
                            "ELI": row['ELI'],
                            "invoke_id": row[DAG_TABLE_ID],
                            "error_msg": str(e),
                            "traceback": format_error,
                            "type": "during_structuring"
                        }
                    },
                    upsert=True
                )
                return {
                    'ELI': row['ELI'],
                    'invoke_id': row[DAG_TABLE_ID],
                    'status': 'failed'
                }


            articles_to_save = [article.to_dict() for article in articles]
            try:
                get_mongodb_collection(
                    db_name="datasets",
                    collection_name="legal_acts_articles"
                ).insert_many(articles_to_save)
            except Exception as e:
                format_error = traceback.format_exc()
                get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    collection_name="document_splitting_error"
                ).update_one(
                    {
                        "ELI": row['ELI'],
                        "invoke_id": row[DAG_TABLE_ID]
                    },
                    {
                        "$set":{
                            "ELI": row['ELI'],
                            "invoke_id": row[DAG_TABLE_ID],
                            "error_msg": str(e),
                            "traceback": format_error,
                            "type": "during_inserting"
                        }
                    },
                    upsert=True
                )
                return {
                    'ELI': row['ELI'],
                    'invoke_id': row[DAG_TABLE_ID],
                    'status': 'failed'
                }

        finally:
            gc.collect()
        return {
            'ELI': row['ELI'],
            'invoke_id': row[DAG_TABLE_ID],
            'status': 'success'
        }

    @classmethod
    @FlowStep.step(task_run_name='extract_table_and_equations_from_legal_acts')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_eli_documents: str):

        ddf_eli_documents = cls.read_from_datalake(s3_path_parquet_with_eli_documents,
                                                   pd.DataFrame({
                                                       'ELI': pd.Series(dtype='str'),
                                                       'invoke_id': pd.Series(dtype='str')
                                                   })
                                                   )

        # DU/2011/696

        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2011/696"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2009/1240"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2023/1407"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2001/27"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1966/151"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2008/1569"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2006/1700"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1994/83"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1982/80"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2004/1206"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2019/1818"].compute()

        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1997/348"].compute()
        # r = cls.worker_task(row=selected_ddf.iloc[0].to_dict())
        #
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1991/350"].compute()
        # r = cls.worker_task(row=selected_ddf.iloc[0].to_dict())

        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1998/930"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1974/117"].compute()
        # r = cls.worker_task(row=selected_ddf.iloc[0].to_dict())
        ## TODO https://api.sejm.gov.pl/eli/acts/DU/2015/478/text/U/D20150478Lj.pdf
        ## TODO D20150021Lj.pdf,  DU/2025/39,  I have some ideas how handle it however for now we gonna skip it
        ## TODO https://api.sejm.gov.pl/eli/acts/DU/1966/151/text/U/D19660151Lj.pdf should be oddział divided
        delayed_tasks = ddf_eli_documents.map_partitions(
            lambda df: [
                delayed(cls.worker_task)(
                    row=row
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

        return cls.save_result_to_datalake(successful_result_ddf, flow_information, cls, result_name="successful_results"), \
            cls.save_result_to_datalake(failed_result_ddf, flow_information, cls, result_name="failed_results")