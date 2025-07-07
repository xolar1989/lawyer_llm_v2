import gc
import io
import pandas as pd
import traceback
from typing import List, Dict

import boto3
import pdfplumber
from datetime import datetime, timedelta

from tqdm import tqdm
from dask import delayed
from dask.distributed import Client, as_completed
import dask.dataframe as dd

from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.mongodb_collections.legal_act_page_metadata import LegalActPageMetadata
from preprocessing.pdf_boundaries.footer_boundaries import LegalActFooterArea
from preprocessing.pdf_boundaries.margin_boundaries import LegalActMarginArea
from preprocessing.pdf_boundaries.paragraph_boundaries import LegalActParagraphArea
from preprocessing.pdf_elements.legal_act_page import LegalActPage
from preprocessing.pdf_utils.area_text_extractor import PageAreaTextExtractor
from preprocessing.pdf_utils.multiline_extractor import MultiLineTextExtractor
from preprocessing.pdf_utils.table_utils import TableDetector
from preprocessing.utils.attachments_extraction import PageRegions
from preprocessing.dask.fargate_dask_cluster import retry_dask_task
from preprocessing.utils.defaults import AWS_REGION
from preprocessing.utils.page_regions import LegalActPageRegionFooterNotes, LegalActPageRegionMarginNotes
from preprocessing.utils.s3_helper import extract_bucket_and_key
from preprocessing.utils.stage_def import FlowStep
from preprocessing.utils.stages_objects import EstablishLegalActSegmentsBoundariesResult


class LegalActLineDivision(FlowStep):
    MAX_RETRY = 3

    @classmethod
    def get_footer_region_of_page(cls, footer_notes: List[LegalActPageRegionFooterNotes], page: pdfplumber.pdf.Page) \
            -> LegalActPageRegionFooterNotes:
        footer_note_match = [element for element in footer_notes if element.page_number == page.page_number]
        return footer_note_match[0] if footer_note_match else None

    @classmethod
    def get_margin_notes_region_of_page(cls, margin_notes: List[LegalActPageRegionMarginNotes],
                                        page: pdfplumber.pdf.Page) \
            -> LegalActPageRegionMarginNotes:
        return [element for element in margin_notes if element.page_number == page.page_number][0]

    @classmethod
    def create_pages(cls, document_id: str, invoke_id: str, page_regions: PageRegions, pdf_content: io.BytesIO) -> List[
        LegalActPage]:

        multiline_text_extractor = MultiLineTextExtractor(max_retries=5)
        fallback_text_extractor = PageAreaTextExtractor(max_retries=3)
        index_of_document = 0
        index_of_footer_document = 0
        index_of_margin_document = 0
        index_of_table = 1
        pages = []
        with pdfplumber.open(pdf_content) as pdf:
            try:
                for paragraph in page_regions.paragraphs:
                    page = pdf.pages[paragraph.page_number - 1]

                    tables_on_page = TableDetector.extract(page, paragraph)

                    paragraph_areas = LegalActParagraphArea.split_into_areas(paragraph, page, tables_on_page)

                    margin = cls.get_margin_notes_region_of_page(page_regions.margin_notes, page)
                    margin_areas = LegalActMarginArea.split_into_areas(paragraph, margin, page, tables_on_page)

                    footer_note = cls.get_footer_region_of_page(page_regions.footer_notes, page)
                    footer_areas = LegalActFooterArea.split_into_areas(footer_note)

                    legal_act_page = LegalActPage.build(
                        page=page,
                        document_id=document_id,
                        invoke_id=invoke_id,
                        paragraph_areas=paragraph_areas,
                        margin_areas=margin_areas,
                        footer_areas=footer_areas,
                        current_index_of_legal_act=index_of_document,
                        current_table_number=index_of_table,
                        current_index_of_footer_legal_act=index_of_footer_document,
                        current_index_of_margin_legal_act=index_of_margin_document,
                        multiline_llm_extractor=multiline_text_extractor,
                        fallback_text_extractor=fallback_text_extractor
                    )
                    index_of_document += legal_act_page.len_paragraph_lines
                    index_of_table += len(legal_act_page.get_table_lines)
                    index_of_footer_document += legal_act_page.len_footer_lines
                    index_of_margin_document += legal_act_page.len_margin_lines
                    pages.append(legal_act_page)
                    page.flush_cache()
                    page.get_textmap.cache_clear()

                    del page
            finally:
                pdf.close()
        return pages

    @classmethod
    @retry_dask_task(retries=MAX_RETRY, delay=10)
    def worker_task(cls, row: Dict[str, str], retry_attempt: int):
        try:
            row_with_details: EstablishLegalActSegmentsBoundariesResult = EstablishLegalActSegmentsBoundariesResult \
                .from_dict(
                get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    collection_name="define_legal_act_pages_boundaries_stage"
                ).find_one(
                    {
                        "general_info.ELI": row['ELI'],
                        # "invoke_id": row[DAG_TABLE_ID]
                        # "invoke_id": row[DAG_TABLE_ID]
                        "invoke_id": row["invoke_id"]
                    },
                    {"_id": 0}
                )
            )
            bucket, key = extract_bucket_and_key(row_with_details.general_info.s3_pdf_path)
            pdf_content = io.BytesIO(
                boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
            pages = []
            for attempt in range(retry_attempt + 1, cls.MAX_RETRY + 1):
                try:
                    aws_logger.info(f"Creating pages for ELI: {row['ELI']}, invoke_id: {row['invoke_id']}")

                    pages = cls.create_pages(row['ELI'], row['invoke_id'], row_with_details.page_regions, pdf_content)
                    break
                except Exception as e:
                    ## TODO the most problematic for openai: DU/2009/1240



                    aws_logger.error("Remote error:\n%s", traceback.format_exc())
                    if attempt == cls.MAX_RETRY:
                        collection_db = get_mongodb_collection(
                            db_name="preprocessing_legal_acts_texts",
                            collection_name="legal_act_page_metadata_document_error"
                        )
                        collection_db.insert_one(
                            {
                                "general_info": row_with_details.general_info.to_dict(),
                                "invoke_id": row_with_details.invoke_id,
                                "error": str(e)
                            }
                        )
                        return row

                    raise RuntimeError(
                        f"Error while processing PDF: {row_with_details.general_info.s3_pdf_path} "
                        f"(ELI: {row_with_details.general_info.ELI}), error: {e}"
                    )
            pages_to_save = []

            for page in pages:
                pages_to_save.append(
                    LegalActPageMetadata(
                        general_info=row_with_details.general_info,
                        page=page,
                        invoke_id=row_with_details.invoke_id,
                        expires_at=datetime.now() + timedelta(days=90)
                    ).to_dict()
                )


            get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="legal_act_page_metadata"
            ).insert_many(pages_to_save)

        finally:
            gc.collect()
        return row

    @classmethod
    @FlowStep.step(task_run_name='define_legal_acts_lines')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_eli_documents: str):

        ddf_eli_documents = cls.read_from_datalake(s3_path_parquet_with_eli_documents,
                                                   pd.DataFrame({
                                                       'ELI': pd.Series(dtype='str'),
                                                       'invoke_id': pd.Series(dtype='str')
                                                   })
                                                   )
        # ## TODO Remove it later
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2011/696"].compute()
        selected_ddf = ddf_eli_documents[
            ddf_eli_documents["ELI"] == "DU/2009/1240"].compute()

        r = cls.worker_task(row=selected_ddf.iloc[0].to_dict())

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

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)

        return cls.save_result_to_datalake(results_ddf, flow_information, cls)