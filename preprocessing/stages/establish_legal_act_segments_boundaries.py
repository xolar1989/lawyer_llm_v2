import io
import logging
from typing import Dict
import gc

import boto3
import pandas as pd
import pdfplumber
import time
from pdf2image import convert_from_bytes
from datetime import datetime, timedelta

from tqdm import tqdm

from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.utils.attachments_extraction import get_pages_with_attachments, get_attachments_headers, \
    AttachmentRegion
from preprocessing.dask.fargate_dask_cluster import retry_dask_task
from preprocessing.utils.defaults import AWS_REGION, DAG_TABLE_ID
from preprocessing.utils.dynamodb_helper import meta_DULegalDocumentsMetaData_with_s3_path
from preprocessing.utils.page_regions import LegalActPageRegionParagraphs, LegalActPageRegionMarginNotes, \
    LegalActPageRegionFooterNotes, LegalActPageRegionAttachment
from preprocessing.utils.s3_helper import extract_bucket_and_key
from preprocessing.utils.stage_def import FlowStep

from dask.distributed import Client, as_completed
import dask.dataframe as dd
from dask import delayed


logger = logging.getLogger()

logger.setLevel(logging.INFO)

class EstablishLegalActSegmentsBoundaries(FlowStep):
    @classmethod
    def declare_part_of_legal_act(cls, document_name: str, pdf_bytes: io.BytesIO):

        attachments_page_regions = []
        page_regions_paragraphs = []
        page_regions_margin_notes = []
        page_regions_footer_notes = []
        attachments_regions = []

        with pdfplumber.open(io.BytesIO(pdf_bytes.getvalue())) as pdf:
            try:
                pages_with_attachments = get_pages_with_attachments(pdf)

                for index in range(len(pdf.pages)):
                    page_image = convert_from_bytes(pdf_bytes.getvalue(), first_page=index + 1, last_page=index + 1)[0]
                    pdf_page = pdf.pages[index]

                    if pdf_page.page_number not in pages_with_attachments:

                        if pdf_page.page_number == 11:
                            s = 4
                        # Build paragraphs and profile memory
                        page_region_text = LegalActPageRegionParagraphs.build(document_name, page_image, pdf_page)
                        page_regions_paragraphs.append(page_region_text)
                        # bbox = (
                        #     page_region_text.start_x, page_region_text.start_y, page_region_text.end_x,
                        #     page_region_text.end_y)
                        # extract_text = pdf_page.within_bbox(bbox).extract_text()

                        margin_notes = LegalActPageRegionMarginNotes.build(document_name, page_image, pdf_page)
                        page_regions_margin_notes.append(margin_notes)
                        # bbox = (margin_notes.start_x, margin_notes.start_y, margin_notes.end_x, margin_notes.end_y)
                        # extract_text_margin = pdf_page.within_bbox(bbox).extract_text()

                        footer_notes = LegalActPageRegionFooterNotes.build(document_name, page_image, pdf_page)
                        if footer_notes:
                            page_regions_footer_notes.append(footer_notes)
                            # bbox = (footer_notes.start_x, footer_notes.start_y, footer_notes.end_x, footer_notes.end_y)
                            # extract_text_footer = pdf_page.within_bbox(bbox).extract_text()
                            # s=4

                    else:
                        # Attachments page regions
                        attachments_page_regions.append(
                            LegalActPageRegionAttachment.build(document_name, page_image, pdf_page))
                        # Clearing cache of pages to reduce memory usage
                    pdf_page.flush_cache()
                    pdf_page.get_textmap.cache_clear()
                    del page_image, pdf_page
                # Process attachment headers
                attachment_headers = get_attachments_headers(pdf, attachments_page_regions)
                for attachment_header_index in range(len(attachment_headers)):
                    attachment_region = AttachmentRegion.build(attachment_header_index, pdf, attachment_headers,
                                                               attachments_page_regions)
                    attachments_regions.append(attachment_region)
                # for attachment_region in attachments_regions:
                #     for attachment_page_region in attachment_region.page_regions:
                #         bbox = (
                #             attachment_page_region.start_x, attachment_page_region.start_y, attachment_page_region.end_x,
                #             attachment_page_region.end_y)
                #         extract_text = pdf.pages[attachment_page_region.page_number-1].within_bbox(bbox).extract_text()
                #         s = 4
                for page_num in pages_with_attachments:
                    pdf_page = pdf.pages[page_num - 1]
                    # Clearing cache for attachments
                    pdf_page.flush_cache()
                    pdf_page.get_textmap.cache_clear()
                    del pdf_page
            finally:
                del pages_with_attachments
                pdf.close()

        return page_regions_paragraphs, page_regions_margin_notes, page_regions_footer_notes, attachments_regions

    @classmethod
    @retry_dask_task(retries=3, delay=10)
    def establish_worker(cls, row, flow_information: Dict):
        start_time = time.time()
        bucket, key = extract_bucket_and_key(row['s3_pdf_path'])
        try:
            pdf_content = io.BytesIO(
                boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
            page_regions_paragraphs, page_regions_margin_notes, page_regions_footer_notes, attachments_regions = \
                cls.declare_part_of_legal_act(row['ELI'], pdf_content)

            get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="define_legal_act_pages_boundaries_stage"
            ).insert_one({
                "general_info": row,
                "invoke_id": flow_information[DAG_TABLE_ID],
                "page_regions": {
                    "paragraphs": [page_region.to_dict() for page_region in page_regions_paragraphs],
                    "margin_notes": [page_region.to_dict() for page_region in page_regions_margin_notes],
                    "footer_notes": [page_region.to_dict() for page_region in page_regions_footer_notes],
                    "attachments": [attachment_region.to_dict() for attachment_region in attachments_regions]
                },
                ## TODO change this, to expire earlier
                "expires_at": datetime.now() + timedelta(days=100)
            })
        finally:
            for var_name in ["page_regions_paragraphs", "page_regions_margin_notes", "page_regions_footer_notes",
                             "attachments_regions", "pdf_content"]:
                if var_name in locals():
                    del locals()[var_name]
            gc.collect()
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Task completed in {elapsed_time} seconds, ELI: {row['ELI']}")
        return {
            'ELI': row['ELI'],
            'invoke_id': flow_information[DAG_TABLE_ID]
        }

    @classmethod
    @FlowStep.step(task_run_name='establish_legal_act_segments_boundaries')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_legal_document_rows: str):
        ddf_legal_document_rows_datalake = cls.read_from_datalake(
            s3_path_parquet_with_legal_document_rows,
            meta_DULegalDocumentsMetaData_with_s3_path)
        delayed_tasks = ddf_legal_document_rows_datalake.map_partitions(
            lambda df: [
                delayed(cls.establish_worker)(
                    row=row,
                    flow_information=flow_information
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
            logger.info(f"Task {future.key} executed on workers: {who_has}")

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)

        return cls.save_result_to_datalake(results_ddf, flow_information, cls)
