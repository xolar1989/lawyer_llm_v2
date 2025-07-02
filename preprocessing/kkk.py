from dotenv import load_dotenv
import gc
import io
import os
# import s3fs
import re
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Union, List, Dict, Mapping, Any, Set, Tuple

import boto3
import cv2
import numpy as np
import pandas as pd
import pdfplumber
from PyPDF2 import PdfFileReader, PdfFileWriter
import dask.dataframe as dd
from dask import delayed
from pdfplumber.table import Table
from pydantic.v1 import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
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
from preprocessing.stages.get_metadatachange_and_filter_legal_documents_which_not_change import \
    GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange
from preprocessing.stages.get_rows_of_legal_acts_for_preprocessing import GetRowsOfLegalActsForPreprocessing
from preprocessing.stages.line_division import LegalActLineDivision
from preprocessing.stages.update_dask_cluster_workers import UpdateDaskClusterWorkers
from preprocessing.utils.stages_objects import EstablishLegalActSegmentsBoundariesResult, \
    EstablishLegalWithTablesResult
from preprocessing.stages.create_dask_cluster import CreateRemoteDaskCluster
from preprocessing.stages.start_dag import StartDag
from preprocessing.utils.attachments_extraction import AttachmentRegion, \
    get_pages_with_attachments
from preprocessing.dask.fargate_dask_cluster import retry_dask_task
from preprocessing.utils.datalake import Datalake
from preprocessing.utils.defaults import ERROR_TOPIC_NAME, AWS_REGION, DAG_TABLE_ID, \
    DATALAKE_BUCKET
from preprocessing.utils.dynamodb_helper import meta_DULegalDocumentsMetaDataChanges_v2, \
    meta_DULegalDocumentsMetaData_with_s3_path
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.mongo_db.mongodb_schema import schema_for_legal_act_row
from preprocessing.utils.page_regions import LegalActPageRegionParagraphs, LegalActPageRegion, \
    LegalActPageRegionTableRegion, \
    TableRegion
from preprocessing.utils.s3_helper import extract_bucket_and_key
from preprocessing.utils.sns_helper import send_sns_email
from preprocessing.utils.stage_def import FlowStep, get_storage_options_for_ddf_dask
from distributed.versions import get_versions

from pdf2image import convert_from_bytes

import logging
from cloudwatch import cloudwatch

load_dotenv()

logger = aws_logger

os.environ['TZ'] = 'UTC'

r = os.environ

get_versions()

s = 4


# messages = [
#     {"role": "user",
#      "content": [
#          {"type": "text",
#           "text": """
#         extract text from this image,
#         pay attention to superscripts or subscripts, digit and letters could be subscripts or superscripts (use unicode subscripts or superscripts,
#         Unicode Reference:
#         Subscripts: ₀₁₂₃₄₅₆₇₈₉ₐₑₒₓₕₖₗₘₙₚₛₜ
#         Superscripts: ⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾ᵃᵇᶜᵈᵉᶠᵍʰⁱʲᵏˡᵐⁿᵒᵖʳˢᵗᵘᵛʷˣʸᶻ
#         and give the text of it,
#          give just result without any other text!!!!"""
#           },
#          {"type": "image_url",
#           "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
#           }
#      ]
#      },
# ]

#
# import dns.resolver
#
# dns.resolver.default_resolver=dns.resolver.Resolver(configure=False)
# dns.resolver.default_resolver.nameservers=['8.8.8.8']


def on_error_handler(flow, flow_run, state):
    logger.error(f"Task {flow} failed with state {state}")
    # k = state.result.exceptions
    send_sns_email(f"Task {flow} failed with state {state}", ERROR_TOPIC_NAME, boto3.Session(region_name=AWS_REGION))
    s = 4


class SaveMetaDataRowsIntoDynamodb(FlowStep):
    @classmethod
    @FlowStep.step(task_run_name='save_meta_data_rows_into_dynamodb')
    def run(cls, flow_information: dict, dask_client: Client, type_document: str, workers_count: int,
            s3_path_parquet_meta_data: str, s3_path_parquet_meta_data_changes: str):
        ddf_meta_data_from_datalake = cls.read_from_datalake(s3_path_parquet_meta_data,
                                                             meta_DULegalDocumentsMetaData_with_s3_path)
        ddf_meta_data_changes_from_datalake = cls.read_from_datalake(s3_path_parquet_meta_data_changes,
                                                                     meta_DULegalDocumentsMetaDataChanges_v2)

        s = ddf_meta_data_from_datalake.compute()
        w = ddf_meta_data_changes_from_datalake.compute()

        w = 4

        # ddf_DULegalDocumentsMetaData_new_rows \
        #     .map_partitions(insert_partition_to_dynamodb,
        #                     table_name="DULegalDocumentsMetaData",
        #                     aws_region=AWS_REGION,
        #                     meta=meta_DULegalDocumentsMetaData
        #                     ) \
        #     .compute()
        #
        # ddf_DULegalDocumentsMetaDataChanges_v2_new_rows \
        #     .map_partitions(insert_partition_to_dynamodb,
        #                     table_name="DULegalDocumentsMetaDataChanges_v2",
        #                     aws_region=AWS_REGION,
        #                     meta=meta_DULegalDocumentsMetaDataChanges_v2
        #                     ) \
        #     .compute()


class TableAnnotation:
    def __init__(self, pages_numbers: list):
        self.pages_numbers = pages_numbers


found_annotation_and_labeled_table_schema = {
    "bsonType": "object",
    "required": [
        "general_info",  # The embedded document for all meta-information
        "invoke_id",  # UUID as a string
        "content",  # Page number and content
        "legal_annotations",  # New field to store page numbers with tables
        "expires_at"  # Expiry date for TTL
    ],
    "properties": {
        "general_info": schema_for_legal_act_row,
        "invoke_id": {
            "bsonType": "string",  # UUID as a string
            "description": "Must be a string representing the UUID and is required."
        },
        "content": {  # Pages is a dictionary, where each page number maps to its text
            "bsonType": "string",
            "description": "The text of each page, stored as a dictionary where keys are page numbers."
        },
        "legal_annotations": {
            "bsonType": "array",
            "items": {
                "bsonType": "object",
                "required": ["header", "content", "type"],
                "properties": {
                    "header": {
                        "bsonType": "string",
                        "description": "The header of the annotation."
                    },
                    "content": {
                        "bsonType": "string",
                        "description": "The content of the annotation."
                    },
                    "type": {
                        "bsonType": "string",
                        "enum": [
                            'wzór_dokumentu',
                            'zawiera_tabele',
                            'wzory_do_obliczeń',
                            'inne'
                        ],
                        "description": "The type of the annotation."
                    }
                }
            },
            "description": "An array of objects, where each object contains the page number and annotation."
        },
        "expires_at": {
            "bsonType": "date",
            "description": "The date the document expires. MongoDB will use this for TTL deletion."
        }
    }
}


def get_cloudwatch_logger(step_name, flow_information):
    aws_info = get_storage_options_for_ddf_dask(AWS_REGION)
    logger = logging.getLogger(f"{step_name}/{flow_information[DAG_TABLE_ID]}")
    handler = cloudwatch.CloudwatchHandler(
        access_id=aws_info['key'],
        access_key=aws_info['secret'],
        region=aws_info['client_kwargs']['region_name'],
        log_group=f'preprocessing/{step_name}',
        log_stream=f'{flow_information[DAG_TABLE_ID]}'
    )
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.WARNING)
    logger.addHandler(handler)
    return logger


class CheckingFOOTERType2(FlowStep):

    @classmethod
    def ll(cls, row):
        uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongodb.net/"
        db_name = "preprocessing_legal_acts_texts"
        bucket, key = extract_bucket_and_key(row['s3_pdf_path'])
        mongo_client = MongoClient(uri, server_api=ServerApi('1'))
        mongo_db = mongo_client[db_name]
        collection = mongo_db["sss"]
        pdf_content = io.BytesIO(
            boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
        with pdfplumber.open(io.BytesIO(pdf_content.getvalue())) as pdf:
            try:
                pages_with_attachments = get_pages_with_attachments(pdf)
                for index in range(len(pdf.pages)):
                    pdf_page = pdf.pages[index]
                    text = pdf_page.extract_text()
                    if pdf_page.page_number in pages_with_attachments:
                        continue
                    footer_match = list(re.finditer(r'\d{2,4}\s*[\/]\s*\d{2}\s*[\/]\s*\d{2,4}', text.strip()))
                    if footer_match:
                        footer_text = footer_match[-1].group(0)
                        logger.warning(f" footer different type 3 in eli: {row['ELI']}, page: {pdf_page.page_number}")
                        footer_text = re.sub(r'[\s\n\r]+', '', footer_text)
                        collection.insert_one({
                            "ELI": row['ELI'],
                            "page_number": pdf_page.page_number,
                            "text": text,
                            "footer_text": footer_text
                        })
                    pdf_page.flush_cache()
                    pdf_page.get_textmap.cache_clear()
                    del pdf_page
            finally:
                del pages_with_attachments
                pdf.close()
        return row

    @classmethod
    @FlowStep.step(task_run_name='kk')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_legal_document_rows: str):
        ddf_legal_document_rows_datalake = cls.read_from_datalake(
            s3_path_parquet_with_legal_document_rows,
            meta_DULegalDocumentsMetaData_with_s3_path)

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2002/676"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2004/1925"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2001/1382"].compute()
        # #
        #
        # r = cls.ll(row=selected_ddf.iloc[0].to_dict())

        delayed_tasks = ddf_legal_document_rows_datalake.map_partitions(
            lambda df: [
                delayed(cls.ll)(
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
            logger.info(f"Task {future.key} executed on workers: {who_has}")

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)


class CheckingHeaderType2(FlowStep):

    @classmethod
    def ll(cls, row):
        uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongodb.net/"
        db_name = "preprocessing_legal_acts_texts"
        bucket, key = extract_bucket_and_key(row['s3_pdf_path'])
        mongo_client = MongoClient(uri, server_api=ServerApi('1'))
        mongo_db = mongo_client[db_name]
        collection = mongo_db["dddd"]
        pdf_content = io.BytesIO(
            boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
        with pdfplumber.open(io.BytesIO(pdf_content.getvalue())) as pdf:
            try:
                pages_with_attachments = get_pages_with_attachments(pdf)
                for index in range(len(pdf.pages)):
                    pdf_page = pdf.pages[index]
                    text = pdf_page.extract_text()
                    page_image = convert_from_bytes(pdf_content.getvalue(), first_page=index + 1, last_page=index + 1)[
                        0]
                    if pdf_page.page_number in pages_with_attachments:
                        continue
                    footer_match = re.search(LegalActPageRegion.FOOTER_PATTERN, text.strip())
                    words = pdf_page.extract_words()
                    image_np = np.array(page_image.convert("RGB"))
                    scaling_factor = LegalActPageRegion.get_scaling_factor(pdf_page, image_np)
                    if footer_match:
                        footer_text = footer_match.group(0)
                        footer_text = re.sub(r'[\s\n\r]+', '', footer_text)  # Clean spaces and newlines

                        # Search for the footer location in the page's layout text
                        for idx, word in enumerate(words):
                            candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(footer_text.split())])
                            if footer_text == candidate_text:
                                footer_y = words[idx]['top']  # Y position of the first line of the footer
                                footer_x_right = words[idx]['x1']  # right x position of the footer
                                line_positions = LegalActPageRegion.get_line_positions(image_np)
                                if len(line_positions) != 0:
                                    header_x_start, header_y, header_width, _ = line_positions[0]
                                    bbox = (0, 0, pdf_page.width, scaling_factor * header_y + 2)
                                    header_right_x = (header_x_start + header_width) * scaling_factor
                                    if header_right_x > footer_x_right + 2:
                                        logger.warning(
                                            f" Header overlow in eli: {row['ELI']}, page: {pdf_page.page_number}")
                                        # collection.insert_one({
                                        #     "ELI": row['ELI'],
                                        #     "page_number": pdf_page.page_number,
                                        #     "text": text,
                                        #     "match": True,
                                        #     "header_overflow":  True
                                        # })

                                footer_x_end_scaled = footer_x_right / scaling_factor
                                cv2.line(
                                    image_np,
                                    (int(footer_x_end_scaled), 0),
                                    (int(footer_x_end_scaled), image_np.shape[0]),
                                    (0, 255, 0),
                                    2
                                )

                                footer_x_end_scaled_2 = (footer_x_right + 2) / scaling_factor
                                cv2.line(
                                    image_np,
                                    (int(footer_x_end_scaled_2), 0),
                                    (int(footer_x_end_scaled_2), image_np.shape[0]),
                                    (0, 255, 255),
                                    2
                                )

                                for x, y, w, h in line_positions:
                                    start_point = (x, y)  # Line start point
                                    end_point = (x + w, y)  # Line end point (horizontal line)
                                    color = (0, 0, 255)  # Line color (red)
                                    thickness = 2  # Line thickness

                                    # Draw the line
                                    cv2.line(image_np, start_point, end_point, color, thickness)

                                output_path = f"./lines/{pdf_page.page_number}-566565.jpg"
                                cv2.imwrite(output_path, image_np)
                                break

                    # header_2_match = re.search(LegalActPageRegion.NAV_PATTERN_2, text.strip())
                    # line_positions = LegalActPageRegion.get_line_positions(image_np)
                    #
                    # if len(line_positions) != 0:
                    #     header_x_start, header_y, header_width, _ = line_positions[0]
                    #     bbox = (0, 0, pdf_page.width, scaling_factor * header_y + 2)
                    #     header_right_x = (header_x_start + header_width) * scaling_factor
                    #     diff_between_footer_header = footer_x_right - header_right_x
                    #
                    #
                    #     header_before_text = pdf_page.within_bbox(bbox).extract_text()
                    #     if header_before_text.strip() == "":
                    #         logger.warning(
                    #             f"There is header 2 type in the document eli: {row['ELI']}, page: {pdf_page.page_number}")
                    #         collection.insert_one({
                    #             "ELI": row['ELI'],
                    #             "page_number": pdf_page.page_number,
                    #             "text": text,
                    #             "match": True
                    #         })
                    # else:
                    #     logger.warning(
                    #         f"There is no header  in the document eli: {row['ELI']}, page: {pdf_page.page_number}")
                    #     collection.insert_one({
                    #         "ELI": row['ELI'],
                    #         "page_number": pdf_page.page_number,
                    #         "text": text,
                    #         "match": False,
                    #         "header": False
                    #     })
                    pdf_page.flush_cache()
                    pdf_page.get_textmap.cache_clear()
                    del pdf_page, page_image, image_np
            finally:
                del pages_with_attachments
                pdf.close()
        return row

    @classmethod
    @FlowStep.step(task_run_name='kk')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_legal_document_rows: str):
        ddf_legal_document_rows_datalake = cls.read_from_datalake(
            s3_path_parquet_with_legal_document_rows,
            meta_DULegalDocumentsMetaData_with_s3_path)

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2002/676"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2004/1925"].compute()

        selected_ddf = ddf_legal_document_rows_datalake[
            ddf_legal_document_rows_datalake["ELI"] == "DU/2001/1382"].compute()
        #

        r = cls.ll(row=selected_ddf.iloc[0].to_dict())

        delayed_tasks = ddf_legal_document_rows_datalake.map_partitions(
            lambda df: [
                delayed(cls.ll)(
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
            logger.info(f"Task {future.key} executed on workers: {who_has}")

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)


class CheckingFooter(FlowStep):

    @classmethod
    def ll(cls, row):
        uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongodb.net/"
        db_name = "preprocessing_legal_acts_texts"
        bucket, key = extract_bucket_and_key(row['s3_pdf_path'])
        mongo_client = MongoClient(uri, server_api=ServerApi('1'))
        mongo_db = mongo_client[db_name]
        collection = mongo_db["footer_check"]
        pdf_content = io.BytesIO(
            boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
        with pdfplumber.open(io.BytesIO(pdf_content.getvalue())) as pdf:
            try:
                pages_with_attachments = get_pages_with_attachments(pdf)
                for index in range(len(pdf.pages)):
                    pdf_page = pdf.pages[index]
                    if pdf_page.page_number in pages_with_attachments:
                        continue
                    text = pdf_page.extract_text()
                    footer_match = re.search(LegalActPageRegion.FOOTER_PATTERN, text.strip())
                    if not footer_match:
                        logger.warning(
                            f"There is no footer in the document eli: {row['ELI']}, page: {pdf_page.page_number}")
                        collection.insert_one({
                            "ELI": row['ELI'],
                            "page_number": pdf_page.page_number,
                            "text": text,
                            "match": False
                        })
                    pdf_page.flush_cache()
                    pdf_page.get_textmap.cache_clear()
                    del pdf_page
            finally:
                del pages_with_attachments
                pdf.close()
        return row

    @classmethod
    @FlowStep.step(task_run_name='kk')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_legal_document_rows: str):
        ddf_legal_document_rows_datalake = cls.read_from_datalake(
            s3_path_parquet_with_legal_document_rows,
            meta_DULegalDocumentsMetaData_with_s3_path)

        delayed_tasks = ddf_legal_document_rows_datalake.map_partitions(
            lambda df: [
                delayed(cls.ll)(
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
            logger.info(f"Task {future.key} executed on workers: {who_has}")

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)


equations_signs = ["=", ">", "<", "≥", "≤", "≠", "≡", "≈", "≅"]


class ExtractTableAndEquationsFromLegalActs(FlowStep):

    @staticmethod
    def get_pages_with_attachments(attachments: List[AttachmentRegion]) -> Set[int]:
        pages_with_attachments = set()
        for attachment in attachments:
            for page_region in attachment.page_regions:
                pages_with_attachments.add(page_region.page_number)

        return pages_with_attachments

    @staticmethod
    def get_indexes_of_table(table: Table, page_paragraph: pdfplumber.pdf.Page, page_text: str) -> Tuple[int, int]:

        text_of_table = page_paragraph.within_bbox(table.bbox).extract_text().strip()
        if text_of_table not in page_text:
            raise ValueError(f"Invalid state: text of table not found in page text, text_of_table: "
                             f"{text_of_table}, page_text: {page_text}, page_number: {page_paragraph.page_number}")
        table_start_index = page_text.index(text_of_table)
        table_end_index = table_start_index + len(text_of_table)
        return table_start_index, table_end_index

    @staticmethod
    def get_sorted_tables(tables_in_page: List[Table], paragraph: LegalActPageRegionParagraphs,
                          page: pdfplumber.pdf.Page) -> List[Table]:
        sorted_tables = []

        bbox = (paragraph.start_x, paragraph.start_y, paragraph.end_x, paragraph.end_y)
        page_paragraph = page.within_bbox(bbox)
        page_text = page_paragraph.extract_text().strip()
        for table_index, table in enumerate(tables_in_page):
            bbox_table_corrected = (
                paragraph.start_x,
                table.bbox[1],
                paragraph.end_x,
                table.bbox[3]
            )
            text_of_table = page_paragraph.within_bbox(bbox_table_corrected).extract_text().strip()

            if text_of_table not in page_text:
                raise ValueError(f"Invalid state: text of table not found in page text, text_of_table: "
                                 f"{text_of_table}, page_text: {page_text}, page_number: {page_paragraph.page_number}")

            # Find the start index of the table in the page text
            table_start_index = page_text.index(text_of_table)
            sorted_tables.append((table_start_index, table))

        # Sort tables by their start index
        sorted_tables.sort(key=lambda x: x[0])

        # Return only the sorted tables
        return [table for _, table in sorted_tables]

    @classmethod
    def extract_tables_from_pdf(cls, pdf_content: io.BytesIO, page_regions: Mapping[str, List[Mapping[str, Any]]]):
        tables_regions = []
        paragraphs = [LegalActPageRegionParagraphs.from_dict(paragraph_mapping) for paragraph_mapping in
                      page_regions['paragraphs']]
        evaluated_table_page_regions: List[LegalActPageRegionTableRegion] = []
        with pdfplumber.open(pdf_content) as pdf:
            try:
                for paragraph in paragraphs:
                    page = pdf.pages[paragraph.page_number - 1]
                    bbox = (paragraph.start_x, paragraph.start_y, paragraph.end_x, paragraph.end_y)
                    page_paragraph = page.within_bbox(bbox)
                    page_text = page_paragraph.extract_text().strip()
                    # if paragraph.page_number == 79:
                    #     s =4

                    tables_in_page = cls.get_sorted_tables(page_paragraph.find_tables(), paragraph, page)
                    for table_index in range(len(tables_in_page)):
                        table = tables_in_page[table_index]
                        bbox_table_corrected = (paragraph.start_x, table.bbox[1], paragraph.end_x, table.bbox[3])
                        text_of_table = page_paragraph.within_bbox(bbox_table_corrected).extract_text().strip()
                        if text_of_table not in page_text:
                            ## TODO 'ELI', 'DU/2022/974' page_number 79
                            raise ValueError(f"Invalid state: text of table not found in page text, text_of_table: "
                                             f"{text_of_table}, page_text: {page_text}, page_number: {page_paragraph.page_number}")

                        table_start_index = page_text.index(text_of_table)
                        table_end_index = table_start_index + len(text_of_table)
                        if len(evaluated_table_page_regions) == 0:
                            # First table
                            evaluated_table_page_regions.append(
                                LegalActPageRegionTableRegion(
                                    start_x=bbox_table_corrected[0],
                                    start_y=bbox_table_corrected[1],
                                    end_x=bbox_table_corrected[2],
                                    end_y=bbox_table_corrected[3],
                                    page_number=page_paragraph.page_number,
                                    start_index=table_start_index,
                                    end_index=table_end_index
                                )
                            )
                        elif LegalActPageRegionTableRegion.have_overlapping_region(
                                table,
                                evaluated_table_page_regions
                        ):
                            evaluated_table_page_regions = LegalActPageRegionTableRegion \
                                .update_table_regions(table, evaluated_table_page_regions)
                        elif page_paragraph.page_number == evaluated_table_page_regions[-1].page_number + 1 \
                                and table_start_index == 0 \
                                and table_index == 0:
                            # There is a table on next page, it is continuation of the previous table
                            evaluated_table_page_regions = LegalActPageRegionTableRegion \
                                .update_table_regions(table, evaluated_table_page_regions)
                        else:
                            table_region = TableRegion(evaluated_table_page_regions)
                            tables_regions.append(table_region)
                            evaluated_table_page_regions = [LegalActPageRegionTableRegion(
                                start_x=bbox_table_corrected[0],
                                start_y=bbox_table_corrected[1],
                                end_x=bbox_table_corrected[2],
                                end_y=bbox_table_corrected[3],
                                page_number=page_paragraph.page_number,
                                start_index=table_start_index,
                                end_index=table_end_index
                            )]
                    # See Github issue https://github.com/jsvine/pdfplumber/issues/193
                    # clearing cache of pages for pdfplumber, to overcome memory leaks
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                if len(evaluated_table_page_regions) > 0:
                    table_region = TableRegion(evaluated_table_page_regions)
                    tables_regions.append(table_region)
            finally:
                pdf.close()

        return tables_regions

    @staticmethod
    def extract_text_from_pdf(pdf_content, page_regions: Mapping[str, List[Mapping[str, Any]]]):
        legal_act_text = ""

        paragraphs = [LegalActPageRegionParagraphs.from_dict(paragraph_mapping) for paragraph_mapping in
                      page_regions['paragraphs']]
        with pdfplumber.open(pdf_content) as pdf:
            try:
                for paragraph in paragraphs:
                    page = pdf.pages[paragraph.page_number - 1]
                    bbox = (paragraph.start_x, paragraph.start_y, paragraph.end_x, paragraph.end_y)
                    page_paragraph = page.within_bbox(bbox)
                    page_text = page_paragraph.extract_text().strip()
                    legal_act_text += page_text + " "
                    # See Github issue https://github.com/jsvine/pdfplumber/issues/193
                    # clearing cache of pages for pdfplumber, to overcome memory leaks
                    page.flush_cache()
                    page.get_textmap.cache_clear()
            finally:
                pdf.close()

        return legal_act_text

    @classmethod
    @retry_dask_task(retries=3, delay=10)
    def annotate_for_functions_worker_task(cls, row: Dict[str, str]):
        try:

            row_with_details: EstablishLegalActSegmentsBoundariesResult = EstablishLegalActSegmentsBoundariesResult \
                .from_dict(
                get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    collection_name="define_legal_act_pages_boundaries_stage"
                ).find_one(
                    {
                        "general_info.ELI": row['ELI'],
                        "invoke_id": row[DAG_TABLE_ID]
                        # "invoke_id": "efb31311-1281-424d-ab14-5916f0ef9259"
                    },
                    {"_id": 0}
                )
            )
            bucket, key = extract_bucket_and_key(row_with_details.general_info['s3_pdf_path'])
            pdf_content = io.BytesIO(
                boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
            extract_tables_from_pdf = cls.extract_tables_from_pdf(pdf_content, row_with_details.page_regions)
            legal_act_text = cls.extract_text_from_pdf(pdf_content, row_with_details.page_regions)
            result_with_tables = {
                **row_with_details.to_dict(),
                "tables_regions": [table.to_dict() for table in extract_tables_from_pdf],
                "paragraphs_text": legal_act_text,
                "expires_at": datetime.now() + timedelta(days=1)
            }
            get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="legal_act_boundaries_with_tables_stage"
            ).insert_one(result_with_tables)
        finally:
            gc.collect()
        # logger.info(f"Task completed in {elapsed_time} seconds, ELI: {row['ELI']}")
        return {
            'ELI': row_with_details.general_info['ELI'],
            'invoke_id': row_with_details.invoke_id
        }

    @classmethod
    @FlowStep.step(task_run_name='extract_table_and_equations_from_legal_acts')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            datalake: Datalake,
            s3_path_parquet_with_eli_documents: str):

        ddf_eli_documents = cls.read_from_datalake(s3_path_parquet_with_eli_documents,
                                                   pd.DataFrame({
                                                       'ELI': pd.Series(dtype='str'),
                                                       'invoke_id': pd.Series(dtype='str')
                                                   })
                                                   # pd.DataFrame({
                                                   #     'ELI': pd.Series(dtype='str'),
                                                   #     'invoke_id': pd.Series(dtype='str')
                                                   # })
                                                   )

        # rrr = ddf_eli_documents.compute()

        w = 4

        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2022/974"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1997/252"].compute()

        # w = selected_ddf.iloc[0].to_dict()

        # r = cls.annotate_for_functions_worker_task(row=selected_ddf.iloc[0].to_dict())

        delayed_tasks = ddf_eli_documents.map_partitions(
            lambda df: [
                delayed(cls.annotate_for_functions_worker_task)(
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
            logger.info(f"Task {future.key} executed on workers: {who_has}")

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)

        return cls.save_result_to_datalake(results_ddf, flow_information, cls)


class PassageSplitOpenAIError(Exception):
    pass


class PassageSplitError(Exception):
    pass


class GetTextBetweenBracketsError(Exception):
    pass


class EraseNumberError(Exception):
    pass


class StatusOfText(Enum):
    UPCOMING_CHANGE = "UPCOMING_CHANGE"
    BEFORE_UPCOMING_CHANGE = "BEFORE_UPCOMING_CHANGE"
    NOT_MATCH_CHANGE = "NON_MATCH"


class TextSplit:

    def __init__(self, text: str, start_index: int, end_index: int, action_type: StatusOfText):
        self.start_index = start_index
        self.end_index = end_index
        self.text = text
        self.action_type = action_type

    def is_up_to_date(self):
        normalized_text = re.sub(r'[\s\n\r]+', '', self.text).strip()
        return normalized_text not in ["(uchylony)", "(uchylona)", "(pominięte)", "(pominięty)"]


class TextChangesPiecesSplitter(ABC):
    # UPCOMING_CHANGE = "UPCOMING_CHANGE"
    # BEFORE_UPCOMING_CHANGE = "BEFORE_UPCOMING_CHANGE"
    # NOT_MATCH_CHANGE = "NON_MATCH"
    BEFORE_UPCOMING_CHANGE_PATTERN = r'^\[([^\[\]]*?)\]$'
    UPCOMING_CHANGE_PATTERN = r'^<([^\[\]]*?)>$'

    def __init__(self):
        pass

    def get_pattern_for_text_with_brackets(self, action_type: StatusOfText):
        return self.BEFORE_UPCOMING_CHANGE_PATTERN if action_type == StatusOfText.BEFORE_UPCOMING_CHANGE else self.UPCOMING_CHANGE_PATTERN

    def _find_changes_text(self, legal_text: str, pattern_str: str, action_type: StatusOfText):
        pattern = re.compile(pattern_str)
        pattern_between_brackets = self.get_pattern_for_text_with_brackets(action_type)

        changes = []
        for match in pattern.finditer(legal_text):
            is_text_between_brackets = re.match(pattern_between_brackets, match.group())
            if is_text_between_brackets:
                changes.append((match.start(), match.end(), is_text_between_brackets.group(1), action_type))
            else:
                raise GetTextBetweenBracketsError(f"Error during getting text between brackets in '{match.group()}'")
        return changes

    def _organise_text(self, text, before_upcoming_changes, after_upcoming_changes):
        upcoming_changes = before_upcoming_changes + after_upcoming_changes
        non_matches_of_not_upcoming_changes = self._find_non_matches(upcoming_changes, text)
        all_pieces = self.sort_pieces_of_art_text(upcoming_changes + non_matches_of_not_upcoming_changes)
        if self._are_pieces_overlap(all_pieces):
            raise PassageSplitError("Upcoming changes are overlapping")
        return [(left_i, right_i, text.strip(), action_type) for left_i, right_i, text, action_type in all_pieces]

    def sort_pieces_of_art_text(self, pieces):
        pieces.sort(key=lambda x: (x[0], x[1]))
        return pieces

    def _are_pieces_overlap(self, pieces):
        for i in range(len(pieces) - 1):
            if pieces[i][1] > pieces[i + 1][0]:
                return True
        return False

    def _find_non_matches(self, upcoming_changes, art_text: str):
        def add_non_match(prev_end, next_start):

            if prev_end < next_start and art_text[prev_end:next_start].strip() != "":
                return [(prev_end, next_start, art_text[prev_end:next_start], StatusOfText.NOT_MATCH_CHANGE)]
            return []

        sorted_upcoming_changes = self.sort_pieces_of_art_text(upcoming_changes)
        non_matches = []
        last_end = 0
        for start, end, text, action_type in sorted_upcoming_changes:
            non_matches += add_non_match(last_end, start)
            last_end = end
        non_matches += add_non_match(last_end, len(art_text))
        return non_matches

    def extract_parts(self, indentification: str) -> Tuple[int, str]:
        # Use regex to extract numeric and alphabetic parts
        match = re.match(r"(\d+)([a-z]?)", indentification)
        if match:
            num_part = int(match.group(1))  # Convert numeric part to an integer
            alpha_part = match.group(2)  # Alphabetic part remains a string
            return num_part, alpha_part

        return int('inf'), ''  # Fallback if the regex fails

    def is_ascending(self, indentification_numbers: List[str]):
        # Extract parts for sorting
        extracted_parts = [self.extract_parts(part_of_text_identification) for part_of_text_identification in
                           indentification_numbers]
        # Check if the list is sorted
        return extracted_parts == sorted(extracted_parts)

    def split(self, prev_split: TextSplit):
        part_of_texts = self._organise_text(prev_split.text,
                                            self._find_changes_text(prev_split.text,
                                                                    self.before_upcoming_change_pattern(),
                                                                    StatusOfText.BEFORE_UPCOMING_CHANGE),
                                            self._find_changes_text(prev_split.text, self.upcoming_change_pattern(),
                                                                    StatusOfText.UPCOMING_CHANGE)
                                            )
        splits: List[TextSplit] = []
        for start_index, end_index, part_text, action_type in part_of_texts:
            ## TODO check this way in the future
            # matches = re.finditer(r'(Art\. \d+[a-zA-Z]?[\s\S]*?)(?=Art\. \d+[a-zA-Z]?|$)', part_text)
            # for match in matches:
            #     r = 4
            #     w = match.group(0)
            #     start_index = match.start()
            #     end_index = match.end()
            for split in self.split_function(part_text):
                current_start_index = prev_split.start_index + prev_split.text.index(split)
                current_end_index = current_start_index + len(split)
                # if current_start_index == current_end_index:
                #     ww = 4
                if current_start_index == -1:
                    raise PassageSplitError(f"Error during splitting text, cannot find start_index of split: {split}")
                # rrr_text = text[current_start_index:current_end_index]
                #
                # prawda = rrr_text == split
                r = 4

                www = split[-10:]
                if current_end_index > current_start_index:
                    splits.append(
                        TextSplit(text=split,
                                  start_index=current_start_index,
                                  end_index=current_end_index,
                                  action_type=prev_split.action_type
                                  if prev_split.action_type != StatusOfText.NOT_MATCH_CHANGE else action_type
                                  )
                    )
        # for split in splits:
        #     if self.can_erase_number(split):

        return splits

    def get_identificator_text(self, split: TextSplit):
        if self.can_erase_number(split) is None:
            raise EraseNumberError("Error during getting identificator text")
        return re.findall(self.can_erase_number_pattern(), split.text)[0].strip()

    def erase_identificator_from_text(self, split: TextSplit):
        if self.can_erase_number(split) is None:
            raise EraseNumberError("Error during getting identificator text")
        match = re.search(self.can_erase_number_pattern(), split.text)
        start_index = match.start()
        end_index = match.end()
        r = split.text[end_index:]
        start_index = split.start_index + end_index
        end_index = split.end_index
        erased_text = re.sub(self.can_erase_number_pattern(), "", split.text).lstrip()
        return TextSplit(text=erased_text, start_index=start_index, end_index=end_index, action_type=split.action_type)

    def can_erase_number(self, split: TextSplit):
        return re.match(self.can_erase_number_pattern(), split.text)

    @abstractmethod
    def can_erase_number_pattern(self):
        pass

    @abstractmethod
    def before_upcoming_change_pattern(self):
        pass  # <....>

    @abstractmethod
    def upcoming_change_pattern(self):
        pass  # [....]

    @abstractmethod
    def split_function(self, text):
        pass

    @abstractmethod
    def process_text(self, text):
        pass


class ArticleSplitter(TextChangesPiecesSplitter):
    ERASE_FORMAT_legal_acts_structure_words_from_art_pattern = r'\s*(?=R\s*o\s*z\s*d\s*z\s*i\s*a\s*[łl]|D\s*Z\s*I\s*A\s*[ŁL]|O\s*d\s*d\s*z\s*i\s*a\s*[łl])'

    def __init__(self):
        super().__init__()

    def before_upcoming_change_pattern(self):
        return r"\[\s*Art\.\s*\d+[a-zA-Z]*\.[^\]\[\>]*\]"  # [Art. 1.]

    def upcoming_change_pattern(self):
        return r"<\s*Art\.\s*\d+[a-zA-Z]*\.[^>\]\[]*>"  # <Art. 1.>

    def split_function(self, text):
        return re.findall(r'(Art\. \d+[a-zA-Z]?[\s\S]*?)(?=Art\. \d+[a-zA-Z]?|$)', text)

    def can_erase_number_pattern(self):
        return r'Art\.\s*(\d*\w*(?:–\d*\w*)?).\s*'

    def process_text(self, text):
        return self.erase_redundant_act_text(text)

    def erase_redundant_act_text(self, act_text):
        return re.split(r'\s*(?=R\s*o\s*z\s*d\s*z\s*i\s*a\s*[łl]|D\s*Z\s*I\s*A\s*[ŁL]|O\s*d\s*d\s*z\s*i\s*a\s*[łl])',
                        act_text)[0].strip()


class PassageSplitter(TextChangesPiecesSplitter):
    def __init__(self):
        super().__init__()

    def before_upcoming_change_pattern(self):
        return r"\[\s*\d+[a-z]*\.[^\]\<\>]*\]"  # [1.]

    def upcoming_change_pattern(self):
        return r"<\s*\d+[a-z]*\.[^>\]\[]*>"  # <1.>

    def split_function(self, text):
        ## todo to test this:
        # (?<!ust\.\s)(?=\b\d+[a-zA-Z]?\.\s[\D(]) or
        # (?<!ust\.\s)(?:(?<=\n)|\b)(?=\b(?:Art\.|[\d]+[a-zA-Z]?)\.\s[\D(]) <- this looks better

        # return re.split(r'(?=[\sa-zA-Z[<]\d{1,}[a-z]*\.\s[\D(])', text)
        # return re.split(r'(?<![-–])(?<!pkt\s)(?<!ust\.\s)(?=\b\d+[a-zA-Z]?\.\s[\D(])', text)
        return re.split(
            r'(?:(?<=\.\s)|(?<=^)|(?<=\(uchylony\)\s)|(?<=\(uchylona\)\s)|(?<=\(pominięte\)\s)|(?<=\(pominięty\)\s))(?<![-–])(?<!pkt\s)(?<!ust\.\s)(?<!art\.\s)(?=(?:§\s*)*\b\d+[a-zA-Z]?\.\s[\D(])',
            text)

    def can_erase_number_pattern(self):
        # return r"^\s*(\d+[a-z]*)\s*\.\s*(?=\w|\[|\()"
        return r"^(?:[§]?)\s*(\d+[a-z]*)\s*\.\s*(?=\w|\[|\()"

    def process_text(self, text):
        return self.remove_redudant_section_sign(
            self.standardize_section_reference_keyword(text)
        )

    # def map_splits_from_api_retry(self, passage_splits: List):
    #     def process_split(passage_split_text):
    #         if re.match(self.get_pattern_for_text_with_brackets(self.BEFORE_UPCOMING_CHANGE), passage_split_text):
    #             return {
    #                 "text": re.match(self.get_pattern_for_text_with_brackets(self.BEFORE_UPCOMING_CHANGE),
    #                                  passage_split_text).group(1).strip(),
    #                 "action_type": self.BEFORE_UPCOMING_CHANGE
    #             }
    #         elif re.match(self.get_pattern_for_text_with_brackets(self.UPCOMING_CHANGE), passage_split_text):
    #             return {
    #                 "text": re.match(self.get_pattern_for_text_with_brackets(self.UPCOMING_CHANGE),
    #                                  passage_split_text).group(1).strip(),
    #                 "action_type": self.UPCOMING_CHANGE
    #             }
    #         else:
    #             return {
    #                 "text": passage_split_text.strip(),
    #                 "action_type": self.NOT_MATCH_CHANGE
    #             }
    #
    #     return list(map(process_split, passage_splits))

    def remove_redudant_section_sign(self, passage_text):
        return passage_text.strip().rstrip("§") if passage_text.rstrip().endswith('§') \
            else passage_text.strip()

    def standardize_section_reference_keyword(self, passage_text):
        return passage_text.replace('§', 'ust.')


class MultiLineTextLLMObject(BaseModel):
    text: str


class PassageTemp:
    def __init__(self, passage_number: str, passage_text: TextSplit, subpoints: List['SubpointTemp']):
        self.passage_number = passage_number
        self.passage_text = passage_text
        self.subpoints = subpoints


class SubpointTemp:
    def __init__(self, subpoint_number: str, subpoint_text: TextSplit, subSubpointsRefs):
        self.subpoint_number = subpoint_number
        self.subpoint_text = subpoint_text
        self.subSubpointsRefs = subSubpointsRefs


class ArticleTemp:
    def __init__(self, art_number: str, art_text: TextSplit, passages: List['PassageTemp'],
                 subpoints: List['SubpointTemp']):
        self.art_number = art_number
        self.art_text = art_text
        self.passages = passages
        self.subpoints = subpoints


class EstablishPartsOfLegalActs(FlowStep):
    article_splitter = ArticleSplitter()
    passage_splitter = PassageSplitter()

    @classmethod
    def split_legal_act(cls, row_with_details: EstablishLegalWithTablesResult, pdf_content: io.BytesIO):

        legal_act_page_info_tab, legal_text_concatenated = LegalActLineDivision.create_lines_of_doc(
            row_with_details.page_regions, row_with_details.tables_regions, pdf_content, row_with_details)
        # article_splits = cls.article_splitter.split(
        #     TextSplit(
        #         text=legal_text_concatenated,
        #         start_index=0,
        #         end_index=len(legal_text_concatenated),
        #         action_type=StatusOfText.NOT_MATCH_CHANGE
        #
        #     )
        # )
        # for split in article_splits:
        #     art_number = cls.article_splitter.get_identificator_text(split)
        #     cleaned_art_text_split = cls.article_splitter.erase_identificator_from_text(split)
        #     cls.split_passage(row_with_details, art_number, cleaned_art_text_split, legal_text_concatenated)
        # r = 4

    @classmethod
    def split_passage(cls, row_with_details: EstablishLegalWithTablesResult, art_number, art_split: TextSplit,
                      legal_text_all: str):
        passage_splits = cls.passage_splitter.split(art_split)
        cls.process_passages(row_with_details, art_number, art_split, passage_splits, legal_text_all)
        rr = 4

    @classmethod
    def process_passages(cls, row_with_details: EstablishLegalWithTablesResult, art_number: str, art_split: TextSplit,
                         passage_splits: List[TextSplit], legal_text_all: str):

        article = ArticleTemp(
            art_number=art_number,
            art_text=art_split,
            passages=[],
            subpoints=[]
        )
        # uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongodb.net/"
        uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongodb.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"
        db_name = "preprocessing_legal_acts_texts"

        mongo_client = MongoClient(uri, server_api=ServerApi('1'))
        mongo_db = mongo_client[db_name]
        collection = mongo_db["not_correct_spliting_of_passage"]
        # cls.passage_splitter.is_ascending
        passage_numbers = []
        passage_texts = []
        for index_split, passage_split in enumerate(passage_splits):
            is_passage = cls.passage_splitter.can_erase_number(passage_split)
            if index_split == 0 and not is_passage:
                art_text, subpoints = cls.split_subpoint(
                    passage_number=None,
                    passage_for_further_split=passage_split
                )
            elif is_passage:
                passage_number = cls.passage_splitter.get_identificator_text(passage_split)
                passage_numbers.append(passage_number)
                cleaned_passage_text_split = cls.passage_splitter.erase_identificator_from_text(passage_split)
                if cleaned_passage_text_split.text.strip() == '':
                    collection.insert_one({
                        "ELI": row_with_details.general_info.ELI,
                        "art_number": art_number,
                        "art_text": art_split.text,
                        "passage_split": passage_split.text,
                        "type": "passage is empty"
                    })
                passage_texts.append(cleaned_passage_text_split.text)
                rrr = legal_text_all[passage_split.start_index:passage_split.end_index]
                miau = legal_text_all[cleaned_passage_text_split.start_index:cleaned_passage_text_split.end_index]
                if not cleaned_passage_text_split.is_up_to_date():
                    rau = 4
            else:
                collection.insert_one({
                    "ELI": row_with_details.general_info.ELI,
                    "art_number": art_number,
                    "art_text": art_split.text,
                    "passage_split": passage_split.text,
                    "type": "passage is not a passage"
                })
        if not cls.passage_splitter.is_ascending(passage_numbers):
            collection.insert_one({
                "ELI": row_with_details.general_info.ELI,
                "art_number": art_number,
                "art_text": art_split.text,
                "passage_splits": [split.text for split in passage_splits],
                "currect_order": passage_numbers,
                "type": "not corrected splitting of passage"
            })

    @classmethod
    def split_subpoint(cls, passage_number, passage_for_further_split: TextSplit):
        return None, None

    @classmethod
    @retry_dask_task(retries=3, delay=10)
    def worker_task(cls, row: Dict[str, str]):
        try:
            row_with_details: EstablishLegalWithTablesResult = EstablishLegalWithTablesResult \
                .from_dict(
                get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    collection_name="legal_act_page_metadata"
                ).find_one(
                    {
                        "general_info.ELI": row['ELI'],
                        # "invoke_id": row[DAG_TABLE_ID]
                        "invoke_id": row[DAG_TABLE_ID]
                    },
                    {"_id": 0}
                )
            )
            bucket, key = extract_bucket_and_key(row_with_details.general_info.s3_pdf_path)
            pdf_content = io.BytesIO(
                boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
            cls.split_legal_act(row_with_details, pdf_content)
            s = 4
        finally:
            gc.collect()

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

        selected_ddf = ddf_eli_documents[
            ddf_eli_documents["ELI"] == "DU/2011/696"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1997/604"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1984/268"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/2022/974"].compute()

        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1960/168"].compute()
        #
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1985/60"].compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["ELI"] == "DU/1974/117"].compute()
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
            logger.info(f"Task {future.key} executed on workers: {who_has}")

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)

        return cls.save_result_to_datalake(results_ddf, flow_information, cls)


class ExtractAttachmentsFromLegalActs(FlowStep):
    # Define the regex pattern for identifying "Załącznik" sections
    LEGAL_ANNOTATION_PATTERN = r"(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[" \
                               r"a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y)\s*?)(.*?)(?=(?:\s*Z\s*a\s*[łl]\s*[" \
                               r"aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y))|$)"

    NAV_PATTERN = r"©\s*K\s*a\s*n\s*c\s*e\s*l\s*a\s*r\s*i\s*a\s+S\s*e\s*j\s*m\s*u\s+s\.\s*\d+\s*/\s*\d+"

    FOOTER_PATTERN = r"\d{2,4}\s*-\s*\d{2}\s*-\s*\d{2}$"

    @classmethod
    def find_content_boundaries(cls, pdf_content: str) -> List[Dict[str, Union[int, float]]]:
        pages_boundaries = []

        with pdfplumber.open(pdf_content) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    words = page.extract_words()
                    navbar_y = None
                    footer_y = None
                    page_height = page.bbox[3]

                    # Find Navbar (Header) Position
                    navbar_match = re.search(cls.NAV_PATTERN, text)
                    if navbar_match:
                        navbar_text = navbar_match.group(0)
                        navbar_text = re.sub(r'[\s\n\r]+', ' ', navbar_text)  # Clean spaces and newlines

                        # Search for the navbar location in the page's layout text
                        for idx, word in enumerate(words):
                            candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(navbar_text.split())])
                            if navbar_text == candidate_text:
                                navbar_y = words[idx]['top']
                                for next_word in words[idx + len(navbar_text.split()):]:
                                    if next_word['top'] > navbar_y:  # Move to the next line's Y position
                                        navbar_end_y = next_word['top']
                                        break
                                break

                    last_text_y_before_footer = None  # This will store the Y position of the last line before the footer

                    # Find Footer Position
                    footer_match = re.search(cls.FOOTER_PATTERN, text)
                    if footer_match:
                        footer_text = footer_match.group(0)
                        footer_text = re.sub(r'[\s\n\r]+', ' ', footer_text)  # Clean spaces and newlines

                        # Search for the footer location in the page's layout text
                        for idx, word in enumerate(words):
                            candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(footer_text.split())])
                            if footer_text == candidate_text:
                                footer_y = words[idx]['top']  # Y position of the first line of the footer
                                break

                        # Find the last line of text before the footer starts
                        for word in words:
                            if word['bottom'] < footer_y:
                                last_text_y_before_footer = word[
                                    'bottom']  # Continuously update until just before footer_y
                            else:
                                break

                    pages_boundaries.append(
                        {
                            'page_num': page_num,
                            'start_y': navbar_end_y,
                            'end_y': last_text_y_before_footer,
                            'page_height': page_height
                        }
                    )

        return pages_boundaries

    @classmethod
    def locate_attachments(cls, pdf_content: str) -> List[Dict[str, Union[int, float]]]:
        with pdfplumber.open(pdf_content) as pdf:
            attachment_boundaries = []  # To hold start and end coordinates for each attachment
            last_boundary = None

            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    # Find all matches of "Załącznik" sections in the page text
                    matches = list(re.finditer(cls.LEGAL_ANNOTATION_PATTERN, text, re.DOTALL))
                    page_boundaries = []
                    for match in matches:
                        # Extract starting Y-axis of the current "Załącznik"
                        not_cleared = match.group(0)
                        match_text = re.sub(r'[\s\n\r]+', ' ', match.group(0))
                        # Default top Y position for the match (entire page if position not found)
                        top_y = page.bbox[1]

                        text = page.extract_text()
                        # Search for the match's location in the page layout text
                        words = page.extract_words()
                        for idx, word in enumerate(words):
                            # Join words to compare with match_text, and check within a reasonable range
                            candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(match_text.split())])
                            if match_text == candidate_text:
                                top_y = words[idx]['top']
                                break
                        # Extract text before `start_y` for `text_before_start`
                        text_before_start = ""
                        for word in words:
                            if word['top'] < top_y:
                                text_before_start += word['text'] + " "

                        # Record the boundary as (page_num, start_y, end_page, end_y)
                        # Assume end of this "Załącznik" starts at the start of the next, or end of page
                        boundary = {
                            'match_text': match_text,
                            'start_page': page_num,
                            'start_y': top_y,
                            'end_page': page_num,  # Update if it spans pages
                            'end_y': page.bbox[3],  # Initial end is bottom of page, can be updated
                            'text_before_start': text_before_start
                        }
                        if last_boundary and re.fullmatch(cls.NAV_PATTERN, text_before_start.strip()):
                            last_boundary['end_page'] = page_num - 1
                            last_boundary['end_y'] = pdf.pages[last_boundary['end_page']].bbox[3]
                        elif last_boundary and last_boundary['end_page'] == last_boundary['start_page']:
                            last_boundary['end_page'] = page_num
                            last_boundary['end_y'] = top_y

                        page_boundaries.append(boundary)
                        last_boundary = boundary

                    attachment_boundaries.extend(page_boundaries)

            if last_boundary and last_boundary['end_page'] == last_boundary['start_page']:
                last_boundary['end_y'] = pdf.pages[last_boundary['end_page']].bbox[3]

        return attachment_boundaries

    @classmethod
    def crop_pdf_attachments(cls, pdf_content: str, datalake: Datalake, attachment_boundaries,
                             pages_boundaries: List[Dict[str, Union[int, float]]],
                             flow_information: Dict,
                             padding: int = 2):
        pdf_reader = PdfFileReader(pdf_content)

        for i, boundary in enumerate(attachment_boundaries):
            pdf_writer = PdfFileWriter()

            start_page = boundary['start_page']
            end_page = boundary['end_page']
            start_y = float(boundary['start_y'])
            end_y = float(boundary['end_y'])

            for page_num in range(start_page, end_page + 1):
                page = pdf_reader.getPage(page_num)
                page_height = pages_boundaries[page_num]['page_height']
                navbar_y = pages_boundaries[page_num]['start_y']
                footer_y = pages_boundaries[page_num]['end_y']

                # Calculate the PDF coordinates
                adjusted_start_y = page_height - start_y + padding if page_num == start_page and start_y > navbar_y else page_height - navbar_y + 3 * padding
                adjusted_end_y = page_height - end_y if page_num == end_page and end_y < footer_y else page_height - footer_y - padding

                page.mediaBox.upperRight = (page.mediaBox.getUpperRight_x(), adjusted_end_y)
                page.mediaBox.lowerLeft = (page.mediaBox.getLowerLeft_x(), adjusted_start_y)

                # Add the cropped page to the new PDF
                pdf_writer.addPage(page)

            datalake.save_to_datalake_pdf(pdf_writer=pdf_writer, flow_information=flow_information,
                                          filename=f"attachment_{i}.pdf", stage_type=cls)

        return
        print(f"Saved {len(attachment_boundaries)} cropped ")

    @classmethod
    def extract_attachments_worker(cls, row, flow_information: Dict, datalake: Datalake):
        bucket, key = extract_bucket_and_key(row['s3_pdf_path'])
        pdf_content = datalake.read_from_datalake_pdf(key)

        pages_boundaries = cls.find_content_boundaries(pdf_content=pdf_content)

    @classmethod
    @FlowStep.step(task_run_name='extract_attachments_from_legal_acts')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            datalake: Datalake,
            s3_path_parquet_with_legal_document_rows: str):

        ddf_legal_document_rows_datalake = cls.read_from_datalake(
            s3_path_parquet_with_legal_document_rows,
            meta_DULegalDocumentsMetaData_with_s3_path)
        df = ddf_legal_document_rows_datalake.compute()

        cls.extract_attachments(row=df.iloc[0].to_dict(), datalake=datalake)


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

    R = 4
    ## TODO when we start flow define if stack name already exist, omit creation of stack
    # dask_cluster = CreateDaskCluster.run(
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

    datalake = Datalake(datalake_bucket=DATALAKE_BUCKET, aws_region=AWS_REGION)

    # dask_cluster = GetExistingDaskCluster.run(stack_name='dask-stack-28429daf-bd4c-4232-95c3-99e530c9d694')
    # #
    # dask_cluster = UpdateDaskClusterWorkers.run(
    #     dask_cluster=dask_cluster,
    #     desired_count=20
    # )

    # dask_cluster = RestartDaskClusterWorkers.run(
    #     dask_cluster=dask_cluster
    # )

    r = 4

    # dask_cluster = None

    client = Client(dask_cluster.cluster_url)
    # client = Client('idealistic-manul-TCP-0cd4f6af06afc9d5.elb.eu-west-1.amazonaws.com:8786')
    # client = Client("tcp://localhost:8786")

    r = 4
    # client = Client("agile-beagle-TCP-671138ce4c7af57a.elb.eu-west-1.amazonaws.com:8786")
    # flow222(flow_information=flow_information, dask_client=client, type_document="DU")

    # ss = download_raw_rows_for_each_year(flow_information=flow_information, dask_client=client, type_document="DU")
    # TODO UNCOMMENT THIS STAGE LATER
    path_to_raw_rows = DownloadRawRowsForEachYear.run(flow_information=flow_information, dask_client=client,
                                                      type_document="DU")

    r = 4
    #

    path_to_raw_rows = 's3://datalake-bucket-123/api-sejm/a04e9d3d-1890-42cc-8cf5-3d190eb617c3/DU/*.parquet'
    # ## here we start new flow for test
    # TODO UNCOMMENT THIS STAGE LATER
    path_to_rows = GetRowsOfLegalActsForPreprocessing.run(flow_information=flow_information,
                                                          dask_client=client,
                                                          s3_path=path_to_raw_rows)

    # path_to_rows = 's3://datalake-bucket-123/stages/$049eec07-0322-4530-a135-f66719696ede/GetRowsOfLegalActsForPreprocessing/rows_filtered.parquet.gzip'

    r = 4
    #
    # TODO UNCOMMENT THIS STAGE LATER
    path_DULegalDocumentsMetaData_rows = FilterRowsFromApiAndUploadToParquetForFurtherProcessing.run(
        flow_information=flow_information,
        dask_client=client,
        workers_count=3,
        s3_path_parquet=path_to_rows)

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

    # TODO UNCOMMENT THIS STAGE LATER
    # path_to_parquet_line_divisions = LegalActLineDivision.run(flow_information=flow_information, dask_client=client, workers_count=8,
    #                                s3_path_parquet_with_eli_documents=path_to_parquet_pages_boundaries)

    path_to_parquet_line_divisions = 's3://datalake-bucket-123/stages/$99be0778-14a7-45f8-9eac-5283278dc945/LegalActLineDivision/results.parquet.gzip'

    path_to_parquet_legal_parts = EstablishPartsOfLegalActs.run(
        flow_information=flow_information, dask_client=client, workers_count=8,
        s3_path_parquet_with_eli_documents=path_to_parquet_line_divisions
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

    path_to_eli_documents_with_extracted_text = ExtractTextAndGetPagesWithTables.run(flow_information=flow_information,
                                                                                     dask_client=client,
                                                                                     workers_count=dask_cluster.get_workers_count(),
                                                                                     s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

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
def for_the_running_without_debugging(local_cluster: bool = True):
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

    ## STEP 7  heavy cost
    path_to_parquet_line_divisions = LegalActLineDivision.run(flow_information=flow_information, dask_client=client,
                                                              workers_count=dask_workers_count,
                                                              s3_path_parquet_with_eli_documents=path_to_parquet_pages_boundaries)

    print(path_to_parquet_line_divisions)

    dask_cluster.delete_dask_cluster()


if __name__ == "__main__":
    # here we define flow
    # StartDag.run() >> CreateDaskCluster
    try:
        ## TODO JUST FOCUS ON THIS
        for_the_running_without_debugging()
    except Exception:
        log.error("Remote error:\n%s", traceback.format_exc())

    # try:
    #     preprocessing_api()
    # except Exception:
    #     log.error("Remote error:\n%s", traceback.format_exc())
    #     raise  # ważne! żeby Prefect widział błąd
    cluster_name = 'Fargate-Dask-Cluster'  # Replace with your ECS cluster name
    service_name = 'Dask-Workers'  # Replace with your ECS service name
    # ecs_client = boto3.client('ecs', region_name='eu-west-1')
    # response = ecs_client.describe_services(cluster=cluster_name, services=[service_name])
    # running_count = response['services'][0]['runningCount']
    # print(f"Currently running tasks: {running_count}")

    download_legal_documents_and_preprocess_them()

    client = Client('tcp://localhost:8786')
    # client = DaskTaskRunner(address='tcp://localhost:8786')
    # kk.task_runner = client
    kk(client=client)
    log_repo_info()

    # log_repo_info()
    # my_flow()
    # elevator()
