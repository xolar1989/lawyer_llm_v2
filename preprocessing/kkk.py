import base64
import gc
import io
import os
import re
import time
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Union, List, Dict, Mapping, Any, Set, Tuple

import boto3
import cv2
import numpy as np
import pandas as pd
import pdfplumber
import unicodedata
from PIL import Image
from PyPDF2 import PdfFileReader, PdfFileWriter
import dask.dataframe as dd
from dask import delayed
from langchain_core.messages import SystemMessage, HumanMessage, BaseMessage
from langchain_openai import ChatOpenAI
from openai import RateLimitError
from pdfplumber.table import Table
from pydantic.v1 import BaseModel
from pymongo import ASCENDING
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dask.distributed import Client, as_completed

from prefect import flow
from prefect.context import get_run_context
from tqdm import tqdm

from preprocessing.pdf_boundaries.margin_boundaries import LegalActMarginArea
from preprocessing.pdf_boundaries.paragraph_boundaries import LegalActParagraphArea, ParagraphAreaType
from preprocessing.pdf_elements.lines import TableRegionPageLegalAct, OrderedObjectLegalAct, TextLinePageLegalAct
from preprocessing.pdf_utils.multiline_extractor import MultiLineTextExtractor
from preprocessing.utils.stages_objects import EstablishLegalActSegmentsBoundariesResult, \
    EstablishLegalWithTablesResult, GeneralInfo
from preprocessing.stages.create_dask_cluster import CreateDaskCluster
from preprocessing.stages.start_dag import StartDag
from preprocessing.utils.attachments_extraction import get_attachments_headers, AttachmentRegion, \
    get_pages_with_attachments, PageRegions
from preprocessing.utils.dask_cluster import retry_dask_task
from preprocessing.utils.datalake import Datalake
from preprocessing.utils.defaults import ERROR_TOPIC_NAME, AWS_REGION, DAG_TABLE_ID, \
    DATALAKE_BUCKET
from preprocessing.utils.dynamodb_helper import meta_DULegalDocumentsMetaDataChanges_v2, \
    meta_DULegalDocumentsMetaData_with_s3_path
from preprocessing.utils.general import get_secret
from preprocessing.mongo_db.mongodb import get_mongodb_collection, MongodbCollection, MongodbCollectionIndex, \
    MongodbObject
from preprocessing.mongo_db.mongodb_schema import extract_text_and_table_page_number_stage_schema, \
    schema_for_legal_act_row, define_legal_act_pages_boundaries_stage_schema, \
    legal_act_boundaries_with_tables_stage_schema
from preprocessing.utils.page_regions import LegalActPageRegionAttachment, LegalActPageRegionMarginNotes, \
    LegalActPageRegionParagraphs, LegalActPageRegionFooterNotes, LegalActPageRegion, LegalActPageRegionTableRegion, \
    TableRegion
from preprocessing.utils.s3_helper import extract_bucket_and_key
from preprocessing.utils.sns_helper import send_sns_email
from preprocessing.utils.stage_def import FlowStep, get_storage_options_for_ddf_dask
from distributed.versions import get_versions

from pdf2image import convert_from_bytes

import logging
from cloudwatch import cloudwatch

logger = logging.getLogger()

logger.setLevel(logging.INFO)

os.environ['TZ'] = 'UTC'

get_versions()


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


def get_mongodb_collection(db_name, collection_name):
    ## localhost
    # uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongodb.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"
    uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongodb.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"

    # uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongodb.net/"

    mongo_client = MongoClient(uri)
    mongo_db = mongo_client[db_name]
    if db_name == "preprocessing_legal_acts_texts":
        if collection_name == "extract_text_and_table_page_number_stage":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                collection_schema=extract_text_and_table_page_number_stage_schema,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
                ]
            )
        elif collection_name == "find_annotation_and_label_table_stage":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                collection_schema=found_annotation_and_labeled_table_schema,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
                ]
            )
        elif collection_name == "define_legal_act_pages_boundaries_stage":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                collection_schema=define_legal_act_pages_boundaries_stage_schema,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
                ]
            )
        elif collection_name == "legal_act_boundaries_with_tables_stage":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                collection_schema=legal_act_boundaries_with_tables_stage_schema,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
                ]
            )
        elif collection_name == 'legal_act_page_metadata':
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING), ("page.page_number", ASCENDING)], unique=True)
                ]
            )
    raise Exception("Invalid db_name or collection_name Mongodb error")


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


# TODO This step is ready
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

    # TODO this cause error : https://api.sejm.gov.pl/eli/acts/DU/2016/2140/text/U/D20162140Lj.pdf
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
                "expires_at": datetime.now() + timedelta(days=10)
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
            datalake: Datalake,
            s3_path_parquet_with_legal_document_rows: str):
        ddf_legal_document_rows_datalake = cls.read_from_datalake(
            s3_path_parquet_with_legal_document_rows,
            meta_DULegalDocumentsMetaData_with_s3_path)

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/1991/72"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2004/1925"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2010/44"].compute()

        selected_ddf = ddf_legal_document_rows_datalake[
            ddf_legal_document_rows_datalake["ELI"] == "DU/2011/696"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2016/2073"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2006/550"].compute()

        #
        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2013/1160"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2002/1287"].compute()

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2017/1051"].compute()
        # <- to check on more

        ## last check , unuusal documents
        # DU/2016/2073
        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2016/2073"].compute() ## is working fine

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2006/550"].compute()

        ## and documents with speaker rulling
        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2016/2140"].compute() ## is working fine

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2018/647"].compute() ## regular header and footer last working!!!

        # selected_ddf = ddf_legal_document_rows_datalake[
        #     ddf_legal_document_rows_datalake["ELI"] == "DU/2012/123"].compute() ## checking header overflow it' working !!!
        #
        # pdf_path = w['s3_pdf_path']
        #
        # bucket, key = extract_bucket_and_key(pdf_path)
        #
        # pdf_content = io.BytesIO(boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
        #
        # r = cls.establish_worker(row=selected_ddf.iloc[0].to_dict(), flow_information=flow_information)
        #
        # selected_ddf = ddf_legal_document_rows_datalake[
        #         ddf_legal_document_rows_datalake["ELI"] == "DU/2017/1051"].compute()
        #
        #
        r = cls.establish_worker(row=selected_ddf.iloc[0].to_dict(), flow_information=flow_information)

        w = 4
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


## TODO remove this class just leave lines and methods of this class
class LegalActPage(MongodbObject):
    LINE_DELIMITER = ' \n'

    ## TODO check this quit well example D20220974Lj
    def __init__(self, page_number: int, paragraph_lines: List[OrderedObjectLegalAct],
                 footer_lines: List[OrderedObjectLegalAct], margin_lines: List[OrderedObjectLegalAct]):
        self.page_number = page_number
        self.paragraph_lines = paragraph_lines
        self.footer_lines = footer_lines
        self.margin_lines = margin_lines

    @property
    def len_paragraph_lines(self):
        return self.get_len_of_lines(self.paragraph_lines)

    @property
    def len_footer_lines(self):
        return self.get_len_of_lines(self.footer_lines)

    @property
    def len_margin_lines(self):
        return self.get_len_of_lines(self.margin_lines)

    @property
    def get_table_lines(self) -> List[TableRegionPageLegalAct]:
        return [line for line in self.paragraph_lines if isinstance(line, TableRegionPageLegalAct)]

    @staticmethod
    def get_len_of_lines(lines: List[OrderedObjectLegalAct]):
        return sum(len(line) + len(LegalActPage.LINE_DELIMITER) for line in lines)

    def to_dict(self):
        return {
            "page_number": self.page_number,
            "paragraph_lines": [line.to_dict() for line in self.paragraph_lines],
            "footer_lines": [line.to_dict() for line in self.footer_lines],
            "margin_lines": [line.to_dict() for line in self.margin_lines]
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            page_number=dict_object["page_number"],
            paragraph_lines=[OrderedObjectLegalAct.from_dict(line) for line in dict_object["paragraph_lines"]],
            footer_lines=[OrderedObjectLegalAct.from_dict(line) for line in dict_object["footer_lines"]],
            margin_lines=[OrderedObjectLegalAct.from_dict(line) for line in dict_object["margin_lines"]]
        )

    # TODO use this for saving attachments as pdf
    # @classmethod
    # def crop_region_to_image(cls, pdf_content: io.BytesIO, line_dict: Dict[str, Any], page_pdfplumber: pdfplumber.pdf.Page):
    #     pdf_reader = PdfFileReader(pdf_content)
    #     pdf_writer = PdfFileWriter()
    #
    #
    #     page_pypdf2 = pdf_reader.getPage(page_pdfplumber.page_number-1)
    #
    #     page_width = page_pypdf2.mediaBox.getUpperRight_x() - page_pypdf2.mediaBox.getLowerLeft_x()
    #
    #     page_height = page_pypdf2.mediaBox.getUpperRight_y() - page_pypdf2.mediaBox.getLowerLeft_y()
    #
    #
    #     adjusted_y_start = page_pdfplumber.height - line_dict['top']  # Flip y-axis for PyPDF2
    #     adjusted_y_end = page_pdfplumber.height - line_dict['bottom']   # Flip y-axis for PyPDF2
    #
    #     # adjusted_start_y = page_height - start_y + padding if page_num == start_page and start_y > navbar_y else page_height - navbar_y + 3 * padding
    #     # adjusted_end_y = page_height - end_y if page_num == end_page and end_y < footer_y else page_height - footer_y - padding
    #
    #     page_pypdf2.mediaBox.upperRight = (line_dict['x1'], adjusted_y_end)
    #     page_pypdf2.mediaBox.lowerLeft = (line_dict['x0'],  adjusted_y_start)
    #
    #     # Add the cropped page to the new PDF
    #     pdf_writer.addPage(page_pypdf2)
    #
    #     with open("lines_2/rr.pdf", "wb") as output_file:
    #         pdf_writer.write(output_file)

    @classmethod
    def merge_dict_text_line(cls, current_text_dict_line: Dict[str, Any],
                             prev_text_dict_line: Dict[str, Any],
                             page: pdfplumber.pdf.Page) -> Dict[str, Any]:
        current_number = max([char['num_line'] for char in prev_text_dict_line['chars']])
        current_text_dict_line['chars'] = [{**char, 'num_line': current_number + 1} for char in
                                           current_text_dict_line['chars']]
        prev_text_dict_line['chars'].extend(current_text_dict_line['chars'])
        prev_text_dict_line['chars'] = sorted(prev_text_dict_line['chars'], key=lambda c: c['x0'])
        prev_text_dict_line['top'] = min(prev_text_dict_line['top'], current_text_dict_line['top'])
        prev_text_dict_line['bottom'] = max(prev_text_dict_line['bottom'], current_text_dict_line['bottom'])
        prev_text_dict_line['x0'] = min(prev_text_dict_line['x0'], current_text_dict_line['x0'])
        prev_text_dict_line['x1'] = max(prev_text_dict_line['x1'], current_text_dict_line['x1'])
        bbox = (
            prev_text_dict_line['x0'], prev_text_dict_line['top'], prev_text_dict_line['x1'],
            prev_text_dict_line['bottom'])
        prev_text_dict_line["text"] = page.within_bbox(bbox).extract_text()
        prev_text_dict_line['merged'] = True
        return prev_text_dict_line

    @classmethod
    def create_lines_of_specific_area(cls, bbox_area: Tuple[float, float, float, float],
                                      current_index_of_page: int,
                                      page: pdfplumber.pdf.Page,
                                      multiline_llm_extractor: MultiLineTextExtractor
                                      ) -> List[TextLinePageLegalAct]:
        text_lines_paragraph_region = page.within_bbox(bbox_area).extract_text_lines()
        text_lines = [
            {
                **line,
                'merged': False,
                'chars': [{**char, 'num_line': 1} for char in line['chars']]
            }
            for line in sorted(text_lines_paragraph_region, key=lambda x: x['top'])
        ]
        lines_dict_list = []
        line_obj_list = []
        current_index = current_index_of_page
        for current_text_dict_line in text_lines:
            if not lines_dict_list:
                lines_dict_list.append(current_text_dict_line)
            elif current_text_dict_line['top'] <= lines_dict_list[-1]['bottom'] and current_text_dict_line[
                'bottom'] >= lines_dict_list[-1]['top']:
                lines_dict_list[-1] = cls.merge_dict_text_line(
                    current_text_dict_line=current_text_dict_line,
                    prev_text_dict_line=lines_dict_list[-1],
                    page=page
                )
            else:
                line_obj = (
                    TextLinePageLegalAct.build_using_multi_line(
                        line_dict=lines_dict_list[-1],
                        page=page,
                        current_index=current_index,
                        multiline_llm_extractor=multiline_llm_extractor
                    ) if lines_dict_list[-1]['merged'] else
                    TextLinePageLegalAct.build_using_single_line(
                        line_dict=lines_dict_list[-1],
                        current_index=current_index,
                        page=page
                    )
                )
                w = line_obj.text
                current_index += len(line_obj) + len(cls.LINE_DELIMITER)
                line_obj_list.append(line_obj)
                lines_dict_list.append(current_text_dict_line)
        if lines_dict_list:
            line_obj = (
                TextLinePageLegalAct.build_using_multi_line(
                    line_dict=lines_dict_list[-1],
                    page=page,
                    current_index=current_index,
                    multiline_llm_extractor=multiline_llm_extractor
                ) if lines_dict_list[-1]['merged'] else
                TextLinePageLegalAct.build_using_single_line(
                    line_dict=lines_dict_list[-1],
                    current_index=current_index,
                    page=page
                )
            )
            current_index += len(line_obj) + len(cls.LINE_DELIMITER)
            line_obj_list.append(line_obj)

        return line_obj_list

    @classmethod
    def build(cls, page: pdfplumber.pdf.Page,
              paragraph_areas: List[LegalActParagraphArea],
              margin_areas: List[LegalActMarginArea],
              footer_area: LegalActPageRegionFooterNotes,
              current_index_of_legal_act: int, current_table_number: int,
              current_index_of_footer_legal_act: int,
              current_index_of_margin_legal_act: int,
              multiline_llm_extractor: MultiLineTextExtractor) -> 'LegalActPage':
        paragraph_lines: List[OrderedObjectLegalAct] = []
        footer_lines: List[OrderedObjectLegalAct] = []
        margin_lines: List[OrderedObjectLegalAct] = []
        current_index_of_page = current_index_of_legal_act
        current_table_number_on_page = current_table_number
        if footer_area:
            footer_lines = cls.create_lines_of_specific_area(
                bbox_area=footer_area.bbox,
                current_index_of_page=current_index_of_footer_legal_act,
                page=page,
                multiline_llm_extractor=multiline_llm_extractor
            )
            # kk = cls.get_len_of_lines(footer_lines)
            # final_text_of_footer = ''
            # for line_obj in footer_lines:
            #     text_line = line_obj.text
            #     final_text_of_footer += text_line + cls.LINE_DELIMITER
            #
            # # footer_lines = page.within_bbox(footer_area.bbox).extract_text_lines()
            # rr = page.within_bbox(footer_area.bbox).extract_text()
            #
            # w = 4
        # else:
        #     nie_ma = 444
        current_index_of_margin = current_index_of_margin_legal_act
        for margin_area in margin_areas:
            margin_lines_region = cls.create_lines_of_specific_area(
                bbox_area=margin_area.bbox,
                current_index_of_page=current_index_of_margin,
                page=page,
                multiline_llm_extractor=multiline_llm_extractor
            )
            current_index_of_margin += cls.get_len_of_lines(margin_lines_region)
            margin_lines.extend(margin_lines_region)
        # final_text_of_margin = ''
        # for line_obj in margin_lines:
        #     text_line = line_obj.text
        #     final_text_of_margin += text_line + cls.LINE_DELIMITER
        #
        # wwww =4
        for paragraph_area in paragraph_areas:
            if paragraph_area.area_type == ParagraphAreaType.TABLE:
                table_region = TableRegionPageLegalAct(
                    start_x=paragraph_area.start_x,
                    end_x=paragraph_area.end_x,
                    start_y=paragraph_area.start_y,
                    end_y=paragraph_area.end_y,
                    page_number=page.page_number,
                    start_index_in_act=current_index_of_page,
                    table_number=current_table_number_on_page
                )
                current_table_number_on_page += 1
                current_index_of_page += len(table_region) + len(cls.LINE_DELIMITER)
                paragraph_lines.append(table_region)
            else:
                paragraph_text_lines_region = cls.create_lines_of_specific_area(
                    bbox_area=paragraph_area.bbox,
                    current_index_of_page=current_index_of_page,
                    page=page,
                    multiline_llm_extractor=multiline_llm_extractor
                )
                current_index_of_page += cls.get_len_of_lines(paragraph_text_lines_region)
                paragraph_lines.extend(paragraph_text_lines_region)
        #
        # final_text_of_page = ''
        # for line_obj in paragraph_lines:
        #     text_line = line_obj.text
        #     final_text_of_page += text_line + cls.LINE_DELIMITER
        #
        # miau = 4

        return cls(
            page_number=page.page_number,
            paragraph_lines=paragraph_lines,
            footer_lines=footer_lines,
            margin_lines=margin_lines
        )


class MultiLineTextLLMObject(BaseModel):
    text: str


class LegalActPageMetadata(MongodbObject):

    def to_dict(self):
        return {
            "general_info": GeneralInfo.to_dict(self.general_info),
            "invoke_id": self.invoke_id,
            "page": LegalActPage.to_dict(self.page),
            "expires_at": self.expires_at
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            general_info=GeneralInfo.from_dict(dict_object['general_info']),
            invoke_id=dict_object['invoke_id'],
            page=LegalActPage.from_dict(dict_object['page']),
            expires_at=dict_object['expires_at']
        )

    def __init__(self, general_info: GeneralInfo, invoke_id: str, page: LegalActPage,
                 expires_at: datetime):
        self.general_info = general_info
        self.invoke_id = invoke_id
        self.page = page
        self.expires_at = expires_at


class LegalActLineDivision(FlowStep):

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
    def create_pages(cls, page_regions: PageRegions, pdf_content: io.BytesIO) -> List[LegalActPage]:
        multiline_text_extractor = MultiLineTextExtractor(max_retries=5)
        index_of_document = 0
        index_of_footer_document = 0
        index_of_margin_document = 0
        index_of_table = 1
        pages = []
        with pdfplumber.open(pdf_content) as pdf:
            try:
                for paragraph in page_regions.paragraphs:
                    page = pdf.pages[paragraph.page_number - 1]

                    paragraph_areas = LegalActParagraphArea.split_paragraph_into_areas(paragraph, page)

                    margin = cls.get_margin_notes_region_of_page(page_regions.margin_notes, page)
                    margin_areas = LegalActMarginArea.split_margin_notes_into_areas(paragraph, margin, page)

                    legal_act_page = LegalActPage.build(
                        page=page,
                        paragraph_areas=paragraph_areas,
                        margin_areas=margin_areas,
                        footer_area=cls.get_footer_region_of_page(page_regions.footer_notes, page),
                        current_index_of_legal_act=index_of_document,
                        current_table_number=index_of_table,
                        current_index_of_footer_legal_act=index_of_footer_document,
                        current_index_of_margin_legal_act=index_of_margin_document,
                        multiline_llm_extractor=multiline_text_extractor
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
    @retry_dask_task(retries=3, delay=10)
    def worker_task(cls, row: Dict[str, str]):
        try:
            row_with_details: EstablishLegalActSegmentsBoundariesResult = EstablishLegalActSegmentsBoundariesResult \
                .from_dict(
                get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    ## TODO change this later
                    collection_name="define_legal_act_pages_boundaries_stage"
                ).find_one(
                    {
                        "general_info.ELI": row['ELI'],
                        # "invoke_id": row[DAG_TABLE_ID]
                        # "invoke_id": row[DAG_TABLE_ID]
                        "invoke_id": "a3412da8-b112-485d-911b-5ed94de9ae7f"
                    },
                    {"_id": 0}
                )
            )
            bucket, key = extract_bucket_and_key(row_with_details.general_info.s3_pdf_path)
            pdf_content = io.BytesIO(
                boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
            pages = cls.create_pages(row_with_details.page_regions, pdf_content)

            pages_to_save = []

            for page in pages:
                pages_to_save.append(
                    LegalActPageMetadata(
                        general_info=row_with_details.general_info,
                        page=page,
                        invoke_id=row_with_details.invoke_id,
                        expires_at=datetime.now() + timedelta(days=1)
                    ).to_dict()
                )

            get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="legal_act_page_metadata"
            ).insert_many(pages_to_save)

            ## TODO here is the result now i need cleanup and it will be ready to use to extract lines
            result: List[LegalActPageMetadata] = [
                LegalActPageMetadata.from_dict(page_metadata)
                for page_metadata in get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    collection_name="legal_act_page_metadata"
                ).find_many(
                    {
                        "general_info.ELI": row['ELI'],
                        # "invoke_id": row[DAG_TABLE_ID]
                        "invoke_id": "a3412da8-b112-485d-911b-5ed94de9ae7f"
                    },
                    {"_id": 0}
                )]
            w = 4
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
                    collection_name="legal_act_boundaries_with_tables_stage"
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

    # dask_cluster = GetExistingDaskCluster.run(stack_name="dask-stack-5582fb00-2f3e-4d1d-8f08-547d601bcc06")
    #
    # dask_cluster = UpdateDaskClusterWorkers.run(
    #     dask_cluster=dask_cluster,
    #     desired_count=12
    # )

    # dask_cluster = RestartDaskClusterWorkers.run(
    #     dask_cluster=dask_cluster
    # )

    r = 4

    # dask_cluster = None

    # client = Client(dask_cluster.cluster_url)
    # client = Client('wild-mongrel-TCP-1dacd18448b358a7.elb.eu-west-1.amazonaws.com:8786')
    client = Client("tcp://localhost:8786")

    r = 4
    # client = Client("agile-beagle-TCP-671138ce4c7af57a.elb.eu-west-1.amazonaws.com:8786")
    # flow222(flow_information=flow_information, dask_client=client, type_document="DU")

    # ss = download_raw_rows_for_each_year(flow_information=flow_information, dask_client=client, type_document="DU")
    # path_to_raw_rows = DownloadRawRowsForEachYear.run(flow_information=flow_information, dask_client=client,
    #                                                   type_document="DU")

    r = 4
    #
    # ## here we start new flow for test
    # path_to_rows = GetRowsOfLegalActsForPreprocessing.run(flow_information=flow_information,
    #                                                       dask_client=client,
    #                                                       s3_path=path_to_raw_rows)
    #
    # path_DULegalDocumentsMetaData_rows = FilterRowsFromApiAndUploadToParquetForFurtherProcessing.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=2,
    #     s3_path_parquet=path_to_rows)
    #
    # path_to_parquet_for_legal_documents, path_to_parquet_for_legal_documents_changes_lists = GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=2,
    #     s3_path_parquet=path_DULegalDocumentsMetaData_rows
    # )
    #
    # path_to_parquet_for_legal_documents_with_s3_pdf = DownloadPdfsForLegalActsRows.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=2,
    #     s3_path_parquet=path_to_parquet_for_legal_documents
    # )

    # CheckingFooter.run(flow_information=flow_information, dask_client=client, workers_count=2,
    #                       s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    path_to_parquet_for_legal_documents_with_s3_pdf = 's3://datalake-bucket-123/stages/extract-text-from-documents-with-pdfs/$74d77c70-0e2f-47d1-a98f-b56b4c181120/documents_with_pdfs.parquet.gzip'

    # CheckingFOOTERType2.run(flow_information=flow_information, dask_client=client, workers_count=2,
    #                         s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)
    #
    # CheckingHeaderType2.run(flow_information=flow_information, dask_client=client, workers_count=2,
    #                         s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    # CheckingFooter.run(flow_information=flow_information, dask_client=client, workers_count=2,
    #                    s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    # path_to_parquet_pages_boundaries = EstablishLegalActSegmentsBoundaries.run(flow_information=flow_information,
    #                                                                            dask_client=client,
    #                                                                            # workers_count=dask_cluster.get_workers_count(),
    #                                                                            workers_count=3,
    #                                                                            datalake=datalake,
    #                                                                            s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    # path_to_parquet_pages_boundaries = 's3://datalake-bucket-123/stages/$5025ccd4-f8f1-4003-8822-7f66592a89a2/EstablishLegalActSegmentsBoundaries/results.parquet.gzip'

    path_to_parquet_pages_boundaries = 's3://datalake-bucket-123/stages/$efb31311-1281-424d-ab14-5916f0ef9259/EstablishLegalActSegmentsBoundaries/results.parquet.gzip'

    # path_with_rows_with_established_tables_and_equations = ExtractTableAndEquationsFromLegalActs.run(
    #     flow_information=flow_information,
    #     dask_client=client,
    #     workers_count=dask_cluster.get_workers_count(),
    #     # workers_count=3,
    #     datalake=datalake,
    #     s3_path_parquet_with_eli_documents=path_to_parquet_pages_boundaries
    #
    # )

    # path_with_rows_with_established_tables_and_equations = 's3://datalake-bucket-123/stages/$9aa21043-923f-42fb-8f3c-3628aab3d0f6/ExtractTableAndEquationsFromLegalActs/results.parquet.gzip'
    path_with_rows_with_established_tables_and_equations = 's3://datalake-bucket-123/stages/$1da9d10e-3468-42dc-adb4-de30beff2960/ExtractTableAndEquationsFromLegalActs/results.parquet.gzip'

    sss = LegalActLineDivision.run(flow_information=flow_information, dask_client=client, workers_count=2,
                                   s3_path_parquet_with_eli_documents=path_with_rows_with_established_tables_and_equations)

    s = 4

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


if __name__ == "__main__":
    # here we define flow
    StartDag >> CreateDaskCluster

    preprocessing_api()

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
