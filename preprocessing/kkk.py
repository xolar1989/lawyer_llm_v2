import ctypes
import gc
import io
import json
import logging
import os
import re
import time
import uuid
from abc import abstractmethod, ABC, ABCMeta
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Union, List, Dict

import backoff
import pyarrow as pa
import pyarrow.parquet as pq
import PyPDF2
import boto3
import numpy as np
import pandas as pd
import pdfplumber
from PyPDF2 import PdfFileReader, PdfFileWriter
from botocore.client import BaseClient
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import dask.dataframe as dd
from dask import delayed
from dask.diagnostics import ProgressBar
from langchain_openai import ChatOpenAI
from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
from requests.adapters import HTTPAdapter
from tqdm.dask import TqdmCallback
from dask.distributed import Client, as_completed
from dask.distributed import get_worker
import requests

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect_dask import DaskTaskRunner
from prefect.futures import resolve_futures_to_states
from tqdm import tqdm

from preprocessing.stages.create_dask_cluster import CreateDaskCluster
from preprocessing.stages.download_pdfs_for_legal_acts_rows import DownloadPdfsForLegalActsRows
from preprocessing.stages.download_raw_rows_for_each_year import DownloadRawRowsForEachYear
from preprocessing.stages.filter_rows_from_api_and_upload_to_parquet_for_further_processing import \
    FilterRowsFromApiAndUploadToParquetForFurtherProcessing
from preprocessing.stages.get_existing_dask_cluster import GetExistingDaskCluster
from preprocessing.stages.get_metadatachange_and_filter_legal_documents_which_not_change import \
    GetMetaDataChangeAndFilterLegalDocumentsWhichNotChange
from preprocessing.stages.get_rows_of_legal_acts_for_preprocessing import GetRowsOfLegalActsForPreprocessing
from preprocessing.stages.restart_dask_cluster_workers import RestartDaskClusterWorkers
from preprocessing.stages.start_dag import StartDag
from preprocessing.stages.update_dask_cluster_workers import UpdateDaskClusterWorkers
from preprocessing.utils.dask_cluster import retry_dask_task
from preprocessing.utils.datalake import Datalake
from preprocessing.utils.defaults import SEJM_API_URL, ERROR_TOPIC_NAME, AWS_REGION, DAG_TABLE_ID, \
    DAG_DYNAMODB_TABLE_NAME, DATALAKE_BUCKET
from preprocessing.utils.dynamodb_helper import put_item_into_table, map_to_update_expression, \
    map_to_expression_attribute_values, update_item, fetch_segment_data, meta_DULegalDocumentsMetaData, \
    meta_DULegalDocumentsMetaDataChanges_v2, insert_partition_to_dynamodb, \
    meta_DULegalDocumentsMetaData_intermediate_result, meta_DULegalDocumentsMetaData_with_s3_path, \
    meta_DULegalDocumentsMetaData_without_filename
from preprocessing.utils.few_shot_examples import TypeOfAttachment
from preprocessing.utils.general import get_secret
from preprocessing.utils.llm_chain import get_table_chains
from preprocessing.utils.llm_chain_type import LLMChainType
from preprocessing.utils.mongodb import get_mongodb_collection, MongodbCollection, MongodbCollectionIndex
from preprocessing.utils.mongodb_schema import extract_text_and_table_page_number_stage_schema, \
    schema_for_legal_act_row
from preprocessing.utils.s3_helper import upload_json_to_s3, extract_bucket_and_key
from preprocessing.utils.sns_helper import send_sns_email
from preprocessing.utils.sejm_api_utils import make_api_call
from preprocessing.utils.stage_def import FlowStep, FlowStepError, get_storage_options_for_ddf_dask
from distributed.versions import get_versions

from preprocessing.utils.table_extraction import TableUtils

logger = logging.getLogger()

logger.setLevel(logging.INFO)

os.environ['TZ'] = 'UTC'

get_versions()


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
    uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongodb.net/"

    mongo_client = MongoClient(uri, server_api=ServerApi('1'))
    mongo_db = mongo_client[db_name]
    if db_name == "preprocessing_legal_acts_texts":
        if collection_name == "extract_text_and_table_page_number_stage":
            return MongodbCollection(
                collection_name="extract_text_and_table_page_number_stage",
                db_instance=mongo_db,
                collection_schema=extract_text_and_table_page_number_stage_schema,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
                ]
            )
        elif collection_name == "find_annotation_and_label_table_stage":
            return MongodbCollection(
                collection_name="find_annotation_and_label_table_stage",
                db_instance=mongo_db,
                collection_schema=found_annotation_and_labeled_table_schema,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
                ]
            )
    raise Exception("Invalid db_name or collection_name Mongodb error")


class EstablishLegalActSegmentsBoundaries(FlowStep):


    ## TODO finished here, then move from ll.py other functions here without already moved functions, put in separated files
    @classmethod
    def establish_worker(cls, row, flow_information: Dict, datalake: Datalake):
        bucket, key = extract_bucket_and_key(row['s3_pdf_path'])
        datalake = Datalake(datalake_bucket=DATALAKE_BUCKET, aws_region=AWS_REGION)

    @classmethod
    @FlowStep.step(task_run_name='extract_attachments_from_legal_acts')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            datalake: Datalake,
            s3_path_parquet_with_legal_document_rows: str):
        pass


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
                                last_text_y_before_footer = word['bottom']  # Continuously update until just before footer_y
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


class ExtractTextAndGetPagesWithTables(FlowStep):

    @staticmethod
    def extract_tables_from_pdf(pdf_content):
        def is_current_table(current_table_pages_numbers):
            return len(current_table_pages_numbers) > 0

        all_tables = []
        current_table_pages_numbers = []
        current_table_pages_text = []
        last_table_page = -1
        data = []
        with pdfplumber.open(pdf_content) as pdf:
            try:
                for page_num, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    page_text = page.extract_text()

                    new_page_text = re.sub("©\s*Kancelaria\s+Sejmu\s*s\s*\.\s*\d+\s*/\s*\d+\s*", '', page_text).replace(
                        "\n",
                        " ")
                    for table_num, table in enumerate(tables):
                        k = str(table[0][0]).replace('\n', ' ')

                        table_start_pos = new_page_text.find(str(table[0][0]).replace('\n', ' ')[:20])

                        if page_num == last_table_page + 1 and table_num == 0:
                            # Check for intervening text
                            intervening_text = new_page_text[:table_start_pos].strip()
                            if intervening_text:
                                # If there's intervening text, it's a new table
                                if is_current_table(current_table_pages_numbers):
                                    all_tables.append(
                                        [str(num) for num in current_table_pages_numbers]
                                    )
                                current_table_pages_numbers = [page_num]
                                current_table_pages_text = [page]
                            else:
                                # No intervening text, so it's a continuation
                                current_table_pages_numbers.append(page_num)
                                current_table_pages_text.append(page)
                        else:
                            if is_current_table(current_table_pages_numbers):
                                all_tables.append(
                                    [str(num) for num in current_table_pages_numbers]
                                )
                            current_table_pages_numbers = [page_num]
                            current_table_pages_text = [page]
                        last_table_page = page_num

                    # See Github issue https://github.com/jsvine/pdfplumber/issues/193
                    # clearing cache of pages for pdfplumber, to overcome memory leaks
                    page.flush_cache()
                    page.get_textmap.cache_clear()
            finally:
                pdf.close()
            if is_current_table(current_table_pages_numbers):
                all_tables.append(
                    [str(num) for num in current_table_pages_numbers]
                )
        return all_tables

    @classmethod
    def remove_header_and_footer_from_pages(cls, text_by_page, pages_with_tables):
        def flat_map_pages(pages):
            return [page for page_table in pages for page in page_table]

        cleaned_text_by_page = {}
        for index, page_num in enumerate(text_by_page):
            cleaned_text_by_page[page_num] = cls.remove_header_and_footer_from_single_page(text_by_page[page_num],
                                                                                           page_num == str(0),
                                                                                           page_num not in flat_map_pages(
                                                                                               pages_with_tables))

        return cleaned_text_by_page

    @staticmethod
    def clean_first_page_of_legal_act(text):
        return re.split("(?=U\s*S\s*T\s*A\s*W\s*A)", text)[-1]

    @staticmethod
    def remove_footer_from_page(text):
        split_point = list(re.finditer("\\n\s+\\n", text))
        if split_point:
            last_split = split_point[-1].end()
            text = text[:last_split].strip()
        return text

    @classmethod
    def remove_header_and_footer_from_single_page(cls, text_page, first_page=False, remove_footer=True):
        cleaned_text = re.sub("^©\s*Kancelaria\s+Sejmu\s*s\.\s+\d+/\d+\s*\\n\s*\\n\d{4}\s*[-]?\s*\d{2}\s*[-]?\s*\d{2}",
                              '', text_page)
        if first_page:
            cleaned_text = cls.clean_first_page_of_legal_act(cleaned_text)
        if remove_footer:
            cleaned_text = cls.remove_footer_from_page(cleaned_text)
        return cleaned_text

    @staticmethod
    def extract_text_from_pdf(pdf_content):
        # Create a PDF reader object
        pdf_reader = PyPDF2.PdfReader(pdf_content)
        text_by_page = {}

        # Extract text from each page
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text = page.extract_text()
            text_by_page[str(page_num)] = text

        return text_by_page

    @classmethod
    @retry_dask_task(retries=5, delay=10)
    def extract_text_and_get_pages_numbers_worker(cls, row, flow_information, storage_options):

        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION
        )
        bucket, key = extract_bucket_and_key(row['s3_pdf_path'])

        pdf_content = io.BytesIO(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read())
        try:
            text_by_page = cls.extract_text_from_pdf(pdf_content)
            pages_with_tables = cls.extract_tables_from_pdf(pdf_content)
            cleaned_text_by_page = cls.remove_header_and_footer_from_pages(text_by_page, pages_with_tables)

            get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="extract_text_and_table_page_number_stage"
            ).insert_one({
                "general_info": row,
                "invoke_id": flow_information[DAG_TABLE_ID],
                "pages": cleaned_text_by_page,
                "pages_with_tables": pages_with_tables,
                "expires_at": datetime.now() + timedelta(days=1)
            })
        finally:
            # Explicitly delete large objects and trigger garbage collection
            del pdf_content, text_by_page, pages_with_tables, cleaned_text_by_page
            gc.collect()
        return {
            'ELI': row['ELI'],
            'invoke_id': flow_information[DAG_TABLE_ID]
        }

    @classmethod
    @FlowStep.step(task_run_name='extract_text_and_get_pages_with_tables')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_legal_document_rows: str):
        ddf_legal_document_rows_datalake = cls.read_from_datalake(
            s3_path_parquet_with_legal_document_rows,
            meta_DULegalDocumentsMetaData_with_s3_path)

        selected_ddf = ddf_legal_document_rows_datalake

        ## here load row, but not execute download_and_upload_pdf_to_s3

        # r = cls.extract_text_and_get_pages_numbers_worker(row = legal_acts_to_extract[0],flow_information=flow_information, storage_options=get_storage_options_for_ddf_dask(AWS_REGION))

        delayed_tasks = selected_ddf.map_partitions(
            lambda df: [
                delayed(cls.extract_text_and_get_pages_numbers_worker)(
                    row=row,
                    flow_information=flow_information,
                    storage_options=get_storage_options_for_ddf_dask(AWS_REGION)
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

        # TODO here create method to run verbose or not verbose
        # delayed_dfs = [delayed(pd.DataFrame)(result, index=[i]) for i, result in enumerate(flat_tasks)]
        #
        # dask_ddf = dd.from_delayed(delayed_dfs, meta={'ELI': str, 'invoke_id': str})
        #
        #
        # w = dask_ddf.compute()

        result_df = pd.DataFrame(results)

        results_ddf = dd.from_pandas(result_df, npartitions=workers_count)

        return cls.save_result_to_datalake(results_ddf, flow_information, cls)

    # delayed_tasks = ddf_legal_document_rows_datalake.map_partitions(
    #     lambda df: [
    #                    delayed(cls.download_and_upload_pdf_to_s3)(
    #                        row=row,
    #                        s3_bucket=DATALAKE_BUCKET,
    #                        s3_key=f'stages/documents-to-download-pdfs/{flow_information[DAG_TABLE_ID]}',
    #                        storage_options=get_storage_options_for_ddf_dask(AWS_REGION)
    #                    )
    #                    for row in df.to_dict(orient='records')
    #                ]


class AnnotateTextWithCreatingFunctions(FlowStep):

    ## TODO correct this because Załącznik nr 2b could occur
    LEGAL_ANNOTATION_PATTERN = r"(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y)\s*?)(.*?)(?=(?:\s*Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y))|$)"

    @staticmethod
    def clean_text(text: str) -> str:
        # Replace newlines, carriage returns, and multiple spaces with a single space
        return re.sub(r'[\s\n\r]+', ' ', text).strip()

    @classmethod
    def get_legal_annotations_for_document(cls, pages):
        all_text = cls.clean_text(" ".join(text for page_num, text in pages.items()))
        legal_annotations_texts = re.findall(cls.LEGAL_ANNOTATION_PATTERN, all_text, re.DOTALL)

        legal_annotations = []
        for i, (header, content) in enumerate(legal_annotations_texts):
            entire_attachment_text = header + " " + content
            annotation_type = TableUtils.establish_type_of_annotation(
                llmchain=get_table_chains(
                    chain_type=LLMChainType.WHAT_TYPE_OF_ANNOTATION_CHAIN,
                    ai_model=ChatOpenAI(
                        model="gpt-4o",
                        temperature=0,
                        api_key=get_secret("OpenAiApiKey", AWS_REGION)["OPEN_API_KEY"]
                    )
                ),
                input={"input": entire_attachment_text}
            )
            if isinstance(annotation_type, TypeOfAttachment):
                legal_annotations.append({
                    "header": header,
                    "content": content,
                    "type": annotation_type.type_of_attachment
                })

        return legal_annotations

    @classmethod
    def merge_overlapping_indexes(cls, tables, text_without_attachments):
        if not tables or len(tables) == 0:
            return []

        # Sort tables by their start_index to ensure proper order
        tables.sort(key=lambda x: x['start_index'])

        merged_tables = []
        current_start = tables[0]['start_index']
        current_end = tables[0]['end_index']
        current_amount_of_tables = 1

        for next_table in tables[1:]:
            # Check if the current range overlaps with the next range
            if current_end >= next_table['start_index']:
                # If they overlap, extend the end index
                current_end = max(current_end, next_table['end_index'])
                current_amount_of_tables += 1
            else:
                # If they don't overlap, extract and store the text for the current range
                expanded_start = max(0, current_start - 1)
                expanded_end = min(len(text_without_attachments), current_end + 1)

                merged_text = text_without_attachments[expanded_start:expanded_end]
                merged_tables.append({
                    'table_text': merged_text,
                    'start_index': current_start,
                    'end_index': current_end,
                    'amount_of_tables': current_amount_of_tables
                })
                # Move to the next range
                current_start = next_table['start_index']
                current_end = next_table['end_index']
                current_amount_of_tables = 1

        expanded_start = max(0, current_start - 1)
        expanded_end = min(len(text_without_attachments), current_end + 1)
        # Add the final range
        merged_text = text_without_attachments[expanded_start:expanded_end]
        merged_tables.append({
            'table_text': merged_text,
            'start_index': current_start,
            'end_index': current_end,
            'amount_of_tables': current_amount_of_tables
        })

        return merged_tables


    @classmethod
    def get_text_with_tables_and_their_indexes(cls, pages: Dict[str, str], tables: List[List[str]], text_without_attachments: str):
        tables_with_their_places = []
        for table in tables:
            pages_text_with_table = ""
            for page_table_index in table:
                pages_text_with_table = pages_text_with_table + " " + pages[page_table_index]
            escaped_sequence = re.escape(pages_text_with_table.strip())
            pattern_to_find_text = rf"({escaped_sequence})"
            match_text = re.search(pattern_to_find_text, text_without_attachments)
            if match_text:
                start_index, end_index = match_text.span()
                tables_with_their_places.append({
                    "table_text": pages_text_with_table,
                    "start_index": start_index,
                    "end_index": end_index
                })
        return cls.merge_overlapping_indexes(tables_with_their_places, text_without_attachments)

    @classmethod
    def annotate_tables_and_merge(cls, pages: Dict[str, str], tables: List[List[str]], row: Dict[str, str]):
        all_text = " ".join(text for page_num, text in pages.items()).strip()
        text_without_attachments = re.sub(cls.LEGAL_ANNOTATION_PATTERN, '', all_text, flags=re.DOTALL).strip()

        merged_tables = cls.get_text_with_tables_and_their_indexes(pages, tables, text_without_attachments)

        final_text = []

        previous_end = 0
        for table in merged_tables:
            non_table_text = text_without_attachments[previous_end:table['start_index']]
            final_text.append(non_table_text)

            text_with_annotated_pages = TableUtils.add_annotation_of_table(
                llmchain=get_table_chains(
                    chain_type=LLMChainType.ADD_ANNOTATION_OF_TABLE_CHAIN,
                    ai_model=ChatOpenAI(
                        model="gpt-4o",
                        temperature=0,
                        api_key=get_secret("OpenAiApiKey", AWS_REGION)["OPEN_API_KEY"]
                    )
                ),
                input={"input": table['table_text']},
                row=row,
                amount_of_table=table['amount_of_tables']
            )
            final_text.append(text_with_annotated_pages)
            previous_end = table['end_index']

        final_text.append(text_without_attachments[previous_end:])

        return cls.clean_text(" ".join(final_text))

    @classmethod
    @retry_dask_task(retries=3, delay=10)
    def annotate_tables_worker_task(cls, row: Dict[str, str]):
        try:
            row_with_details = get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="extract_text_and_table_page_number_stage"
            ).find_one(
                {
                    "general_info.ELI": row['ELI'],
                    "invoke_id": row[DAG_TABLE_ID]
                },
                {"_id": 0}
            )
            # row_with_details = get_mongodb_collection(
            #     db_name="preprocessing_legal_acts_texts",
            #     collection_name="extract_text_and_table_page_number_stage"
            # ).find_one(
            #     {
            #         "general_info.ELI": 'DU/2011/696',
            #         "invoke_id": '6129d63e-e933-4dcd-9f0a-a809c1cd7464'
            #     },
            #     {"_id": 0}
            # )
            pages = row_with_details['pages']

            legal_act_text = cls.annotate_tables_and_merge(pages, row_with_details['pages_with_tables'], row)

            legal_annotations = cls.get_legal_annotations_for_document(pages)
            get_mongodb_collection(
                db_name="preprocessing_legal_acts_texts",
                collection_name="find_annotation_and_label_table_stage"
            ).insert_one({
                "general_info": row_with_details['general_info'],
                "invoke_id": row[DAG_TABLE_ID],
                "content": legal_act_text,
                "legal_annotations": legal_annotations,
                "expires_at": datetime.now() + timedelta(days=1)
            })
            return row
        finally:
            if 'row_with_details' in locals():
                del row_with_details
            if 'pages' in locals():
                del pages
            if 'legal_annotations' in locals():
                del legal_annotations
            if 'legal_act_text' in locals():
                del legal_act_text
            gc.collect()

    @classmethod
    @FlowStep.step(task_run_name='annotate_text_with_creating_functions')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int,
            s3_path_parquet_with_eli_documents: str):
        ddf_eli_documents = cls.read_from_datalake(s3_path_parquet_with_eli_documents,
                                                   pd.DataFrame({
                                                       'ELI': pd.Series(dtype='str'),
                                                       'invoke_id': pd.Series(dtype='str')
                                                   })
                                                   )

        # df = ddf_eli_documents.compute()
        #
        # cls.annotate_tables_worker_task(row=df.iloc[0].to_dict())

        r = 4

        ss = cls.__name__
        rr = 4

        delayed_tasks = ddf_eli_documents.map_partitions(
            lambda df: [
                delayed(cls.annotate_tables_worker_task)(
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
    dask_cluster = CreateDaskCluster.run(
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

    # dask_cluster = GetExistingDaskCluster.run(stack_name="dask-stack-f122019f-6c3e-423b-9212-068778e13c58")

    dask_cluster = UpdateDaskClusterWorkers.run(
        dask_cluster=dask_cluster,
        desired_count=12
    )

    # dask_cluster = RestartDaskClusterWorkers.run(
    #     dask_cluster=dask_cluster
    # )

    r = 4

    # dask_cluster = None

    client = Client(dask_cluster.cluster_url)
    # client = Client('wild-mongrel-TCP-1dacd18448b358a7.elb.eu-west-1.amazonaws.com:8786')
    # client = Client("tcp://localhost:8786")

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

    path_to_parquet_for_legal_documents_with_s3_pdf = 's3://datalake-bucket-123/stages/extract-text-from-documents-with-pdfs/$74d77c70-0e2f-47d1-a98f-b56b4c181120/documents_with_pdfs.parquet.gzip'

    path_to_eli_documents_with_extracted_text = ExtractTextAndGetPagesWithTables.run(flow_information=flow_information,
                                                                                     dask_client=client,
                                                                                     workers_count=dask_cluster.get_workers_count(),
                                                                                     s3_path_parquet_with_legal_document_rows=path_to_parquet_for_legal_documents_with_s3_pdf)

    # path_to_eli_documents_with_extracted_text = 's3://datalake-bucket-123/stages/$6129d63e-e933-4dcd-9f0a-a809c1cd7464/ExtractTextAndGetPagesWithTables/results.parquet.gzip'

    path_to_annotated_text_with_creating_functions = AnnotateTextWithCreatingFunctions.run(flow_information=flow_information, dask_client=client,
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

# %%
