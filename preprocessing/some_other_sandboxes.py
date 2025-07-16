import logging

logger = logging.getLogger(__name__)
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
