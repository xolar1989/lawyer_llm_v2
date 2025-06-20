
## TODO make this schema automatically generated from the dataclass

def array_of_strings_schema(item_def: object):
    return {
        "bsonType": "array",
        "items": item_def
    }

schema_for_legal_act_row = {  # Embedded document containing meta-information
    "bsonType": "object",
    "required": ["ELI", "document_year", "status", "announcementDate", "volume", "address", "displayAddress",
                 "promulgation", "pos", "publisher", "changeDate", "textHTML", "textPDF",
                 "title", "type", "filename_of_pdf", "s3_pdf_path"],
    "properties": {
        "ELI": {
            "bsonType": "string",
            "description": "Must be a string and is required."
        },
        "document_year": {
            "bsonType": "int",
            "description": "Must be an integer representing the document's year."
        },
        "status": {
            "bsonType": "string",
            "description": "Must be a string describing the document's status."
        },
        "announcementDate": {
            "bsonType": ["string", "null"],
            "description": "Must be a string representing the announcement date."
        },
        "volume": {
            "bsonType": "int",
            "description": "Must be an integer representing the volume."
        },
        "address": {
            "bsonType": "string",
            "description": "Must be a string representing the address."
        },
        "displayAddress": {
            "bsonType": "string",
            "description": "Must be a string for the display address."
        },
        "promulgation": {
            "bsonType": ["string", "null"],
            "description": "Must be a string representing the promulgation date."
        },
        "pos": {
            "bsonType": "int",
            "description": "Must be an integer representing the position."
        },
        "publisher": {
            "bsonType": "string",
            "description": "Must be a string representing the publisher."
        },
        "changeDate": {
            "bsonType": ["string"],
            "description": "Must be a string representing the change date."
        },
        "textHTML": {
            "bsonType": "bool",
            "description": "Must be a boolean representing if the document has HTML text."
        },
        "textPDF": {
            "bsonType": "bool",
            "description": "Must be a boolean representing if the document has a PDF."
        },
        "title": {
            "bsonType": "string",
            "description": "Must be a string representing the title of the document."
        },
        "type": {
            "bsonType": "string",
            "description": "Must be a string representing the type of document."
        },
        "filename_of_pdf": {
            "bsonType": "string",
            "description": "Must be a string representing the PDF filename."
        },
        "s3_pdf_path": {
            "bsonType": "string",
            "description": "Must be a string representing the S3 path to the PDF."
        }
    }
}

page_region_schema_with_height = {
    "bsonType": "object",
    "required": [
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "page_number",
        "page_height"
    ],
    "properties": {
        "start_x": {
            "bsonType": "number",
            "description": "The starting X coordinate of the region."
        },
        "start_y": {
            "bsonType": "number",
            "description": "The starting Y coordinate of the region."
        },
        "end_x": {
            "bsonType": "number",
            "description": "The ending X coordinate of the region."
        },
        "end_y": {
            "bsonType": "number",
            "description": "The ending Y coordinate of the region."
        },
        "page_number": {
            "bsonType": "number",
            "description": "The page number of the region."
        },
        "page_height": {
            "bsonType": "number",
            "description": "The height of the page."
        }
    }
}

attachment_region_schema = {
    "bsonType": "object",
    "required": [
        "page_regions",
        "name"
    ],
    "properties": {
        "page_regions": array_of_strings_schema(page_region_schema_with_height),
        "name": {
            "bsonType": "string",
            "description": "The name of the attachment."
        }
    }
}

page_region_schema = {
    "bsonType": "object",
    "required": [
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "page_number"
    ],
    "properties": {
        "start_x": {
            "bsonType": "number",
            "description": "The starting X coordinate of the region."
        },
        "start_y": {
            "bsonType": "number",
            "description": "The starting Y coordinate of the region."
        },
        "end_x": {
            "bsonType": "number",
            "description": "The ending X coordinate of the region."
        },
        "end_y": {
            "bsonType": "number",
            "description": "The ending Y coordinate of the region."
        },
        "page_number": {
            "bsonType": "number",
            "description": "The page number of the region."
        }
    }
}
page_region_for_table_schema = {
    "bsonType": "object",
    "required": [
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "page_number",
        "start_index",
        "end_index"
    ],
    "properties": {
        "start_x": {
            "bsonType": "number",
            "description": "The starting X coordinate of the region."
        },
        "start_y": {
            "bsonType": "number",
            "description": "The starting Y coordinate of the region."
        },
        "end_x": {
            "bsonType": "number",
            "description": "The ending X coordinate of the region."
        },
        "end_y": {
            "bsonType": "number",
            "description": "The ending Y coordinate of the region."
        },
        "page_number": {
            "bsonType": "number",
            "description": "The page number of the region."
        },
        "start_index": {
            "bsonType": "number",
            "description": "The starting index of the table."
        },
        "end_index": {
            "bsonType": "number",
            "description": "The ending index of the table."
        }
    }
}

table_region_schema = {
    "bsonType": "object",
    "required": [
        "page_regions"
    ],
    "properties": {
        "page_regions": array_of_strings_schema(page_region_for_table_schema),
    }
}


define_legal_act_pages_boundaries_stage_schema = {
    "bsonType": "object",
    "required": [
        "general_info",  # The embedded document for all meta-information
        "invoke_id",  # UUID as a string
        "page_regions",  # Page number and content
        "expires_at"  # Expiry date for TTL
    ],
    "properties": {
        "general_info": schema_for_legal_act_row,
        "invoke_id": {
            "bsonType": "string",  # UUID as a string
            "description": "Must be a string representing the UUID and is required."
        },
        "page_regions": {
            "bsonType": "object",
            "required": [
                "paragraphs",
                "margin_notes",
                "footer_notes",
                "attachments"
            ],
            "properties": {
                "paragraphs": array_of_strings_schema(page_region_schema),
                "margin_notes": array_of_strings_schema(page_region_schema),
                "footer_notes": array_of_strings_schema(page_region_schema),
                "attachments": array_of_strings_schema(attachment_region_schema)
            }
        },
        "expires_at": {
            "bsonType": "date",
            "description": "The date the document expires. MongoDB will use this for TTL deletion."
        }
    }
}


legal_act_boundaries_with_tables_stage_schema = {
    "bsonType": "object",
    "required": [
        "general_info",  # The embedded document for all meta-information
        "invoke_id",  # UUID as a string
        "page_regions",  # Page number and content
        "tables_regions",
        "paragraphs_text",
        "expires_at"  # Expiry date for TTL
    ],
    "properties": {
        "general_info": schema_for_legal_act_row,
        "invoke_id": {
            "bsonType": "string",  # UUID as a string
            "description": "Must be a string representing the UUID and is required."
        },
        "page_regions": {
            "bsonType": "object",
            "required": [
                "paragraphs",
                "margin_notes",
                "footer_notes",
                "attachments"
            ],
            "properties": {
                "paragraphs": array_of_strings_schema(page_region_schema),
                "margin_notes": array_of_strings_schema(page_region_schema),
                "footer_notes": array_of_strings_schema(page_region_schema),
                "attachments": array_of_strings_schema(attachment_region_schema)
            }
        },
        "tables_regions": array_of_strings_schema(table_region_schema),
        "paragraphs_text": {
            "bsonType": "string",
            "description": "The text of all paragraphs."
        },
        "expires_at": {
            "bsonType": "date",
            "description": "The date the document expires. MongoDB will use this for TTL deletion."
        }
    }
}



extract_text_and_table_page_number_stage_schema = {
    "bsonType": "object",
    "required": [
        "general_info",  # The embedded document for all meta-information
        "invoke_id",  # UUID as a string
        "pages",  # Page number and content
        "pages_with_tables",  # New field to store page numbers with tables
        "expires_at"  # Expiry date for TTL
    ],
    "properties": {
        "general_info": schema_for_legal_act_row,
        "invoke_id": {
            "bsonType": "string",  # UUID as a string
            "description": "Must be a string representing the UUID and is required."
        },
        "pages": {  # Pages is a dictionary, where each page number maps to its text
            "bsonType": "object",
            "additionalProperties": {
                "bsonType": "string",
                "description": "The text of each page, stored as a dictionary where keys are page numbers."
            }
        },
        "pages_with_tables": {  # This is the new field you're adding
            "bsonType": "array",
            "items": {
                "bsonType": "array",
                "items": {
                    "bsonType": "string",
                    "description": "Page number that contains a table."
                }
            },
            "description": "An array of arrays, where each inner array contains page numbers with tables."
        },
        "expires_at": {
            "bsonType": "date",
            "description": "The date the document expires. MongoDB will use this for TTL deletion."
        }
    }
}

found_annotation_and_labeled_table_schema = {
    "bsonType": "object",
    "required": [
        "general_info",  # The embedded document for all meta-information
        "invoke_id",  # UUID as a string
        "content",  # Page number and content
        "legal_annotations",  # New field to store page numbers with tables
        "expires_at"  # Expiry date for TTL
    ],
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
                    "enum": ['wzór dokumentu',
                             'tabela',
                             'wzory do obliczeń',
                             'inne'],
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

[
    'wzór_dokumentu',
    'zawiera_tabele',
    'wzory_do_obliczeń',
    'inne'
]