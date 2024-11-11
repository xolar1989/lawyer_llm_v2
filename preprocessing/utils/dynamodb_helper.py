import logging

import boto3
import pandas as pd

logger = logging.getLogger()

logger.setLevel(logging.INFO)


meta_DULegalDocumentsMetaData_without_filename = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'address': pd.Series(dtype='str'),
    'announcementDate': pd.Series(dtype='str'),
    'changeDate': pd.Series(dtype='str'),
    'displayAddress': pd.Series(dtype='str'),
    'pos': pd.Series(dtype='int'),
    'promulgation': pd.Series(dtype='str'),
    'publisher': pd.Series(dtype='str'),
    'status': pd.Series(dtype='str'),
    'textHTML': pd.Series(dtype='bool'),
    'textPDF': pd.Series(dtype='bool'),
    'title': pd.Series(dtype='str'),
    'type': pd.Series(dtype='str'),
    'volume': pd.Series(dtype='int'),
    'year': pd.Series(dtype='int')
})

meta_DULegalDocumentsMetaData = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'document_year': pd.Series(dtype='int'),
    'status': pd.Series(dtype='str'),
    'announcementDate': pd.Series(dtype='str'),
    'volume': pd.Series(dtype='int'),
    'address': pd.Series(dtype='str'),
    'displayAddress': pd.Series(dtype='str'),
    'promulgation': pd.Series(dtype='str'),
    'pos': pd.Series(dtype='int'),
    'publisher': pd.Series(dtype='str'),
    'changeDate': pd.Series(dtype='str'),
    'textHTML': pd.Series(dtype='bool'),
    'textPDF': pd.Series(dtype='bool'),
    'title': pd.Series(dtype='str'),
    'type': pd.Series(dtype='str'),
    'filename_of_pdf': pd.Series(dtype='str')
})

meta_DULegalDocumentsMetaData_with_s3_path = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'document_year': pd.Series(dtype='int'),
    'status': pd.Series(dtype='str'),
    'announcementDate': pd.Series(dtype='str'),
    'volume': pd.Series(dtype='int'),
    'address': pd.Series(dtype='str'),
    'displayAddress': pd.Series(dtype='str'),
    'promulgation': pd.Series(dtype='str'),
    'pos': pd.Series(dtype='int'),
    'publisher': pd.Series(dtype='str'),
    'changeDate': pd.Series(dtype='str'),
    'textHTML': pd.Series(dtype='bool'),
    'textPDF': pd.Series(dtype='bool'),
    'title': pd.Series(dtype='str'),
    'type': pd.Series(dtype='str'),
    'filename_of_pdf': pd.Series(dtype='str'),
    's3_pdf_path': pd.Series(dtype='str')
})

meta_DULegalDocumentsMetaData_intermediate_result = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'document_year': pd.Series(dtype='int'),
    'status': pd.Series(dtype='str'),
    'announcementDate': pd.Series(dtype='str'),
    'volume': pd.Series(dtype='int'),
    'address': pd.Series(dtype='str'),
    'displayAddress': pd.Series(dtype='str'),
    'promulgation': pd.Series(dtype='str'),
    'pos': pd.Series(dtype='int'),
    'publisher': pd.Series(dtype='str'),
    'changeDate': pd.Series(dtype='str'),
    'textHTML': pd.Series(dtype='bool'),
    'textPDF': pd.Series(dtype='bool'),
    'title': pd.Series(dtype='str'),
    'type': pd.Series(dtype='str'),
    'filename_of_pdf': pd.Series(dtype='str'),
    'missing_in_dynamodb': pd.Series(dtype='bool')
})

meta_DULegalDocumentsMetaDataChanges_v2 = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'ELI_annotation': pd.Series(dtype='str'),
    'type_annotation': pd.Series(dtype='str'),
    'title_annotation': pd.Series(dtype='str'),
    'TYPE_OF_CHANGE': pd.Series(dtype='str'),
    'date_of_change_annotation': pd.Series(dtype='str'),
})

def insert_partition_to_dynamodb(df_partition, table_name, aws_region):
    boto_session = boto3.Session(region_name=aws_region)
    dynamodb = boto_session.resource('dynamodb')
    documents_metadata_table = dynamodb.Table(table_name)
    # Insert rows into DynamoDB
    with documents_metadata_table.batch_writer() as batch:
        for _, row in df_partition.iterrows():
            try:
                batch.put_item(Item=row.to_dict())
            except Exception as e:
                print(f"Error inserting row: {row.get('id', 'unknown id')}, error: {e}")


def scan_dynamodb_segment(segment, table_name, aws_region, total_segments, columns_order, meta):
    boto_session = boto3.Session(region_name=aws_region)
    dynamodb = boto_session.resource('dynamodb')
    documents_metadata_table = dynamodb.Table(table_name)
    response = documents_metadata_table.scan(
        Segment=segment,
        TotalSegments=total_segments
    )

    items = response['Items']

    # Handle pagination if necessary
    while 'LastEvaluatedKey' in response:
        response = documents_metadata_table.scan(
            Segment=segment,
            TotalSegments=total_segments,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    # If in any rows there are missing columns, it will raise an error
    # TODO handling this error to send a massage about invalid dynamodb table state
    return pd.DataFrame(items).reindex(columns=columns_order).astype(meta.dtypes.to_dict())


def fetch_segment_data(df_partition, table_name, aws_region, total_segments, columns_order, meta):
    segment = int(df_partition['segment'].iloc[0])  # Get the segment number
    return scan_dynamodb_segment(segment, table_name, aws_region, total_segments, columns_order, meta)


def put_item_into_table(table_name, item, boto_session):

    dynamodb = boto_session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.put_item(Item=item)


        logger.info(f"Item inserted into table: {item}, successfully")
        return item
    except Exception as e:
        logger.error(f"Error inserting item into table: {e}")
        raise Exception(f"Error inserting item into table: {e}")


def map_to_update_expression(key_value_dict):
    update_expression = "SET "
    for index in range(len(key_value_dict)):
        if index != 0:
            update_expression += f", "
        update_expression += f"{list(key_value_dict.keys())[index]} = :{list(key_value_dict.keys())[index]}"
    return update_expression

def map_to_expression_attribute_values(key_value_dict):
    expression_attribute_values = {}
    for key, value in key_value_dict.items():
        expression_attribute_values[f":{key}"] = value
    return expression_attribute_values

def map_keys_to_values(key_value_dict):
    def map_to_update_expression(key_value_dict):
        update_expression = "SET "
        for key, value in key_value_dict.items():
            update_expression += f"{key} = :{key}, "
        return update_expression

def update_item(table_name, key, items, boto_session):
    dynamodb = boto_session.resource('dynamodb')
    table = dynamodb.Table(table_name)
    l = map_to_update_expression(items)
    w = map_to_expression_attribute_values(items)

    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=map_to_update_expression(items),
            ExpressionAttributeValues=map_to_expression_attribute_values(items),
            ReturnValues="ALL_NEW"
        )

        logger.info(f"Item updated in table: {response}, successfully")
        return response
    except Exception as e:
        logger.error(f"Error updating item in table: {e}")
        raise Exception(f"Error updating item in table: {e}")

