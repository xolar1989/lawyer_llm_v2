from abc import ABC, abstractmethod
from typing import Union, Sequence, Tuple, Mapping, Any, List, Dict

from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
from pymongo.typings import  _Pipeline

from preprocessing.mongo_db.mongodb_schema import extract_text_and_table_page_number_stage_schema, \
    found_annotation_and_labeled_table_schema, define_legal_act_pages_boundaries_stage_schema, \
    legal_act_boundaries_with_tables_stage_schema

_IndexList = Union[
    Sequence[Union[str, Tuple[str, Union[int, str, Mapping[str, Any]]]]], Mapping[str, Any]
]
_IndexKeyHint = Union[str, _IndexList]


class MongodbObject(ABC):

    @abstractmethod
    def to_dict(self):
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        pass


class MongodbCollectionIndex:
    def __init__(self, indexes: _IndexKeyHint, **kwargs: Any):
        self.indexes = indexes
        self.kwargs = kwargs


class MongodbCollection:

    def __init__(self, collection_name, db_instance, indexes: List[MongodbCollectionIndex], collection_schema=None):
        self.collection = db_instance[collection_name]
        self.db_instance = db_instance

        if collection_schema is not None:
            cmd_db = {
                "collMod": collection_name,
                "validator": {"$jsonSchema": collection_schema},
                "validationLevel": "strict"  # Enforce strict validation
            }
            db_instance.command(cmd_db)
        for index in indexes:
            self.collection.create_index(index.indexes, **index.kwargs)

    def update_one(self, filter: Mapping[str, Any],
                   update: Union[Mapping[str, Any], _Pipeline],
                   upsert: bool = False) -> None:
        self.collection.update_one(filter, update, upsert)

    def insert_one(self, document: Mapping[str, Any]) -> None:
        self.collection.insert_one(document)

    def insert_many(self, documents: List[Mapping[str, Any]]) -> None:
        self.collection.insert_many(documents)

    def find_one(self, filter: Mapping[str, Any], *args) -> Mapping[str, Any]:
        return self.collection.find_one(filter, *args)

    def find_many(self, filter: Mapping[str, Any], *args, limit: int = 0) -> List[Mapping[str, Any]]:
        """
        Retrieve multiple documents matching the filter.

        :param filter: A dictionary specifying the query filter.
        :param args: Additional arguments for the `find` method.
        :param limit: Number of documents to limit the query to. Default is 0 (no limit).
        :return: A list of matching documents.
        """
        cursor = self.collection.find(filter, *args)
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)

    def aggregate(self, pipeline: List):
        return self.collection.aggregate(pipeline)


def get_mongodb_collection(db_name, collection_name):
    ## localhost
    # uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongodb.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"
    uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongodb.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"
    # r = 'mongodb+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongodb.net/'

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
                    MongodbCollectionIndex(
                        [("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING), ("page.page_number", ASCENDING)],
                        unique=True)
                ]
            )
        elif collection_name == 'legal_act_page_metadata_multiline_error':
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex(
                        [("ELI", ASCENDING), ("invoke_id", ASCENDING), ("page_number", ASCENDING),
                         ("num_line", ASCENDING)],
                        unique=True)
                ]
            )
        elif collection_name == 'legal_act_page_metadata_table_extraction':
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("ELI", ASCENDING), ("invoke_id", ASCENDING), ("page_number", ASCENDING), ("table_index", ASCENDING)],
                        unique=True)
                ]
            )
        elif collection_name == "legal_act_page_metadata_document_error":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)],
                        unique=True)
                ]
            )
        elif collection_name == "legal_act_page_metadata_crop_bbox_image":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("ELI", ASCENDING), ("invoke_id", ASCENDING), ("page_number", ASCENDING), ("bbox", ASCENDING)],
                        unique=True)
                ]
            )
        elif collection_name == "document_splitting_error":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("ELI", ASCENDING), ("invoke_id", ASCENDING)],
                        unique=True)
                ]
            )
    if db_name == "datasets":
        if collection_name == "legal_acts_articles":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("unit_id", ASCENDING), ("metadata.ELI", ASCENDING), ("metadata.invoke_id", ASCENDING)],
                        unique=True)
                ]
            )
    if db_name == "preparing_dataset_for_embedding":
        if collection_name == "question_with_annotations":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("nro", ASCENDING)],
                        unique=True)
                ]
            )
        elif collection_name == "creating_annotations_for_question_error":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("nro", ASCENDING), ("invoke_id", ASCENDING)],
                        unique=True)
                ]
            )
        elif collection_name == "documents_with_title_embedding":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("legal_act.ELI", ASCENDING), ("legal_act.invoke_id", ASCENDING)],
                        unique=True)
                ]
            )
    if db_name == "costs_of_runs":
        if collection_name == "dataset_preparations_costs":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("name", ASCENDING), ("invoke_id", ASCENDING)],
                        unique=True)
                ]
            )
    if db_name == "scraping_lex":
        if collection_name == "list_questions":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("nro", ASCENDING)],
                        unique=True)
                    ]
            )
        elif collection_name == "questions_with_html":
            return MongodbCollection(
                collection_name=collection_name,
                db_instance=mongo_db,
                indexes=[
                    MongodbCollectionIndex(
                        [("nro", ASCENDING)],
                        unique=True)
                ]
            )

    raise Exception("Invalid db_name or collection_name Mongodb error")
