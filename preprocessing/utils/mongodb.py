from typing import Union, Sequence, Tuple, Mapping, Any, List

from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi

from preprocessing.utils.mongodb_schema import extract_text_and_table_page_number_stage_schema



_IndexList = Union[
    Sequence[Union[str, Tuple[str, Union[int, str, Mapping[str, Any]]]]], Mapping[str, Any]
]
_IndexKeyHint = Union[str, _IndexList]


class MongodbCollectionIndex:
    def __init__(self, indexes: _IndexKeyHint, **kwargs: Any):
        self.indexes = indexes
        self.kwargs = kwargs


class MongodbCollection:

    def __init__(self, collection_name, db_instance, collection_schema, indexes:List[MongodbCollectionIndex]):
        self.collection = db_instance[collection_name]
        self.db_instance = db_instance

        cmd_db = {
            "collMod": collection_name,
            "validator": {"$jsonSchema": collection_schema},
            "validationLevel": "strict"  # Enforce strict validation
        }
        db_instance.command(cmd_db)
        for index in indexes:
            self.collection.create_index(index.indexes, **index.kwargs)

    def insert_one(self, document: Mapping[str, Any]) -> None:
        self.collection.insert_one(document)

    def find_one(self, filter: Mapping[str, Any], *args) -> Mapping[str, Any]:
        return self.collection.find_one(filter, *args)


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
    raise Exception("Invalid db_name or collection_name Mongodb error")

