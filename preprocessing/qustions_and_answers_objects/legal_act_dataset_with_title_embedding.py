from typing import Dict

from langchain_core.embeddings import Embeddings

from preprocessing.legal_acts.legal_act_document import LegalActDocument
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.qustions_and_answers_objects.legal_act_title_embedding_pair import LegalActTitleEmbeddingPair


class LegalActDatasetWithTitleEmbedding:

    def __init__(self, embeddings_model: Embeddings, documents_from_db: Dict[int, LegalActTitleEmbeddingPair]):
        self.embeddings_model = embeddings_model
        self.documents_from_db = documents_from_db

    @classmethod
    def build(cls, embeddings_model: Embeddings, invoke_id: str):
        documents_from_etl = list(get_mongodb_collection(
            db_name="datasets",
            collection_name="legal_acts_articles"
        ).aggregate(
            [
                {
                    "$match": {
                        "metadata.invoke_id": invoke_id
                    }
                },
                {
                    "$group": {
                        "_id": "$metadata.ELI",  # group by metadata.ELI
                        "count": {"$sum": 1},  # count how many documents per group
                        "legal_act_name": {"$first": "$metadata.legal_act_name"},
                        "invoke_id": {"$first": "$metadata.invoke_id"}
                    }
                },
                {
                    "$project": {
                        "ELI": "$_id",  # rename _id to ELI
                        "invoke_id": 1,
                        "legal_act_name": 1,
                        "count": 1,
                        "_id": 0  # exclude original _id
                    }
                }
            ]
        )
        )
        documents_embeddings = embeddings_model.embed_documents(
            [document["legal_act_name"].lower() for document in documents_from_etl])

        for embedding_index in range(len(documents_embeddings)):
            documents_from_etl[embedding_index]['legal_act_name_embedding'] = documents_embeddings[embedding_index]

        legal_act_dict = {
            i: LegalActTitleEmbeddingPair(
                legal_act=LegalActDocument(
                    ELI=doc["ELI"],
                    invoke_id=doc["invoke_id"],
                    legal_act_name=doc["legal_act_name"]),
                title_embedding=doc['legal_act_name_embedding']
            )
            for i, doc in enumerate(documents_from_etl)
        }
        return cls(
            embeddings_model=embeddings_model,
            documents_from_db=legal_act_dict
        )

    @classmethod
    def from_db(cls, embeddings_model: Embeddings, invoke_id: str):
        rows_of_documents = get_mongodb_collection(
            db_name="preparing_dataset_for_embedding",
            collection_name="documents_with_title_embedding"
        ).find_many({
            "legal_act.invoke_id": invoke_id
        })
        legal_act_dict = {
            i: LegalActTitleEmbeddingPair.from_dict(doc)
            for i, doc in enumerate(rows_of_documents)

        }

        return cls(
            embeddings_model=embeddings_model,
            documents_from_db=legal_act_dict
        )

    def insert_into_db(self):
        get_mongodb_collection(
            db_name="preparing_dataset_for_embedding",
            collection_name="documents_with_title_embedding"
        ).insert_many([pair.to_dict() for key, pair in self.documents_from_db.items()])

