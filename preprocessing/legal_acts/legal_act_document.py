from typing import List, Mapping, Any

from preprocessing.mongo_db.mongodb import get_mongodb_collection, MongodbObject
from preprocessing.pdf_structure.elements.article import Article


class LegalActDocument(MongodbObject):


    def __init__(self, ELI: str, invoke_id: str, legal_act_name: str, load_articles: bool = False):
        self.ELI = ELI
        self.invoke_id = invoke_id
        self.legal_act_name = legal_act_name
        self.articles: List[Article] = self.lazy_loading_articles() if load_articles else []

    def lazy_loading_articles(self):
        ## TODO, mongodb will return sorted documents as were sorted during inserting
        articles_rows = get_mongodb_collection(
            db_name="datasets",
            collection_name="legal_acts_articles"
        ).find_many({
            "metadata.ELI": self.ELI,
            "metadata.invoke_id": self.invoke_id
        })
        self.articles = list(map(lambda doc: Article.from_dict(doc), articles_rows))
        return self

    def to_dict(self):
        return {
            'ELI': self.ELI,
            'invoke_id': self.invoke_id,
            'legal_act_name': self.legal_act_name
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            ELI=dict_object['ELI'],
            invoke_id=dict_object['invoke_id'],
            legal_act_name=dict_object['legal_act_name']
        )
