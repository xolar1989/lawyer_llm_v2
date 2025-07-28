from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Mapping, Any

from preprocessing.mongo_db.mongodb import MongodbObject


@dataclass
class QuestionWithHtml(MongodbObject):
    create_date: datetime
    nro: int
    title: str
    question_content: str
    answer_content: str

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(**dict_object)