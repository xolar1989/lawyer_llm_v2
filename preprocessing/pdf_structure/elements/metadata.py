from typing import Mapping, Any

from preprocessing.mongo_db.mongodb import MongodbObject


class LegalPartInfo:

    def __init__(self, part_number: str, title: str):
        self.part_number = part_number
        self.title = title


class ChapterInfo:

    def __init__(self, chapter_number: str, title: str):
        self.chapter_number = chapter_number
        self.title = title


class ArticleMetadata(MongodbObject):

    def __init__(self, ELI: str, legal_act_name: str, invoke_id: str,
                 legal_part_info: LegalPartInfo, chapter_info: ChapterInfo):
        self.ELI = ELI
        self.legal_act_name = legal_act_name
        self.invoke_id = invoke_id
        self.legal_part_info = legal_part_info
        self.chapter_info = chapter_info

    def to_dict(self):
        return {
            'ELI': self.ELI,
            'legal_act_name': self.legal_act_name,
            'invoke_id': self.invoke_id,
            'legal_part_info': self.legal_part_info.__dict__ if self.legal_part_info else None,
            'chapter_info': self.chapter_info.__dict__ if self.chapter_info else None
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            ELI=dict_object['ELI'],
            legal_act_name=dict_object['legal_act_name'],
            invoke_id=dict_object['invoke_id'],
            legal_part_info=LegalPartInfo(**dict_object['legal_part_info']) if dict_object['legal_part_info'] is not None else None,
            chapter_info=ChapterInfo(**dict_object['chapter_info']) if dict_object['chapter_info'] is not None else None
        )