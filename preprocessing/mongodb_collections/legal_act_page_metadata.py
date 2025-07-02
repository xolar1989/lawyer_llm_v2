import datetime
from typing import Mapping, Any

from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.pdf_elements.legal_act_page import LegalActPage
from preprocessing.utils.stages_objects import GeneralInfo


class LegalActPageMetadata(MongodbObject):

    def to_dict(self):
        return {
            "general_info": GeneralInfo.to_dict(self.general_info),
            "invoke_id": self.invoke_id,
            "page": LegalActPage.to_dict(self.page),
            "expires_at": self.expires_at
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            general_info=GeneralInfo.from_dict(dict_object['general_info']),
            invoke_id=dict_object['invoke_id'],
            page=LegalActPage.from_dict(dict_object['page']),
            expires_at=dict_object['expires_at']
        )

    def __init__(self, general_info: GeneralInfo, invoke_id: str, page: LegalActPage,
                 expires_at: datetime):
        self.general_info = general_info
        self.invoke_id = invoke_id
        self.page = page
        self.expires_at = expires_at